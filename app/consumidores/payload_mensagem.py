"""Parsing do corpo (payload) das mensagens SQS enviadas pela API_Backend.

Este módulo define o **contrato produtor/consumidor** do corpo da mensagem SQS e
uma função pura para interpretá-lo. O produtor
(:func:`app.api.servico_envio._corpo_mensagem`) serializa o corpo como um objeto
JSON com três campos:

.. code-block:: json

    {"order_id": "<id>", "processing_seconds": <int>, "cenario": "<cenario>"}

Os consumidores (Standard e FIFO) precisam **desserializar** esse corpo para
recuperar o ``order_id`` real (usado na idempotência e no particionamento do
histórico), o ``processing_seconds`` e o ``cenario`` a aplicar no processamento.

Fallback seguro (compatibilidade retroativa)
---------------------------------------------
Nem toda mensagem recebida vem necessariamente nesse formato JSON. Testes unitários
e mensagens simples usam o corpo como ``order_id`` puro (ex.: ``"pedido-0001"``).
Por isso, o parser aplica o seguinte **fallback**:

- Se o corpo for um JSON válido **e** for um objeto (``dict``) **que contém**
  ``order_id``, extraímos os três campos do objeto.
- Caso contrário (corpo não-JSON, JSON que não é objeto — como ``"[]"`` ou
  ``"123"`` —, ou objeto sem ``order_id``), tratamos o **corpo cru** como o
  ``order_id`` e deixamos ``processing_seconds`` e ``cenario`` como ``None``.

Dessa forma, o contrato JSON é respeitado quando presente, sem quebrar mensagens
simples nem os testes que usam o corpo diretamente como ``order_id``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional

from app.consumidores.processamento import CenarioFalha


@dataclass(frozen=True)
class PayloadMensagem:
    """Resultado do parsing do corpo de uma mensagem SQS.

    Atributos:
        order_id: Identificador do pedido extraído do corpo (ou o corpo cru, no
            fallback). ``None`` apenas quando o corpo é vazio.
        processing_seconds: Tempo_Processamento_Simulado em segundos, quando
            presente e conversível para inteiro; ``None`` caso contrário.
        cenario: :class:`CenarioFalha` extraído do corpo, quando presente e válido;
            ``None`` caso contrário (ausente ou valor desconhecido).
    """

    order_id: Optional[str]
    processing_seconds: Optional[int] = None
    cenario: Optional[CenarioFalha] = None


def extrair_payload(body: str) -> PayloadMensagem:
    """Interpreta o corpo da mensagem seguindo o contrato produtor/consumidor.

    Tenta desserializar ``body`` como JSON. Quando o resultado é um objeto
    (``dict``) que contém a chave ``order_id``, extrai os três campos do contrato
    (ver docstring do módulo). Em qualquer outro caso, aplica o **fallback**:
    trata o corpo cru como ``order_id`` e deixa os demais campos como ``None``.

    A conversão dos campos é tolerante a dados inválidos:

    - ``cenario`` é convertido via ``CenarioFalha(valor)``; valores desconhecidos
      viram ``None`` (não interrompem o parsing).
    - ``processing_seconds`` é convertido via ``int(valor)``; valores não
      conversíveis viram ``None``.

    Args:
        body: Corpo bruto da mensagem recebida do SQS.

    Returns:
        Um :class:`PayloadMensagem`. No fallback, ``order_id`` é ``body or None``.
    """
    # Fallback padrão: o corpo cru é o próprio order_id (sem os demais campos).
    fallback = PayloadMensagem(order_id=body or None)

    try:
        decodificado = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        # Corpo não é JSON válido (ex.: "pedido-0001"): usa o fallback.
        return fallback

    # Só seguimos o contrato quando o JSON é um objeto com order_id. JSON que não
    # é objeto (listas, números, strings) cai no fallback.
    if not isinstance(decodificado, dict) or "order_id" not in decodificado:
        return fallback

    order_id = decodificado.get("order_id")
    order_id = str(order_id) if order_id is not None else None

    return PayloadMensagem(
        order_id=order_id,
        processing_seconds=_converter_processing_seconds(
            decodificado.get("processing_seconds")
        ),
        cenario=_converter_cenario(decodificado.get("cenario")),
    )


def _converter_cenario(valor: object) -> Optional[CenarioFalha]:
    """Converte o valor de ``cenario`` em :class:`CenarioFalha` (ou ``None``)."""
    if valor is None:
        return None
    try:
        return CenarioFalha(valor)
    except ValueError:
        # Valor de cenário desconhecido: ignora (não interrompe o parsing).
        return None


def _converter_processing_seconds(valor: object) -> Optional[int]:
    """Converte o valor de ``processing_seconds`` em ``int`` (ou ``None``)."""
    if valor is None:
        return None
    try:
        return int(valor)
    except (TypeError, ValueError):
        return None
