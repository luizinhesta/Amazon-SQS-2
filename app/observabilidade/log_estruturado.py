"""Logging estruturado de eventos de resiliência (frente A, observabilidade).

Este módulo oferece uma função única, :func:`log_evento`, que emite uma linha de
log em formato JSON por meio do logger ``sqs_lab.resiliencia``. O objetivo é
registrar, de forma estruturada e legível por máquina, as falhas e os eventos de
resiliência dos consumidores (ex.: falha de ``DeleteMessage`` por ReceiptHandle
expirado, encerramento inesperado de thread, reinício supervisionado), sem
esconder os erros reais nem apresentar dados simulados como confirmados.

Princípios:

- **Não lança.** O logging é um efeito colateral de observabilidade; uma falha ao
  serializar ou emitir o log nunca deve interromper o consumidor. Qualquer erro é
  contido internamente.
- **JSON em uma linha.** Cada chamada produz uma única linha com os campos
  informados, facilitando a coleta e a busca.
- **pt-BR nos textos, nomes técnicos em inglês.** Segue a convenção do codebase.
"""

from __future__ import annotations

import json
import logging
from typing import Any

# Logger dedicado aos eventos de resiliência. A configuração de handlers/formato
# fica a cargo da aplicação (ou do pytest/caplog nos testes); aqui apenas obtemos
# o logger pelo nome combinado.
LOGGER_NOME: str = "sqs_lab.resiliencia"
_logger = logging.getLogger(LOGGER_NOME)


def log_evento(nivel: int, evento: str, **campos: Any) -> None:
    """Emite uma linha de log JSON para o logger ``sqs_lab.resiliencia``.

    A mensagem registrada é um objeto JSON contendo a chave ``evento`` e todos os
    ``campos`` adicionais fornecidos. A serialização usa ``ensure_ascii=False``
    (preserva acentuação pt-BR) e ``default=str`` (torna serializável qualquer
    valor que não seja JSON-nativo, como exceções ou datetimes).

    Esta função nunca levanta: qualquer erro ao montar ou emitir o log é contido,
    para que a observabilidade jamais derrube o consumidor.

    Args:
        nivel: nível de logging padrão (ex.: ``logging.INFO``, ``logging.ERROR``).
        evento: identificador curto do evento (ex.: ``"delete_falhou"``).
        **campos: campos adicionais a incluir na linha JSON (ex.: ``order_id``,
            ``error_code``, ``consumer_id``).
    """
    try:
        linha = json.dumps(
            {"evento": evento, **campos},
            ensure_ascii=False,
            default=str,
        )
        _logger.log(nivel, linha)
    except Exception:  # noqa: BLE001 - observabilidade nunca derruba o consumidor
        # Último recurso: tenta registrar que o próprio log estruturado falhou,
        # sem propagar a exceção.
        try:
            _logger.exception("Falha ao emitir log estruturado para evento %r", evento)
        except Exception:  # noqa: BLE001
            pass
