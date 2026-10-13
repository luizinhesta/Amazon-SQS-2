"""StateDeriver — derivação pura do Estado de uma mensagem a partir de eventos.

Este módulo implementa o componente **StateDeriver** descrito no design.md
(seção "Máquina de estados da mensagem"). É uma função pura, sem efeitos
colaterais: recebe a lista de eventos de uma mensagem (log append-only da
Tabela_Historico) e retorna exatamente um valor do conjunto fechado de
``Estado`` (Req. 5.4, 5.9).

Regras fundamentais (ver design.md e Req. 5.4, 5.9, 5.10, 6.6, 7.9, 7.10):

- O Estado é **derivado** dos eventos, nunca armazenado como campo mutável.
- ``Processada`` e ``Encaminhada para DLQ`` são estados **terminais** e têm
  precedência sobre os demais (regra de precedência do design).
- ``Encaminhada para DLQ`` só é derivado quando há um evento ``encaminhada_dlq``
  confirmado (Evento_Observado) **e** ``receive_count >= maxReceiveCount``.
  Nunca se deriva DLQ apenas por expiração do Visibility_Timeout
  (Req. 6.6, 7.9, 7.10).

O módulo é intencionalmente independente da camada de acesso (DynamoDB): recebe
qualquer objeto que exponha os atributos ``event_type``, ``event_source`` e
``receive_count`` (ver ``EventoLike``), de modo que possa ser testado sem AWS.
"""

from __future__ import annotations

from typing import Iterable, Optional, Protocol, runtime_checkable

from app.dominio.constantes import MAX_RECEIVE_COUNT
from app.dominio.tipos import (
    Estado,
    EventSource,
    EventType,
)


@runtime_checkable
class EventoLike(Protocol):
    """Contrato mínimo de um evento consumido pelo StateDeriver.

    Qualquer objeto (dataclass, item desserializado da Tabela_Historico, mock de
    teste) que exponha estes atributos pode ser derivado. ``receive_count`` é
    opcional porque nem todo evento o carrega (ex.: ``enviada``).
    """

    event_type: EventType
    event_source: EventSource
    receive_count: Optional[int]


def _tipo(evento: object) -> Optional[EventType]:
    """Normaliza ``event_type`` para ``EventType`` (aceita o enum ou sua string)."""
    valor = getattr(evento, "event_type", None)
    if isinstance(valor, EventType):
        return valor
    if isinstance(valor, str):
        try:
            return EventType(valor)
        except ValueError:
            return None
    return None


def _origem(evento: object) -> Optional[EventSource]:
    """Normaliza ``event_source`` para ``EventSource`` (aceita o enum ou string)."""
    valor = getattr(evento, "event_source", None)
    if isinstance(valor, EventSource):
        return valor
    if isinstance(valor, str):
        try:
            return EventSource(valor)
        except ValueError:
            return None
    return None


def _receive_count(evento: object) -> int:
    """Extrai ``receive_count`` como inteiro não-negativo (ausente/None vira 0)."""
    valor = getattr(evento, "receive_count", None)
    try:
        numero = int(valor) if valor is not None else 0
    except (TypeError, ValueError):
        return 0
    return numero if numero >= 0 else 0


def _dlq_confirmada(eventos: list[object], max_receive_count: int) -> bool:
    """Indica se há encaminhamento para DLQ confirmado.

    A DLQ só é considerada confirmada quando existe um evento ``encaminhada_dlq``
    que seja Evento_Observado **e** cuja contagem de recebimentos atinja o
    ``maxReceiveCount`` (Req. 6.6, 7.9, 7.10). A expiração do Visibility_Timeout,
    por si só, nunca satisfaz esta condição.
    """
    for evento in eventos:
        if _tipo(evento) is not EventType.ENCAMINHADA_DLQ:
            continue
        if _origem(evento) is not EventSource.OBSERVADO:
            continue
        if _receive_count(evento) >= max_receive_count:
            return True
    return False


def derivar_estado(
    eventos: Iterable[object],
    *,
    max_receive_count: int = MAX_RECEIVE_COUNT,
) -> Estado:
    """Deriva exatamente um ``Estado`` a partir dos eventos de uma mensagem.

    Função pura: não realiza E/S, não altera a entrada e sempre retorna um valor
    pertencente ao conjunto fechado ``Estado`` (Req. 5.4, 5.9).

    A ordem de avaliação implementa a precedência da máquina de estados do design:

    1. ``Encaminhada para DLQ`` (terminal) — exige ``encaminhada_dlq`` OBSERVADO
       com ``receive_count >= max_receive_count``.
    2. ``Processada`` (terminal) — há evento ``processada``.
    3. ``Em processamento`` — há ``processamento_iniciado`` sem desfecho posterior.
    3b. ``Exclusão não confirmada`` (**não terminal**) — o processamento terminou,
       mas o ``DeleteMessage`` falhou (ex.: ReceiptHandle expirado). É resolvido
       dentro do laço como o desfecho mais recente; por não ser terminal, uma
       reentrega ativa posterior (``recebida``/``processamento_iniciado``) o
       sobrepõe, retornando ``Recebida``/``Em processamento``. ``Processada`` e
       ``Encaminhada para DLQ`` continuam resolvidos antes do laço e têm
       precedência sobre ele.
    4. ``Falha`` — última falha com ``receive_count < max_receive_count`` e sem
       reentrega posterior.
    5. ``Aguardando nova tentativa`` — houve falha/expiração e a mensagem voltou a
       ficar visível (ainda abaixo do ``max_receive_count``).
    6. ``Recebida`` — há ``recebida`` sem início de processamento.
    7. ``Aguardando`` — há ``enviada`` e nenhum ``recebida`` (estado inicial e
       também o fallback para log vazio).

    Args:
        eventos: iterável de eventos da mensagem (qualquer objeto compatível com
            ``EventoLike``). A ordem fornecida é a ordem cronológica de registro.
        max_receive_count: limite da Redrive_Policy; padrão ``MAX_RECEIVE_COUNT``.

    Returns:
        Um único valor de ``Estado``.
    """
    lista = list(eventos)

    # 1) Terminal: Encaminhada para DLQ (precedência máxima). Só confirmada por
    #    evento OBSERVADO com receive_count >= maxReceiveCount; nunca por expiração
    #    de Visibility_Timeout (Req. 6.6, 7.9, 7.10).
    if _dlq_confirmada(lista, max_receive_count):
        return Estado.ENCAMINHADA_DLQ

    tipos_presentes = {t for t in (_tipo(e) for e in lista) if t is not None}

    # 2) Terminal: Processada (sucesso). A exclusão (`excluida`) é consequência e
    #    não altera o Estado terminal de sucesso.
    if EventType.PROCESSADA in tipos_presentes:
        return Estado.PROCESSADA

    # A partir daqui, importa o último evento relevante do ciclo de vida para
    # distinguir estados intermediários. Percorre-se de trás para frente.
    for evento in reversed(lista):
        tipo = _tipo(evento)
        if tipo is None:
            continue

        if tipo is EventType.PROCESSAMENTO_INICIADO:
            # Processamento em andamento sem desfecho posterior (processada/falha
            # já teriam sido tratados ou apareceriam depois na lista).
            return Estado.EM_PROCESSAMENTO

        if tipo is EventType.EXCLUSAO_NAO_CONFIRMADA:
            # Desfecho não terminal: o processamento terminou, mas o DeleteMessage
            # falhou (ex.: ReceiptHandle expirado). Como `reversed` garante que o
            # evento cronologicamente mais recente vence, uma reentrega posterior
            # (recebida/processamento_iniciado) sobrepõe este estado antes de aqui
            # chegar, retornando RECEBIDA/EM_PROCESSAMENTO.
            return Estado.EXCLUSAO_NAO_CONFIRMADA

        if tipo is EventType.FALHA:
            # Falha recente: distingue-se entre "Falha" (ainda sem reentrega) e
            # "Aguardando nova tentativa" conforme a contagem de recebimentos.
            if _receive_count(evento) < max_receive_count:
                return Estado.FALHA
            # receive_count >= max_receive_count mas DLQ ainda não confirmada:
            # a mensagem aguarda a confirmação do encaminhamento.
            return Estado.AGUARDANDO_NOVA_TENTATIVA

        if tipo in (
            EventType.PROVAVEL_EXPIRACAO_VISIBILITY,
            EventType.AGUARDANDO_NOVA_TENTATIVA,
        ):
            # Expiração/dedução de reentrega: a mensagem voltou a ficar visível.
            return Estado.AGUARDANDO_NOVA_TENTATIVA

        if tipo is EventType.RECEBIDA:
            return Estado.RECEBIDA

        if tipo is EventType.RECEBIMENTO_REPETIDO:
            # Recebimento repetido sem processamento concluído: trata-se como
            # recebida (a mensagem está sob um consumidor).
            return Estado.RECEBIDA

        if tipo is EventType.ENVIADA:
            return Estado.AGUARDANDO

        # Eventos sem efeito direto no Estado (ex.: excluida isolada, erros de
        # validação/acesso) são ignorados; segue para o evento anterior.

    # 7) Fallback: log vazio ou apenas eventos irrelevantes → estado inicial.
    return Estado.AGUARDANDO
