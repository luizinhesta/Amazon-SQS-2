"""Classificador de origem do evento (Evento_Observado vs Evento_Inferido).

Este módulo implementa a classificação da origem (`event_source`) de cada tipo de
evento (`event_type`) do ciclo de vida de uma mensagem, conforme a tabela
"Evento_Observado vs Evento_Inferido" do documento de design.

Regra de domínio (Req. 6.4, 6.5, 11.9):

- **Evento_Observado (OBSERVADO)**: resultante de resposta direta de um serviço AWS
  ou de uma ação concreta confirmada pela aplicação. Exemplos: `enviada` (retorno de
  SendMessage), `recebida` (retorno de ReceiveMessage), `excluida` (DeleteMessage
  confirmado) e `encaminhada_dlq` (presença confirmada na DLQ).
- **Evento_Inferido (INFERIDO)**: deduzido pela aplicação a partir de condições
  indiretas, sem confirmação direta do SQS. Exemplos:
  `provavel_expiracao_visibility` e `aguardando_nova_tentativa`.

O contrato dos dois tipos cuja origem é fixa por exigência explícita do design
(`provavel_expiracao_visibility` → INFERIDO e `encaminhada_dlq` → OBSERVADO) é
reutilizado de ``ORIGEM_OBRIGATORIA_POR_TIPO`` em ``app.dominio.tipos`` e verificado
em tempo de importação para evitar divergências. Este módulo estende essa tabela,
cobrindo **todos** os ``EventType`` definidos.

Nenhum valor é persistido ou calculado dinamicamente aqui: a classificação é um
mapeamento determinístico e total sobre o enum ``EventType``.
"""

from __future__ import annotations

from app.dominio.tipos import (
    ORIGEM_OBRIGATORIA_POR_TIPO,
    EventSource,
    EventType,
)

__all__ = [
    "ORIGEM_POR_TIPO",
    "classificar_origem",
    "validar_origem",
    "OrigemInvalidaError",
]


# ---------------------------------------------------------------------------
# Mapeamento total: cada EventType -> sua origem obrigatória
# ---------------------------------------------------------------------------
# A classificação segue a tabela "Evento_Observado vs Evento_Inferido" do design.
#
# INFERIDO (deduzido sem confirmação direta do SQS):
#   - provavel_expiracao_visibility: o SQS não emite confirmação da expiração do
#     Visibility_Timeout; a aplicação apenas deduz (Req. 6.5, 7.9).
#   - aguardando_nova_tentativa: deduzido antes de uma nova entrega efetiva.
#
# OBSERVADO (resposta direta da AWS ou ação concreta confirmada pela aplicação):
#   - enviada / recebida / excluida: retornos diretos de SendMessage /
#     ReceiveMessage / DeleteMessage.
#   - encaminhada_dlq: presença confirmada da mensagem na DLQ (Req. 6.6, 7.10).
#   - processamento_iniciado / processada / falha: ações concretas do consumidor.
#   - recebimento_repetido: detecção concreta de reentrega de um order_id já
#     processado (Req. 9.4).
#   - erro_validacao_order_id / erro_acesso_historico: falhas concretas constatadas
#     pela aplicação (Req. 9.3, 9.5).
ORIGEM_POR_TIPO: dict[EventType, EventSource] = {
    EventType.ENVIADA: EventSource.OBSERVADO,
    EventType.RECEBIDA: EventSource.OBSERVADO,
    EventType.PROCESSAMENTO_INICIADO: EventSource.OBSERVADO,
    EventType.PROCESSADA: EventSource.OBSERVADO,
    EventType.EXCLUIDA: EventSource.OBSERVADO,
    EventType.FALHA: EventSource.OBSERVADO,
    EventType.RECEBIMENTO_REPETIDO: EventSource.OBSERVADO,
    EventType.PROVAVEL_EXPIRACAO_VISIBILITY: EventSource.INFERIDO,
    EventType.AGUARDANDO_NOVA_TENTATIVA: EventSource.INFERIDO,
    EventType.ENCAMINHADA_DLQ: EventSource.OBSERVADO,
    EventType.ERRO_VALIDACAO_ORDER_ID: EventSource.OBSERVADO,
    EventType.ERRO_ACESSO_HISTORICO: EventSource.OBSERVADO,
}


# Verificações de integridade em tempo de importação. Garantem que o mapeamento
# permaneça total sobre EventType e consistente com o contrato fixo do design.
assert set(ORIGEM_POR_TIPO) == set(EventType), (
    "ORIGEM_POR_TIPO deve cobrir todos os EventType; "
    f"faltando: {set(EventType) - set(ORIGEM_POR_TIPO)}"
)
for _tipo, _origem_fixa in ORIGEM_OBRIGATORIA_POR_TIPO.items():
    assert ORIGEM_POR_TIPO[_tipo] is _origem_fixa, (
        f"Origem de {_tipo} deve ser {_origem_fixa} conforme o contrato do design"
    )


class OrigemInvalidaError(ValueError):
    """A `event_source` informada não corresponde à origem obrigatória do tipo.

    Usada por ``validar_origem`` quando um evento tenta registrar uma origem
    inconsistente com a classificação determinada pelo design (ex.: marcar
    `provavel_expiracao_visibility` como OBSERVADO).
    """


def classificar_origem(event_type: EventType) -> EventSource:
    """Retorna a ``EventSource`` obrigatória para um ``EventType``.

    A classificação é total e determinística: todo ``EventType`` possui exatamente
    uma origem (OBSERVADO ou INFERIDO), conforme a tabela do design.

    Args:
        event_type: o tipo de evento a classificar.

    Returns:
        A ``EventSource`` correta (OBSERVADO ou INFERIDO) para o tipo.

    Raises:
        TypeError: se ``event_type`` não for um membro de ``EventType``.
    """
    if not isinstance(event_type, EventType):
        raise TypeError(
            f"event_type deve ser um EventType, recebido: {type(event_type)!r}"
        )
    # O mapeamento é garantidamente total (verificado na importação), portanto o
    # acesso direto nunca levanta KeyError.
    return ORIGEM_POR_TIPO[event_type]


def validar_origem(event_type: EventType, event_source: EventSource) -> EventSource:
    """Valida que ``event_source`` corresponde à origem obrigatória do tipo.

    Útil ao construir/persistir um evento: confirma que a origem fornecida é a
    esperada para aquele ``event_type`` (Req. 6.4, 6.5, 11.9).

    Args:
        event_type: o tipo do evento.
        event_source: a origem informada para o evento.

    Returns:
        A própria ``event_source`` quando válida (facilita o uso encadeado).

    Raises:
        TypeError: se os argumentos não forem dos tipos esperados.
        OrigemInvalidaError: se ``event_source`` divergir da origem obrigatória.
    """
    if not isinstance(event_source, EventSource):
        raise TypeError(
            f"event_source deve ser um EventSource, recebido: {type(event_source)!r}"
        )
    origem_esperada = classificar_origem(event_type)
    if event_source is not origem_esperada:
        raise OrigemInvalidaError(
            f"Evento '{event_type.value}' deve ter origem "
            f"{origem_esperada.value}, mas recebeu {event_source.value}."
        )
    return event_source
