"""Tipos e enums de domínio do laboratório Amazon SQS.

Define:
- EventType: os tipos de evento do ciclo de vida de uma mensagem registrados na
  Tabela_Historico (ver tabela de atributos no design.md).
- EventSource: a origem de cada evento, OBSERVADO (resposta direta/ação confirmada)
  ou INFERIDO (deduzido pela aplicação) (Req. 6.4, 6.5, 11.9).
- Estado: o conjunto de Estados exibidos na coluna Estado do monitoramento (Req. 5.4).

Todos os valores são strings legíveis para facilitar a persistência no DynamoDB e a
exibição no Dashboard em pt-BR.
"""

from __future__ import annotations

from enum import Enum


class EventType(str, Enum):
    """Tipos de evento do ciclo de vida de uma mensagem (campo `event_type`).

    Os valores correspondem exatamente aos listados na tabela de atributos da
    Tabela_Historico no documento de design.
    """

    ENVIADA = "enviada"
    RECEBIDA = "recebida"
    PROCESSAMENTO_INICIADO = "processamento_iniciado"
    PROCESSADA = "processada"
    EXCLUIDA = "excluida"
    FALHA = "falha"
    RECEBIMENTO_REPETIDO = "recebimento_repetido"
    PROVAVEL_EXPIRACAO_VISIBILITY = "provavel_expiracao_visibility"
    AGUARDANDO_NOVA_TENTATIVA = "aguardando_nova_tentativa"
    ENCAMINHADA_DLQ = "encaminhada_dlq"
    ERRO_VALIDACAO_ORDER_ID = "erro_validacao_order_id"
    ERRO_ACESSO_HISTORICO = "erro_acesso_historico"


class EventSource(str, Enum):
    """Origem de um evento registrado na Tabela_Historico (campo `event_source`).

    - OBSERVADO: resultante de resposta direta de um serviço AWS ou ação concreta
      confirmada pela aplicação.
    - INFERIDO: deduzido pela aplicação a partir de condições indiretas, sem
      confirmação direta do SQS.

    Ver Req. 6.4, 6.5, 11.9.
    """

    OBSERVADO = "OBSERVADO"
    INFERIDO = "INFERIDO"


class Estado(str, Enum):
    """Estados derivados de uma mensagem (coluna Estado do monitoramento).

    O conjunto é fechado e corresponde exatamente ao exigido pelo Req. 5.4. O Estado
    nunca é persistido como campo mutável — é derivado dos eventos pelo StateDeriver.
    """

    AGUARDANDO = "Aguardando"
    RECEBIDA = "Recebida"
    EM_PROCESSAMENTO = "Em processamento"
    PROCESSADA = "Processada"
    FALHA = "Falha"
    AGUARDANDO_NOVA_TENTATIVA = "Aguardando nova tentativa"
    ENCAMINHADA_DLQ = "Encaminhada para DLQ"


# Conjunto permitido de Estados (Req. 5.4). Útil para validações e testes de
# propriedade que verificam que o StateDeriver retorna exatamente um Estado válido.
ESTADOS_PERMITIDOS: frozenset[Estado] = frozenset(Estado)

# Estados terminais: uma vez atingidos, não há transição de saída (ver máquina de
# estados no design.md). Usados pela regra de precedência do StateDeriver.
ESTADOS_TERMINAIS: frozenset[Estado] = frozenset(
    {Estado.PROCESSADA, Estado.ENCAMINHADA_DLQ}
)


# Origens obrigatórias por tipo de evento (contrato base para o classificador).
# O design exige que `provavel_expiracao_visibility` seja sempre INFERIDO e que
# `encaminhada_dlq` seja sempre OBSERVADO (Req. 6.5, 6.6, 7.9). A lógica completa de
# classificação é implementada na camada de domínio em tarefa posterior; aqui
# registra-se apenas o contrato dos tipos cuja origem é fixa.
ORIGEM_OBRIGATORIA_POR_TIPO: dict[EventType, EventSource] = {
    EventType.PROVAVEL_EXPIRACAO_VISIBILITY: EventSource.INFERIDO,
    EventType.ENCAMINHADA_DLQ: EventSource.OBSERVADO,
}
