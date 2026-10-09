"""Testes unitários das consultas do HistoricoRepository (Tarefa 7.3).

Cobrem os métodos de leitura da Tabela_Historico:

- ``consultar_por_pedido`` — Query na tabela base por ``PK = PEDIDO#<order_id>``
  (Req. 11.3).
- ``consultar_por_fila`` — Query no ``GSI1`` por ``GSI1PK = FILA#<queue_name>``
  (Req. 11.4).
- ``listar_eventos_cronologico`` — ordem cronológica crescente (Req. 11.6).
- ``contar_tentativas`` — inteiro ≥ 0 (Req. 11.7).
- ``pedidos_em_falha`` — identificação de mensagens em estado de falha (Req. 11.8).
- Retorno vazio sem erro quando não há registros (Req. 11.5).

Os testes usam um *fake* de DynamoDB em memória que suporta ``query`` por ``PK``
(tabela base) e por ``GSI1PK`` (índice ``GSI1``), interpretando o
``KeyConditionExpression`` construído por ``boto3.dynamodb.conditions.Key`` — sem
depender da AWS real. O *fake* também simula paginação (``LastEvaluatedKey``) para
exercitar a iteração de páginas do repositório.

Referências de requisitos: 11.3, 11.4, 11.5, 11.6, 11.7, 11.8.
"""

from __future__ import annotations

from typing import Any

import pytest

from app.acesso.historico_repository import HistoricoRepository
from app.dominio.constantes import INDICE_GSI1, MAX_RECEIVE_COUNT
from app.dominio.evento import Evento
from app.dominio.tipos import EventSource, EventType


# ---------------------------------------------------------------------------
# Fake de DynamoDB em memória com suporte a Query (tabela base e GSI1)
# ---------------------------------------------------------------------------


def _nome_e_valor_da_condicao(condicao: Any) -> tuple[str, Any]:
    """Extrai ``(nome_do_atributo, valor)`` de um ``Key(...).eq(valor)`` do boto3.

    O objeto de condição de igualdade exposto por
    ``boto3.dynamodb.conditions.Key`` descreve a comparação em ``get_expression()``,
    cujos ``values`` são ``(<objeto Key>, valor)``. O *fake* usa isso para resolver
    a Query sem reimplementar o parser completo do DynamoDB.
    """
    expressao = condicao.get_expression()
    assert expressao["operator"] == "=", "o fake só suporta igualdade na chave"
    chave, valor = expressao["values"]
    return chave.name, valor


class TabelaDynamoDBFalsa:
    """Fake de ``Table`` do boto3 em memória, com Query por PK e por GSI1.

    Guarda os itens escritos por ``put_item`` e resolve ``query`` filtrando por
    igualdade na chave de partição apropriada:

    - sem ``IndexName``: consulta a tabela base por ``PK`` e ordena por ``SK``;
    - com ``IndexName`` igual ao ``GSI1``: consulta por ``GSI1PK`` e ordena por
      ``GSI1SK``.

    ``ScanIndexForward`` controla a direção da ordenação. O parâmetro
    ``tamanho_pagina`` força paginação para exercitar o acompanhamento de
    ``LastEvaluatedKey`` pelo repositório.
    """

    def __init__(
        self,
        itens: list[dict[str, Any]] | None = None,
        *,
        tamanho_pagina: int | None = None,
    ) -> None:
        self.itens: list[dict[str, Any]] = list(itens or [])
        self._tamanho_pagina = tamanho_pagina
        self.consultas: list[dict[str, Any]] = []

    def put_item(self, **kwargs: Any) -> dict[str, Any]:
        self.itens.append(kwargs["Item"])
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.consultas.append(kwargs)

        index_name = kwargs.get("IndexName")
        nome_chave, valor = _nome_e_valor_da_condicao(
            kwargs["KeyConditionExpression"]
        )

        if index_name is None:
            # Tabela base: filtra por PK, ordena pela SK.
            atributo_pk, atributo_ordenacao = "PK", "SK"
        else:
            assert index_name == INDICE_GSI1, f"índice desconhecido: {index_name}"
            atributo_pk, atributo_ordenacao = "GSI1PK", "GSI1SK"
            assert nome_chave == atributo_pk

        correspondentes = [
            item for item in self.itens if item.get(atributo_pk) == valor
        ]
        crescente = kwargs.get("ScanIndexForward", True)
        correspondentes.sort(
            key=lambda item: item.get(atributo_ordenacao, ""),
            reverse=not crescente,
        )

        # Paginação: respeita ExclusiveStartKey e devolve LastEvaluatedKey.
        inicio = 0
        chave_inicial = kwargs.get("ExclusiveStartKey")
        if chave_inicial is not None:
            marcador = chave_inicial[atributo_ordenacao]
            for indice, item in enumerate(correspondentes):
                if item.get(atributo_ordenacao) == marcador:
                    inicio = indice + 1
                    break

        if self._tamanho_pagina is None:
            fatia = correspondentes[inicio:]
            fim = len(correspondentes)
        else:
            fim = min(inicio + self._tamanho_pagina, len(correspondentes))
            fatia = correspondentes[inicio:fim]

        resposta: dict[str, Any] = {"Items": fatia}
        if fim < len(correspondentes) and fatia:
            ultimo = fatia[-1]
            resposta["LastEvaluatedKey"] = {atributo_ordenacao: ultimo[atributo_ordenacao]}
        return resposta


# ---------------------------------------------------------------------------
# Helpers de construção de eventos
# ---------------------------------------------------------------------------


def _evento(
    order_id: str,
    event_type: EventType,
    app_time_ms: int,
    *,
    seq: int = 1,
    queue_name: str = "lab-sqs-standard",
    receive_count: int | None = None,
    event_source: EventSource = EventSource.OBSERVADO,
) -> Evento:
    segundos = app_time_ms // 1000
    ms = app_time_ms % 1000
    app_time = f"2024-06-25T12:00:{segundos % 60:02d}.{ms:03d}Z"
    return Evento(
        order_id=order_id,
        queue_name=queue_name,
        event_type=event_type,
        event_source=event_source,
        app_time=app_time,
        app_time_ms=app_time_ms,
        seq=seq,
        receive_count=receive_count,
    )


def _tabela_com(*eventos: Evento, **kwargs: Any) -> TabelaDynamoDBFalsa:
    return TabelaDynamoDBFalsa([e.para_item() for e in eventos], **kwargs)


# ---------------------------------------------------------------------------
# consultar_por_pedido (Req. 11.3)
# ---------------------------------------------------------------------------


def test_consultar_por_pedido_retorna_apenas_eventos_do_pedido() -> None:
    e1 = _evento("pedido-0001", EventType.ENVIADA, 1719322800000, seq=1)
    e2 = _evento(
        "pedido-0001", EventType.RECEBIDA, 1719322805000, seq=2, receive_count=1
    )
    outro = _evento("pedido-0002", EventType.ENVIADA, 1719322801000, seq=1)
    tabela = _tabela_com(e1, e2, outro)
    repositorio = HistoricoRepository(tabela)

    eventos = repositorio.consultar_por_pedido("pedido-0001")

    assert [ev.event_type for ev in eventos] == [
        EventType.ENVIADA,
        EventType.RECEBIDA,
    ]
    assert all(ev.order_id == "pedido-0001" for ev in eventos)


def test_consultar_por_pedido_sem_registros_retorna_vazio_sem_erro() -> None:
    repositorio = HistoricoRepository(TabelaDynamoDBFalsa())

    assert repositorio.consultar_por_pedido("inexistente") == []


def test_consultar_por_pedido_pagina_todas_as_paginas() -> None:
    eventos = [
        _evento("pedido-0001", EventType.RECEBIDA, 1719322800000 + i, seq=i + 1)
        for i in range(5)
    ]
    tabela = _tabela_com(*eventos, tamanho_pagina=2)
    repositorio = HistoricoRepository(tabela)

    resultado = repositorio.consultar_por_pedido("pedido-0001")

    # Todas as 5 ocorrências retornadas, apesar da paginação de 2 em 2.
    assert len(resultado) == 5
    # Mais de uma chamada de query foi necessária (houve paginação).
    assert len(tabela.consultas) >= 3


# ---------------------------------------------------------------------------
# consultar_por_fila via GSI1 (Req. 11.4)
# ---------------------------------------------------------------------------


def test_consultar_por_fila_usa_gsi1_e_filtra_pela_fila() -> None:
    e_std = _evento(
        "pedido-0001", EventType.ENVIADA, 1719322800000, queue_name="lab-sqs-standard"
    )
    e_fifo = _evento(
        "pedido-0100",
        EventType.ENVIADA,
        1719322801000,
        queue_name="lab-sqs-fifo.fifo",
    )
    tabela = _tabela_com(e_std, e_fifo)
    repositorio = HistoricoRepository(tabela)

    eventos = repositorio.consultar_por_fila("lab-sqs-standard")

    assert [ev.order_id for ev in eventos] == ["pedido-0001"]
    # A consulta foi direcionada ao índice GSI1.
    assert tabela.consultas[0]["IndexName"] == INDICE_GSI1


def test_consultar_por_fila_sem_registros_retorna_vazio_sem_erro() -> None:
    repositorio = HistoricoRepository(TabelaDynamoDBFalsa())

    assert repositorio.consultar_por_fila("lab-sqs-standard") == []


# ---------------------------------------------------------------------------
# listar_eventos_cronologico (Req. 11.6)
# ---------------------------------------------------------------------------


def test_listar_eventos_cronologico_ordena_crescente() -> None:
    # Inseridos fora de ordem; devem sair ordenados por (app_time_ms, seq).
    tardio = _evento("pedido-0001", EventType.PROCESSADA, 1719322899000, seq=1)
    cedo = _evento("pedido-0001", EventType.ENVIADA, 1719322800000, seq=1)
    meio_a = _evento("pedido-0001", EventType.RECEBIDA, 1719322850000, seq=1)
    meio_b = _evento(
        "pedido-0001", EventType.PROCESSAMENTO_INICIADO, 1719322850000, seq=2
    )
    tabela = _tabela_com(tardio, meio_b, cedo, meio_a)
    repositorio = HistoricoRepository(tabela)

    eventos = repositorio.listar_eventos_cronologico("pedido-0001")

    chaves = [(ev.app_time_ms, ev.seq) for ev in eventos]
    assert chaves == sorted(chaves)
    assert [ev.event_type for ev in eventos] == [
        EventType.ENVIADA,
        EventType.RECEBIDA,
        EventType.PROCESSAMENTO_INICIADO,
        EventType.PROCESSADA,
    ]


def test_listar_eventos_cronologico_sem_registros_retorna_vazio() -> None:
    repositorio = HistoricoRepository(TabelaDynamoDBFalsa())

    assert repositorio.listar_eventos_cronologico("inexistente") == []


# ---------------------------------------------------------------------------
# contar_tentativas (Req. 11.7)
# ---------------------------------------------------------------------------


def test_contar_tentativas_retorna_maior_receive_count() -> None:
    e1 = _evento("pedido-0001", EventType.ENVIADA, 1719322800000, seq=1)
    r1 = _evento(
        "pedido-0001", EventType.RECEBIDA, 1719322805000, seq=2, receive_count=1
    )
    r2 = _evento(
        "pedido-0001", EventType.RECEBIDA, 1719322900000, seq=3, receive_count=2
    )
    tabela = _tabela_com(e1, r1, r2)
    repositorio = HistoricoRepository(tabela)

    assert repositorio.contar_tentativas("pedido-0001") == 2


def test_contar_tentativas_sem_recebimentos_retorna_zero() -> None:
    # Apenas envio: nenhum evento de recebimento, contagem = 0 (≥ 0, sem erro).
    e1 = _evento("pedido-0001", EventType.ENVIADA, 1719322800000, seq=1)
    tabela = _tabela_com(e1)
    repositorio = HistoricoRepository(tabela)

    assert repositorio.contar_tentativas("pedido-0001") == 0


def test_contar_tentativas_pedido_inexistente_retorna_zero() -> None:
    repositorio = HistoricoRepository(TabelaDynamoDBFalsa())

    assert repositorio.contar_tentativas("inexistente") == 0


def test_contar_tentativas_nunca_e_negativa() -> None:
    e1 = _evento("pedido-0001", EventType.ENVIADA, 1719322800000, seq=1)
    tabela = _tabela_com(e1)
    repositorio = HistoricoRepository(tabela)

    assert repositorio.contar_tentativas("pedido-0001") >= 0


# ---------------------------------------------------------------------------
# pedidos_em_falha (Req. 11.8)
# ---------------------------------------------------------------------------


def test_pedidos_em_falha_identifica_pedido_com_estado_falha() -> None:
    # Pedido em falha: recebida + falha com receive_count < maxReceiveCount.
    recebida = _evento(
        "pedido-falha", EventType.RECEBIDA, 1719322805000, seq=1, receive_count=1
    )
    falha = _evento(
        "pedido-falha", EventType.FALHA, 1719322810000, seq=2, receive_count=1
    )
    # Pedido saudável (processado): não deve ser considerado em falha.
    ok_env = _evento("pedido-ok", EventType.ENVIADA, 1719322800000, seq=1)
    ok_proc = _evento("pedido-ok", EventType.PROCESSADA, 1719322820000, seq=2)
    tabela = _tabela_com(recebida, falha, ok_env, ok_proc)
    repositorio = HistoricoRepository(tabela)

    em_falha = repositorio.pedidos_em_falha("lab-sqs-standard")

    assert em_falha == ["pedido-falha"]


def test_pedidos_em_falha_sem_registros_retorna_vazio() -> None:
    repositorio = HistoricoRepository(TabelaDynamoDBFalsa())

    assert repositorio.pedidos_em_falha("lab-sqs-standard") == []


def test_pedidos_em_falha_respeita_max_receive_count_customizado() -> None:
    recebida = _evento(
        "pedido-x", EventType.RECEBIDA, 1719322805000, seq=1, receive_count=1
    )
    falha = _evento(
        "pedido-x", EventType.FALHA, 1719322810000, seq=2, receive_count=1
    )
    tabela = _tabela_com(recebida, falha)
    repositorio = HistoricoRepository(tabela)

    # Com max_receive_count=1, a falha com receive_count=1 não é "Falha" simples
    # (receive_count >= limite), então o pedido não aparece como Falha.
    assert repositorio.pedidos_em_falha("lab-sqs-standard", max_receive_count=1) == []
    # Com o padrão (2), receive_count=1 < 2 → estado Falha.
    assert repositorio.pedidos_em_falha(
        "lab-sqs-standard", max_receive_count=MAX_RECEIVE_COUNT
    ) == ["pedido-x"]
