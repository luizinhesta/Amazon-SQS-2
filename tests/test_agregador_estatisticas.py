"""Testes unitários do agregador de estatísticas por fluxo (Tarefa 5.3).

Cobrem a derivação dos contadores por fluxo (Standard/FIFO) a partir do log de
eventos: enviadas, disponíveis, em processamento, processadas, com falha,
encaminhadas para DLQ, recebidas por consumidor e tentativas por mensagem. Também
verificam a invariante de não-negatividade, o log vazio (todos 0) e a separação
correta por fluxo, inclusive quando a mensagem passa pela DLQ.

Referências de requisitos: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.12, 5.2, 11.7.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.dominio.agregador_estatisticas import (
    FLUXO_FIFO,
    FLUXO_STANDARD,
    EstatisticasFluxo,
    agregar_estatisticas,
    classificar_fluxo,
)
from app.dominio.constantes import (
    FILA_FIFO,
    FILA_FIFO_DLQ,
    FILA_STANDARD,
    FILA_STANDARD_DLQ,
    MAX_RECEIVE_COUNT,
)
from app.dominio.tipos import EventSource, EventType


@dataclass
class _Evento:
    """Evento mínimo para os testes (compatível com o agregador e o StateDeriver)."""

    event_type: EventType
    queue_name: str
    order_id: Optional[str] = None
    event_source: EventSource = EventSource.OBSERVADO
    receive_count: Optional[int] = None
    consumer_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers de construção de eventos
# ---------------------------------------------------------------------------


def _enviada(order_id: str, fila: str = FILA_STANDARD) -> _Evento:
    return _Evento(EventType.ENVIADA, fila, order_id=order_id)


def _recebida(
    order_id: str,
    *,
    fila: str = FILA_STANDARD,
    receive_count: int = 1,
    consumidor: str = "consumidor-1",
) -> _Evento:
    return _Evento(
        EventType.RECEBIDA,
        fila,
        order_id=order_id,
        receive_count=receive_count,
        consumer_id=consumidor,
    )


def _processada(order_id: str, fila: str = FILA_STANDARD) -> _Evento:
    return _Evento(EventType.PROCESSADA, fila, order_id=order_id)


def _falha(order_id: str, *, fila: str = FILA_STANDARD, receive_count: int = 1) -> _Evento:
    return _Evento(
        EventType.FALHA, fila, order_id=order_id, receive_count=receive_count
    )


def _encaminhada_dlq(order_id: str, *, fila: str, receive_count: int) -> _Evento:
    return _Evento(
        EventType.ENCAMINHADA_DLQ,
        fila,
        order_id=order_id,
        receive_count=receive_count,
        event_source=EventSource.OBSERVADO,
    )


# ---------------------------------------------------------------------------
# Classificação de fluxo
# ---------------------------------------------------------------------------


def test_classificar_fluxo_standard_e_dlq() -> None:
    assert classificar_fluxo(FILA_STANDARD) == FLUXO_STANDARD
    assert classificar_fluxo(FILA_STANDARD_DLQ) == FLUXO_STANDARD


def test_classificar_fluxo_fifo_e_dlq() -> None:
    assert classificar_fluxo(FILA_FIFO) == FLUXO_FIFO
    assert classificar_fluxo(FILA_FIFO_DLQ) == FLUXO_FIFO


def test_classificar_fluxo_desconhecido_retorna_none() -> None:
    assert classificar_fluxo(None) is None
    assert classificar_fluxo("") is None
    assert classificar_fluxo("outra-fila") is None


# ---------------------------------------------------------------------------
# Log vazio: todos os contadores em 0 (Req. 3.12)
# ---------------------------------------------------------------------------


def test_log_vazio_produz_zero_em_todos_os_contadores() -> None:
    resultado = agregar_estatisticas([])

    for fluxo in (resultado.standard, resultado.fifo):
        assert fluxo.enviadas == 0
        assert fluxo.disponiveis == 0
        assert fluxo.em_processamento == 0
        assert fluxo.processadas == 0
        assert fluxo.com_falha == 0
        assert fluxo.encaminhadas_dlq == 0
        assert fluxo.recebidas_por_consumidor == {}
        assert fluxo.tentativas_por_mensagem == {}


# ---------------------------------------------------------------------------
# Contadores por Estado derivado
# ---------------------------------------------------------------------------


def test_enviadas_conta_eventos_enviada_por_fluxo() -> None:
    eventos = [
        _enviada("p-1", FILA_STANDARD),
        _enviada("p-2", FILA_STANDARD),
        _enviada("p-3", FILA_FIFO),
    ]

    resultado = agregar_estatisticas(eventos)

    assert resultado.standard.enviadas == 2
    assert resultado.fifo.enviadas == 1


def test_mensagem_apenas_enviada_fica_disponivel() -> None:
    # Estado derivado "Aguardando" é contado como disponível (Req. 3.2).
    resultado = agregar_estatisticas([_enviada("p-1")])

    assert resultado.standard.disponiveis == 1
    assert resultado.standard.processadas == 0


def test_mensagem_processada_conta_em_processadas() -> None:
    eventos = [_enviada("p-1"), _recebida("p-1"), _processada("p-1")]

    resultado = agregar_estatisticas(eventos)

    assert resultado.standard.processadas == 1
    assert resultado.standard.disponiveis == 0


def test_mensagem_em_falha_conta_em_com_falha() -> None:
    eventos = [_enviada("p-1"), _recebida("p-1"), _falha("p-1", receive_count=1)]

    resultado = agregar_estatisticas(eventos)

    assert resultado.standard.com_falha == 1


def test_mensagem_encaminhada_dlq_conta_no_fluxo_de_origem() -> None:
    # A mensagem é enviada/recebida na fila principal Standard e depois confirmada
    # na DLQ Standard. Deve ser contada como DLQ do fluxo Standard (Req. 3.6, 3.8).
    eventos = [
        _enviada("p-1", FILA_STANDARD),
        _recebida("p-1", fila=FILA_STANDARD, receive_count=1),
        _recebida("p-1", fila=FILA_STANDARD, receive_count=2),
        _encaminhada_dlq("p-1", fila=FILA_STANDARD_DLQ, receive_count=MAX_RECEIVE_COUNT),
    ]

    resultado = agregar_estatisticas(eventos)

    assert resultado.standard.encaminhadas_dlq == 1
    assert resultado.fifo.encaminhadas_dlq == 0


# ---------------------------------------------------------------------------
# Recebidas por consumidor (Req. 3.7)
# ---------------------------------------------------------------------------


def test_recebidas_por_consumidor_agrupa_por_consumer_id() -> None:
    eventos = [
        _recebida("p-1", consumidor="c-1", receive_count=1),
        _recebida("p-2", consumidor="c-1", receive_count=1),
        _recebida("p-3", consumidor="c-2", receive_count=1),
        _recebida("p-4", fila=FILA_FIFO, consumidor="c-fifo", receive_count=1),
    ]

    resultado = agregar_estatisticas(eventos)

    assert resultado.standard.recebidas_por_consumidor == {"c-1": 2, "c-2": 1}
    assert resultado.fifo.recebidas_por_consumidor == {"c-fifo": 1}


# ---------------------------------------------------------------------------
# Tentativas por mensagem (Req. 5.2, 11.7)
# ---------------------------------------------------------------------------


def test_tentativas_por_mensagem_usa_maior_receive_count() -> None:
    eventos = [
        _enviada("p-1"),
        _recebida("p-1", receive_count=1),
        _recebida("p-1", receive_count=2),
    ]

    resultado = agregar_estatisticas(eventos)

    assert resultado.standard.tentativas_por_mensagem == {"p-1": 2}


def test_tentativas_nao_negativas_com_receive_count_invalido() -> None:
    # receive_count ausente ou negativo é normalizado para 0 (não-negatividade).
    eventos = [
        _Evento(EventType.RECEBIDA, FILA_STANDARD, order_id="p-1", receive_count=None),
        _Evento(EventType.RECEBIDA, FILA_STANDARD, order_id="p-2", receive_count=-5),
    ]

    resultado = agregar_estatisticas(eventos)

    assert resultado.standard.tentativas_por_mensagem["p-1"] == 0
    assert resultado.standard.tentativas_por_mensagem["p-2"] == 0


# ---------------------------------------------------------------------------
# Não-negatividade e separação por fluxo (Req. 3.1-3.8, 3.12)
# ---------------------------------------------------------------------------


def _todos_contadores(fluxo: EstatisticasFluxo) -> list[int]:
    valores = [
        fluxo.enviadas,
        fluxo.disponiveis,
        fluxo.em_processamento,
        fluxo.processadas,
        fluxo.com_falha,
        fluxo.encaminhadas_dlq,
    ]
    valores.extend(fluxo.recebidas_por_consumidor.values())
    valores.extend(fluxo.tentativas_por_mensagem.values())
    return valores


def test_todos_contadores_sao_inteiros_nao_negativos() -> None:
    eventos = [
        _enviada("p-1", FILA_STANDARD),
        _recebida("p-1", fila=FILA_STANDARD, receive_count=1),
        _processada("p-1", FILA_STANDARD),
        _enviada("f-1", FILA_FIFO),
        _recebida("f-1", fila=FILA_FIFO, consumidor="c-fifo", receive_count=1),
        _falha("f-1", fila=FILA_FIFO, receive_count=1),
    ]

    resultado = agregar_estatisticas(eventos)

    for fluxo in (resultado.standard, resultado.fifo):
        for valor in _todos_contadores(fluxo):
            assert isinstance(valor, int)
            assert valor >= 0


def test_fluxos_sao_separados_sem_contaminacao_cruzada() -> None:
    eventos = [
        _enviada("p-1", FILA_STANDARD),
        _processada("p-1", FILA_STANDARD),
        _enviada("f-1", FILA_FIFO),
    ]

    resultado = agregar_estatisticas(eventos)

    assert resultado.standard.enviadas == 1
    assert resultado.standard.processadas == 1
    assert resultado.fifo.enviadas == 1
    assert resultado.fifo.processadas == 0
