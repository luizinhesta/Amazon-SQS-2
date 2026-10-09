"""Testes unitários da ordenação cronológica de eventos (Tarefa 5.1).

Cobrem a função pura `ordenar_cronologicamente`:

- ordenação não-decrescente por `app_time_ms`;
- desempate estável por `seq` entre eventos de mesmo horário;
- preservação da ordem de registro quando a chave `(app_time_ms, seq)` empata
  (estabilidade do `sorted`);
- pureza (a coleção de entrada não é modificada);
- casos de borda (lista vazia e lista de um único elemento).

Referências de requisitos: 6.1, 11.6.
"""

from __future__ import annotations

from app.dominio.evento import Evento
from app.dominio.ordenacao import chave_cronologica, ordenar_cronologicamente
from app.dominio.tipos import EventSource, EventType


def _evento(
    *,
    order_id: str = "pedido-0001",
    app_time_ms: int,
    seq: int,
    event_type: EventType = EventType.RECEBIDA,
) -> Evento:
    """Cria um Evento mínimo para os testes de ordenação.

    Apenas `app_time_ms` e `seq` influenciam a ordenação; os demais campos recebem
    valores coerentes com o modelo.
    """
    return Evento(
        order_id=order_id,
        queue_name="lab-sqs-standard",
        event_type=event_type,
        event_source=EventSource.OBSERVADO,
        app_time="2024-06-25T12:00:00.000Z",
        app_time_ms=app_time_ms,
        seq=seq,
    )


def test_ordena_por_app_time_ms_crescente() -> None:
    # Req. 6.1/11.6: ordem cronológica crescente pelo horário real da aplicação.
    e1 = _evento(app_time_ms=3000, seq=1)
    e2 = _evento(app_time_ms=1000, seq=1)
    e3 = _evento(app_time_ms=2000, seq=1)

    resultado = ordenar_cronologicamente([e1, e2, e3])

    assert [e.app_time_ms for e in resultado] == [1000, 2000, 3000]


def test_desempate_por_seq_entre_eventos_de_mesmo_horario() -> None:
    # Req. 6.1: usa a ordem de registro (seq) como desempate entre eventos de
    # mesmo app_time_ms.
    e_seq3 = _evento(app_time_ms=1000, seq=3)
    e_seq1 = _evento(app_time_ms=1000, seq=1)
    e_seq2 = _evento(app_time_ms=1000, seq=2)

    resultado = ordenar_cronologicamente([e_seq3, e_seq1, e_seq2])

    assert [e.seq for e in resultado] == [1, 2, 3]


def test_ordenacao_combina_app_time_ms_e_seq() -> None:
    # Chave composta (app_time_ms, seq): horário tem precedência sobre seq.
    a = _evento(app_time_ms=1000, seq=2)
    b = _evento(app_time_ms=1000, seq=1)
    c = _evento(app_time_ms=2000, seq=1)
    d = _evento(app_time_ms=2000, seq=5)

    resultado = ordenar_cronologicamente([d, a, c, b])

    assert [(e.app_time_ms, e.seq) for e in resultado] == [
        (1000, 1),
        (1000, 2),
        (2000, 1),
        (2000, 5),
    ]


def test_e_estavel_quando_a_chave_empata() -> None:
    # Estabilidade: eventos com mesma chave (app_time_ms, seq) preservam a ordem de
    # entrada. Distinguimos pelos tipos para verificar a ordem relativa.
    primeiro = _evento(app_time_ms=1000, seq=1, event_type=EventType.ENVIADA)
    segundo = _evento(app_time_ms=1000, seq=1, event_type=EventType.RECEBIDA)

    resultado = ordenar_cronologicamente([primeiro, segundo])

    assert [e.event_type for e in resultado] == [
        EventType.ENVIADA,
        EventType.RECEBIDA,
    ]


def test_nao_modifica_a_colecao_de_entrada() -> None:
    # A função é pura: a lista original permanece inalterada.
    e1 = _evento(app_time_ms=3000, seq=1)
    e2 = _evento(app_time_ms=1000, seq=1)
    entrada = [e1, e2]

    resultado = ordenar_cronologicamente(entrada)

    assert entrada == [e1, e2]  # ordem original preservada
    assert resultado == [e2, e1]  # nova lista ordenada
    assert resultado is not entrada


def test_lista_vazia_retorna_lista_vazia() -> None:
    assert ordenar_cronologicamente([]) == []


def test_lista_unitaria_retorna_mesmo_evento() -> None:
    unico = _evento(app_time_ms=1234, seq=7)
    assert ordenar_cronologicamente([unico]) == [unico]


def test_aceita_iteravel_generico() -> None:
    # A função aceita qualquer iterável, não apenas listas.
    e1 = _evento(app_time_ms=2000, seq=1)
    e2 = _evento(app_time_ms=1000, seq=1)

    resultado = ordenar_cronologicamente(iter([e1, e2]))

    assert resultado == [e2, e1]


def test_chave_cronologica_retorna_tupla_app_time_ms_e_seq() -> None:
    evento = _evento(app_time_ms=1719322800123, seq=42)
    assert chave_cronologica(evento) == (1719322800123, 42)
