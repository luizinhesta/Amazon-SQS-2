"""Testes unitários do modelo de evento da Tabela_Historico (Tarefa 2.1).

Cobrem a construção das chaves (PK/SK/GSI1PK/GSI1SK), a serialização para item
DynamoDB (`para_item`), a desserialização (`de_item`) e o round-trip, incluindo os
exemplos JSON do design.md.

Referências de requisitos: 11.1, 11.3, 11.4, 11.6, 6.1.
"""

from __future__ import annotations

import pytest

from app.dominio.evento import (
    Evento,
    construir_gsi1pk,
    construir_gsi1sk,
    construir_pk,
    construir_sk,
)
from app.dominio.tipos import EventSource, EventType


# ---------------------------------------------------------------------------
# Construção das chaves
# ---------------------------------------------------------------------------


def test_construir_pk() -> None:
    # Req. 11.3: PK agrupa eventos por pedido.
    assert construir_pk("pedido-0001") == "PEDIDO#pedido-0001"


def test_construir_sk_aplica_zero_padding_no_seq() -> None:
    # Req. 6.1/11.6: SK ordena cronologicamente; seq com 4 dígitos para desempate.
    assert construir_sk(1719322800123, 1) == "EVENTO#1719322800123#0001"
    assert construir_sk(1719322800123, 42) == "EVENTO#1719322800123#0042"


def test_construir_gsi1pk() -> None:
    # Req. 11.4: GSI1PK habilita consulta por fila.
    assert construir_gsi1pk("lab-sqs-standard") == "FILA#lab-sqs-standard"


def test_construir_gsi1sk() -> None:
    assert (
        construir_gsi1sk(1719322800123, "pedido-0001") == "1719322800123#pedido-0001"
    )


def test_ordenacao_lexicografica_da_sk_respeita_seq() -> None:
    # O desempate estável (Req. 6.1) depende da ordenação lexicográfica coincidir
    # com a ordem numérica de seq para eventos de mesmo app_time_ms.
    sks = [construir_sk(1719322800123, s) for s in (3, 1, 2, 10)]
    assert sorted(sks) == [
        construir_sk(1719322800123, 1),
        construir_sk(1719322800123, 2),
        construir_sk(1719322800123, 3),
        construir_sk(1719322800123, 10),
    ]


# ---------------------------------------------------------------------------
# Propriedades de chave na instância
# ---------------------------------------------------------------------------


def test_propriedades_de_chave_do_evento() -> None:
    evento = Evento(
        order_id="pedido-0001",
        queue_name="lab-sqs-standard",
        event_type=EventType.ENVIADA,
        event_source=EventSource.OBSERVADO,
        app_time="2024-06-25T12:00:00.123Z",
        app_time_ms=1719322800123,
        seq=1,
    )
    assert evento.pk == "PEDIDO#pedido-0001"
    assert evento.sk == "EVENTO#1719322800123#0001"
    assert evento.gsi1pk == "FILA#lab-sqs-standard"
    assert evento.gsi1sk == "1719322800123#pedido-0001"


# ---------------------------------------------------------------------------
# Serialização — para_item
# ---------------------------------------------------------------------------


def test_para_item_omite_campos_opcionais_ausentes() -> None:
    # Evento "enviada" não carrega consumer_id nem receive_count (ver design.md).
    evento = Evento(
        order_id="pedido-0001",
        queue_name="lab-sqs-standard",
        event_type=EventType.ENVIADA,
        event_source=EventSource.OBSERVADO,
        app_time="2024-06-25T12:00:00.123Z",
        app_time_ms=1719322800123,
        seq=1,
        sqs_message_id="a1b2c3d4-0001",
        processing_seconds=45,
    )
    item = evento.para_item()

    assert item == {
        "PK": "PEDIDO#pedido-0001",
        "SK": "EVENTO#1719322800123#0001",
        "order_id": "pedido-0001",
        "queue_name": "lab-sqs-standard",
        "event_type": "enviada",
        "event_source": "OBSERVADO",
        "app_time": "2024-06-25T12:00:00.123Z",
        "app_time_ms": 1719322800123,
        "sqs_message_id": "a1b2c3d4-0001",
        "processing_seconds": 45,
        "GSI1PK": "FILA#lab-sqs-standard",
        "GSI1SK": "1719322800123#pedido-0001",
    }
    # Nenhum atributo opcional ausente deve aparecer no item.
    assert "consumer_id" not in item
    assert "receive_count" not in item
    assert "message_group_id" not in item
    assert "error_detail" not in item


def test_para_item_grava_enums_como_strings() -> None:
    evento = Evento(
        order_id="pedido-0001",
        queue_name="lab-sqs-standard",
        event_type=EventType.PROVAVEL_EXPIRACAO_VISIBILITY,
        event_source=EventSource.INFERIDO,
        app_time="2024-06-25T12:00:35.460Z",
        app_time_ms=1719322835460,
        seq=3,
        receive_count=1,
    )
    item = evento.para_item()
    assert item["event_type"] == "provavel_expiracao_visibility"
    assert item["event_source"] == "INFERIDO"
    assert isinstance(item["event_type"], str)
    assert isinstance(item["event_source"], str)
    assert item["receive_count"] == 1


def test_para_item_inclui_receive_count_zero() -> None:
    # receive_count = 0 é um valor válido (≥ 0) e NÃO deve ser omitido.
    evento = Evento(
        order_id="pedido-0001",
        queue_name="lab-sqs-standard",
        event_type=EventType.RECEBIDA,
        event_source=EventSource.OBSERVADO,
        app_time="2024-06-25T12:00:05.456Z",
        app_time_ms=1719322805456,
        seq=2,
        receive_count=0,
    )
    item = evento.para_item()
    assert item["receive_count"] == 0


# ---------------------------------------------------------------------------
# Desserialização — de_item e round-trip
# ---------------------------------------------------------------------------


def test_de_item_reconstroi_evento_de_recebimento() -> None:
    item = {
        "PK": "PEDIDO#pedido-0001",
        "SK": "EVENTO#1719322805456#0002",
        "order_id": "pedido-0001",
        "queue_name": "lab-sqs-standard",
        "event_type": "recebida",
        "event_source": "OBSERVADO",
        "app_time": "2024-06-25T12:00:05.456Z",
        "app_time_ms": 1719322805456,
        "consumer_id": "consumidor-standard-03",
        "receive_count": 1,
        "GSI1PK": "FILA#lab-sqs-standard",
        "GSI1SK": "1719322805456#pedido-0001",
    }
    evento = Evento.de_item(item)

    assert evento.order_id == "pedido-0001"
    assert evento.queue_name == "lab-sqs-standard"
    assert evento.event_type is EventType.RECEBIDA
    assert evento.event_source is EventSource.OBSERVADO
    assert evento.app_time_ms == 1719322805456
    assert evento.seq == 2  # recuperado da SK
    assert evento.consumer_id == "consumidor-standard-03"
    assert evento.receive_count == 1
    # Opcionais ausentes retornam None.
    assert evento.processing_seconds is None
    assert evento.message_group_id is None
    assert evento.error_detail is None


@pytest.mark.parametrize(
    "evento",
    [
        # Evento de envio (OBSERVADO).
        Evento(
            order_id="pedido-0001",
            queue_name="lab-sqs-standard",
            event_type=EventType.ENVIADA,
            event_source=EventSource.OBSERVADO,
            app_time="2024-06-25T12:00:00.123Z",
            app_time_ms=1719322800123,
            seq=1,
            sqs_message_id="a1b2c3d4-0001",
            processing_seconds=45,
        ),
        # Evento de recebimento com contagem de tentativas (OBSERVADO).
        Evento(
            order_id="pedido-0001",
            queue_name="lab-sqs-standard",
            event_type=EventType.RECEBIDA,
            event_source=EventSource.OBSERVADO,
            app_time="2024-06-25T12:00:05.456Z",
            app_time_ms=1719322805456,
            seq=2,
            consumer_id="consumidor-standard-03",
            receive_count=1,
        ),
        # Evento inferido de provável expiração do Visibility_Timeout.
        Evento(
            order_id="pedido-0001",
            queue_name="lab-sqs-standard",
            event_type=EventType.PROVAVEL_EXPIRACAO_VISIBILITY,
            event_source=EventSource.INFERIDO,
            app_time="2024-06-25T12:00:35.460Z",
            app_time_ms=1719322835460,
            seq=3,
            receive_count=1,
        ),
        # Evento de encaminhamento para a DLQ (OBSERVADO).
        Evento(
            order_id="pedido-0001",
            queue_name="lab-sqs-standard-dlq",
            event_type=EventType.ENCAMINHADA_DLQ,
            event_source=EventSource.OBSERVADO,
            app_time="2024-06-25T12:01:40.000Z",
            app_time_ms=1719322900000,
            seq=7,
            receive_count=2,
        ),
        # Evento FIFO com message_group_id e error_detail.
        Evento(
            order_id="pedido-fifo-9",
            queue_name="lab-sqs-fifo.fifo",
            event_type=EventType.FALHA,
            event_source=EventSource.OBSERVADO,
            app_time="2024-06-25T12:02:00.000Z",
            app_time_ms=1719322920000,
            seq=5,
            consumer_id="consumidor-fifo-01",
            receive_count=1,
            message_group_id="grupo-A",
            error_detail="RuntimeError: falha proposital",
        ),
    ],
)
def test_round_trip_para_item_de_item(evento: Evento) -> None:
    # para_item -> de_item deve preservar o evento integralmente.
    reconstruido = Evento.de_item(evento.para_item())
    assert reconstruido == evento


def test_de_item_sk_fora_do_formato_levanta_erro() -> None:
    item = {
        "PK": "PEDIDO#pedido-0001",
        "SK": "FORMATO_INVALIDO",
        "order_id": "pedido-0001",
        "queue_name": "lab-sqs-standard",
        "event_type": "enviada",
        "event_source": "OBSERVADO",
        "app_time": "2024-06-25T12:00:00.123Z",
        "app_time_ms": 1719322800123,
        "GSI1PK": "FILA#lab-sqs-standard",
        "GSI1SK": "1719322800123#pedido-0001",
    }
    with pytest.raises(ValueError):
        Evento.de_item(item)


def test_de_item_sk_inconsistente_com_app_time_ms_levanta_erro() -> None:
    item = {
        "PK": "PEDIDO#pedido-0001",
        "SK": "EVENTO#9999999999999#0001",  # ms diferente do atributo
        "order_id": "pedido-0001",
        "queue_name": "lab-sqs-standard",
        "event_type": "enviada",
        "event_source": "OBSERVADO",
        "app_time": "2024-06-25T12:00:00.123Z",
        "app_time_ms": 1719322800123,
        "GSI1PK": "FILA#lab-sqs-standard",
        "GSI1SK": "1719322800123#pedido-0001",
    }
    with pytest.raises(ValueError):
        Evento.de_item(item)


def test_de_item_event_type_invalido_levanta_erro() -> None:
    item = {
        "PK": "PEDIDO#pedido-0001",
        "SK": "EVENTO#1719322800123#0001",
        "order_id": "pedido-0001",
        "queue_name": "lab-sqs-standard",
        "event_type": "tipo_inexistente",
        "event_source": "OBSERVADO",
        "app_time": "2024-06-25T12:00:00.123Z",
        "app_time_ms": 1719322800123,
        "GSI1PK": "FILA#lab-sqs-standard",
        "GSI1SK": "1719322800123#pedido-0001",
    }
    with pytest.raises(ValueError):
        Evento.de_item(item)
