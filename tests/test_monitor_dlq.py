"""Testes unitários do MonitorDlq (observação das DLQs e registro de encaminhada_dlq).

Validam, com dublês em memória de ``SqsGateway`` e ``HistoricoRepository`` (sem AWS
real), o comportamento do :class:`app.consumidores.monitor_dlq.MonitorDlq`:

(a) uma mensagem presente na DLQ gera exatamente 1 evento ``encaminhada_dlq``
    OBSERVADO, com ``receive_count >= MAX_RECEIVE_COUNT``, associado à fila
    PRINCIPAL correspondente e com o ``order_id`` extraído do corpo JSON;
(b) varrer a mesma DLQ duas vezes com a mesma mensagem NÃO duplica o evento
    (idempotência — a mensagem permanece na DLQ, pois nunca é excluída);
(c) uma DLQ vazia não gera nenhum evento;
(d) após o registro, ``derivar_estado`` dos eventos do pedido retorna
    :attr:`Estado.ENCAMINHADA_DLQ`.

Referências de requisitos: 6.6, 7.9, 7.10, 11.3.
"""

from __future__ import annotations

import json
from typing import Optional

from app.acesso.sqs_gateway import MensagemRecebida
from app.consumidores.monitor_dlq import (
    CONSUMER_ID_MONITOR_DLQ,
    FILA_PRINCIPAL_POR_DLQ,
    MonitorDlq,
)
from app.dominio import constantes as c
from app.dominio.evento import Evento
from app.dominio.state_deriver import derivar_estado
from app.dominio.tipos import Estado, EventSource, EventType


# ---------------------------------------------------------------------------
# Dublês (fakes) das dependências injetáveis
# ---------------------------------------------------------------------------
class HistoricoFake:
    """Fake de HistoricoRepository que acumula eventos e consulta por pedido."""

    def __init__(self) -> None:
        self.eventos: list[Evento] = []

    def registrar_evento(self, evento: Evento) -> None:
        self.eventos.append(evento)

    def consultar_por_pedido(self, order_id: str) -> list[Evento]:
        return [e for e in self.eventos if e.order_id == order_id]


class SqsFake:
    """Fake de SqsGateway que entrega, a cada DLQ, sempre o mesmo lote.

    Diferentemente de um consumidor real (que exclui a mensagem), o monitor NÃO
    exclui mensagens da DLQ; portanto este fake devolve o mesmo lote em toda chamada,
    simulando a reaparição da mensagem após o Visibility_Timeout. Isso permite testar
    a idempotência do registro em varreduras sucessivas.
    """

    def __init__(self, mensagens_por_dlq: dict[str, list[MensagemRecebida]]) -> None:
        self._mensagens_por_dlq = mensagens_por_dlq
        self.chamadas_receive: list[dict[str, object]] = []
        self.exclusoes: list[tuple[str, str]] = []

    def receive_messages(
        self,
        nome_fila: str,
        *,
        max_mensagens: int,
        wait_time_seconds: int,
        visibility_timeout: Optional[int] = None,
    ) -> list[MensagemRecebida]:
        self.chamadas_receive.append(
            {
                "nome_fila": nome_fila,
                "max_mensagens": max_mensagens,
                "wait_time_seconds": wait_time_seconds,
            }
        )
        # Lista nova a cada chamada (a mensagem "reaparece"); nunca é excluída.
        return list(self._mensagens_por_dlq.get(nome_fila, []))

    def delete_message(self, nome_fila: str, receipt_handle: str) -> None:
        # Registrado apenas para garantir, nos testes, que o monitor NÃO exclui.
        self.exclusoes.append((nome_fila, receipt_handle))


def _mensagem_dlq(
    order_id: str,
    *,
    processing_seconds: int = 10,
    cenario: str = "falha_processamento",
    receive_count: int = 3,
    message_id: str = "dlq-msg-1",
    message_group_id: Optional[str] = None,
) -> MensagemRecebida:
    """Monta uma MensagemRecebida com o corpo JSON do contrato do produtor."""
    corpo = json.dumps(
        {
            "order_id": order_id,
            "processing_seconds": processing_seconds,
            "cenario": cenario,
        }
    )
    return MensagemRecebida(
        message_id=message_id,
        receipt_handle=f"rh-{message_id}",
        body=corpo,
        approximate_receive_count=receive_count,
        attributes={"ApproximateReceiveCount": str(receive_count)},
        message_attributes={},
        message_group_id=message_group_id,
    )


# ---------------------------------------------------------------------------
# (a) mensagem presente na DLQ -> registra 1 evento OBSERVADO na fila principal
# ---------------------------------------------------------------------------
def test_registra_encaminhada_dlq_observado_na_fila_principal() -> None:
    order_id = "teste2-visibility-0001"
    sqs = SqsFake(
        {c.FILA_STANDARD_DLQ: [_mensagem_dlq(order_id, message_id="m-1")]}
    )
    historico = HistoricoFake()
    monitor = MonitorDlq(sqs=sqs, historico=historico)

    registrados = monitor.varrer_uma_vez()

    assert registrados == 1
    eventos = historico.consultar_por_pedido(order_id)
    assert len(eventos) == 1

    evento = eventos[0]
    assert evento.event_type is EventType.ENCAMINHADA_DLQ
    assert evento.event_source is EventSource.OBSERVADO
    # Associado à fila PRINCIPAL (não à DLQ), para agregar junto do histórico.
    assert evento.queue_name == c.FILA_STANDARD
    assert FILA_PRINCIPAL_POR_DLQ[c.FILA_STANDARD_DLQ] == c.FILA_STANDARD
    # receive_count satisfaz a condição do StateDeriver (>= maxReceiveCount).
    assert evento.receive_count is not None
    assert evento.receive_count >= c.MAX_RECEIVE_COUNT
    # order_id extraído do corpo JSON; metadados preservados.
    assert evento.order_id == order_id
    assert evento.consumer_id == CONSUMER_ID_MONITOR_DLQ
    assert evento.sqs_message_id == "m-1"
    # Nunca exclui a mensagem da DLQ (restrição de não apagar mensagens).
    assert sqs.exclusoes == []


# ---------------------------------------------------------------------------
# (b) varrer duas vezes a mesma DLQ/mensagem -> não duplica (idempotência)
# ---------------------------------------------------------------------------
def test_idempotente_entre_varreduras_sucessivas() -> None:
    order_id = "pedido-dlq-idem"
    sqs = SqsFake(
        {c.FILA_FIFO_DLQ: [_mensagem_dlq(order_id, message_id="m-2", message_group_id="g1")]}
    )
    historico = HistoricoFake()
    monitor = MonitorDlq(sqs=sqs, historico=historico)

    primeira = monitor.varrer_uma_vez()
    segunda = monitor.varrer_uma_vez()

    assert primeira == 1
    assert segunda == 0  # a mensagem reaparece, mas o evento não é duplicado.
    eventos = historico.consultar_por_pedido(order_id)
    assert len(eventos) == 1
    assert eventos[0].queue_name == c.FILA_FIFO
    assert eventos[0].message_group_id == "g1"


# ---------------------------------------------------------------------------
# (c) DLQ vazia -> nenhum evento
# ---------------------------------------------------------------------------
def test_dlq_vazia_nao_registra_evento() -> None:
    sqs = SqsFake({c.FILA_STANDARD_DLQ: [], c.FILA_FIFO_DLQ: []})
    historico = HistoricoFake()
    monitor = MonitorDlq(sqs=sqs, historico=historico)

    registrados = monitor.varrer_uma_vez()

    assert registrados == 0
    assert historico.eventos == []


# ---------------------------------------------------------------------------
# (d) após o registro, derivar_estado -> Estado.ENCAMINHADA_DLQ
# ---------------------------------------------------------------------------
def test_estado_derivado_apos_registro_e_encaminhada_dlq() -> None:
    order_id = "pedido-dlq-estado"
    sqs = SqsFake({c.FILA_STANDARD_DLQ: [_mensagem_dlq(order_id, message_id="m-3")]})
    historico = HistoricoFake()
    monitor = MonitorDlq(sqs=sqs, historico=historico)

    monitor.varrer_uma_vez()

    eventos = historico.consultar_por_pedido(order_id)
    estado = derivar_estado(eventos, max_receive_count=c.MAX_RECEIVE_COUNT)
    assert estado is Estado.ENCAMINHADA_DLQ


# ---------------------------------------------------------------------------
# Tolerância a falhas: erro ao ler uma DLQ não derruba as demais
# ---------------------------------------------------------------------------
def test_falha_em_uma_dlq_nao_impede_as_demais() -> None:
    order_id = "pedido-ok"

    class SqsComFalha(SqsFake):
        def receive_messages(self, nome_fila: str, **kwargs):  # type: ignore[override]
            if nome_fila == c.FILA_STANDARD_DLQ:
                raise RuntimeError("falha simulada de leitura da DLQ")
            return super().receive_messages(nome_fila, **kwargs)

    sqs = SqsComFalha(
        {c.FILA_FIFO_DLQ: [_mensagem_dlq(order_id, message_id="m-4", message_group_id="g")]}
    )
    historico = HistoricoFake()
    monitor = MonitorDlq(sqs=sqs, historico=historico)

    registrados = monitor.varrer_uma_vez()

    # A DLQ Standard falhou, mas a FIFO foi varrida normalmente.
    assert registrados == 1
    assert len(historico.consultar_por_pedido(order_id)) == 1
