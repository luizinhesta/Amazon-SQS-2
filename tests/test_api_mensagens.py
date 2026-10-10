"""Testes unitários do endpoint de envio de mensagens da API (Tarefa 15.2).

Validam, com ``TestClient`` do FastAPI (httpx) e dependências injetadas por fakes
(sem AWS real nem ``time.sleep``), o endpoint ``POST /api/mensagens``:

- envio bem-sucedido registra **exatamente um** Evento_Observado ``enviada`` por
  mensagem, com o ``order_id`` correspondente (Req. 4.10, 4.11 / Property 9);
- payload inválido é rejeitado, sem nenhum envio, apontando o campo inválido em
  pt-BR e o índice da mensagem (Req. 4.9);
- falha parcial preserva os envios anteriores (registrados como ``enviada``) e o
  erro original da AWS no item que falhou (Req. 4.12 / Property 10).

O ``SqsGateway`` recebe um cliente SQS simulado (``MagicMock``); o
``HistoricoRepository`` é substituído por um fake em memória que captura os eventos
registrados, de modo que os testes não dependam da AWS real.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from app.acesso.sqs_gateway import SqsGateway
from app.api.app import create_app
from app.api.estado_simulacao import EstadoConfigSimulacao
from app.dominio import constantes as c
from app.dominio.evento import Evento
from app.dominio.tipos import EventSource, EventType


class _HistoricoFake:
    """Fake em memória do HistoricoRepository: captura os eventos registrados.

    Expõe a mesma assinatura de ``registrar_evento`` usada pelo serviço de envio.
    Não acessa a AWS; apenas acumula os eventos para inspeção nos testes.
    """

    def __init__(self) -> None:
        self.eventos: list[Evento] = []

    def registrar_evento(self, evento: Evento) -> None:
        self.eventos.append(evento)


class _ErroAwsSimulado(Exception):
    """Erro que imita uma exceção do botocore (ex.: ClientError) do SendMessage."""


def _cliente_sqs_ok() -> MagicMock:
    """Cliente SQS simulado: resolve URLs e aceita todos os SendMessage."""
    cliente = MagicMock()
    cliente.get_queue_url.side_effect = lambda QueueName: {
        "QueueUrl": f"https://sqs.us-east-1.amazonaws.com/123456789012/{QueueName}"
    }
    contador = {"n": 0}

    def _send_message(**kwargs):
        contador["n"] += 1
        return {"MessageId": f"msg-{contador['n']:04d}"}

    cliente.send_message.side_effect = _send_message
    return cliente


def _criar_cliente(
    *,
    cliente_sqs=None,
    historico=None,
    estado_simulacao: EstadoConfigSimulacao | None = None,
) -> tuple[TestClient, _HistoricoFake]:
    """Monta um TestClient com dependências injetadas (sem AWS real)."""
    gateway = SqsGateway(cliente_sqs=cliente_sqs or _cliente_sqs_ok())
    hist = historico or _HistoricoFake()
    app = create_app(
        gateway=gateway,
        verificador_tabela=MagicMock(return_value={"Table": {}}),
        estado_simulacao=estado_simulacao or EstadoConfigSimulacao(),
        historico=hist,
    )
    return TestClient(app), hist


# --- Envio bem-sucedido (Property 9) -----------------------------------------


def test_envio_standard_registra_um_evento_enviada_por_mensagem() -> None:
    cliente, historico = _criar_cliente()

    resposta = cliente.post(
        "/api/mensagens",
        json={
            "tipo_fila": "standard",
            "mensagens": [
                {"order_id": "pedido-0001"},
                {"order_id": "pedido-0002"},
                {"order_id": "pedido-0003"},
            ],
        },
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["aceito"] is True
    assert corpo["houve_falha"] is False
    assert corpo["total_solicitadas"] == 3
    assert corpo["total_enviadas"] == 3
    assert corpo["nome_fila"] == c.FILA_STANDARD
    assert all(item["enviada"] for item in corpo["resultados"])

    # Exatamente um Evento_Observado 'enviada' por mensagem (Property 9, Req. 4.11).
    enviados = [e for e in historico.eventos if e.event_type is EventType.ENVIADA]
    assert len(enviados) == 3
    assert all(e.event_source is EventSource.OBSERVADO for e in enviados)
    assert [e.order_id for e in enviados] == [
        "pedido-0001",
        "pedido-0002",
        "pedido-0003",
    ]
    # Cada evento tem o nome da fila e o MessageId retornado pelo SQS.
    assert all(e.queue_name == c.FILA_STANDARD for e in enviados)
    assert all(e.sqs_message_id for e in enviados)


def test_envio_standard_poe_order_id_no_corpo_da_mensagem() -> None:
    cliente_sqs = _cliente_sqs_ok()
    cliente, _ = _criar_cliente(cliente_sqs=cliente_sqs)

    cliente.post(
        "/api/mensagens",
        json={
            "tipo_fila": "standard",
            "mensagens": [{"order_id": "pedido-xyz", "cenario_falha": "encaminhar_dlq"}],
        },
    )

    # O order_id (e os parâmetros de simulação) vão no corpo da mensagem (Req. 4.10).
    _, kwargs = cliente_sqs.send_message.call_args
    corpo = json.loads(kwargs["MessageBody"])
    assert corpo["order_id"] == "pedido-xyz"
    assert corpo["cenario"] == "encaminhar_dlq"


def test_envio_fifo_repassa_message_group_id() -> None:
    cliente_sqs = _cliente_sqs_ok()
    cliente, historico = _criar_cliente(cliente_sqs=cliente_sqs)

    resposta = cliente.post(
        "/api/mensagens",
        json={
            "tipo_fila": "fifo",
            "deduplicacao_por_conteudo": True,
            "mensagens": [
                {"order_id": "pedido-f1", "message_group_id": "grupo-A"},
            ],
        },
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["aceito"] is True
    assert corpo["nome_fila"] == c.FILA_FIFO
    # Com deduplicação por conteúdo habilitada, não se envia MessageDeduplicationId.
    _, kwargs = cliente_sqs.send_message.call_args
    assert kwargs.get("MessageGroupId") == "grupo-A"
    assert "MessageDeduplicationId" not in kwargs
    # O evento registrado carrega o MessageGroupId (apenas FIFO).
    enviados = [e for e in historico.eventos if e.event_type is EventType.ENVIADA]
    assert enviados[0].message_group_id == "grupo-A"


def test_envio_fifo_sem_dedup_por_conteudo_exige_e_repassa_deduplication_id() -> None:
    cliente_sqs = _cliente_sqs_ok()
    cliente, _ = _criar_cliente(cliente_sqs=cliente_sqs)

    resposta = cliente.post(
        "/api/mensagens",
        json={
            "tipo_fila": "fifo",
            "deduplicacao_por_conteudo": False,
            "mensagens": [
                {
                    "order_id": "pedido-f2",
                    "message_group_id": "grupo-B",
                    "message_deduplication_id": "dedup-123",
                }
            ],
        },
    )

    assert resposta.status_code == 200
    assert resposta.json()["aceito"] is True
    _, kwargs = cliente_sqs.send_message.call_args
    assert kwargs.get("MessageDeduplicationId") == "dedup-123"


# --- Payload inválido (Req. 4.9) ---------------------------------------------


def test_order_id_vazio_e_rejeitado_com_campo_em_pt_br_e_sem_envio() -> None:
    cliente_sqs = _cliente_sqs_ok()
    cliente, historico = _criar_cliente(cliente_sqs=cliente_sqs)

    resposta = cliente.post(
        "/api/mensagens",
        json={
            "tipo_fila": "standard",
            "mensagens": [
                {"order_id": "pedido-ok"},
                {"order_id": ""},  # inválido: 0 caracteres
            ],
        },
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["aceito"] is False
    assert corpo["total_enviadas"] == 0
    # Aponta o campo inválido em pt-BR e o índice da mensagem (Req. 4.9).
    invalidos = corpo["campos_invalidos"]
    assert any(ci["campo"] == "order_id" and ci["indice_mensagem"] == 1 for ci in invalidos)
    assert any("identificador do pedido" in ci["mensagem"] for ci in invalidos)
    # Nenhuma mensagem foi enviada quando o payload é inválido (Req. 4.9).
    cliente_sqs.send_message.assert_not_called()
    assert historico.eventos == []


def test_fifo_sem_message_group_id_e_rejeitado() -> None:
    cliente_sqs = _cliente_sqs_ok()
    cliente, _ = _criar_cliente(cliente_sqs=cliente_sqs)

    resposta = cliente.post(
        "/api/mensagens",
        json={
            "tipo_fila": "fifo",
            "mensagens": [{"order_id": "pedido-f3"}],  # falta message_group_id
        },
    )

    corpo = resposta.json()
    assert corpo["aceito"] is False
    assert any(ci["campo"] == "message_group_id" for ci in corpo["campos_invalidos"])
    cliente_sqs.send_message.assert_not_called()


def test_lote_vazio_e_rejeitado_por_quantidade() -> None:
    cliente, _ = _criar_cliente()

    resposta = cliente.post(
        "/api/mensagens",
        json={"tipo_fila": "standard", "mensagens": []},
    )

    corpo = resposta.json()
    assert corpo["aceito"] is False
    assert any(ci["campo"] == "quantidade" for ci in corpo["campos_invalidos"])


def test_tempo_processamento_fora_da_faixa_e_rejeitado() -> None:
    cliente, _ = _criar_cliente()

    resposta = cliente.post(
        "/api/mensagens",
        json={
            "tipo_fila": "standard",
            "mensagens": [
                {"order_id": "pedido-t1", "tempo_processamento_simulado_segundos": 999}
            ],
        },
    )

    corpo = resposta.json()
    assert corpo["aceito"] is False
    assert any(
        ci["campo"] == "tempo_processamento_simulado"
        for ci in corpo["campos_invalidos"]
    )


# --- Falha parcial (Property 10) ---------------------------------------------


def test_falha_parcial_preserva_envios_anteriores_e_erro_original() -> None:
    # O SQS envia com sucesso as duas primeiras e falha na terceira (posição k=2).
    cliente_sqs = _cliente_sqs_ok()
    chamadas = {"n": 0}

    def _send_message(**kwargs):
        chamadas["n"] += 1
        if chamadas["n"] == 3:
            raise _ErroAwsSimulado("AWS.SimpleQueueService.QueueDoesNotExist")
        return {"MessageId": f"msg-{chamadas['n']:04d}"}

    cliente_sqs.send_message.side_effect = _send_message
    cliente, historico = _criar_cliente(cliente_sqs=cliente_sqs)

    resposta = cliente.post(
        "/api/mensagens",
        json={
            "tipo_fila": "standard",
            "mensagens": [
                {"order_id": "pedido-A"},
                {"order_id": "pedido-B"},
                {"order_id": "pedido-C"},  # falha aqui
                {"order_id": "pedido-D"},  # não deve ser tentada
            ],
        },
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["aceito"] is True
    assert corpo["houve_falha"] is True
    assert corpo["total_solicitadas"] == 4
    assert corpo["total_enviadas"] == 2

    # Os envios anteriores à falha permanecem registrados como 'enviada' (Property 10).
    enviados = [e for e in historico.eventos if e.event_type is EventType.ENVIADA]
    assert [e.order_id for e in enviados] == ["pedido-A", "pedido-B"]

    # O item que falhou preserva o erro original da AWS (Req. 4.12).
    resultados = corpo["resultados"]
    assert len(resultados) == 3  # A, B e o C que falhou; D não foi tentada
    assert resultados[0]["enviada"] is True
    assert resultados[1]["enviada"] is True
    item_falho = resultados[2]
    assert item_falho["enviada"] is False
    assert item_falho["order_id"] == "pedido-C"
    assert "QueueDoesNotExist" in item_falho["detalhe_erro"]

    # Após a falha, o lote é interrompido: o SQS não é chamado para 'pedido-D'.
    assert cliente_sqs.send_message.call_count == 3
