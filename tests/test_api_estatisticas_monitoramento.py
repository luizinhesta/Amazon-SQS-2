"""Testes unitários dos endpoints de estatísticas e monitoramento (Tarefa 15.5).

Validam, com ``TestClient`` do FastAPI (httpx) e um ``HistoricoRepository`` fake em
memória (sem AWS real nem ``time.sleep`` de 45 s), os endpoints:

- ``GET /api/estatisticas``: contadores não-negativos, particionados e rotulados por
  fluxo Standard/FIFO (Req. 3.1-3.8, 3.12);
- ``GET /api/monitoramento``: uma linha por mensagem com o Estado derivado correto e
  as colunas ID, Fila, Consumidor, Tentativas, Processamento, Estado e Último evento
  (Req. 5.1-5.5, 5.9); estado vazio com mensagem em pt-BR (Req. 5.8);
- caminho de timeout de ambos os endpoints, forçado de forma determinística (sem
  esperar 3 s reais), retornando a indicação de timeout (Req. 14.3, 14.4).

O ``HistoricoRepository`` fake expõe ``consultar_por_fila`` devolvendo os eventos de
cada fila a partir de uma lista em memória, exatamente como o serviço espera.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from app.api.app import create_app
from app.api.esquemas import MENSAGEM_SEM_MENSAGENS_MONITORADAS
from app.api import servico_monitoramento as servico
from app.dominio import constantes as c
from app.dominio.evento import Evento
from app.dominio.tipos import Estado, EventSource, EventType


# ---------------------------------------------------------------------------
# Fake em memória do HistoricoRepository
# ---------------------------------------------------------------------------
class _HistoricoFake:
    """Fake de HistoricoRepository: ``consultar_por_fila`` lê de uma lista em memória.

    Os eventos são indexados por ``queue_name`` no construtor, de modo que
    ``consultar_por_fila`` devolve apenas os eventos daquela fila (como o GSI1 real).
    """

    def __init__(self, eventos: list[Evento] | None = None) -> None:
        self.eventos: list[Evento] = list(eventos or [])

    def consultar_por_fila(self, queue_name: str) -> list[Evento]:
        return [e for e in self.eventos if e.queue_name == queue_name]


def _criar_cliente(historico: _HistoricoFake) -> TestClient:
    """Monta um TestClient com o histórico fake injetado (sem AWS real)."""
    app = create_app(
        gateway=MagicMock(),
        verificador_tabela=MagicMock(return_value={"Table": {}}),
        historico=historico,
    )
    return TestClient(app)


# ---------------------------------------------------------------------------
# Construção de eventos de teste
# ---------------------------------------------------------------------------
_ms = {"n": 0}


def _evento(
    *,
    order_id: str,
    queue_name: str,
    event_type: EventType,
    event_source: EventSource = EventSource.OBSERVADO,
    receive_count: int | None = None,
    consumer_id: str | None = None,
    processing_seconds: int | None = None,
    seq: int = 1,
) -> Evento:
    """Cria um Evento com um app_time_ms monotônico (ordem de registro estável)."""
    _ms["n"] += 1
    app_time_ms = _ms["n"]
    app_time = f"2024-06-25T12:00:00.{app_time_ms:03d}Z"
    return Evento(
        order_id=order_id,
        queue_name=queue_name,
        event_type=event_type,
        event_source=event_source,
        app_time=app_time,
        app_time_ms=app_time_ms,
        seq=seq,
        receive_count=receive_count,
        consumer_id=consumer_id,
        processing_seconds=processing_seconds,
    )


def _mensagem_processada(order_id: str, consumidor: str, tempo: int = 10) -> list[Evento]:
    """Fluxo feliz Standard: enviada -> recebida -> processamento -> processada."""
    return [
        _evento(
            order_id=order_id,
            queue_name=c.FILA_STANDARD,
            event_type=EventType.ENVIADA,
            processing_seconds=tempo,
        ),
        _evento(
            order_id=order_id,
            queue_name=c.FILA_STANDARD,
            event_type=EventType.RECEBIDA,
            receive_count=1,
            consumer_id=consumidor,
        ),
        _evento(
            order_id=order_id,
            queue_name=c.FILA_STANDARD,
            event_type=EventType.PROCESSAMENTO_INICIADO,
            consumer_id=consumidor,
        ),
        _evento(
            order_id=order_id,
            queue_name=c.FILA_STANDARD,
            event_type=EventType.PROCESSADA,
            consumer_id=consumidor,
        ),
    ]


# ===========================================================================
# GET /api/estatisticas
# ===========================================================================
def test_estatisticas_particiona_por_fluxo_e_conta_enviadas() -> None:
    eventos = [
        _evento(order_id="s1", queue_name=c.FILA_STANDARD, event_type=EventType.ENVIADA),
        _evento(order_id="s2", queue_name=c.FILA_STANDARD, event_type=EventType.ENVIADA),
        _evento(order_id="f1", queue_name=c.FILA_FIFO, event_type=EventType.ENVIADA),
    ]
    cliente = _criar_cliente(_HistoricoFake(eventos))

    corpo = cliente.get("/api/estatisticas").json()

    assert corpo["timeout"] is False
    # Rotulados e separados por fluxo (Req. 3.8).
    assert corpo["standard"]["fluxo"] == "standard"
    assert corpo["fifo"]["fluxo"] == "fifo"
    assert corpo["standard"]["enviadas"] == 2
    assert corpo["fifo"]["enviadas"] == 1


def test_estatisticas_sao_inteiros_nao_negativos() -> None:
    eventos = _mensagem_processada("s1", "consumidor-1")
    eventos += [
        _evento(order_id="f1", queue_name=c.FILA_FIFO, event_type=EventType.ENVIADA),
    ]
    cliente = _criar_cliente(_HistoricoFake(eventos))

    corpo = cliente.get("/api/estatisticas").json()

    for fluxo in ("standard", "fifo"):
        bloco = corpo[fluxo]
        for campo in (
            "enviadas",
            "disponiveis",
            "em_processamento",
            "processadas",
            "com_falha",
            "encaminhadas_dlq",
        ):
            assert isinstance(bloco[campo], int)
            assert bloco[campo] >= 0
        for item in bloco["recebidas_por_consumidor"]:
            assert item["recebidas"] >= 0


def test_estatisticas_conta_processadas_e_recebidas_por_consumidor() -> None:
    eventos = _mensagem_processada("s1", "consumidor-1")
    cliente = _criar_cliente(_HistoricoFake(eventos))

    corpo = cliente.get("/api/estatisticas").json()

    assert corpo["standard"]["processadas"] == 1
    recebidas = {
        r["consumidor"]: r["recebidas"]
        for r in corpo["standard"]["recebidas_por_consumidor"]
    }
    assert recebidas == {"consumidor-1": 1}


def test_estatisticas_log_vazio_retorna_zeros() -> None:
    cliente = _criar_cliente(_HistoricoFake([]))

    corpo = cliente.get("/api/estatisticas").json()

    assert corpo["timeout"] is False
    assert corpo["standard"]["enviadas"] == 0
    assert corpo["fifo"]["enviadas"] == 0
    assert corpo["standard"]["recebidas_por_consumidor"] == []


def test_estatisticas_timeout_retorna_indicacao_sem_dados() -> None:
    # Força o caminho de timeout com um limite <= 0 (determinístico, sem esperar 3 s).
    resposta = servico.obter_estatisticas(
        _HistoricoFake([]),
        timeout_segundos=0,
        calcular=lambda: _bloquear_para_sempre(),
    )

    assert resposta.timeout is True
    assert resposta.standard is None
    assert resposta.fifo is None
    assert "Tempo limite excedido" in resposta.mensagem


# ===========================================================================
# GET /api/monitoramento
# ===========================================================================
def test_monitoramento_colunas_corretas() -> None:
    cliente = _criar_cliente(_HistoricoFake([]))

    corpo = cliente.get("/api/monitoramento").json()

    assert corpo["colunas"] == [
        "ID",
        "Fila",
        "Consumidor",
        "Tentativas",
        "Processamento",
        "Estado",
        "Último evento",
    ]


def test_monitoramento_estado_vazio_mensagem_pt_br() -> None:
    cliente = _criar_cliente(_HistoricoFake([]))

    corpo = cliente.get("/api/monitoramento").json()

    assert corpo["timeout"] is False
    assert corpo["vazio"] is True
    assert corpo["linhas"] == []
    assert corpo["mensagem_vazia"] == MENSAGEM_SEM_MENSAGENS_MONITORADAS


def test_monitoramento_linha_com_estado_derivado_processada() -> None:
    eventos = _mensagem_processada("pedido-1", "consumidor-1", tempo=12)
    cliente = _criar_cliente(_HistoricoFake(eventos))

    corpo = cliente.get("/api/monitoramento").json()

    assert corpo["vazio"] is False
    assert len(corpo["linhas"]) == 1
    linha = corpo["linhas"][0]
    assert linha["id"] == "pedido-1"
    assert linha["fila"] == c.FILA_STANDARD
    assert linha["consumidor"] == "consumidor-1"
    assert linha["tentativas"] == 1
    assert linha["processamento_segundos"] == 12
    # Estado derivado correto (Req. 5.4, 5.9).
    assert linha["estado"] == Estado.PROCESSADA.value
    assert linha["ultimo_evento"] is not None


def test_monitoramento_estado_dlq_encaminhada() -> None:
    # enviada -> recebida (rc=1) -> falha -> recebida (rc=2) -> encaminhada_dlq (rc=2).
    oid = "pedido-dlq"
    eventos = [
        _evento(order_id=oid, queue_name=c.FILA_STANDARD, event_type=EventType.ENVIADA),
        _evento(
            order_id=oid,
            queue_name=c.FILA_STANDARD,
            event_type=EventType.RECEBIDA,
            receive_count=1,
            consumer_id="consumidor-1",
        ),
        _evento(
            order_id=oid,
            queue_name=c.FILA_STANDARD,
            event_type=EventType.FALHA,
            receive_count=1,
            consumer_id="consumidor-1",
        ),
        _evento(
            order_id=oid,
            queue_name=c.FILA_STANDARD,
            event_type=EventType.RECEBIDA,
            receive_count=2,
            consumer_id="consumidor-1",
        ),
        _evento(
            order_id=oid,
            queue_name=c.FILA_STANDARD_DLQ,
            event_type=EventType.ENCAMINHADA_DLQ,
            event_source=EventSource.OBSERVADO,
            receive_count=2,
        ),
    ]
    cliente = _criar_cliente(_HistoricoFake(eventos))

    corpo = cliente.get("/api/monitoramento").json()

    linha = next(l for l in corpo["linhas"] if l["id"] == oid)
    assert linha["estado"] == Estado.ENCAMINHADA_DLQ.value
    assert linha["tentativas"] == 2
    # A estatística do fluxo contabiliza a DLQ no fluxo Standard de origem (Req. 3.8).
    est = cliente.get("/api/estatisticas").json()
    assert est["standard"]["encaminhadas_dlq"] == 1


def test_monitoramento_estado_aguardando_para_apenas_enviada() -> None:
    eventos = [
        _evento(order_id="novo", queue_name=c.FILA_FIFO, event_type=EventType.ENVIADA),
    ]
    cliente = _criar_cliente(_HistoricoFake(eventos))

    corpo = cliente.get("/api/monitoramento").json()

    linha = corpo["linhas"][0]
    assert linha["estado"] == Estado.AGUARDANDO.value
    assert linha["tentativas"] == 0
    assert linha["consumidor"] is None


def test_monitoramento_timeout_retorna_indicacao_sem_linhas() -> None:
    resposta = servico.obter_monitoramento(
        _HistoricoFake([]),
        timeout_segundos=0,
        calcular=lambda: _bloquear_para_sempre(),
    )

    assert resposta.timeout is True
    assert resposta.linhas == []
    assert resposta.vazio is False
    assert "Tempo limite excedido" in resposta.mensagem


# ---------------------------------------------------------------------------
# Auxiliar: bloqueia por um tempo para forçar o caminho de timeout
# ---------------------------------------------------------------------------
def _bloquear_para_sempre():
    """Simula um cálculo lento o suficiente para estourar o timeout de 0 s.

    Não usa ``time.sleep`` longo: com ``timeout_segundos=0`` o
    ``executar_com_timeout`` já expira antes de a função concluir. Um pequeno sleep
    garante que a thread de trabalho não termine antes da verificação do timeout.
    """
    import time

    time.sleep(0.5)
    raise AssertionError("resultado descartado pelo timeout")
