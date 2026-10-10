"""Testes leves da composição da aplicação na EC2 (Tarefa 18.1).

Validam, com dependências injetadas por fakes (sem AWS real nem threads vivas), as
três responsabilidades da composição em :mod:`app.main`:

- o Dashboard estático (``web/``) é servido quando a verificação de inicialização é
  bem-sucedida (Req. 8.1);
- a disponibilização do Dashboard é **condicionada** à verificação: quando um recurso
  AWS está ausente/inacessível, o Dashboard não fica disponível normalmente e a causa
  é exposta em pt-BR, identificando o recurso por nome e tipo (Req. 1.6, 1.8);
- os consumidores Standard e FIFO são iniciados junto com a aplicação quando a
  verificação tem sucesso, e **não** são iniciados quando ela falha (Req. 8.1, 8.2,
  10.1, 10.2).

Nenhum teste sobe threads reais de consumidor contra a AWS: usamos uma fábrica de
gerenciador falsa que apenas registra a chamada de ``iniciar``/``parar``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from app.acesso.sqs_gateway import SqsGateway
from app.api.estado_instancias import EstadoInstanciasConsumidores
from app.main import GerenciadorConsumidores, compor_aplicacao
from app.dominio import constantes as c


class _ErroAwsSimulado(Exception):
    """Erro que imita uma exceção do botocore (ex.: ClientError)."""


def _cliente_sqs_ok() -> MagicMock:
    """Cliente SQS simulado em que todas as filas existem e são acessíveis."""
    cliente = MagicMock()
    cliente.get_queue_url.side_effect = lambda QueueName: {
        "QueueUrl": f"https://sqs.us-east-1.amazonaws.com/123456789012/{QueueName}"
    }
    cliente.get_queue_attributes.return_value = {
        "Attributes": {"SqsManagedSseEnabled": "true"}
    }
    return cliente


def _verificador_tabela_ok() -> MagicMock:
    """Verificador de tabela simulado que confirma a existência (describe)."""
    return MagicMock(return_value={"Table": {"TableStatus": "ACTIVE"}})


class _GerenciadorFalso:
    """Dublê do GerenciadorConsumidores: registra iniciar/parar sem threads reais."""

    def __init__(self) -> None:
        self.iniciado = 0
        self.parado = 0

    def iniciar(self) -> None:
        self.iniciado += 1

    def parar(self) -> None:
        self.parado += 1


def _compor(*, cliente_sqs=None, verificador_tabela=None, **kwargs):
    gateway = SqsGateway(cliente_sqs=cliente_sqs or _cliente_sqs_ok())
    return compor_aplicacao(
        gateway=gateway,
        verificador_tabela=verificador_tabela or _verificador_tabela_ok(),
        historico=MagicMock(),
        **kwargs,
    )


# --- Verificação bem-sucedida: Dashboard disponível + consumidores iniciados ---


def test_dashboard_servido_quando_verificacao_ok() -> None:
    app = _compor(iniciar_consumidores=False)

    assert app.state.resultado_verificacao.sucesso is True

    cliente = TestClient(app)
    # A raiz serve o index.html do Dashboard (StaticFiles com html=True).
    resposta = cliente.get("/")
    assert resposta.status_code == 200
    assert "Laboratório Amazon SQS" in resposta.text
    # A API continua respondendo sob /api (precedência sobre os estáticos).
    assert cliente.get("/api/saude").json()["saudavel"] is True


def test_consumidores_iniciam_e_param_no_ciclo_de_vida_quando_ok() -> None:
    gerenciador = _GerenciadorFalso()
    app = _compor(fabrica_gerenciador=lambda g, h, e: gerenciador)

    # Startup/shutdown são disparados pelo context manager do TestClient.
    with TestClient(app):
        assert gerenciador.iniciado == 1
        assert gerenciador.parado == 0
    assert gerenciador.parado == 1


# --- Verificação falha: Dashboard indisponível + consumidores NÃO iniciados ---


def test_dashboard_indisponivel_quando_recurso_ausente() -> None:
    cliente_sqs = _cliente_sqs_ok()
    cliente_sqs.get_queue_attributes.side_effect = _ErroAwsSimulado(
        "AWS.SimpleQueueService.NonExistentQueue"
    )

    app = _compor(cliente_sqs=cliente_sqs)
    assert app.state.resultado_verificacao.sucesso is False
    # Nenhum gerenciador de consumidores foi criado (não iniciamos consumidores).
    assert app.state.gerenciador_consumidores is None

    cliente = TestClient(app)
    resposta = cliente.get("/")
    assert resposta.status_code == 503
    corpo = resposta.json()
    # Causa em pt-BR identificando o recurso indisponível por nome e tipo (Req. 1.8).
    assert c.FILA_STANDARD in corpo["detail"]
    assert "fila SQS" in corpo["detail"]
    assert corpo["recurso_indisponivel"]["nome"] == c.FILA_STANDARD
    assert corpo["recurso_indisponivel"]["tipo"] == "fila SQS"

    # A API de saúde segue inspecionável (saudavel=False).
    assert cliente.get("/api/saude").json()["saudavel"] is False


def test_consumidores_nao_iniciam_quando_verificacao_falha() -> None:
    verificador = MagicMock(side_effect=_ErroAwsSimulado("ResourceNotFoundException"))
    gerenciador = _GerenciadorFalso()

    app = _compor(
        verificador_tabela=verificador,
        fabrica_gerenciador=lambda g, h, e: gerenciador,
    )

    with TestClient(app):
        pass

    assert gerenciador.iniciado == 0
    assert gerenciador.parado == 0


# --- GerenciadorConsumidores: inicia o número de instâncias configurado ---


def test_gerenciador_inicia_threads_por_instancia_configurada() -> None:
    estado = EstadoInstanciasConsumidores(standard=2, fifo=1)
    gateway = MagicMock()
    # receive_messages sempre vazio: os laços não processam nada e param ao sinal.
    gateway.receive_messages.return_value = []

    gerenciador = GerenciadorConsumidores(
        gateway=gateway,
        historico=MagicMock(),
        estado_instancias=estado,
    )
    try:
        gerenciador.iniciar()
        # 2 Standard + 1 FIFO = 3 threads de consumidor (Req. 10.1, 10.2).
        assert gerenciador.quantidade_threads == 3
    finally:
        gerenciador.parar(timeout=2.0)
    assert gerenciador.quantidade_threads == 0
