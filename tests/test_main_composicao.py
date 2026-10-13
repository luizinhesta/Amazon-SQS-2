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
from app.main import (
    ENV_LIBERAR_TODAS_ORIGENS,
    ENV_ORIGENS_AUTORIZADAS,
    ENV_SENHA,
    ENV_USUARIO,
    GerenciadorConsumidores,
    compor_aplicacao,
)
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
    """Dublê de gerenciador (consumidores/monitor): registra iniciar/parar sem threads."""

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
    monitor = _GerenciadorFalso()
    app = _compor(
        fabrica_gerenciador=lambda g, h, e: gerenciador,
        fabrica_monitor_dlq=lambda g, h: monitor,
    )

    # Startup/shutdown são disparados pelo context manager do TestClient.
    with TestClient(app):
        assert gerenciador.iniciado == 1
        assert gerenciador.parado == 0
        # O monitor de DLQ segue o mesmo ciclo de vida dos consumidores.
        assert monitor.iniciado == 1
        assert monitor.parado == 0
    assert gerenciador.parado == 1
    assert monitor.parado == 1


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


# --- Autenticação configurada por variáveis de ambiente (Req. 12.5, 12.7) ---


def _limpar_envs_autenticacao(monkeypatch) -> None:
    """Remove as envs de autenticação para isolar cada cenário."""
    for env in (
        ENV_ORIGENS_AUTORIZADAS,
        ENV_LIBERAR_TODAS_ORIGENS,
        ENV_USUARIO,
        ENV_SENHA,
    ):
        monkeypatch.delenv(env, raising=False)


def test_origem_na_lista_autorizada_e_avaliada_normalmente(monkeypatch) -> None:
    _limpar_envs_autenticacao(monkeypatch)
    monkeypatch.setenv(ENV_ORIGENS_AUTORIZADAS, "189.115.219.11,203.0.113.9")

    app = _compor(iniciar_consumidores=False)
    controlador = app.state.controlador_autenticacao

    # As origens configuradas são autorizadas; outras não.
    assert controlador.origem_autorizada("189.115.219.11") is True
    assert controlador.origem_autorizada("203.0.113.9") is True
    assert controlador.origem_autorizada("10.0.0.1") is False


def test_liberar_todas_origens_via_env_autoriza_qualquer_origem(monkeypatch) -> None:
    _limpar_envs_autenticacao(monkeypatch)
    monkeypatch.setenv(ENV_LIBERAR_TODAS_ORIGENS, "true")

    app = _compor(iniciar_consumidores=False)
    controlador = app.state.controlador_autenticacao

    assert controlador.origem_autorizada("qualquer-ip") is True


def test_asterisco_nas_origens_libera_todas(monkeypatch) -> None:
    _limpar_envs_autenticacao(monkeypatch)
    monkeypatch.setenv(ENV_ORIGENS_AUTORIZADAS, "*")

    app = _compor(iniciar_consumidores=False)
    assert app.state.controlador_autenticacao.origem_autorizada("10.0.0.5") is True


def test_credenciais_via_env_sao_aplicadas(monkeypatch) -> None:
    _limpar_envs_autenticacao(monkeypatch)
    monkeypatch.setenv(ENV_USUARIO, "operador")
    monkeypatch.setenv(ENV_SENHA, "senha-forte-123")

    app = _compor(iniciar_consumidores=False)
    credenciais = app.state.credenciais_dashboard

    assert credenciais.conferem("operador", "senha-forte-123") is True
    assert credenciais.conferem("admin", "trocar-no-console") is False


def test_sem_envs_registra_aviso_e_nao_libera_todas(monkeypatch, capsys) -> None:
    _limpar_envs_autenticacao(monkeypatch)

    app = _compor(iniciar_consumidores=False)
    controlador = app.state.controlador_autenticacao

    # Comportamento seguro: nenhuma origem é liberada por padrão.
    assert controlador.origem_autorizada("10.0.0.1") is False
    # O aviso em pt-BR é emitido no stderr.
    assert ENV_ORIGENS_AUTORIZADAS in capsys.readouterr().err


def test_controlador_injetado_tem_precedencia_sobre_env(monkeypatch) -> None:
    _limpar_envs_autenticacao(monkeypatch)
    monkeypatch.setenv(ENV_LIBERAR_TODAS_ORIGENS, "true")

    from app.seguranca.autenticacao import ControladorAutenticacao

    controlador_injetado = ControladorAutenticacao(["203.0.113.9"])
    app = _compor(
        iniciar_consumidores=False,
        controlador_autenticacao=controlador_injetado,
    )

    # A injeção explícita prevalece sobre a env (não vira modo aberto).
    assert app.state.controlador_autenticacao is controlador_injetado
    assert app.state.controlador_autenticacao.origem_autorizada("qualquer") is False


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


# --- GerenciadorConsumidores: resiliência (slots + supervisor + reinício) ---


def test_iniciar_e_idempotente_nao_duplica_slots() -> None:
    estado = EstadoInstanciasConsumidores(standard=1, fifo=1)
    gateway = MagicMock()
    gateway.receive_messages.return_value = []

    gerenciador = GerenciadorConsumidores(
        gateway=gateway, historico=MagicMock(), estado_instancias=estado
    )
    try:
        gerenciador.iniciar()
        gerenciador.iniciar()  # segunda chamada: não deve duplicar
        assert gerenciador.quantidade_threads == 2
    finally:
        gerenciador.parar(timeout=2.0)


def test_estado_slots_expoe_tipo_id_e_vivo() -> None:
    estado = EstadoInstanciasConsumidores(standard=1, fifo=1)
    gateway = MagicMock()
    gateway.receive_messages.return_value = []

    gerenciador = GerenciadorConsumidores(
        gateway=gateway, historico=MagicMock(), estado_instancias=estado
    )
    try:
        gerenciador.iniciar()
        estados = gerenciador.estado_slots()
        tipos = sorted(s.tipo for s in estados)
        assert tipos == ["fifo", "standard"]
        assert all(s.consumer_id for s in estados)
        assert all(s.degradado is False and s.reinicios == 0 for s in estados)
    finally:
        gerenciador.parar(timeout=2.0)


def test_supervisor_recria_slot_morto_sem_duplicar() -> None:
    import time as _time

    from app.dominio import constantes as const

    # Encurta o intervalo de supervisão para o teste rodar rápido.
    original = const.INTERVALO_SUPERVISAO_SEGUNDOS
    const.INTERVALO_SUPERVISAO_SEGUNDOS = 0.05

    gateway = MagicMock()
    gateway.receive_messages.return_value = []
    estado = EstadoInstanciasConsumidores(standard=1, fifo=0)

    # Primeira instância criada "morre" assim que roda (executar retorna de imediato),
    # simulando uma thread encerrada inesperadamente; as instâncias seguintes rodam o
    # laço cooperativo normal. Assim o supervisor observa o slot morto e o recria.
    class _GerenciadorComMorteInicial(GerenciadorConsumidores):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.criados = 0

        def _criar_consumidor(self, tipo):
            consumidor = super()._criar_consumidor(tipo)
            self.criados += 1
            if self.criados == 1:
                # Força a primeira thread a encerrar imediatamente (slot morto).
                consumidor.executar = lambda **_k: None  # type: ignore[attr-defined]
            return consumidor

    gerenciador = _GerenciadorComMorteInicial(
        gateway=gateway, historico=MagicMock(), estado_instancias=estado
    )
    try:
        gerenciador.iniciar()
        id_inicial = gerenciador.estado_slots()[0].consumer_id
        # Aguarda o supervisor recriar o slot morto (novo consumer_id, thread viva).
        prazo = _time.monotonic() + 3.0
        while _time.monotonic() < prazo:
            estados = gerenciador.estado_slots()
            if estados and estados[0].reinicios >= 1 and estados[0].vivo:
                break
            _time.sleep(0.05)
        estados = gerenciador.estado_slots()
        # Continua havendo exatamente UM slot (sem duplicar) e com novo id.
        assert gerenciador.quantidade_threads == 1
        assert estados[0].reinicios >= 1
        assert estados[0].consumer_id != id_inicial
        assert estados[0].vivo is True
    finally:
        gerenciador.parar(timeout=2.0)
        const.INTERVALO_SUPERVISAO_SEGUNDOS = original


def test_supervisor_marca_degradado_ao_exceder_teto_de_reinicios() -> None:
    import time as _time

    from app.dominio import constantes as const

    original_intervalo = const.INTERVALO_SUPERVISAO_SEGUNDOS
    const.INTERVALO_SUPERVISAO_SEGUNDOS = 0.02

    gateway = MagicMock()
    gateway.receive_messages.return_value = []
    estado = EstadoInstanciasConsumidores(standard=1, fifo=0)

    # TODAS as instâncias "morrem" na hora: o supervisor reinicia repetidamente até
    # exceder MAX_REINICIOS_JANELA e marcar o slot como degradado (para de reiniciar).
    class _GerenciadorSempreMorto(GerenciadorConsumidores):
        def _criar_consumidor(self, tipo):
            consumidor = super()._criar_consumidor(tipo)
            consumidor.executar = lambda **_k: None  # type: ignore[attr-defined]
            return consumidor

    gerenciador = _GerenciadorSempreMorto(
        gateway=gateway, historico=MagicMock(), estado_instancias=estado
    )
    try:
        gerenciador.iniciar()
        prazo = _time.monotonic() + 3.0
        while _time.monotonic() < prazo:
            estados = gerenciador.estado_slots()
            if estados and estados[0].degradado:
                break
            _time.sleep(0.02)
        estados = gerenciador.estado_slots()
        assert estados[0].degradado is True
        # Excedeu o teto da janela: não reinicia além do limite esperado.
        assert estados[0].reinicios >= const.MAX_REINICIOS_JANELA
    finally:
        gerenciador.parar(timeout=2.0)
        const.INTERVALO_SUPERVISAO_SEGUNDOS = original_intervalo


def test_parar_nao_recria_slots_e_zera_quantidade() -> None:
    estado = EstadoInstanciasConsumidores(standard=1, fifo=1)
    gateway = MagicMock()
    gateway.receive_messages.return_value = []

    gerenciador = GerenciadorConsumidores(
        gateway=gateway, historico=MagicMock(), estado_instancias=estado
    )
    gerenciador.iniciar()
    gerenciador.parar(timeout=2.0)

    # Após parar: sem slots, sem recriação, quantidade zerada e estado vazio.
    assert gerenciador.quantidade_threads == 0
    assert gerenciador.estado_slots() == []
