"""Testes unitários dos endpoints de configuração e saúde da API (Tarefa 15.1).

Validam, com ``TestClient`` do FastAPI (httpx) e dependências injetadas por fakes
(sem AWS real nem ``time.sleep``):

- ``GET /api/saude``: sucesso com todos os recursos presentes (Req. 1.6) e falha
  identificando o recurso indisponível por nome e tipo, preservando o erro
  original da AWS (Req. 1.8);
- ``GET /api/config``: retorna Config_Aplicada e Config_Simulacao com rótulos
  textuais distintos e orienta que a Config_Aplicada só muda no Console AWS
  (Req. 2.1-2.7, 2.10, 2.13);
- ``PUT /api/config/simulacao``: aceita valores dentro das faixas e rejeita valores
  fora, preservando o valor anterior e indicando a faixa válida (Req. 2.11, 2.12).

Os testes não dependem da AWS real: o ``SqsGateway`` recebe um cliente SQS
simulado (``MagicMock``) e o verificador da Tabela_Historico é um callable fake.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.acesso.sqs_gateway import SqsGateway
from app.api.app import ORIENTACAO_CONFIG_APLICADA, create_app
from app.api.esquemas import ROTULO_CONFIG_APLICADA, ROTULO_CONFIG_SIMULACAO
from app.api.estado_simulacao import EstadoConfigSimulacao
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


def _criar_cliente(
    *,
    cliente_sqs=None,
    verificador_tabela=None,
    estado_simulacao: EstadoConfigSimulacao | None = None,
) -> TestClient:
    """Monta um TestClient com dependências injetadas (sem AWS real)."""
    gateway = SqsGateway(cliente_sqs=cliente_sqs or _cliente_sqs_ok())
    app = create_app(
        gateway=gateway,
        verificador_tabela=verificador_tabela or _verificador_tabela_ok(),
        estado_simulacao=estado_simulacao or EstadoConfigSimulacao(),
    )
    return TestClient(app)


# --- GET /api/saude ----------------------------------------------------------


def test_saude_todos_recursos_presentes_retorna_saudavel() -> None:
    cliente = _criar_cliente()

    resposta = cliente.get("/api/saude")

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["saudavel"] is True
    # As 4 filas + a Tabela_Historico.
    assert len(corpo["recursos"]) == len(c.FILAS) + 1
    assert all(r["disponivel"] for r in corpo["recursos"])
    assert all(r["detalhe_erro"] is None for r in corpo["recursos"])


def test_saude_fila_ausente_retorna_nao_saudavel_com_nome_e_tipo() -> None:
    cliente_sqs = _cliente_sqs_ok()
    cliente_sqs.get_queue_attributes.side_effect = _ErroAwsSimulado(
        "AWS.SimpleQueueService.NonExistentQueue"
    )
    cliente = _criar_cliente(cliente_sqs=cliente_sqs)

    resposta = cliente.get("/api/saude")

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["saudavel"] is False
    # Identifica o recurso indisponível por nome e tipo em pt-BR (Req. 1.8).
    assert c.FILA_STANDARD in corpo["mensagem"]
    assert "fila SQS" in corpo["mensagem"]
    indisponiveis = [r for r in corpo["recursos"] if not r["disponivel"]]
    assert len(indisponiveis) == 1
    assert indisponiveis[0]["nome"] == c.FILA_STANDARD
    assert indisponiveis[0]["tipo"] == "fila SQS"
    # O erro original da AWS é preservado no detalhe (Req. 1.8).
    assert "NonExistentQueue" in indisponiveis[0]["detalhe_erro"]


def test_saude_tabela_ausente_retorna_nao_saudavel() -> None:
    verificador = MagicMock(side_effect=_ErroAwsSimulado("ResourceNotFoundException"))
    cliente = _criar_cliente(verificador_tabela=verificador)

    resposta = cliente.get("/api/saude")

    corpo = resposta.json()
    assert corpo["saudavel"] is False
    assert "tabela DynamoDB" in corpo["mensagem"]
    assert c.TABELA_HISTORICO in corpo["mensagem"]


# --- GET /api/config ---------------------------------------------------------


def test_config_retorna_aplicada_e_simulacao_com_rotulos_distintos() -> None:
    cliente = _criar_cliente()

    resposta = cliente.get("/api/config")

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert len(corpo["config_aplicada"]) > 0
    assert len(corpo["config_simulacao"]) > 0
    # Rótulos textuais distintos (Req. 2.10).
    assert all(p["rotulo"] == ROTULO_CONFIG_APLICADA for p in corpo["config_aplicada"])
    assert all(
        p["rotulo"] == ROTULO_CONFIG_SIMULACAO for p in corpo["config_simulacao"]
    )
    assert ROTULO_CONFIG_APLICADA != ROTULO_CONFIG_SIMULACAO


def test_config_aplicada_nao_ajustavel_e_orienta_console_aws() -> None:
    cliente = _criar_cliente()

    corpo = cliente.get("/api/config").json()

    # Config_Aplicada não é ajustável e orienta mudança no Console AWS (Req. 2.13).
    assert all(p["ajustavel"] is False for p in corpo["config_aplicada"])
    assert corpo["orientacao_config_aplicada"] == ORIENTACAO_CONFIG_APLICADA
    assert "Console AWS" in corpo["orientacao_config_aplicada"]


def test_config_aplicada_contem_parametros_do_laboratorio() -> None:
    cliente = _criar_cliente()

    corpo = cliente.get("/api/config").json()
    valores = {p["nome"]: p["valor"] for p in corpo["config_aplicada"]}

    # Valores iniciais da Config_Aplicada (Req. 2.1-2.5).
    assert valores["Visibility Timeout"] == c.VISIBILITY_TIMEOUT_SEGUNDOS
    assert valores["maxReceiveCount da Redrive Policy"] == c.MAX_RECEIVE_COUNT
    assert valores["Long Polling (WaitTimeSeconds)"] == c.LONG_POLLING_SEGUNDOS


def test_config_simulacao_tem_faixas_e_e_ajustavel() -> None:
    cliente = _criar_cliente()

    corpo = cliente.get("/api/config").json()
    por_nome = {p["nome"]: p for p in corpo["config_simulacao"]}

    tempo = por_nome["Tempo de processamento simulado"]
    assert tempo["ajustavel"] is True
    assert tempo["faixa_minima"] == c.TEMPO_PROCESSAMENTO_SIMULADO_MIN
    assert tempo["faixa_maxima"] == c.TEMPO_PROCESSAMENTO_SIMULADO_MAX
    assert tempo["valor"] == c.TEMPO_PROCESSAMENTO_SIMULADO_SEGUNDOS


# --- PUT /api/config/simulacao -----------------------------------------------


def test_ajuste_dentro_da_faixa_e_aceito_e_persiste() -> None:
    cliente = _criar_cliente()

    resposta = cliente.put(
        "/api/config/simulacao",
        json={"tempo_processamento_simulado_segundos": 120},
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["aceito"] is True
    assert corpo["ajustes"][0]["valor_efetivo"] == 120
    # O novo valor persiste na próxima consulta de configuração.
    config = cliente.get("/api/config").json()
    tempo = next(
        p for p in config["config_simulacao"]
        if p["nome"] == "Tempo de processamento simulado"
    )
    assert tempo["valor"] == 120


def test_ajuste_fora_da_faixa_e_rejeitado_preservando_valor_anterior() -> None:
    cliente = _criar_cliente()

    resposta = cliente.put(
        "/api/config/simulacao",
        json={"tempo_processamento_simulado_segundos": 999},
    )

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["aceito"] is False
    ajuste = corpo["ajustes"][0]
    assert ajuste["aceito"] is False
    # Preserva o valor anterior (Req. 2.12).
    assert ajuste["valor_efetivo"] == c.TEMPO_PROCESSAMENTO_SIMULADO_SEGUNDOS
    # Mensagem em pt-BR indica a faixa válida (Req. 2.12).
    assert "0 a 300" in ajuste["mensagem"]
    # O valor anterior permanece na configuração.
    config = cliente.get("/api/config").json()
    tempo = next(
        p for p in config["config_simulacao"]
        if p["nome"] == "Tempo de processamento simulado"
    )
    assert tempo["valor"] == c.TEMPO_PROCESSAMENTO_SIMULADO_SEGUNDOS


def test_ajuste_multiplos_parametros_mistura_aceito_e_rejeitado() -> None:
    cliente = _criar_cliente()

    resposta = cliente.put(
        "/api/config/simulacao",
        json={
            "quantidade_mensagens": 7,  # dentro de [1, 10]
            "intervalo_atualizacao_segundos": 1,  # fora de [5, 300]
        },
    )

    corpo = resposta.json()
    assert corpo["aceito"] is False
    por_parametro = {a["parametro"]: a for a in corpo["ajustes"]}
    assert por_parametro["Quantidade de mensagens por envio"]["aceito"] is True
    assert por_parametro["Quantidade de mensagens por envio"]["valor_efetivo"] == 7
    assert por_parametro["Intervalo de atualização"]["aceito"] is False
    assert (
        por_parametro["Intervalo de atualização"]["valor_efetivo"]
        == c.INTERVALO_ATUALIZACAO_PADRAO
    )


def test_ajuste_sem_parametros_nao_altera_nada() -> None:
    cliente = _criar_cliente()

    resposta = cliente.put("/api/config/simulacao", json={})

    corpo = resposta.json()
    # Nenhum ajuste informado: lista vazia e all([]) == True.
    assert corpo["ajustes"] == []
    assert corpo["aceito"] is True


def test_ajuste_limites_inclusivos_sao_aceitos() -> None:
    cliente = _criar_cliente()

    # Limites mínimo e máximo das faixas devem ser aceitos (faixa fechada).
    resposta = cliente.put(
        "/api/config/simulacao",
        json={
            "tempo_processamento_simulado_segundos": c.TEMPO_PROCESSAMENTO_SIMULADO_MIN,
            "quantidade_mensagens": c.QUANTIDADE_MENSAGENS_MAX,
        },
    )

    corpo = resposta.json()
    assert corpo["aceito"] is True


# --- GET /api/consumidores/saude ---------------------------------------------
# A saúde dos consumidores é lida do estado em memória do GerenciadorConsumidores
# (método estado_slots()), sem tocar na AWS. Aqui usamos fakes de slot/gerenciador
# e injetamos o gerenciador em app.state, como faz a composição da aplicação.
from dataclasses import dataclass


@dataclass
class _SlotFake:
    tipo: str
    consumer_id: str
    vivo: bool
    degradado: bool
    reinicios: int


class _GerenciadorFake:
    def __init__(self, slots: list[_SlotFake]) -> None:
        self._slots = slots

    def estado_slots(self) -> list[_SlotFake]:
        return list(self._slots)


def test_saude_consumidores_sem_gerenciador_retorna_nao_iniciados() -> None:
    cliente = _criar_cliente()
    # Em create_app não há atributo gerenciador_consumidores; o endpoint trata como None.

    resposta = cliente.get("/api/consumidores/saude")

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["iniciados"] is False
    assert corpo["algum_morto"] is False
    assert corpo["algum_degradado"] is False
    assert corpo["consumidores"] == []


def test_saude_consumidores_gerenciador_none_retorna_nao_iniciados() -> None:
    cliente = _criar_cliente()
    cliente.app.state.gerenciador_consumidores = None

    corpo = cliente.get("/api/consumidores/saude").json()

    assert corpo["iniciados"] is False
    assert corpo["consumidores"] == []


def test_saude_consumidores_lista_slots_e_agrega_morto_e_degradado() -> None:
    cliente = _criar_cliente()
    cliente.app.state.gerenciador_consumidores = _GerenciadorFake(
        [
            _SlotFake("standard", "consumidor-standard-1", True, False, 0),
            _SlotFake("fifo", "consumidor-fifo-1", False, False, 2),
            _SlotFake("fifo", "consumidor-fifo-2", True, True, 5),
        ]
    )

    resposta = cliente.get("/api/consumidores/saude")

    assert resposta.status_code == 200
    corpo = resposta.json()
    assert corpo["iniciados"] is True
    # Há um slot não vivo (fifo-1) e um degradado (fifo-2).
    assert corpo["algum_morto"] is True
    assert corpo["algum_degradado"] is True
    assert len(corpo["consumidores"]) == 3
    primeiro = corpo["consumidores"][0]
    assert primeiro["tipo"] == "standard"
    assert primeiro["consumer_id"] == "consumidor-standard-1"
    assert primeiro["vivo"] is True
    assert primeiro["degradado"] is False
    assert primeiro["reinicios"] == 0


def test_saude_consumidores_todos_vivos_sem_degradacao() -> None:
    cliente = _criar_cliente()
    cliente.app.state.gerenciador_consumidores = _GerenciadorFake(
        [
            _SlotFake("standard", "consumidor-standard-1", True, False, 0),
            _SlotFake("fifo", "consumidor-fifo-1", True, False, 0),
        ]
    )

    corpo = cliente.get("/api/consumidores/saude").json()

    assert corpo["iniciados"] is True
    assert corpo["algum_morto"] is False
    assert corpo["algum_degradado"] is False
    assert len(corpo["consumidores"]) == 2


def test_saude_consumidores_nao_conflita_com_rota_de_instancias() -> None:
    # A rota literal '/saude' não deve ser capturada por '/{tipo}/instancias'.
    cliente = _criar_cliente()

    saude = cliente.get("/api/consumidores/saude")
    instancias = cliente.put(
        "/api/consumidores/standard/instancias", json={"instancias": 3}
    )

    assert saude.status_code == 200
    assert "iniciados" in saude.json()
    assert instancias.status_code == 200
    assert instancias.json()["tipo"] == "standard"
