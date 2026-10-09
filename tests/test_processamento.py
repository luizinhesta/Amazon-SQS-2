"""Testes unitários da lógica de processamento e exclusão (Tarefa 10.4).

Validam o comportamento de :func:`app.consumidores.processamento.processar_mensagem`
sob os diferentes Cenario_Falha, usando dublês (fakes) de ``SqsGateway`` e
``HistoricoRepository`` — sem AWS real e sem espera real (a função ``sleep`` é
injetada por um dublê que apenas registra as chamadas).

Requisitos cobertos:
- 8.3: horário real da aplicação (UTC, ms) em cada evento registrado.
- 8.4: registro do ``consumer_id`` e da contagem de recebimentos (``receive_count``).
- 8.5: espera equivalente ao Tempo_Processamento_Simulado antes de concluir.
- 8.6/8.7/8.9: DeleteMessage se e somente se o processamento concluir com sucesso.
- 8.8: em erro, preservar a mensagem e registrar o erro original.
"""

from __future__ import annotations

from typing import Any

import pytest

from app.acesso.sqs_gateway import MensagemRecebida
from app.consumidores.processamento import (
    CENARIOS_DE_SUCESSO,
    CenarioFalha,
    ErroProcessamentoSimulado,
    InterrupcaoProcessamento,
    processar_mensagem,
)
from app.dominio.evento import Evento
from app.dominio.tipos import EventSource, EventType


# ---------------------------------------------------------------------------
# Dublês (fakes) das dependências injetáveis
# ---------------------------------------------------------------------------


class HistoricoFake:
    """Fake de HistoricoRepository que acumula os eventos registrados em memória."""

    def __init__(self) -> None:
        self.eventos: list[Evento] = []

    def registrar_evento(self, evento: Evento) -> None:
        self.eventos.append(evento)

    def tipos(self) -> list[EventType]:
        return [e.event_type for e in self.eventos]


class SqsFake:
    """Fake de SqsGateway que registra as chamadas de ``delete_message``."""

    def __init__(self, erro_ao_excluir: Exception | None = None) -> None:
        self.exclusoes: list[tuple[str, str]] = []
        self._erro_ao_excluir = erro_ao_excluir

    def delete_message(self, nome_fila: str, receipt_handle: str) -> dict[str, Any]:
        if self._erro_ao_excluir is not None:
            raise self._erro_ao_excluir
        self.exclusoes.append((nome_fila, receipt_handle))
        return {}


class SleepFake:
    """Dublê de ``sleep`` que registra as durações, sem esperar de verdade."""

    def __init__(self) -> None:
        self.chamadas: list[float] = []

    def __call__(self, segundos: float) -> None:
        self.chamadas.append(segundos)


def _mensagem(
    *,
    body: str = "pedido-0001",
    message_id: str = "msg-001",
    receipt_handle: str = "rh-001",
    receive_count: int = 1,
    message_group_id: str | None = None,
) -> MensagemRecebida:
    return MensagemRecebida(
        message_id=message_id,
        receipt_handle=receipt_handle,
        body=body,
        approximate_receive_count=receive_count,
        message_group_id=message_group_id,
    )


def _processar(
    *,
    cenario: CenarioFalha,
    mensagem: MensagemRecebida | None = None,
    nome_fila: str = "lab-sqs-standard",
    consumer_id: str = "consumidor-standard-01",
    processing_seconds: int = 45,
    sqs: SqsFake | None = None,
    historico: HistoricoFake | None = None,
    sleep: SleepFake | None = None,
    **extras: Any,
):
    sqs = sqs or SqsFake()
    historico = historico or HistoricoFake()
    sleep = sleep or SleepFake()
    resultado = processar_mensagem(
        mensagem or _mensagem(),
        nome_fila=nome_fila,
        consumer_id=consumer_id,
        cenario=cenario,
        processing_seconds=processing_seconds,
        sqs=sqs,  # type: ignore[arg-type]
        historico=historico,  # type: ignore[arg-type]
        sleep=sleep,
        **extras,
    )
    return resultado, sqs, historico, sleep


# ---------------------------------------------------------------------------
# Cenário de sucesso: exclui a mensagem e registra o ciclo completo
# ---------------------------------------------------------------------------


def test_sucesso_executa_delete_message_e_registra_excluida() -> None:
    # Req. 8.6, 8.7, 8.9: exclusão ocorre no sucesso.
    resultado, sqs, historico, _ = _processar(cenario=CenarioFalha.SUCESSO)

    assert resultado.sucesso is True
    assert resultado.excluida is True
    assert resultado.erro_detalhe is None
    # DeleteMessage foi executado exatamente uma vez, com fila e receipt handle.
    assert sqs.exclusoes == [("lab-sqs-standard", "rh-001")]
    # Ciclo completo registrado em ordem.
    assert historico.tipos() == [
        EventType.RECEBIDA,
        EventType.PROCESSAMENTO_INICIADO,
        EventType.PROCESSADA,
        EventType.EXCLUIDA,
    ]


def test_sucesso_registra_recebida_com_consumidor_e_tentativas() -> None:
    # Req. 8.4: consumer_id e receive_count no evento de recebimento.
    _, _, historico, _ = _processar(
        cenario=CenarioFalha.SUCESSO,
        consumer_id="consumidor-standard-07",
        mensagem=_mensagem(receive_count=3),
    )
    recebida = next(e for e in historico.eventos if e.event_type is EventType.RECEBIDA)
    assert recebida.consumer_id == "consumidor-standard-07"
    assert recebida.receive_count == 3
    assert recebida.event_source is EventSource.OBSERVADO


def test_sucesso_aguarda_tempo_processamento_simulado() -> None:
    # Req. 8.5: espera equivalente ao Tempo_Processamento_Simulado.
    _, _, _, sleep = _processar(cenario=CenarioFalha.SUCESSO, processing_seconds=45)
    assert sleep.chamadas == [45]


def test_eventos_tem_horario_com_precisao_de_milissegundos() -> None:
    # Req. 8.3: app_time em UTC (sufixo Z) com milissegundos (3 casas decimais).
    _, _, historico, _ = _processar(cenario=CenarioFalha.SUCESSO)
    for evento in historico.eventos:
        assert evento.app_time.endswith("Z")
        # Ex.: 2024-06-25T12:00:00.123Z -> fração de 3 dígitos antes do Z.
        fracao = evento.app_time.split(".")[-1]
        assert len(fracao) == 4  # "123Z"
        assert isinstance(evento.app_time_ms, int)


# ---------------------------------------------------------------------------
# Cenários de falha: preservam a mensagem (sem DeleteMessage) e registram o erro
# ---------------------------------------------------------------------------


def test_excecao_proposital_preserva_mensagem_e_registra_erro() -> None:
    # Req. 8.3, 8.8: em erro, não exclui e registra o erro original.
    resultado, sqs, historico, _ = _processar(
        cenario=CenarioFalha.EXCECAO_PROPOSITAL
    )
    assert resultado.sucesso is False
    assert resultado.excluida is False
    assert sqs.exclusoes == []  # mensagem preservada na fila
    assert EventType.FALHA in historico.tipos()
    assert EventType.EXCLUIDA not in historico.tipos()
    falha = next(e for e in historico.eventos if e.event_type is EventType.FALHA)
    assert falha.error_detail is not None
    assert "ErroProcessamentoSimulado" in falha.error_detail


def test_interrupcao_preserva_mensagem_sem_delete() -> None:
    # Req. 7.4, 8.9: interrupção não exclui a mensagem.
    resultado, sqs, historico, _ = _processar(cenario=CenarioFalha.INTERRUPCAO)
    assert resultado.sucesso is False
    assert resultado.excluida is False
    assert sqs.exclusoes == []
    falha = next(e for e in historico.eventos if e.event_type is EventType.FALHA)
    assert "InterrupcaoProcessamento" in (falha.error_detail or "")


@pytest.mark.parametrize(
    "cenario",
    [
        CenarioFalha.DURACAO_SUPERIOR_VISIBILITY,
        CenarioFalha.RECEBIMENTO_REPETIDO,
        CenarioFalha.ENCAMINHAR_DLQ,
    ],
)
def test_cenarios_sem_confirmacao_nao_excluem(cenario: CenarioFalha) -> None:
    # Req. 8.9: cenários "sem confirmação" preservam a mensagem (sem DeleteMessage).
    resultado, sqs, historico, _ = _processar(cenario=cenario)
    assert resultado.sucesso is False
    assert resultado.excluida is False
    assert sqs.exclusoes == []
    # Registra uma falha com motivo descritivo (erro original preservado).
    falha = next(e for e in historico.eventos if e.event_type is EventType.FALHA)
    assert falha.error_detail


def test_cenario_sem_confirmacao_ainda_aguarda_processamento() -> None:
    # Req. 8.5: a espera ocorre antes de decidir não excluir.
    _, _, _, sleep = _processar(
        cenario=CenarioFalha.DURACAO_SUPERIOR_VISIBILITY, processing_seconds=10
    )
    assert sleep.chamadas == [10]


# ---------------------------------------------------------------------------
# Regra "se e somente se": exclusão ocorre exatamente nos cenários de sucesso
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cenario", list(CenarioFalha))
def test_exclusao_ocorre_se_e_somente_se_sucesso(cenario: CenarioFalha) -> None:
    # Req. 8.6, 8.7, 8.9: DeleteMessage <=> processamento concluiu com sucesso.
    resultado, sqs, _, _ = _processar(cenario=cenario)
    exclusao_esperada = cenario in CENARIOS_DE_SUCESSO
    assert resultado.excluida is exclusao_esperada
    assert bool(sqs.exclusoes) is exclusao_esperada
    assert resultado.sucesso is exclusao_esperada


# ---------------------------------------------------------------------------
# order_id e message_group_id
# ---------------------------------------------------------------------------


def test_order_id_explicito_sobrepoe_corpo_da_mensagem() -> None:
    _, _, historico, _ = _processar(
        cenario=CenarioFalha.SUCESSO,
        mensagem=_mensagem(body="corpo-qualquer"),
        order_id="pedido-9999",
    )
    assert all(e.order_id == "pedido-9999" for e in historico.eventos)


def test_message_group_id_da_mensagem_e_propagado() -> None:
    _, _, historico, _ = _processar(
        cenario=CenarioFalha.SUCESSO,
        nome_fila="lab-sqs-fifo.fifo",
        mensagem=_mensagem(message_group_id="grupo-A"),
    )
    recebida = next(e for e in historico.eventos if e.event_type is EventType.RECEBIDA)
    assert recebida.message_group_id == "grupo-A"


# ---------------------------------------------------------------------------
# Falha de infraestrutura no DeleteMessage: erro original é propagado
# ---------------------------------------------------------------------------


def test_erro_no_delete_message_propaga_erro_original() -> None:
    # Req. 8.8: falha de infraestrutura (não cenário simulado) propaga o erro.
    class ErroAwsSimulado(Exception):
        pass

    sqs = SqsFake(erro_ao_excluir=ErroAwsSimulado("DeleteMessage falhou"))
    with pytest.raises(ErroAwsSimulado):
        _processar(cenario=CenarioFalha.SUCESSO, sqs=sqs)
