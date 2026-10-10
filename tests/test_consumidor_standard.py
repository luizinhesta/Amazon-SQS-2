"""Testes unitários do Consumidor_Standard (Tarefa 12.1 / 12.2).

Validam o laço de :class:`app.consumidores.consumidor_standard.ConsumidorStandard`
usando dublês (fakes) de ``SqsGateway`` e ``HistoricoRepository`` — sem AWS real e
sem espera real (a função ``sleep`` é um dublê que apenas registra as chamadas).

Cobertura:
- **Fluxo feliz**: recebe um lote, processa com sucesso e exclui cada mensagem,
  usando o ``consumer_id`` da instância (Req. 8.1, 10.4).
- **Recebimento vazio**: nenhuma mensagem disponível não gera erro; o laço repete
  (Req. 8.11).
- Parâmetros do Long_Polling (lote 1–10 e WaitTimeSeconds=20) são repassados ao
  gateway (Req. 8.1, 14.5).
- Critérios de parada testáveis: ``max_iteracoes``, ``continuar`` e ``parar()``.
- ``consumer_id`` único por instância (Req. 10.4).
"""

from __future__ import annotations

from typing import Any, Optional

from app.acesso.sqs_gateway import MensagemRecebida
from app.consumidores.consumidor_standard import (
    ConsumidorStandard,
    cenario_do_atributo,
    criar_consumidor_standard,
    gerar_consumer_id,
)
from app.consumidores.processamento import CenarioFalha
from app.dominio import constantes as c
from app.dominio.evento import Evento
from app.dominio.tipos import EventType


# ---------------------------------------------------------------------------
# Dublês (fakes) das dependências injetáveis
# ---------------------------------------------------------------------------


class HistoricoFake:
    """Fake de HistoricoRepository que acumula eventos e permite consultas simples."""

    def __init__(self) -> None:
        self.eventos: list[Evento] = []

    def registrar_evento(self, evento: Evento) -> None:
        self.eventos.append(evento)

    def consultar_por_pedido(self, order_id: str) -> list[Evento]:
        return [e for e in self.eventos if e.order_id == order_id]

    def tipos(self) -> list[EventType]:
        return [e.event_type for e in self.eventos]


class SqsFake:
    """Fake de SqsGateway que entrega lotes pré-programados e registra chamadas.

    ``lotes`` é uma lista de lotes; cada chamada a ``receive_messages`` consome o
    próximo lote. Esgotados os lotes, passa a retornar lista vazia (recebimento
    vazio), permitindo testar a repetição sem erro.
    """

    def __init__(self, lotes: Optional[list[list[MensagemRecebida]]] = None) -> None:
        self._lotes = list(lotes or [])
        self.chamadas_receive: list[dict[str, Any]] = []
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
        if self._lotes:
            return self._lotes.pop(0)
        return []

    def delete_message(self, nome_fila: str, receipt_handle: str) -> dict[str, Any]:
        self.exclusoes.append((nome_fila, receipt_handle))
        return {}


class SleepFake:
    """Dublê de ``sleep`` que registra durações, sem esperar de verdade."""

    def __init__(self) -> None:
        self.chamadas: list[float] = []

    def __call__(self, segundos: float) -> None:
        self.chamadas.append(segundos)


def _mensagem(
    *,
    body: str,
    message_id: str = "msg",
    receipt_handle: str = "rh",
    receive_count: int = 1,
    message_group_id: Optional[str] = None,
    message_attributes: Optional[dict[str, Any]] = None,
) -> MensagemRecebida:
    return MensagemRecebida(
        message_id=message_id,
        receipt_handle=receipt_handle,
        body=body,
        approximate_receive_count=receive_count,
        message_group_id=message_group_id,
        message_attributes=message_attributes or {},
    )


def _consumidor(
    sqs: SqsFake,
    historico: HistoricoFake,
    *,
    consumer_id: str = "consumidor-standard-01",
    **kwargs: Any,
) -> ConsumidorStandard:
    return ConsumidorStandard(
        sqs=sqs,  # type: ignore[arg-type]
        historico=historico,  # type: ignore[arg-type]
        consumer_id=consumer_id,
        sleep=SleepFake(),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Fluxo feliz
# ---------------------------------------------------------------------------


def test_fluxo_feliz_processa_e_exclui_cada_mensagem_do_lote() -> None:
    # Req. 8.1, 8.6, 10.4: recebe um lote, processa com sucesso e exclui cada msg.
    lote = [
        _mensagem(body="pedido-0001", message_id="m1", receipt_handle="rh1"),
        _mensagem(body="pedido-0002", message_id="m2", receipt_handle="rh2"),
    ]
    sqs = SqsFake(lotes=[lote])
    historico = HistoricoFake()
    consumidor = _consumidor(sqs, historico)

    estat = consumidor.executar(max_iteracoes=1)

    assert estat.mensagens_recebidas == 2
    assert estat.processadas_com_sucesso == 2
    assert estat.falhas_processamento == 0
    # Cada mensagem foi excluída com a fila e o receipt handle corretos.
    assert sqs.exclusoes == [
        (c.FILA_STANDARD, "rh1"),
        (c.FILA_STANDARD, "rh2"),
    ]
    # O ciclo completo foi registrado para cada pedido.
    assert historico.tipos().count(EventType.PROCESSADA) == 2
    assert historico.tipos().count(EventType.EXCLUIDA) == 2


def test_fluxo_feliz_registra_consumer_id_da_instancia() -> None:
    # Req. 10.4: eventos de recebimento carregam o consumer_id da instância.
    sqs = SqsFake(lotes=[[_mensagem(body="pedido-0001")]])
    historico = HistoricoFake()
    consumidor = _consumidor(sqs, historico, consumer_id="consumidor-standard-42")

    consumidor.executar(max_iteracoes=1)

    recebida = next(e for e in historico.eventos if e.event_type is EventType.RECEBIDA)
    assert recebida.consumer_id == "consumidor-standard-42"


def test_long_polling_usa_wait_time_20_e_lote_configuravel() -> None:
    # Req. 8.1, 14.5: WaitTimeSeconds=20 (padrão) e lote de 1 a 10.
    sqs = SqsFake(lotes=[[_mensagem(body="pedido-0001")]])
    historico = HistoricoFake()
    consumidor = _consumidor(sqs, historico, max_mensagens=5)

    consumidor.executar(max_iteracoes=1)

    assert sqs.chamadas_receive[0] == {
        "nome_fila": c.FILA_STANDARD,
        "max_mensagens": 5,
        "wait_time_seconds": c.LONG_POLLING_SEGUNDOS,
    }
    assert c.LONG_POLLING_SEGUNDOS == 20


def test_cenario_falha_injetado_preserva_mensagem_sem_excluir() -> None:
    # O cenário pode ser injetado; falha preserva a mensagem (sem DeleteMessage).
    sqs = SqsFake(lotes=[[_mensagem(body="pedido-0001")]])
    historico = HistoricoFake()
    consumidor = _consumidor(
        sqs,
        historico,
        seletor_cenario=lambda _m: CenarioFalha.EXCECAO_PROPOSITAL,
    )

    estat = consumidor.executar(max_iteracoes=1)

    assert estat.falhas_processamento == 1
    assert estat.processadas_com_sucesso == 0
    assert sqs.exclusoes == []  # mensagem preservada na fila
    assert EventType.FALHA in historico.tipos()


def test_cenario_derivado_do_atributo_da_mensagem() -> None:
    # O Cenario_Falha pode vir dos atributos da mensagem.
    msg = _mensagem(
        body="pedido-0001",
        message_attributes={
            "cenario_falha": {"StringValue": "excecao_proposital", "DataType": "String"}
        },
    )
    sqs = SqsFake(lotes=[[msg]])
    historico = HistoricoFake()
    consumidor = _consumidor(sqs, historico, seletor_cenario=cenario_do_atributo)

    estat = consumidor.executar(max_iteracoes=1)

    assert estat.falhas_processamento == 1
    assert sqs.exclusoes == []


# ---------------------------------------------------------------------------
# Recebimento vazio
# ---------------------------------------------------------------------------


def test_recebimento_vazio_repete_sem_erro() -> None:
    # Req. 8.11: recebimento vazio não é erro; o laço repete sem exclusões/eventos.
    sqs = SqsFake(lotes=[])  # sempre vazio
    historico = HistoricoFake()
    consumidor = _consumidor(sqs, historico)

    estat = consumidor.executar(max_iteracoes=3)

    assert estat.iteracoes == 3
    assert estat.recebimentos_vazios == 3
    assert estat.mensagens_recebidas == 0
    assert sqs.exclusoes == []
    assert historico.eventos == []
    # O recebimento foi tentado uma vez por iteração.
    assert len(sqs.chamadas_receive) == 3


def test_lote_cheio_seguido_de_recebimento_vazio() -> None:
    # Primeira iteração entrega um lote; a seguinte vem vazia e não gera erro.
    sqs = SqsFake(lotes=[[_mensagem(body="pedido-0001", receipt_handle="rh1")]])
    historico = HistoricoFake()
    consumidor = _consumidor(sqs, historico)

    estat = consumidor.executar(max_iteracoes=2)

    assert estat.mensagens_recebidas == 1
    assert estat.processadas_com_sucesso == 1
    assert estat.recebimentos_vazios == 1
    assert sqs.exclusoes == [(c.FILA_STANDARD, "rh1")]


# ---------------------------------------------------------------------------
# Idempotência: mensagem já processada é pulada (sem exclusão)
# ---------------------------------------------------------------------------


def test_recebimento_repetido_e_pulado_por_idempotencia() -> None:
    # Pré-carrega o histórico com o ciclo completo do pedido (já processado).
    historico = HistoricoFake()
    # Processa uma vez.
    sqs1 = SqsFake(lotes=[[_mensagem(body="pedido-0001", receipt_handle="rh1")]])
    _consumidor(sqs1, historico).executar(max_iteracoes=1)
    assert sqs1.exclusoes == [(c.FILA_STANDARD, "rh1")]

    # Recebe a mesma mensagem de novo: deve ser pulada, sem nova exclusão.
    sqs2 = SqsFake(lotes=[[_mensagem(body="pedido-0001", receipt_handle="rh1b")]])
    consumidor2 = _consumidor(sqs2, historico)
    estat = consumidor2.executar(max_iteracoes=1)

    assert estat.puladas_por_idempotencia == 1
    assert estat.processadas_com_sucesso == 0
    assert sqs2.exclusoes == []  # não reexecuta efeitos
    assert EventType.RECEBIMENTO_REPETIDO in historico.tipos()


# ---------------------------------------------------------------------------
# Critérios de parada testáveis
# ---------------------------------------------------------------------------


def test_criterio_continuar_para_o_laco() -> None:
    sqs = SqsFake(lotes=[])
    historico = HistoricoFake()
    consumidor = _consumidor(sqs, historico)

    # Para após exatamente 2 iterações via predicado 'continuar'.
    contador = {"n": 0}

    def continuar() -> bool:
        contador["n"] += 1
        return contador["n"] <= 2

    estat = consumidor.executar(continuar=continuar)
    assert estat.iteracoes == 2


def test_metodo_parar_interrompe_o_laco() -> None:
    sqs = SqsFake(lotes=[])
    historico = HistoricoFake()
    consumidor = _consumidor(sqs, historico)

    # continuar() sinaliza parar() na terceira consulta, interrompendo o laço.
    chamadas = {"n": 0}

    def continuar() -> bool:
        chamadas["n"] += 1
        if chamadas["n"] == 3:
            consumidor.parar()
            return False
        return True

    estat = consumidor.executar(continuar=continuar)
    # Executa 2 iterações; na 3ª verificação, parar() interrompe antes de iterar.
    assert estat.iteracoes == 2


# ---------------------------------------------------------------------------
# consumer_id único por instância
# ---------------------------------------------------------------------------


def test_gerar_consumer_id_unico_por_instancia() -> None:
    ids = {gerar_consumer_id() for _ in range(5)}
    assert len(ids) == 5
    assert all(cid.startswith("consumidor-standard-") for cid in ids)


def test_fabrica_atribui_consumer_id_quando_ausente() -> None:
    sqs = SqsFake(lotes=[])
    historico = HistoricoFake()
    c1 = criar_consumidor_standard(sqs, historico)  # type: ignore[arg-type]
    c2 = criar_consumidor_standard(sqs, historico)  # type: ignore[arg-type]
    assert c1.consumer_id != c2.consumer_id
