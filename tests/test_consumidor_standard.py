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

from dataclasses import replace
from typing import Any, Optional

import json

from botocore.exceptions import ClientError

from app.acesso.sqs_gateway import MensagemRecebida
from app.consumidores.consumidor_standard import (
    ConsumidorStandard,
    cenario_do_atributo,
    criar_consumidor_standard,
    gerar_consumer_id,
)
from app.consumidores.idempotencia import DecisaoIdempotencia, ResultadoIdempotencia
from app.consumidores.processamento import CenarioFalha, ResultadoProcessamento
from app.dominio import constantes as c
from app.dominio.evento import Evento
from app.dominio.tipos import EventType


# Objeto de erro padrão no formato botocore real (ClientError com ``.response``),
# usado pelos testes de reentrega para exercitar de verdade os classificadores de
# erro do processamento (ReceiptHandle expirado).
ERRO_HANDLE_EXPIRADO = ClientError(
    {
        "Error": {
            "Code": "InvalidParameterValue",
            "Message": "The receipt handle has expired.",
        }
    },
    "DeleteMessage",
)


def _corpo_contrato(
    order_id: str,
    *,
    cenario: str = "sucesso",
    processing_seconds: int = 5,
) -> str:
    """Corpo JSON de contrato do produtor com ``order_id`` explícito.

    Obrigatório nos testes do Standard porque o consumidor deriva o ``order_id`` de
    ``payload.order_id`` sem fallback cru; sem isso a idempotência decidiria
    REJEITAR_VALIDACAO e o processamento nunca rodaria.
    """
    return json.dumps(
        {
            "order_id": order_id,
            "cenario": cenario,
            "processing_seconds": processing_seconds,
        }
    )


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

    Modelo de reentrega (FEAT-006): quando ``efeitos_delete`` é fornecido, cada
    ``delete_message`` consome o próximo item (``Exception`` -> levanta; ``None``
    -> registra a exclusão e retorna ``{}``). Toda mensagem cujo ``delete_message``
    **falhou** (ou que não foi excluída) **permanece** na fila do fake e é
    **devolvida no próximo ``receive_messages``**, com ``approximate_receive_count``
    incrementado — só saem as mensagens com delete confirmado. Isso reproduz a
    reentrega real do SQS após a expiração do Visibility_Timeout.
    """

    def __init__(
        self,
        lotes: Optional[list[list[MensagemRecebida]]] = None,
        efeitos_delete: Optional[list[Optional[Exception]]] = None,
    ) -> None:
        self._lotes = list(lotes or [])
        self.chamadas_receive: list[dict[str, Any]] = []
        self.exclusoes: list[tuple[str, str]] = []
        self._efeitos_delete = list(efeitos_delete) if efeitos_delete else None
        # Mensagens recebidas e ainda não excluídas, por receipt_handle, para o
        # modelo de reentrega: reaparecem no próximo receive_messages.
        self._pendentes: list[MensagemRecebida] = []
        self._handles_pendentes: set[str] = set()

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
        # O modelo de reentrega só atua quando ``efeitos_delete`` é usado (testes
        # que exercitam o delete falho). Sem ele, mantém o comportamento simples
        # original: cada chamada consome o próximo lote.
        if self._efeitos_delete is None:
            if self._lotes:
                return self._lotes.pop(0)
            return []
        # Reentrega: mensagens pendentes (delete não confirmado) voltam primeiro,
        # com approximate_receive_count incrementado.
        if self._pendentes:
            reentregues = [
                replace(
                    m,
                    approximate_receive_count=m.approximate_receive_count + 1,
                )
                for m in self._pendentes
            ]
            self._pendentes = list(reentregues)
            self._handles_pendentes = {m.receipt_handle for m in reentregues}
            return reentregues
        if self._lotes:
            lote = self._lotes.pop(0)
            self._pendentes = list(lote)
            self._handles_pendentes = {m.receipt_handle for m in lote}
            return lote
        return []

    def delete_message(self, nome_fila: str, receipt_handle: str) -> dict[str, Any]:
        if self._efeitos_delete is not None and self._efeitos_delete:
            efeito = self._efeitos_delete.pop(0)
            if efeito is not None:
                # Delete falhou: a mensagem permanece pendente (será reentregue).
                raise efeito
        # Delete confirmado: remove a mensagem das pendentes (não reaparece).
        self.exclusoes.append((nome_fila, receipt_handle))
        if receipt_handle in self._handles_pendentes:
            self._handles_pendentes.discard(receipt_handle)
            self._pendentes = [
                m for m in self._pendentes if m.receipt_handle != receipt_handle
            ]
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


# ---------------------------------------------------------------------------
# Contrato produtor/consumidor: corpo JSON é desserializado (não vira order_id)
# ---------------------------------------------------------------------------


def test_corpo_json_do_produtor_usa_order_id_extraido_na_idempotencia_e_processamento() -> None:
    # Bug de contrato: o produtor envia o corpo como JSON; o consumidor deve
    # extrair o order_id real, e NÃO usar o JSON inteiro como order_id.
    corpo = json.dumps(
        {
            "order_id": "teste1-standard-0001",
            "processing_seconds": 10,
            "cenario": "sucesso",
        }
    )

    order_ids_idempotencia: list[str] = []
    parametros_processamento: list[dict[str, Any]] = []

    def idem_fake(
        repositorio: Any,
        *,
        order_id: Optional[str],
        **kwargs: Any,
    ) -> ResultadoIdempotencia:
        order_ids_idempotencia.append(order_id)
        return ResultadoIdempotencia(
            decisao=DecisaoIdempotencia.PROCESSAR, order_id=order_id
        )

    def processar_fake(
        mensagem: MensagemRecebida,
        *,
        order_id: Optional[str] = None,
        cenario: CenarioFalha,
        processing_seconds: int,
        **kwargs: Any,
    ) -> ResultadoProcessamento:
        parametros_processamento.append(
            {
                "order_id": order_id,
                "cenario": cenario,
                "processing_seconds": processing_seconds,
            }
        )
        return ResultadoProcessamento(
            order_id=order_id or mensagem.body,
            cenario=cenario,
            sucesso=True,
            excluida=True,
        )

    sqs = SqsFake(lotes=[[_mensagem(body=corpo, receipt_handle="rh1")]])
    historico = HistoricoFake()
    consumidor = _consumidor(
        sqs,
        historico,
        verificar_idempotencia_fn=idem_fake,
        processar_mensagem_fn=processar_fake,
    )

    consumidor.executar(max_iteracoes=1)

    # O order_id usado é o EXTRAÍDO, não o JSON inteiro.
    assert order_ids_idempotencia == ["teste1-standard-0001"]
    assert parametros_processamento[0]["order_id"] == "teste1-standard-0001"
    # Cenário e tempo também vêm do corpo JSON do produtor.
    assert parametros_processamento[0]["cenario"] is CenarioFalha.SUCESSO
    assert parametros_processamento[0]["processing_seconds"] == 10


def test_corpo_json_do_produtor_deriva_cenario_de_falha_do_corpo() -> None:
    # O cenário derivado do corpo preserva a mensagem (sem exclusão) em falha.
    corpo = json.dumps(
        {
            "order_id": "teste1-standard-0002",
            "processing_seconds": 1,
            "cenario": "excecao_proposital",
        }
    )
    sqs = SqsFake(lotes=[[_mensagem(body=corpo, receipt_handle="rh2")]])
    historico = HistoricoFake()
    consumidor = _consumidor(sqs, historico)

    estat = consumidor.executar(max_iteracoes=1)

    assert estat.falhas_processamento == 1
    assert estat.processadas_com_sucesso == 0
    assert sqs.exclusoes == []  # mensagem preservada
    # O histórico foi particionado pelo order_id real (não pelo JSON inteiro).
    assert historico.consultar_por_pedido("teste1-standard-0002")
    assert historico.consultar_por_pedido(corpo) == []


# ---------------------------------------------------------------------------
# Resiliência: guarda por iteração e desfecho de exclusão não confirmada (FEAT-003)
# ---------------------------------------------------------------------------


class _SqsFalhaEntaoVazio:
    """SQS cujo ``receive_messages`` lança uma vez e depois volta a funcionar vazio.

    Permite verificar que uma exceção pontual numa iteração NÃO encerra a thread do
    consumidor: o laço registra a falha e continua nas iterações seguintes.
    """

    def __init__(self) -> None:
        self.chamadas = 0

    def receive_messages(self, nome_fila, *, max_mensagens, wait_time_seconds, visibility_timeout=None):
        self.chamadas += 1
        if self.chamadas == 1:
            raise RuntimeError("falha pontual simulada do SQS")
        return []

    def delete_message(self, nome_fila, receipt_handle):  # pragma: no cover - não usado
        return {}


def test_excecao_pontual_na_iteracao_nao_encerra_o_laco() -> None:
    # Critério de aceitação: uma exceção pontual é logada (WARNING) e NÃO encerra o
    # laço; iteracoes_com_erro reflete a ocorrência.
    sqs = _SqsFalhaEntaoVazio()
    historico = HistoricoFake()
    consumidor = _consumidor(sqs, historico)  # type: ignore[arg-type]

    estat = consumidor.executar(max_iteracoes=3)

    assert estat.iteracoes == 3
    assert estat.iteracoes_com_erro == 1  # apenas a primeira iteração falhou
    assert estat.recebimentos_vazios == 2  # as duas seguintes correram normalmente


def test_parada_cooperativa_nao_e_capturada_pela_guarda() -> None:
    # Critério de aceitação: BaseException (parada cooperativa) NÃO é capturada pela
    # guarda (que captura apenas Exception). Usamos KeyboardInterrupt (BaseException).
    class _SqsBaseException:
        def receive_messages(self, *a, **k):
            raise KeyboardInterrupt("parada cooperativa simulada")

        def delete_message(self, *a, **k):  # pragma: no cover
            return {}

    consumidor = _consumidor(_SqsBaseException(), HistoricoFake())  # type: ignore[arg-type]

    import pytest

    with pytest.raises(KeyboardInterrupt):
        consumidor.executar(max_iteracoes=3)


def test_exclusao_nao_confirmada_conta_separado_das_falhas() -> None:
    # Critério de aceitação: resultado com exclusao_nao_confirmada=True incrementa
    # exclusoes_nao_confirmadas e NÃO falhas_processamento.
    def processar_fake(
        mensagem: MensagemRecebida,
        *,
        order_id: Optional[str] = None,
        cenario: CenarioFalha,
        **kwargs: Any,
    ) -> ResultadoProcessamento:
        return ResultadoProcessamento(
            order_id=order_id or mensagem.body,
            cenario=cenario,
            sucesso=False,
            excluida=False,
            exclusao_nao_confirmada=True,
            erro_detalhe="ReceiptHandle expirado (simulado)",
        )

    sqs = SqsFake(lotes=[[_mensagem(body="pedido-X", receipt_handle="rh1")]])
    historico = HistoricoFake()
    consumidor = _consumidor(sqs, historico, processar_mensagem_fn=processar_fake)

    estat = consumidor.executar(max_iteracoes=1)

    assert estat.exclusoes_nao_confirmadas == 1
    assert estat.falhas_processamento == 0
    assert estat.processadas_com_sucesso == 0


# ---------------------------------------------------------------------------
# FEAT-006 / Teste obrigatório 10: Standard permanece funcional após falhas
# semelhantes (reentrega com delete falho na 1ª e confirmado na 2ª iteração),
# usando a função real processar_mensagem (sem AWS real e sem espera real).
# ---------------------------------------------------------------------------


def test_standard_segue_operacional_apos_falha_de_delete_e_reentrega() -> None:
    # Mensagem com corpo JSON de contrato (order_id explícito) + SqsFake com
    # efeitos_delete=[ERRO_HANDLE_EXPIRADO, None] reentregando: após a 1ª iteração,
    # exclusoes_nao_confirmadas == 1 e falhas_processamento == 0 (contador dedicado
    # da Opção A); após a 2ª (delete confirmado), processadas_com_sucesso == 1. O
    # laço continua operacional em ambas as iterações.
    corpo = _corpo_contrato("pedido-std-10", processing_seconds=5)
    sqs = SqsFake(
        lotes=[[_mensagem(body=corpo, message_id="m1", receipt_handle="rh1")]],
        efeitos_delete=[ERRO_HANDLE_EXPIRADO, None],
    )
    historico = HistoricoFake()
    # Usa a função real processar_mensagem (default), com SleepFake (sem espera).
    consumidor = _consumidor(sqs, historico)

    # 1ª iteração: delete falha por ReceiptHandle expirado -> exclusão não confirmada.
    consumidor.executar(max_iteracoes=1)
    assert consumidor.estatisticas.exclusoes_nao_confirmadas == 1
    assert consumidor.estatisticas.falhas_processamento == 0
    assert consumidor.estatisticas.processadas_com_sucesso == 0
    assert sqs.exclusoes == []  # nada excluído ainda

    # 2ª iteração: a mensagem reentregue (handle válido) é processada e excluída.
    consumidor.executar(max_iteracoes=1)
    assert consumidor.estatisticas.processadas_com_sucesso == 1
    assert consumidor.estatisticas.falhas_processamento == 0
    assert len(sqs.exclusoes) == 1  # delete confirmado na reentrega

    # Idempotência: exatamente um `processada` e um `excluida` no total.
    tipos = historico.tipos()
    assert tipos.count(EventType.PROCESSADA) == 1
    assert tipos.count(EventType.EXCLUIDA) == 1
    assert tipos.count(EventType.EXCLUSAO_NAO_CONFIRMADA) == 1
