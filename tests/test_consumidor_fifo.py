"""Testes unitários do loop do Consumidor_FIFO (Tarefa 13.1).

Validam o comportamento de :mod:`app.consumidores.consumidor_fifo` usando dublês
(fakes) de ``SqsGateway`` e ``HistoricoRepository`` — sem AWS real e sem espera real.

Foco dos testes (requisitos cobertos):
- 8.2 / 14.6: recebimento em lotes de 1 a 10 com Long_Polling (WaitTimeSeconds=20).
- 10.5: a ordem de recebimento dentro de um ``MessageGroupId`` é preservada no
  processamento.
- 10.6: a instância processa **um único** ``MessageGroupId`` por iteração; os demais
  grupos do lote não são processados.
- 4.6 / 4.7: envio com Deduplicacao_Por_Conteudo e, quando desabilitada,
  ``MessageDeduplicationId`` obrigatório.
- Idempotência (Req. 9) orienta o processamento; ``consumer_id`` é único; o loop é
  determinístico via critério de parada injetável.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest

from app.acesso.sqs_gateway import MensagemRecebida
from app.consumidores.consumidor_fifo import (
    ConsumidorFifo,
    agrupar_por_message_group_id,
    enviar_mensagem_fifo,
    selecionar_grupo,
)
from app.consumidores.idempotencia import DecisaoIdempotencia, ResultadoIdempotencia
from app.consumidores.processamento import CenarioFalha, ResultadoProcessamento
from app.dominio import constantes as c


# ---------------------------------------------------------------------------
# Dublês (fakes) das dependências injetáveis
# ---------------------------------------------------------------------------
class HistoricoFake:
    """Fake mínimo de HistoricoRepository (acumula eventos em memória)."""

    def __init__(self) -> None:
        self.eventos: list[Any] = []

    def registrar_evento(self, evento: Any) -> None:  # pragma: no cover - trivial
        self.eventos.append(evento)

    def consultar_por_pedido(self, order_id: str) -> list[Any]:  # pragma: no cover
        return []


class SqsFake:
    """Fake de SqsGateway: devolve lotes pré-programados e registra chamadas.

    - ``lotes``: fila de lotes a devolver em cada ``receive_messages`` (quando
      esgota, devolve lista vazia, simulando recebimento vazio).
    - ``recebimentos``: registra os kwargs de cada ``receive_messages`` para
      verificar os parâmetros do Long_Polling.
    - ``envios``: registra os kwargs de cada ``send_message``.
    - ``exclusoes``: registra as chamadas de ``delete_message``.
    """

    def __init__(self, lotes: Optional[list[list[MensagemRecebida]]] = None) -> None:
        self._lotes = list(lotes or [])
        self.recebimentos: list[dict[str, Any]] = []
        self.envios: list[dict[str, Any]] = []
        self.exclusoes: list[tuple[str, str]] = []

    def receive_messages(
        self,
        nome_fila: str,
        *,
        max_mensagens: int = 10,
        wait_time_seconds: int = 20,
        visibility_timeout: Optional[int] = None,
    ) -> list[MensagemRecebida]:
        self.recebimentos.append(
            {
                "nome_fila": nome_fila,
                "max_mensagens": max_mensagens,
                "wait_time_seconds": wait_time_seconds,
            }
        )
        if self._lotes:
            return self._lotes.pop(0)
        return []

    def send_message(
        self,
        nome_fila: str,
        corpo: str,
        *,
        message_group_id: Optional[str] = None,
        message_deduplication_id: Optional[str] = None,
        message_attributes: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        self.envios.append(
            {
                "nome_fila": nome_fila,
                "corpo": corpo,
                "message_group_id": message_group_id,
                "message_deduplication_id": message_deduplication_id,
                "message_attributes": message_attributes,
            }
        )
        return {"MessageId": f"msg-{len(self.envios)}"}

    def delete_message(self, nome_fila: str, receipt_handle: str) -> dict[str, Any]:
        self.exclusoes.append((nome_fila, receipt_handle))
        return {}


def _msg(
    *,
    order_id: str,
    grupo: str,
    message_id: Optional[str] = None,
    receive_count: int = 1,
) -> MensagemRecebida:
    """Cria uma MensagemRecebida FIFO. O corpo carrega o ``order_id`` (laboratório)."""
    return MensagemRecebida(
        message_id=message_id or f"mid-{order_id}",
        receipt_handle=f"rh-{order_id}",
        body=order_id,
        approximate_receive_count=receive_count,
        message_group_id=grupo,
    )


def _idempotencia_sempre_processar(
    repositorio: Any,
    *,
    order_id: Optional[str],
    queue_name: str,
    consumer_id: Optional[str] = None,
    receive_count: Optional[int] = None,
    message_group_id: Optional[str] = None,
    sqs_message_id: Optional[str] = None,
    max_receive_count: int = 2,
) -> ResultadoIdempotencia:
    """Dublê de idempotência que sempre autoriza o processamento."""
    return ResultadoIdempotencia(
        decisao=DecisaoIdempotencia.PROCESSAR, order_id=order_id, detalhe=None
    )


class ProcessadorFake:
    """Dublê de processar_mensagem: registra a ordem de processamento por order_id."""

    def __init__(self) -> None:
        self.ordem_processada: list[str] = []
        self.grupos_processados: list[Optional[str]] = []

    def __call__(
        self,
        mensagem: MensagemRecebida,
        *,
        nome_fila: str,
        consumer_id: str,
        cenario: CenarioFalha,
        processing_seconds: int,
        sqs: Any,
        historico: Any,
        sleep: Any = None,
        message_group_id: Optional[str] = None,
        order_id: Optional[str] = None,
    ) -> ResultadoProcessamento:
        order = order_id if order_id is not None else mensagem.body
        self.ordem_processada.append(order)
        self.grupos_processados.append(message_group_id)
        return ResultadoProcessamento(
            order_id=order, cenario=cenario, sucesso=True, excluida=True
        )


def _consumidor(
    sqs: SqsFake,
    *,
    processador: Optional[ProcessadorFake] = None,
    historico: Optional[HistoricoFake] = None,
    consumer_id: Optional[str] = None,
) -> ConsumidorFifo:
    return ConsumidorFifo(
        sqs=sqs,  # type: ignore[arg-type]
        historico=historico or HistoricoFake(),  # type: ignore[arg-type]
        consumer_id=consumer_id,
        verificar_idempotencia_fn=_idempotencia_sempre_processar,
        processar_mensagem_fn=processador or ProcessadorFake(),
        sleep=lambda _s: None,
    )


def _parar_apos(n: int):
    """Critério de parada que permite exatamente ``n`` iterações."""
    estado = {"restantes": n}

    def criterio() -> bool:
        if estado["restantes"] <= 0:
            return True
        estado["restantes"] -= 1
        return False

    return criterio


# ---------------------------------------------------------------------------
# Agrupamento e seleção de grupo (núcleo da regra FIFO)
# ---------------------------------------------------------------------------
def test_agrupar_preserva_ordem_de_recebimento_dentro_do_grupo() -> None:
    # Req. 10.5: ordem de recebimento preservada por grupo.
    lote = [
        _msg(order_id="a1", grupo="G1"),
        _msg(order_id="b1", grupo="G2"),
        _msg(order_id="a2", grupo="G1"),
        _msg(order_id="a3", grupo="G1"),
    ]
    grupos = agrupar_por_message_group_id(lote)
    assert [m.body for m in grupos["G1"]] == ["a1", "a2", "a3"]
    assert [m.body for m in grupos["G2"]] == ["b1"]


def test_selecionar_grupo_escolhe_o_primeiro_na_ordem_de_recebimento() -> None:
    # Req. 10.6: um único grupo por vez; escolhe o primeiro do lote.
    lote = [
        _msg(order_id="b1", grupo="G2"),
        _msg(order_id="a1", grupo="G1"),
        _msg(order_id="b2", grupo="G2"),
    ]
    grupo, mensagens = selecionar_grupo(lote)
    assert grupo == "G2"
    assert [m.body for m in mensagens] == ["b1", "b2"]


def test_selecionar_grupo_lote_vazio() -> None:
    grupo, mensagens = selecionar_grupo([])
    assert grupo is None
    assert mensagens == []


# ---------------------------------------------------------------------------
# Loop: preservação da ordem e processamento de um único grupo por vez
# ---------------------------------------------------------------------------
def test_iteracao_preserva_ordem_dentro_do_grupo() -> None:
    # Req. 10.5: processa na ordem de recebimento dentro do grupo.
    lote = [
        _msg(order_id="p1", grupo="G1"),
        _msg(order_id="p2", grupo="G1"),
        _msg(order_id="p3", grupo="G1"),
    ]
    sqs = SqsFake(lotes=[lote])
    processador = ProcessadorFake()
    consumidor = _consumidor(sqs, processador=processador)

    resultado = consumidor.processar_uma_iteracao()

    assert resultado.grupo_selecionado == "G1"
    assert resultado.ordem_recebimento_do_grupo == ["p1", "p2", "p3"]
    assert resultado.processadas_do_grupo == ["p1", "p2", "p3"]
    # A ordem de processamento no processador segue a ordem de recebimento.
    assert processador.ordem_processada == ["p1", "p2", "p3"]


def test_iteracao_processa_apenas_um_grupo_por_vez() -> None:
    # Req. 10.6: só o grupo selecionado é processado; os demais ficam na fila.
    lote = [
        _msg(order_id="a1", grupo="G1"),
        _msg(order_id="b1", grupo="G2"),
        _msg(order_id="a2", grupo="G1"),
        _msg(order_id="b2", grupo="G2"),
    ]
    sqs = SqsFake(lotes=[lote])
    processador = ProcessadorFake()
    consumidor = _consumidor(sqs, processador=processador)

    resultado = consumidor.processar_uma_iteracao()

    # Apenas o grupo G1 (primeiro do lote) foi processado nesta iteração.
    assert resultado.grupo_selecionado == "G1"
    assert processador.ordem_processada == ["a1", "a2"]
    assert set(processador.grupos_processados) == {"G1"}
    # Mensagens do G2 NÃO foram processadas (preservadas na fila).
    assert "b1" not in processador.ordem_processada
    assert "b2" not in processador.ordem_processada


def test_loop_consome_grupos_em_iteracoes_sucessivas() -> None:
    # Dois lotes: cada iteração processa um único grupo; o segundo grupo é
    # processado em uma iteração posterior (Req. 10.6).
    lote1 = [_msg(order_id="a1", grupo="G1"), _msg(order_id="b1", grupo="G2")]
    lote2 = [_msg(order_id="b2", grupo="G2")]
    sqs = SqsFake(lotes=[lote1, lote2])
    processador = ProcessadorFake()
    consumidor = _consumidor(sqs, processador=processador)

    resumo = consumidor.executar(criterio_parada=_parar_apos(2))

    assert len(resumo.iteracoes) == 2
    assert resumo.iteracoes[0].grupo_selecionado == "G1"
    assert resumo.iteracoes[0].processadas_do_grupo == ["a1"]
    assert resumo.iteracoes[1].grupo_selecionado == "G2"
    assert resumo.iteracoes[1].processadas_do_grupo == ["b2"]
    assert resumo.total_processadas == 2


# ---------------------------------------------------------------------------
# Long_Polling e recebimento vazio
# ---------------------------------------------------------------------------
def test_recebimento_usa_long_polling_20s_e_lote_valido() -> None:
    # Req. 8.2, 14.6: WaitTimeSeconds=20 e lote de 1 a 10.
    sqs = SqsFake(lotes=[[_msg(order_id="p1", grupo="G1")]])
    consumidor = _consumidor(sqs)

    consumidor.processar_uma_iteracao()

    assert len(sqs.recebimentos) == 1
    chamada = sqs.recebimentos[0]
    assert chamada["nome_fila"] == c.FILA_FIFO
    assert chamada["wait_time_seconds"] == c.LONG_POLLING_SEGUNDOS == 20
    assert 1 <= chamada["max_mensagens"] <= 10


def test_recebimento_vazio_repete_sem_erro() -> None:
    # Req. 8.11: recebimento vazio não gera erro; o loop apenas repete.
    sqs = SqsFake(lotes=[])  # sempre vazio
    consumidor = _consumidor(sqs)

    resumo = consumidor.executar(criterio_parada=_parar_apos(3))

    assert len(resumo.iteracoes) == 3
    assert all(it.recebidas == 0 for it in resumo.iteracoes)
    assert all(it.grupo_selecionado is None for it in resumo.iteracoes)
    assert resumo.total_processadas == 0


def test_max_mensagens_fora_da_faixa_rejeitado() -> None:
    # Req. 8.2: lote deve estar entre 1 e 10.
    with pytest.raises(ValueError):
        ConsumidorFifo(
            sqs=SqsFake(),  # type: ignore[arg-type]
            historico=HistoricoFake(),  # type: ignore[arg-type]
            max_mensagens=11,
        )


# ---------------------------------------------------------------------------
# Idempotência orienta o processamento
# ---------------------------------------------------------------------------
def test_idempotencia_ja_processado_nao_processa_mas_preserva_ordem() -> None:
    # Quando a idempotência decide JA_PROCESSADO, o efeito não é executado,
    # mas a ordem de recebimento do grupo continua registrada (Req. 9, 10.5).
    lote = [
        _msg(order_id="p1", grupo="G1"),
        _msg(order_id="p2", grupo="G1"),
    ]

    def idem(repositorio, *, order_id, **kwargs) -> ResultadoIdempotencia:
        decisao = (
            DecisaoIdempotencia.JA_PROCESSADO
            if order_id == "p1"
            else DecisaoIdempotencia.PROCESSAR
        )
        return ResultadoIdempotencia(decisao=decisao, order_id=order_id)

    sqs = SqsFake(lotes=[lote])
    processador = ProcessadorFake()
    consumidor = ConsumidorFifo(
        sqs=sqs,  # type: ignore[arg-type]
        historico=HistoricoFake(),  # type: ignore[arg-type]
        verificar_idempotencia_fn=idem,
        processar_mensagem_fn=processador,
        sleep=lambda _s: None,
    )

    resultado = consumidor.processar_uma_iteracao()

    assert resultado.ordem_recebimento_do_grupo == ["p1", "p2"]
    assert resultado.processadas_do_grupo == ["p2"]  # p1 não reexecuta
    assert processador.ordem_processada == ["p2"]


# ---------------------------------------------------------------------------
# consumer_id único
# ---------------------------------------------------------------------------
def test_consumer_id_gerado_e_unico() -> None:
    # Req. 8.4, 10.4: cada instância recebe um consumer_id único.
    c1 = _consumidor(SqsFake()).consumer_id
    c2 = _consumidor(SqsFake()).consumer_id
    assert c1 != c2
    assert c1.startswith("consumidor-fifo-")


def test_consumer_id_informado_eh_respeitado_e_usado_na_idempotencia() -> None:
    capturados: list[Optional[str]] = []

    def idem(repositorio, *, order_id, consumer_id=None, **kwargs) -> ResultadoIdempotencia:
        capturados.append(consumer_id)
        return ResultadoIdempotencia(
            decisao=DecisaoIdempotencia.PROCESSAR, order_id=order_id
        )

    sqs = SqsFake(lotes=[[_msg(order_id="p1", grupo="G1")]])
    consumidor = ConsumidorFifo(
        sqs=sqs,  # type: ignore[arg-type]
        historico=HistoricoFake(),  # type: ignore[arg-type]
        consumer_id="consumidor-fifo-fixo",
        verificar_idempotencia_fn=idem,
        processar_mensagem_fn=ProcessadorFake(),
        sleep=lambda _s: None,
    )

    consumidor.processar_uma_iteracao()

    assert consumidor.consumer_id == "consumidor-fifo-fixo"
    assert capturados == ["consumidor-fifo-fixo"]


# ---------------------------------------------------------------------------
# Envio FIFO: Deduplicacao_Por_Conteudo on/off (Req. 4.6, 4.7)
# ---------------------------------------------------------------------------
def test_envio_com_deduplicacao_por_conteudo_nao_envia_dedup_id() -> None:
    # Req. 4.7: com Deduplicacao_Por_Conteudo, o SQS calcula o dedup pelo conteúdo.
    sqs = SqsFake()
    enviar_mensagem_fifo(
        sqs,  # type: ignore[arg-type]
        corpo="pedido-1",
        message_group_id="G1",
        deduplicacao_por_conteudo=True,
        # Mesmo informando um dedup id, ele é ignorado neste modo.
        message_deduplication_id="ignorado",
    )
    assert len(sqs.envios) == 1
    envio = sqs.envios[0]
    assert envio["nome_fila"] == c.FILA_FIFO
    assert envio["message_group_id"] == "G1"
    assert envio["message_deduplication_id"] is None


def test_envio_sem_deduplicacao_por_conteudo_usa_dedup_id_informado() -> None:
    # Req. 4.7: sem Deduplicacao_Por_Conteudo, o MessageDeduplicationId é usado.
    sqs = SqsFake()
    enviar_mensagem_fifo(
        sqs,  # type: ignore[arg-type]
        corpo="pedido-1",
        message_group_id="G1",
        deduplicacao_por_conteudo=False,
        message_deduplication_id="dedup-123",
    )
    envio = sqs.envios[0]
    assert envio["message_group_id"] == "G1"
    assert envio["message_deduplication_id"] == "dedup-123"


def test_envio_sem_deduplicacao_por_conteudo_sem_dedup_id_falha() -> None:
    # Req. 4.7: dedup id é obrigatório quando a dedup por conteúdo está desabilitada.
    sqs = SqsFake()
    with pytest.raises(ValueError):
        enviar_mensagem_fifo(
            sqs,  # type: ignore[arg-type]
            corpo="pedido-1",
            message_group_id="G1",
            deduplicacao_por_conteudo=False,
            message_deduplication_id=None,
        )
    assert sqs.envios == []  # nenhuma chamada à AWS antes da validação


def test_envio_sem_message_group_id_falha() -> None:
    # Req. 4.6: MessageGroupId é obrigatório em fila FIFO.
    sqs = SqsFake()
    with pytest.raises(ValueError):
        enviar_mensagem_fifo(
            sqs,  # type: ignore[arg-type]
            corpo="pedido-1",
            message_group_id="",
            deduplicacao_por_conteudo=True,
        )
    assert sqs.envios == []


# ---------------------------------------------------------------------------
# Resiliência: guarda por iteração e exclusão não confirmada (FEAT-003)
# ---------------------------------------------------------------------------


class _SqsReceberLancaUmaVez(SqsFake):
    """SqsFake cujo ``receive_messages`` lança na primeira chamada e depois volta.

    Após a primeira falha, devolve lotes vazios (recebimento normal), para verificar
    que a guarda por iteração NÃO encerra a thread do consumidor FIFO.
    """

    def __init__(self) -> None:
        super().__init__(lotes=[])
        self.chamadas = 0

    def receive_messages(self, nome_fila, *, max_mensagens=10, wait_time_seconds=20, visibility_timeout=None):
        self.chamadas += 1
        if self.chamadas == 1:
            raise RuntimeError("falha pontual simulada do SQS")
        return []


def test_excecao_pontual_na_iteracao_nao_encerra_o_laco_fifo() -> None:
    # Critério de aceitação: exceção pontual é logada (WARNING), SEMPRE anexa um
    # ResultadoIteracaoFifo (append nunca pulado) e o laço continua.
    sqs = _SqsReceberLancaUmaVez()
    consumidor = _consumidor(sqs)

    resumo = consumidor.executar(criterio_parada=_parar_apos(3))

    assert len(resumo.iteracoes) == 3  # o append da iteração de erro não foi pulado
    assert resumo.iteracoes_com_erro == 1
    assert resumo.iteracoes[0].erro is not None
    # As duas iterações seguintes correram normalmente (recebimento vazio).
    assert resumo.iteracoes[1].erro is None
    assert resumo.iteracoes[2].erro is None


def test_parada_cooperativa_nao_e_capturada_pela_guarda_fifo() -> None:
    # Critério de aceitação: BaseException NÃO é capturada pela guarda (só Exception).
    class _SqsBaseException(SqsFake):
        def __init__(self) -> None:
            super().__init__(lotes=[])

        def receive_messages(self, *a, **k):
            raise KeyboardInterrupt("parada cooperativa simulada")

    consumidor = _consumidor(_SqsBaseException())

    with pytest.raises(KeyboardInterrupt):
        consumidor.executar(criterio_parada=_parar_apos(3))


class _ProcessadorExclusaoNaoConfirmada:
    """Dublê de processar_mensagem que sinaliza exclusao_nao_confirmada por order_id."""

    def __init__(self, order_ids_nao_confirmados: set[str]) -> None:
        self._nao_confirmados = order_ids_nao_confirmados

    def __call__(self, mensagem, *, order_id=None, cenario, **kwargs) -> ResultadoProcessamento:
        order = order_id if order_id is not None else mensagem.body
        nao_confirmada = order in self._nao_confirmados
        return ResultadoProcessamento(
            order_id=order,
            cenario=cenario,
            sucesso=not nao_confirmada,
            excluida=not nao_confirmada,
            exclusao_nao_confirmada=nao_confirmada,
        )


def test_exclusao_nao_confirmada_contada_separada_no_fifo() -> None:
    # Critério de aceitação (Opção A): a exclusão não confirmada é contabilizada
    # separada das falhas; a iteração expõe os order_id afetados.
    lote = [
        _msg(order_id="p1", grupo="G1"),
        _msg(order_id="p2", grupo="G1"),
    ]
    sqs = SqsFake(lotes=[lote])
    processador = _ProcessadorExclusaoNaoConfirmada({"p2"})
    consumidor = _consumidor(sqs, processador=processador)  # type: ignore[arg-type]

    resultado = consumidor.processar_uma_iteracao()

    assert resultado.processadas_do_grupo == ["p1", "p2"]
    assert resultado.exclusoes_nao_confirmadas == ["p2"]
