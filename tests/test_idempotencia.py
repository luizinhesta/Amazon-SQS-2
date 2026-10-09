"""Testes unitários da verificação de idempotência por ``order_id`` (Tarefa 10.1).

Cobrem a função :func:`app.consumidores.idempotencia.verificar_idempotencia` e os
auxiliares do módulo, validando as regras do Req. 9:

- 9.1/9.4/9.6 — pedido já processado => ``JA_PROCESSADO`` + evento
  ``recebimento_repetido`` (OBSERVADO) com ``order_id`` e instante; efeitos não
  reexecutados; exatamente um processamento por ``order_id``.
- 9.2 — a verificação consulta a Tabela_Historico por ``order_id``.
- 9.3 — ``order_id`` ausente/inválido => ``REJEITAR_VALIDACAO`` + evento
  ``erro_validacao_order_id`` (OBSERVADO); mensagem preservada.
- 9.5 — falha de acesso à Tabela_Historico => ``ERRO_HISTORICO``, sem efeitos,
  mensagem preservada e evento ``erro_acesso_historico`` (OBSERVADO).

Os testes usam um ``HistoricoRepository`` **fake** em memória: ele guarda os eventos
registrados e deriva as consultas por pedido a partir deles, sem depender da AWS.
Variações do fake simulam falha de leitura e falha de escrita.

Referências de requisitos: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6.
"""

from __future__ import annotations

from typing import Optional

import pytest

from app.acesso.historico_repository import FalhaRegistroEvento
from app.consumidores.idempotencia import (
    DecisaoIdempotencia,
    ResultadoIdempotencia,
    order_id_valido,
    verificar_idempotencia,
)
from app.dominio.constantes import FILA_STANDARD, ORDER_ID_TAMANHO_MAX
from app.dominio.evento import Evento
from app.dominio.tipos import EventSource, EventType


# ---------------------------------------------------------------------------
# Fake de HistoricoRepository em memória
# ---------------------------------------------------------------------------


class HistoricoRepositoryFake:
    """Fake do :class:`HistoricoRepository` para os testes de idempotência.

    Mantém os eventos em memória, agrupados por ``order_id``. Expõe a mesma interface
    usada pela verificação de idempotência: ``consultar_por_pedido`` e
    ``registrar_evento``. Flags permitem simular falhas de leitura e de escrita para
    exercitar o caminho ``ERRO_HISTORICO`` (Req. 9.5).
    """

    def __init__(
        self,
        *,
        falhar_leitura: bool = False,
        falhar_escrita: bool = False,
    ) -> None:
        self._eventos: dict[str, list[Evento]] = {}
        self.falhar_leitura = falhar_leitura
        self.falhar_escrita = falhar_escrita
        self.eventos_registrados: list[Evento] = []
        self.consultas: list[str] = []

    # -- API usada pela verificação de idempotência -------------------------

    def consultar_por_pedido(self, order_id: str) -> list[Evento]:
        self.consultas.append(order_id)
        if self.falhar_leitura:
            raise RuntimeError("falha simulada de leitura da Tabela_Historico")
        return list(self._eventos.get(order_id, []))

    def registrar_evento(self, evento: Evento) -> None:
        if self.falhar_escrita:
            raise FalhaRegistroEvento(
                evento, tentativas=3, erro_original=RuntimeError("escrita falhou")
            )
        self.eventos_registrados.append(evento)
        self._eventos.setdefault(evento.order_id, []).append(evento)

    # -- Helper para pré-popular um pedido já processado --------------------

    def semear_pedido_processado(
        self, order_id: str, *, queue_name: str = FILA_STANDARD
    ) -> None:
        """Insere a sequência mínima para o Estado derivado ser ``Processada``."""
        base = 1_719_322_800_000
        self._eventos[order_id] = [
            _evento(order_id, EventType.ENVIADA, base, queue_name=queue_name),
            _evento(
                order_id,
                EventType.RECEBIDA,
                base + 1000,
                queue_name=queue_name,
                receive_count=1,
            ),
            _evento(order_id, EventType.PROCESSADA, base + 2000, queue_name=queue_name),
        ]


def _evento(
    order_id: str,
    event_type: EventType,
    app_time_ms: int,
    *,
    queue_name: str = FILA_STANDARD,
    receive_count: Optional[int] = None,
    event_source: EventSource = EventSource.OBSERVADO,
) -> Evento:
    return Evento(
        order_id=order_id,
        queue_name=queue_name,
        event_type=event_type,
        event_source=event_source,
        app_time="2024-06-25T12:00:00.000Z",
        app_time_ms=app_time_ms,
        receive_count=receive_count,
    )


# ---------------------------------------------------------------------------
# order_id_valido (Req. 9.3)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "order_id, esperado",
    [
        ("pedido-0001", True),
        ("x", True),  # tamanho mínimo (1 caractere)
        ("a" * ORDER_ID_TAMANHO_MAX, True),  # tamanho máximo (80)
        (None, False),  # ausente
        ("", False),  # vazio
        ("a" * (ORDER_ID_TAMANHO_MAX + 1), False),  # acima do máximo
    ],
)
def test_order_id_valido(order_id: Optional[str], esperado: bool) -> None:
    assert order_id_valido(order_id) is esperado


def test_order_id_valido_rejeita_tipo_nao_string() -> None:
    # Entradas que não são str são tratadas como inválidas.
    assert order_id_valido(123) is False  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Pedido ainda não processado => PROCESSAR (Req. 9.6)
# ---------------------------------------------------------------------------


def test_pedido_novo_autoriza_processamento() -> None:
    repo = HistoricoRepositoryFake()
    resultado = verificar_idempotencia(
        repo, order_id="pedido-0001", queue_name=FILA_STANDARD
    )

    assert resultado.decisao is DecisaoIdempotencia.PROCESSAR
    assert resultado.efeitos_permitidos is True
    assert resultado.order_id == "pedido-0001"
    # Consultou a Tabela_Historico por order_id (Req. 9.2) e não registrou eventos.
    assert repo.consultas == ["pedido-0001"]
    assert repo.eventos_registrados == []


def test_pedido_recebido_mas_nao_processado_autoriza_processamento() -> None:
    # Há eventos (enviada, recebida) mas nenhum "processada": ainda deve processar.
    repo = HistoricoRepositoryFake()
    base = 1_719_322_800_000
    repo._eventos["pedido-0002"] = [
        _evento("pedido-0002", EventType.ENVIADA, base),
        _evento("pedido-0002", EventType.RECEBIDA, base + 1000, receive_count=1),
    ]

    resultado = verificar_idempotencia(
        repo, order_id="pedido-0002", queue_name=FILA_STANDARD
    )

    assert resultado.decisao is DecisaoIdempotencia.PROCESSAR
    assert resultado.efeitos_permitidos is True


# ---------------------------------------------------------------------------
# Pedido já processado => JA_PROCESSADO + recebimento_repetido (Req. 9.1, 9.4, 9.6)
# ---------------------------------------------------------------------------


def test_pedido_ja_processado_detecta_recebimento_repetido() -> None:
    repo = HistoricoRepositoryFake()
    repo.semear_pedido_processado("pedido-0003")

    resultado = verificar_idempotencia(
        repo,
        order_id="pedido-0003",
        queue_name=FILA_STANDARD,
        consumer_id="consumidor-standard-01",
        receive_count=2,
    )

    assert resultado.decisao is DecisaoIdempotencia.JA_PROCESSADO
    # Efeitos colaterais NÃO são autorizados (Req. 9.1).
    assert resultado.efeitos_permitidos is False

    # Exatamente um evento recebimento_repetido foi registrado (OBSERVADO) com
    # order_id e instante (Req. 9.4).
    repetidos = [
        e
        for e in repo.eventos_registrados
        if e.event_type is EventType.RECEBIMENTO_REPETIDO
    ]
    assert len(repetidos) == 1
    evento = repetidos[0]
    assert evento.order_id == "pedido-0003"
    assert evento.event_source is EventSource.OBSERVADO
    assert evento.app_time_ms > 0  # instante da detecção registrado
    assert evento.app_time.endswith("Z")


def test_idempotencia_garante_um_processamento_por_order_id() -> None:
    # Property 11 em forma de exemplo: N recebimentos após o sucesso geram sempre
    # JA_PROCESSADO e nunca reautorizam efeitos (Req. 9.6).
    repo = HistoricoRepositoryFake()
    repo.semear_pedido_processado("pedido-0004")

    decisoes = [
        verificar_idempotencia(
            repo, order_id="pedido-0004", queue_name=FILA_STANDARD, receive_count=n
        ).decisao
        for n in range(1, 6)
    ]

    assert decisoes == [DecisaoIdempotencia.JA_PROCESSADO] * 5
    repetidos = [
        e
        for e in repo.eventos_registrados
        if e.event_type is EventType.RECEBIMENTO_REPETIDO
    ]
    assert len(repetidos) == 5  # um por recebimento repetido
    # Nenhum desses recebimentos autorizou efeitos.
    assert all(
        verificar_idempotencia(
            repo, order_id="pedido-0004", queue_name=FILA_STANDARD
        ).efeitos_permitidos
        is False
        for _ in range(2)
    )


# ---------------------------------------------------------------------------
# order_id inválido => REJEITAR_VALIDACAO + erro_validacao_order_id (Req. 9.3)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "order_id",
    [None, "", "a" * (ORDER_ID_TAMANHO_MAX + 1)],
)
def test_order_id_invalido_rejeita_processamento(order_id: Optional[str]) -> None:
    repo = HistoricoRepositoryFake()

    resultado = verificar_idempotencia(
        repo, order_id=order_id, queue_name=FILA_STANDARD
    )

    assert resultado.decisao is DecisaoIdempotencia.REJEITAR_VALIDACAO
    # Mensagem preservada: efeitos não autorizados.
    assert resultado.efeitos_permitidos is False
    # Não consultou idempotência (rejeita antes), apenas registrou o erro.
    assert repo.consultas == []

    erros = [
        e
        for e in repo.eventos_registrados
        if e.event_type is EventType.ERRO_VALIDACAO_ORDER_ID
    ]
    assert len(erros) == 1
    assert erros[0].event_source is EventSource.OBSERVADO
    assert erros[0].error_detail is not None  # mensagem em pt-BR descrevendo a causa


def test_order_id_invalido_com_detalhe_em_portugues() -> None:
    repo = HistoricoRepositoryFake()
    resultado = verificar_idempotencia(repo, order_id=None, queue_name=FILA_STANDARD)
    assert resultado.detalhe is not None
    assert "ausente" in resultado.detalhe.lower()


# ---------------------------------------------------------------------------
# Falha de acesso ao histórico => ERRO_HISTORICO + erro_acesso_historico (Req. 9.5)
# ---------------------------------------------------------------------------


def test_falha_de_leitura_do_historico_preserva_mensagem() -> None:
    repo = HistoricoRepositoryFake(falhar_leitura=True)

    resultado = verificar_idempotencia(
        repo, order_id="pedido-0005", queue_name=FILA_STANDARD
    )

    assert resultado.decisao is DecisaoIdempotencia.ERRO_HISTORICO
    # Não executa efeitos; preserva a mensagem (Req. 9.5).
    assert resultado.efeitos_permitidos is False
    # Tentou consultar e, após a falha, registrou erro_acesso_historico (OBSERVADO).
    assert repo.consultas == ["pedido-0005"]
    erros = [
        e
        for e in repo.eventos_registrados
        if e.event_type is EventType.ERRO_ACESSO_HISTORICO
    ]
    assert len(erros) == 1
    assert erros[0].event_source is EventSource.OBSERVADO
    assert erros[0].order_id == "pedido-0005"


def test_falha_de_escrita_do_recebimento_repetido_vira_erro_historico() -> None:
    # Pedido já processado, mas a escrita do recebimento_repetido falha: deve virar
    # ERRO_HISTORICO e preservar a mensagem, sem reexecutar efeitos (Req. 9.5).
    repo = HistoricoRepositoryFake()
    repo.semear_pedido_processado("pedido-0006")
    repo.falhar_escrita = True

    resultado = verificar_idempotencia(
        repo, order_id="pedido-0006", queue_name=FILA_STANDARD
    )

    assert resultado.decisao is DecisaoIdempotencia.ERRO_HISTORICO
    assert resultado.efeitos_permitidos is False


def test_falha_de_escrita_do_erro_de_validacao_vira_erro_historico() -> None:
    # order_id inválido E a escrita do erro_validacao também falha: trata como
    # ERRO_HISTORICO, preservando a mensagem (Req. 9.5).
    repo = HistoricoRepositoryFake(falhar_escrita=True)

    resultado = verificar_idempotencia(repo, order_id="", queue_name=FILA_STANDARD)

    assert resultado.decisao is DecisaoIdempotencia.ERRO_HISTORICO
    assert resultado.efeitos_permitidos is False


# ---------------------------------------------------------------------------
# ResultadoIdempotencia (contrato)
# ---------------------------------------------------------------------------


def test_resultado_efeitos_permitidos_somente_para_processar() -> None:
    assert (
        ResultadoIdempotencia(DecisaoIdempotencia.PROCESSAR, "p").efeitos_permitidos
        is True
    )
    for decisao in (
        DecisaoIdempotencia.JA_PROCESSADO,
        DecisaoIdempotencia.REJEITAR_VALIDACAO,
        DecisaoIdempotencia.ERRO_HISTORICO,
    ):
        assert (
            ResultadoIdempotencia(decisao, "p").efeitos_permitidos is False
        )
