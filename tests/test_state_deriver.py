"""Testes unitários básicos do StateDeriver (Tarefa 4.1).

Cobrem casos concretos de cada estado intermediário e terminal da máquina de
estados do design.md, além da regra crítica de que ``Encaminhada para DLQ`` só é
derivado com ``encaminhada_dlq`` confirmado (OBSERVADO) e
``receive_count >= maxReceiveCount`` — nunca por expiração de Visibility_Timeout
(Req. 5.4, 5.9, 5.10, 6.6, 7.9, 7.10).

Os testes de propriedade (unicidade do Estado e não-inferência de DLQ) são as
tarefas separadas 4.2-4.4.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.dominio.constantes import MAX_RECEIVE_COUNT
from app.dominio.state_deriver import derivar_estado
from app.dominio.tipos import (
    ESTADOS_PERMITIDOS,
    Estado,
    EventSource,
    EventType,
)


@dataclass
class _Evento:
    """Evento mínimo para os testes (compatível com o contrato EventoLike)."""

    event_type: EventType
    event_source: EventSource = EventSource.OBSERVADO
    receive_count: Optional[int] = None


def _enviada() -> _Evento:
    return _Evento(EventType.ENVIADA)


def _recebida(n: int) -> _Evento:
    return _Evento(EventType.RECEBIDA, receive_count=n)


# ---------------------------------------------------------------------------
# Log vazio e estado inicial
# ---------------------------------------------------------------------------


def test_log_vazio_resulta_em_aguardando() -> None:
    assert derivar_estado([]) is Estado.AGUARDANDO


def test_apenas_enviada_resulta_em_aguardando() -> None:
    # Req. 5.4: há "enviada" e nenhum "recebida" → Aguardando.
    assert derivar_estado([_enviada()]) is Estado.AGUARDANDO


# ---------------------------------------------------------------------------
# Estados intermediários
# ---------------------------------------------------------------------------


def test_recebida_resulta_em_recebida() -> None:
    eventos = [_enviada(), _recebida(1)]
    assert derivar_estado(eventos) is Estado.RECEBIDA


def test_processamento_iniciado_resulta_em_em_processamento() -> None:
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(EventType.PROCESSAMENTO_INICIADO),
    ]
    assert derivar_estado(eventos) is Estado.EM_PROCESSAMENTO


def test_falha_abaixo_do_limite_resulta_em_falha() -> None:
    # receive_count < maxReceiveCount → Falha (ainda sem esgotar tentativas).
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(EventType.PROCESSAMENTO_INICIADO),
        _Evento(EventType.FALHA, receive_count=1),
    ]
    assert derivar_estado(eventos) is Estado.FALHA


def test_expiracao_visibility_resulta_em_aguardando_nova_tentativa() -> None:
    # Evento_Inferido de provável expiração → mensagem voltou a ficar visível.
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(
            EventType.PROVAVEL_EXPIRACAO_VISIBILITY,
            event_source=EventSource.INFERIDO,
            receive_count=1,
        ),
    ]
    assert derivar_estado(eventos) is Estado.AGUARDANDO_NOVA_TENTATIVA


def test_reentrega_apos_falha_volta_para_recebida() -> None:
    # Após expiração/falha, nova "recebida" com receive_count incrementado.
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(EventType.FALHA, receive_count=1),
        _Evento(
            EventType.PROVAVEL_EXPIRACAO_VISIBILITY,
            event_source=EventSource.INFERIDO,
            receive_count=1,
        ),
        _recebida(2),
    ]
    assert derivar_estado(eventos) is Estado.RECEBIDA


# ---------------------------------------------------------------------------
# Estados terminais
# ---------------------------------------------------------------------------


def test_processada_resulta_em_processada() -> None:
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(EventType.PROCESSAMENTO_INICIADO),
        _Evento(EventType.PROCESSADA),
    ]
    assert derivar_estado(eventos) is Estado.PROCESSADA


def test_processada_com_excluida_mantem_processada() -> None:
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(EventType.PROCESSAMENTO_INICIADO),
        _Evento(EventType.PROCESSADA),
        _Evento(EventType.EXCLUIDA),
    ]
    assert derivar_estado(eventos) is Estado.PROCESSADA


def test_dlq_confirmada_resulta_em_encaminhada_dlq() -> None:
    # encaminhada_dlq OBSERVADO com receive_count >= maxReceiveCount → terminal DLQ.
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(EventType.FALHA, receive_count=1),
        _recebida(2),
        _Evento(EventType.FALHA, receive_count=2),
        _Evento(
            EventType.ENCAMINHADA_DLQ,
            event_source=EventSource.OBSERVADO,
            receive_count=MAX_RECEIVE_COUNT,
        ),
    ]
    assert derivar_estado(eventos) is Estado.ENCAMINHADA_DLQ


# ---------------------------------------------------------------------------
# Regra crítica: DLQ nunca por expiração de Visibility_Timeout (Req. 7.9, 7.10)
# ---------------------------------------------------------------------------


def test_expiracao_nao_deriva_dlq_mesmo_atingindo_o_limite() -> None:
    # Mesmo com várias expirações e receive_count >= limite, sem o evento
    # encaminhada_dlq OBSERVADO o Estado nunca é "Encaminhada para DLQ".
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(
            EventType.PROVAVEL_EXPIRACAO_VISIBILITY,
            event_source=EventSource.INFERIDO,
            receive_count=1,
        ),
        _recebida(2),
        _Evento(
            EventType.PROVAVEL_EXPIRACAO_VISIBILITY,
            event_source=EventSource.INFERIDO,
            receive_count=MAX_RECEIVE_COUNT,
        ),
    ]
    estado = derivar_estado(eventos)
    assert estado is not Estado.ENCAMINHADA_DLQ
    assert estado is Estado.AGUARDANDO_NOVA_TENTATIVA


def test_dlq_inferida_nao_confirma_encaminhamento() -> None:
    # Um evento encaminhada_dlq marcado como INFERIDO não confirma a DLQ.
    eventos = [
        _enviada(),
        _recebida(2),
        _Evento(
            EventType.ENCAMINHADA_DLQ,
            event_source=EventSource.INFERIDO,
            receive_count=MAX_RECEIVE_COUNT,
        ),
    ]
    assert derivar_estado(eventos) is not Estado.ENCAMINHADA_DLQ


def test_dlq_observada_abaixo_do_limite_nao_confirma() -> None:
    # encaminhada_dlq OBSERVADO porém com receive_count < maxReceiveCount não deve
    # ser aceito como confirmação de encaminhamento.
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(
            EventType.ENCAMINHADA_DLQ,
            event_source=EventSource.OBSERVADO,
            receive_count=MAX_RECEIVE_COUNT - 1,
        ),
    ]
    assert derivar_estado(eventos) is not Estado.ENCAMINHADA_DLQ


# ---------------------------------------------------------------------------
# Contrato geral: sempre retorna exatamente um Estado permitido
# ---------------------------------------------------------------------------


def test_retorno_pertence_sempre_ao_conjunto_permitido() -> None:
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(EventType.PROCESSAMENTO_INICIADO),
        _Evento(EventType.PROCESSADA),
    ]
    assert derivar_estado(eventos) in ESTADOS_PERMITIDOS


def test_aceita_eventos_com_strings_em_vez_de_enums() -> None:
    # O deriver normaliza event_type/event_source vindos como string (ex.: item
    # desserializado da Tabela_Historico).
    eventos = [
        _Evento(event_type="enviada"),  # type: ignore[arg-type]
        _Evento(event_type="recebida", receive_count=1),  # type: ignore[arg-type]
    ]
    assert derivar_estado(eventos) is Estado.RECEBIDA


# ---------------------------------------------------------------------------
# FEAT-006 — Novo estado EXCLUSAO_NAO_CONFIRMADA: desfecho recente, reentrega
# ativa e precedência da DLQ (apoio aos testes obrigatórios 5 e 8).
# ---------------------------------------------------------------------------


def test_exclusao_nao_confirmada_como_desfecho_mais_recente() -> None:
    # O processamento terminou mas o DeleteMessage falhou (ReceiptHandle expirado):
    # quando `exclusao_nao_confirmada` é o desfecho mais recente do ciclo, o estado
    # derivado é EXCLUSAO_NAO_CONFIRMADA (não terminal).
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(EventType.PROCESSAMENTO_INICIADO),
        _Evento(EventType.EXCLUSAO_NAO_CONFIRMADA, receive_count=1),
    ]
    assert derivar_estado(eventos) is Estado.EXCLUSAO_NAO_CONFIRMADA


def test_reentrega_ativa_sobrepoe_exclusao_nao_confirmada() -> None:
    # Numa reentrega ativa, um processamento_iniciado posterior sobrepõe o
    # EXCLUSAO_NAO_CONFIRMADA anterior (não terminal): o estado volta a
    # EM_PROCESSAMENTO enquanto a mensagem é reprocessada.
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(EventType.PROCESSAMENTO_INICIADO),
        _Evento(EventType.EXCLUSAO_NAO_CONFIRMADA, receive_count=1),
        _recebida(2),
        _Evento(EventType.PROCESSAMENTO_INICIADO),
    ]
    assert derivar_estado(eventos) is Estado.EM_PROCESSAMENTO


def test_processada_tem_precedencia_sobre_exclusao_nao_confirmada() -> None:
    # Se a reentrega concluir com sucesso (processada), o estado terminal PROCESSADA
    # prevalece sobre o EXCLUSAO_NAO_CONFIRMADA anterior (teste obrigatório 5).
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(EventType.PROCESSAMENTO_INICIADO),
        _Evento(EventType.EXCLUSAO_NAO_CONFIRMADA, receive_count=1),
        _recebida(2),
        _Evento(EventType.PROCESSAMENTO_INICIADO),
        _Evento(EventType.PROCESSADA),
        _Evento(EventType.EXCLUIDA),
    ]
    assert derivar_estado(eventos) is Estado.PROCESSADA


def test_dlq_tem_precedencia_sobre_exclusao_nao_confirmada() -> None:
    # Teste obrigatório 8: encaminhada_dlq OBSERVADO com receive_count >=
    # MAX_RECEIVE_COUNT -> ENCAMINHADA_DLQ, com precedência sobre
    # EXCLUSAO_NAO_CONFIRMADA (DLQ é resolvida antes do laço).
    eventos = [
        _enviada(),
        _recebida(1),
        _Evento(EventType.EXCLUSAO_NAO_CONFIRMADA, receive_count=1),
        _recebida(MAX_RECEIVE_COUNT),
        _Evento(
            EventType.ENCAMINHADA_DLQ,
            event_source=EventSource.OBSERVADO,
            receive_count=MAX_RECEIVE_COUNT,
        ),
    ]
    assert derivar_estado(eventos) is Estado.ENCAMINHADA_DLQ
