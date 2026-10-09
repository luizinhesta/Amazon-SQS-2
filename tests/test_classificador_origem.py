"""Testes unitários do classificador de origem (Tarefa 2.2).

Verificam que ``app.dominio.classificador_origem`` classifica cada ``EventType``
na origem correta (OBSERVADO/INFERIDO), conforme a tabela "Evento_Observado vs
Evento_Inferido" do design (Req. 6.4, 6.5, 11.9).

A validação por propriedades (Property 7) é implementada na tarefa 2.3.
"""

from __future__ import annotations

import pytest

from app.dominio.classificador_origem import (
    ORIGEM_POR_TIPO,
    OrigemInvalidaError,
    classificar_origem,
    validar_origem,
)
from app.dominio.tipos import (
    ORIGEM_OBRIGATORIA_POR_TIPO,
    EventSource,
    EventType,
)

# Origens esperadas por tipo, derivadas da tabela do design (fonte da verdade
# independente do mapeamento do módulo, para detectar divergências).
INFERIDOS = {
    EventType.PROVAVEL_EXPIRACAO_VISIBILITY,
    EventType.AGUARDANDO_NOVA_TENTATIVA,
}
OBSERVADOS = set(EventType) - INFERIDOS


def test_mapeamento_cobre_todos_os_event_types() -> None:
    # A classificação deve ser total sobre EventType.
    assert set(ORIGEM_POR_TIPO) == set(EventType)


@pytest.mark.parametrize("event_type", sorted(OBSERVADOS, key=lambda t: t.value))
def test_tipos_observados(event_type: EventType) -> None:
    assert classificar_origem(event_type) is EventSource.OBSERVADO


@pytest.mark.parametrize("event_type", sorted(INFERIDOS, key=lambda t: t.value))
def test_tipos_inferidos(event_type: EventType) -> None:
    assert classificar_origem(event_type) is EventSource.INFERIDO


def test_classificar_origem_retorna_exatamente_observado_ou_inferido() -> None:
    for event_type in EventType:
        origem = classificar_origem(event_type)
        assert origem in (EventSource.OBSERVADO, EventSource.INFERIDO)


def test_consistencia_com_contrato_fixo_do_design() -> None:
    # Req. 6.5/6.6/7.9: os dois tipos de origem fixa devem bater com tipos.py.
    for tipo, origem_fixa in ORIGEM_OBRIGATORIA_POR_TIPO.items():
        assert classificar_origem(tipo) is origem_fixa
    assert (
        classificar_origem(EventType.PROVAVEL_EXPIRACAO_VISIBILITY)
        is EventSource.INFERIDO
    )
    assert classificar_origem(EventType.ENCAMINHADA_DLQ) is EventSource.OBSERVADO


def test_classificar_origem_rejeita_tipo_invalido() -> None:
    with pytest.raises(TypeError):
        classificar_origem("enviada")  # type: ignore[arg-type]


def test_validar_origem_aceita_origem_correta() -> None:
    assert (
        validar_origem(EventType.ENVIADA, EventSource.OBSERVADO)
        is EventSource.OBSERVADO
    )
    assert (
        validar_origem(
            EventType.PROVAVEL_EXPIRACAO_VISIBILITY, EventSource.INFERIDO
        )
        is EventSource.INFERIDO
    )


def test_validar_origem_rejeita_origem_incorreta() -> None:
    # provavel_expiracao_visibility nunca pode ser OBSERVADO.
    with pytest.raises(OrigemInvalidaError):
        validar_origem(
            EventType.PROVAVEL_EXPIRACAO_VISIBILITY, EventSource.OBSERVADO
        )
    # encaminhada_dlq nunca pode ser INFERIDO.
    with pytest.raises(OrigemInvalidaError):
        validar_origem(EventType.ENCAMINHADA_DLQ, EventSource.INFERIDO)


def test_validar_origem_rejeita_tipos_invalidos() -> None:
    with pytest.raises(TypeError):
        validar_origem(EventType.ENVIADA, "OBSERVADO")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        validar_origem("enviada", EventSource.OBSERVADO)  # type: ignore[arg-type]
