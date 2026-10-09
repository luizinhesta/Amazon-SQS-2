"""Testes unitários do validador de faixa genérico (Tarefa 3.1).

Verificam que o validador aceita um valor se e somente se ele estiver dentro da
faixa fechada [min, max], que ao rejeitar o valor anterior é preservado e que a
mensagem em pt-BR indica a faixa válida.

As faixas cobertas são: Tempo_Processamento_Simulado [0, 300], quantidade [1, 10],
instâncias [1, 10] e intervalo de atualização [5, 300].

Referências de requisitos: 2.8, 2.9, 2.11, 2.12, 10.3, 13.2, 13.3.
"""

from __future__ import annotations

import pytest

from app.dominio import constantes as c
from app.dominio.validacao_faixa import (
    FAIXA_INSTANCIAS_CONSUMIDOR,
    FAIXA_INTERVALO_ATUALIZACAO,
    FAIXA_QUANTIDADE_MENSAGENS,
    FAIXA_TEMPO_PROCESSAMENTO,
    Faixa,
    validar_faixa,
    validar_instancias_consumidor,
    validar_intervalo_atualizacao,
    validar_quantidade_mensagens,
    validar_tempo_processamento,
)


# ---------------------------------------------------------------------------
# Comportamento genérico de validar_faixa
# ---------------------------------------------------------------------------

def test_aceita_valor_dentro_da_faixa() -> None:
    faixa = Faixa(nome="Teste", minimo=0, maximo=10)
    resultado = validar_faixa(faixa, valor_informado=5, valor_anterior=3)
    assert resultado.aceito is True
    assert resultado.valor_efetivo == 5
    assert resultado.mensagem == ""


def test_aceita_valor_no_limite_inferior() -> None:
    # min <= valor: o próprio mínimo é aceito.
    faixa = Faixa(nome="Teste", minimo=5, maximo=300)
    resultado = validar_faixa(faixa, valor_informado=5, valor_anterior=10)
    assert resultado.aceito is True
    assert resultado.valor_efetivo == 5


def test_aceita_valor_no_limite_superior() -> None:
    # valor <= max: o próprio máximo é aceito.
    faixa = Faixa(nome="Teste", minimo=0, maximo=300)
    resultado = validar_faixa(faixa, valor_informado=300, valor_anterior=10)
    assert resultado.aceito is True
    assert resultado.valor_efetivo == 300


def test_rejeita_abaixo_do_minimo_preservando_anterior() -> None:
    faixa = Faixa(nome="Teste", minimo=5, maximo=300)
    resultado = validar_faixa(faixa, valor_informado=4, valor_anterior=42)
    assert resultado.aceito is False
    # Ao rejeitar, o valor anterior é preservado (Req. 2.12, 10.3, 13.3).
    assert resultado.valor_efetivo == 42


def test_rejeita_acima_do_maximo_preservando_anterior() -> None:
    faixa = Faixa(nome="Teste", minimo=0, maximo=10)
    resultado = validar_faixa(faixa, valor_informado=11, valor_anterior=7)
    assert resultado.aceito is False
    assert resultado.valor_efetivo == 7


def test_mensagem_em_ptbr_indica_a_faixa_e_valor_anterior() -> None:
    faixa = Faixa(nome="Intervalo de atualização", minimo=5, maximo=300, unidade="segundos")
    resultado = validar_faixa(faixa, valor_informado=1000, valor_anterior=10)
    assert resultado.aceito is False
    # A mensagem deve conter a faixa válida e o valor anterior preservado.
    assert "de 5 a 300 segundos" in resultado.mensagem
    assert "Intervalo de atualização" in resultado.mensagem
    assert "10" in resultado.mensagem


def test_faixa_invalida_min_maior_que_max() -> None:
    with pytest.raises(ValueError):
        Faixa(nome="Inválida", minimo=10, maximo=0)


# ---------------------------------------------------------------------------
# Faixas específicas do laboratório — limites a partir das constantes
# ---------------------------------------------------------------------------

def test_faixas_predefinidas_usam_as_constantes() -> None:
    assert FAIXA_TEMPO_PROCESSAMENTO.minimo == c.TEMPO_PROCESSAMENTO_SIMULADO_MIN
    assert FAIXA_TEMPO_PROCESSAMENTO.maximo == c.TEMPO_PROCESSAMENTO_SIMULADO_MAX
    assert FAIXA_QUANTIDADE_MENSAGENS.minimo == c.QUANTIDADE_MENSAGENS_MIN
    assert FAIXA_QUANTIDADE_MENSAGENS.maximo == c.QUANTIDADE_MENSAGENS_MAX
    assert FAIXA_INSTANCIAS_CONSUMIDOR.minimo == c.INSTANCIAS_CONSUMIDOR_MIN
    assert FAIXA_INSTANCIAS_CONSUMIDOR.maximo == c.INSTANCIAS_CONSUMIDOR_MAX
    assert FAIXA_INTERVALO_ATUALIZACAO.minimo == c.INTERVALO_ATUALIZACAO_MIN
    assert FAIXA_INTERVALO_ATUALIZACAO.maximo == c.INTERVALO_ATUALIZACAO_MAX


def test_tempo_processamento_aceita_limites_e_rejeita_fora() -> None:
    # Faixa [0, 300] (Req. 2.8).
    assert validar_tempo_processamento(0, 45).aceito is True
    assert validar_tempo_processamento(300, 45).aceito is True
    r = validar_tempo_processamento(301, 45)
    assert r.aceito is False and r.valor_efetivo == 45
    r = validar_tempo_processamento(-1, 45)
    assert r.aceito is False and r.valor_efetivo == 45


def test_quantidade_mensagens_aceita_limites_e_rejeita_fora() -> None:
    # Faixa [1, 10] (Req. 2.9).
    assert validar_quantidade_mensagens(1, 5).aceito is True
    assert validar_quantidade_mensagens(10, 5).aceito is True
    r = validar_quantidade_mensagens(0, 5)
    assert r.aceito is False and r.valor_efetivo == 5
    r = validar_quantidade_mensagens(11, 5)
    assert r.aceito is False and r.valor_efetivo == 5


def test_instancias_consumidor_aceita_limites_e_rejeita_fora() -> None:
    # Faixa [1, 10] (Req. 10.3).
    assert validar_instancias_consumidor(1, 1).aceito is True
    assert validar_instancias_consumidor(10, 1).aceito is True
    r = validar_instancias_consumidor(0, 2)
    assert r.aceito is False and r.valor_efetivo == 2
    r = validar_instancias_consumidor(11, 2)
    assert r.aceito is False and r.valor_efetivo == 2


def test_intervalo_atualizacao_aceita_limites_e_rejeita_fora() -> None:
    # Faixa [5, 300] (Req. 13.2, 13.3).
    assert validar_intervalo_atualizacao(5, 10).aceito is True
    assert validar_intervalo_atualizacao(300, 10).aceito is True
    r = validar_intervalo_atualizacao(4, 10)
    assert r.aceito is False and r.valor_efetivo == 10
    r = validar_intervalo_atualizacao(301, 10)
    assert r.aceito is False and r.valor_efetivo == 10
