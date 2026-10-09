"""Testes unitários do validador de payload de envio (Tarefa 3.3).

Cobrem os critérios de aceitação do Requisito 4:
- quantidade [1, 10] (Req. 4.2);
- order_id [1, 80] caracteres (Req. 4.3);
- Tempo_Processamento_Simulado [0, 300] (Req. 4.4);
- FIFO: MessageGroupId [1, 128] (Req. 4.6);
- FIFO com dedup por conteúdo desabilitada: MessageDeduplicationId [1, 128]
  (Req. 4.7);
- ao rejeitar, identifica ao menos um campo inválido e preserva os valores
  informados, com mensagem em pt-BR (Req. 4.9).
"""

from __future__ import annotations

from app.dominio import constantes as c
from app.dominio.validacao_envio import (
    CAMPO_MESSAGE_DEDUPLICATION_ID,
    CAMPO_MESSAGE_GROUP_ID,
    CAMPO_ORDER_ID,
    CAMPO_QUANTIDADE,
    CAMPO_TEMPO_PROCESSAMENTO,
    CAMPO_TIPO_FILA,
    PayloadEnvio,
    TipoFila,
    validar_payload_envio,
)


def _payload_standard_valido(**overrides) -> PayloadEnvio:
    base = dict(
        tipo_fila=TipoFila.STANDARD,
        quantidade=5,
        order_id="pedido-0001",
        tempo_processamento_simulado=45,
    )
    base.update(overrides)
    return PayloadEnvio(**base)


def _payload_fifo_valido(**overrides) -> PayloadEnvio:
    base = dict(
        tipo_fila=TipoFila.FIFO,
        quantidade=3,
        order_id="pedido-fifo-01",
        tempo_processamento_simulado=10,
        message_group_id="grupo-A",
        deduplicacao_por_conteudo=True,
    )
    base.update(overrides)
    return PayloadEnvio(**base)


# --- Casos válidos -----------------------------------------------------------


def test_payload_standard_valido_e_aceito() -> None:
    resultado = validar_payload_envio(_payload_standard_valido())
    assert resultado.valido is True
    assert resultado.campos_invalidos == []
    assert resultado.mensagens == {}


def test_payload_fifo_valido_com_dedup_por_conteudo_e_aceito() -> None:
    # Com dedup por conteúdo habilitada, MessageDeduplicationId não é exigido.
    resultado = validar_payload_envio(_payload_fifo_valido())
    assert resultado.valido is True


def test_payload_fifo_valido_sem_dedup_por_conteudo_exige_dedup_id() -> None:
    resultado = validar_payload_envio(
        _payload_fifo_valido(
            deduplicacao_por_conteudo=False,
            message_deduplication_id="dedup-123",
        )
    )
    assert resultado.valido is True


def test_tipo_fila_aceita_string() -> None:
    resultado = validar_payload_envio(_payload_standard_valido(tipo_fila="standard"))
    assert resultado.valido is True


# --- Limites de faixa (fronteiras) -------------------------------------------


def test_quantidade_nos_limites_e_aceita() -> None:
    for q in (c.QUANTIDADE_MENSAGENS_MIN, c.QUANTIDADE_MENSAGENS_MAX):
        assert validar_payload_envio(_payload_standard_valido(quantidade=q)).valido


def test_tempo_processamento_nos_limites_e_aceito() -> None:
    for t in (
        c.TEMPO_PROCESSAMENTO_SIMULADO_MIN,
        c.TEMPO_PROCESSAMENTO_SIMULADO_MAX,
    ):
        assert validar_payload_envio(
            _payload_standard_valido(tempo_processamento_simulado=t)
        ).valido


def test_order_id_nos_limites_e_aceito() -> None:
    for tamanho in (c.ORDER_ID_TAMANHO_MIN, c.ORDER_ID_TAMANHO_MAX):
        assert validar_payload_envio(
            _payload_standard_valido(order_id="x" * tamanho)
        ).valido


def test_message_group_id_nos_limites_e_aceito() -> None:
    for tamanho in (
        c.MESSAGE_GROUP_ID_TAMANHO_MIN,
        c.MESSAGE_GROUP_ID_TAMANHO_MAX,
    ):
        assert validar_payload_envio(
            _payload_fifo_valido(message_group_id="g" * tamanho)
        ).valido


# --- Rejeições por campo (Req. 4.2, 4.3, 4.4, 4.6, 4.7) ----------------------


def test_quantidade_abaixo_do_minimo_e_rejeitada() -> None:
    resultado = validar_payload_envio(_payload_standard_valido(quantidade=0))
    assert resultado.valido is False
    assert CAMPO_QUANTIDADE in resultado.campos_invalidos


def test_quantidade_acima_do_maximo_e_rejeitada() -> None:
    resultado = validar_payload_envio(_payload_standard_valido(quantidade=11))
    assert resultado.valido is False
    assert CAMPO_QUANTIDADE in resultado.campos_invalidos


def test_quantidade_nao_inteira_e_rejeitada() -> None:
    resultado = validar_payload_envio(_payload_standard_valido(quantidade="5"))
    assert resultado.valido is False
    assert CAMPO_QUANTIDADE in resultado.campos_invalidos


def test_quantidade_booleana_e_rejeitada() -> None:
    # bool é subtipo de int; não deve ser aceito como quantidade.
    resultado = validar_payload_envio(_payload_standard_valido(quantidade=True))
    assert resultado.valido is False
    assert CAMPO_QUANTIDADE in resultado.campos_invalidos


def test_order_id_vazio_e_rejeitado() -> None:
    resultado = validar_payload_envio(_payload_standard_valido(order_id=""))
    assert resultado.valido is False
    assert CAMPO_ORDER_ID in resultado.campos_invalidos


def test_order_id_acima_de_80_e_rejeitado() -> None:
    resultado = validar_payload_envio(
        _payload_standard_valido(order_id="x" * (c.ORDER_ID_TAMANHO_MAX + 1))
    )
    assert resultado.valido is False
    assert CAMPO_ORDER_ID in resultado.campos_invalidos


def test_order_id_nulo_e_rejeitado() -> None:
    resultado = validar_payload_envio(_payload_standard_valido(order_id=None))
    assert resultado.valido is False
    assert CAMPO_ORDER_ID in resultado.campos_invalidos


def test_tempo_abaixo_do_minimo_e_rejeitado() -> None:
    resultado = validar_payload_envio(
        _payload_standard_valido(tempo_processamento_simulado=-1)
    )
    assert resultado.valido is False
    assert CAMPO_TEMPO_PROCESSAMENTO in resultado.campos_invalidos


def test_tempo_acima_do_maximo_e_rejeitado() -> None:
    resultado = validar_payload_envio(
        _payload_standard_valido(
            tempo_processamento_simulado=c.TEMPO_PROCESSAMENTO_SIMULADO_MAX + 1
        )
    )
    assert resultado.valido is False
    assert CAMPO_TEMPO_PROCESSAMENTO in resultado.campos_invalidos


def test_tipo_fila_invalido_e_rejeitado() -> None:
    resultado = validar_payload_envio(_payload_standard_valido(tipo_fila="xpto"))
    assert resultado.valido is False
    assert CAMPO_TIPO_FILA in resultado.campos_invalidos


# --- Regras específicas de FIFO ----------------------------------------------


def test_fifo_sem_message_group_id_e_rejeitado() -> None:
    resultado = validar_payload_envio(_payload_fifo_valido(message_group_id=None))
    assert resultado.valido is False
    assert CAMPO_MESSAGE_GROUP_ID in resultado.campos_invalidos


def test_fifo_message_group_id_acima_de_128_e_rejeitado() -> None:
    resultado = validar_payload_envio(
        _payload_fifo_valido(
            message_group_id="g" * (c.MESSAGE_GROUP_ID_TAMANHO_MAX + 1)
        )
    )
    assert resultado.valido is False
    assert CAMPO_MESSAGE_GROUP_ID in resultado.campos_invalidos


def test_fifo_sem_dedup_por_conteudo_e_sem_dedup_id_e_rejeitado() -> None:
    resultado = validar_payload_envio(
        _payload_fifo_valido(
            deduplicacao_por_conteudo=False, message_deduplication_id=None
        )
    )
    assert resultado.valido is False
    assert CAMPO_MESSAGE_DEDUPLICATION_ID in resultado.campos_invalidos


def test_fifo_dedup_id_acima_de_128_e_rejeitado() -> None:
    resultado = validar_payload_envio(
        _payload_fifo_valido(
            deduplicacao_por_conteudo=False,
            message_deduplication_id="d"
            * (c.MESSAGE_DEDUPLICATION_ID_TAMANHO_MAX + 1),
        )
    )
    assert resultado.valido is False
    assert CAMPO_MESSAGE_DEDUPLICATION_ID in resultado.campos_invalidos


def test_standard_ignora_campos_fifo() -> None:
    # Em envios Standard, MessageGroupId/DeduplicationId não são exigidos.
    resultado = validar_payload_envio(
        _payload_standard_valido(message_group_id=None, message_deduplication_id=None)
    )
    assert resultado.valido is True


# --- Req. 4.9: ao menos um campo inválido, mensagens pt-BR, valores preservados


def test_rejeicao_identifica_ao_menos_um_campo_invalido() -> None:
    resultado = validar_payload_envio(
        _payload_standard_valido(quantidade=0, order_id="", tempo_processamento_simulado=-5)
    )
    assert resultado.valido is False
    assert len(resultado.campos_invalidos) >= 1
    # Cada campo inválido tem uma mensagem associada em pt-BR.
    for campo in resultado.campos_invalidos:
        assert campo in resultado.mensagens
        assert resultado.mensagens[campo].strip() != ""


def test_rejeicao_preserva_valores_informados() -> None:
    payload = _payload_standard_valido(quantidade=99, order_id="", tempo_processamento_simulado=999)
    resultado = validar_payload_envio(payload)
    assert resultado.valido is False
    # Os valores informados são preservados exatamente como recebidos (Req. 4.9).
    assert resultado.valores_informados[CAMPO_QUANTIDADE] == 99
    assert resultado.valores_informados[CAMPO_ORDER_ID] == ""
    assert resultado.valores_informados[CAMPO_TEMPO_PROCESSAMENTO] == 999


def test_campos_invalidos_sem_duplicatas() -> None:
    resultado = validar_payload_envio(
        _payload_fifo_valido(
            quantidade=0,
            order_id="",
            message_group_id=None,
        )
    )
    assert resultado.valido is False
    assert len(resultado.campos_invalidos) == len(set(resultado.campos_invalidos))
