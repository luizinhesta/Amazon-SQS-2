"""Testes unitários da verificação de inicialização dos recursos (Tarefa 9.1).

Validam, com SqsGateway sobre cliente SQS simulado e um verificador de tabela
DynamoDB injetado (sem AWS real):

- recursos presentes: todas as 4 filas e a Tabela_Historico disponíveis → sucesso
  (Req. 1.6);
- recursos ausentes/inacessíveis: fila ou tabela faltando → falha identificando o
  recurso por nome e tipo em pt-BR e preservando o erro original (Req. 1.8);
- interrupção *fail-fast* sem alterar recursos: para no primeiro indisponível e
  não executa operações de escrita/criação/exclusão (Req. 1.5, 1.7, 1.9);
- modo sem interrupção: devolve o resultado com sucesso=False sem levantar exceção.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from app.acesso.sqs_gateway import SqsGateway
from app.dominio import constantes as c
from app.verificacao_inicializacao import (
    FalhaVerificacaoInicializacao,
    TipoRecurso,
    descrever_tabela_boto3,
    verificar_inicializacao,
)


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
    verificador = MagicMock(return_value={"Table": {"TableStatus": "ACTIVE"}})
    return verificador


# --- Recursos presentes ------------------------------------------------------


def test_todos_os_recursos_presentes_retorna_sucesso() -> None:
    gateway = SqsGateway(cliente_sqs=_cliente_sqs_ok())
    verificador = _verificador_tabela_ok()

    resultado = verificar_inicializacao(gateway, verificador)

    assert resultado.sucesso is True
    assert resultado.recurso_indisponivel is None
    assert resultado.erro_original is None
    # Verifica as 4 filas + a Tabela_Historico (5 recursos).
    assert len(resultado.recursos_verificados) == len(c.FILAS) + 1
    assert all(r.disponivel for r in resultado.recursos_verificados)
    # A tabela foi verificada pelo nome esperado.
    verificador.assert_called_once_with(c.TABELA_HISTORICO)


def test_sucesso_verifica_exatamente_as_quatro_filas_esperadas() -> None:
    cliente = _cliente_sqs_ok()
    gateway = SqsGateway(cliente_sqs=cliente)

    resultado = verificar_inicializacao(gateway, _verificador_tabela_ok())

    nomes_filas = [
        r.nome for r in resultado.recursos_verificados if r.tipo is TipoRecurso.FILA_SQS
    ]
    assert nomes_filas == list(c.FILAS)


def test_verificacao_e_somente_leitura_nao_altera_recursos() -> None:
    cliente = _cliente_sqs_ok()
    gateway = SqsGateway(cliente_sqs=cliente)

    verificar_inicializacao(gateway, _verificador_tabela_ok())

    # Nenhuma operação de escrita/criação/exclusão foi chamada (Req. 1.5, 1.7, 1.9).
    cliente.send_message.assert_not_called()
    cliente.delete_message.assert_not_called()
    cliente.create_queue.assert_not_called()
    cliente.delete_queue.assert_not_called()
    cliente.set_queue_attributes.assert_not_called()


# --- Fila ausente ------------------------------------------------------------


def test_fila_ausente_levanta_falha_identificando_nome_e_tipo() -> None:
    cliente = _cliente_sqs_ok()
    # A primeira fila (FILA_STANDARD) está inacessível.
    cliente.get_queue_attributes.side_effect = _ErroAwsSimulado(
        "AWS.SimpleQueueService.NonExistentQueue"
    )
    gateway = SqsGateway(cliente_sqs=cliente)

    with pytest.raises(FalhaVerificacaoInicializacao) as exc:
        verificar_inicializacao(gateway, _verificador_tabela_ok())

    resultado = exc.value.resultado
    assert resultado.sucesso is False
    recurso = resultado.recurso_indisponivel
    assert recurso is not None
    assert recurso.nome == c.FILA_STANDARD
    assert recurso.tipo is TipoRecurso.FILA_SQS
    # Mensagem em pt-BR identifica o recurso por tipo e nome (Req. 1.8).
    assert "fila SQS" in resultado.mensagem_pt_br
    assert c.FILA_STANDARD in resultado.mensagem_pt_br


def test_fila_ausente_preserva_erro_original_da_aws() -> None:
    cliente = _cliente_sqs_ok()
    erro = _ErroAwsSimulado("fila inexistente")
    cliente.get_queue_attributes.side_effect = erro
    gateway = SqsGateway(cliente_sqs=cliente)

    with pytest.raises(FalhaVerificacaoInicializacao) as exc:
        verificar_inicializacao(gateway, _verificador_tabela_ok())

    # O erro original é preservado e encadeado como causa (Req. 1.8).
    assert exc.value.erro_original is erro
    assert exc.value.__cause__ is erro
    assert exc.value.resultado.recurso_indisponivel.erro_original is erro


def test_fila_ausente_interrompe_fail_fast_sem_verificar_tabela() -> None:
    cliente = _cliente_sqs_ok()
    cliente.get_queue_attributes.side_effect = _ErroAwsSimulado("falha")
    gateway = SqsGateway(cliente_sqs=cliente)
    verificador = _verificador_tabela_ok()

    with pytest.raises(FalhaVerificacaoInicializacao):
        verificar_inicializacao(gateway, verificador)

    # Para na primeira fila: a tabela nem chega a ser verificada (fail-fast).
    verificador.assert_not_called()


# --- Tabela ausente ----------------------------------------------------------


def test_tabela_ausente_levanta_falha_identificando_nome_e_tipo() -> None:
    gateway = SqsGateway(cliente_sqs=_cliente_sqs_ok())
    erro = _ErroAwsSimulado("ResourceNotFoundException")
    verificador = MagicMock(side_effect=erro)

    with pytest.raises(FalhaVerificacaoInicializacao) as exc:
        verificar_inicializacao(gateway, verificador)

    resultado = exc.value.resultado
    recurso = resultado.recurso_indisponivel
    assert recurso is not None
    assert recurso.nome == c.TABELA_HISTORICO
    assert recurso.tipo is TipoRecurso.TABELA_DYNAMODB
    assert recurso.erro_original is erro
    # Mensagem em pt-BR identifica o recurso por tipo e nome (Req. 1.8).
    assert "tabela DynamoDB" in resultado.mensagem_pt_br
    assert c.TABELA_HISTORICO in resultado.mensagem_pt_br


def test_tabela_ausente_so_e_verificada_apos_todas_as_filas() -> None:
    gateway = SqsGateway(cliente_sqs=_cliente_sqs_ok())
    verificador = MagicMock(side_effect=_ErroAwsSimulado("tabela sumiu"))

    with pytest.raises(FalhaVerificacaoInicializacao) as exc:
        verificar_inicializacao(gateway, verificador)

    # As 4 filas foram verificadas (disponíveis) antes de chegar à tabela.
    verificados = exc.value.resultado.recursos_verificados
    filas_ok = [r for r in verificados if r.tipo is TipoRecurso.FILA_SQS]
    assert len(filas_ok) == len(c.FILAS)
    assert all(r.disponivel for r in filas_ok)


# --- Modo sem interrupção ----------------------------------------------------


def test_sem_interromper_em_falha_retorna_resultado_sem_excecao() -> None:
    cliente = _cliente_sqs_ok()
    erro = _ErroAwsSimulado("fila inacessível")
    cliente.get_queue_attributes.side_effect = erro
    gateway = SqsGateway(cliente_sqs=cliente)

    resultado = verificar_inicializacao(
        gateway, _verificador_tabela_ok(), interromper_em_falha=False
    )

    assert resultado.sucesso is False
    assert resultado.recurso_indisponivel is not None
    assert resultado.recurso_indisponivel.nome == c.FILA_STANDARD
    assert resultado.erro_original is erro


# --- Verificador boto3 (describe_table) --------------------------------------


def test_descrever_tabela_boto3_usa_describe_table_somente_leitura() -> None:
    cliente_ddb = MagicMock()
    cliente_ddb.describe_table.return_value = {"Table": {"TableStatus": "ACTIVE"}}
    verificador = descrever_tabela_boto3(cliente_ddb)

    verificador(c.TABELA_HISTORICO)

    cliente_ddb.describe_table.assert_called_once_with(TableName=c.TABELA_HISTORICO)
    # Confirma que não houve operação de criação/exclusão (somente leitura).
    cliente_ddb.create_table.assert_not_called()
    cliente_ddb.delete_table.assert_not_called()


def test_descrever_tabela_boto3_repassa_erro_original_da_aws() -> None:
    cliente_ddb = MagicMock()
    cliente_ddb.describe_table.side_effect = _ErroAwsSimulado("ResourceNotFoundException")
    gateway = SqsGateway(cliente_sqs=_cliente_sqs_ok())

    with pytest.raises(FalhaVerificacaoInicializacao) as exc:
        verificar_inicializacao(gateway, descrever_tabela_boto3(cliente_ddb))

    assert isinstance(exc.value.erro_original, _ErroAwsSimulado)
    assert exc.value.resultado.recurso_indisponivel.tipo is TipoRecurso.TABELA_DYNAMODB
