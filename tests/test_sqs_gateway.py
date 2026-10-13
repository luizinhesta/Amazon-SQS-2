"""Testes unitários do SqsGateway (Tarefas 8.1 e 8.2).

Validam, usando um cliente SQS simulado (sem chamadas reais à AWS):

- envio Standard e FIFO com passagem correta de parâmetros (Req. 4.10);
- recebimento com Long_Polling, WaitTimeSeconds de 1 a 20 e leitura do
  ApproximateReceiveCount convertido para inteiro (Req. 8.1, 8.2, 8.10, 5.2);
- rejeição de WaitTimeSeconds e tamanho de lote fora da faixa (Req. 8.10);
- exclusão de mensagem (Req. 8.6);
- leitura de atributos de fila (Req. 1.8, 7.10, 12.8);
- reprocessamento (redrive) da DLQ para a fila principal (Req. 7.7);
- preservação do erro original da AWS, sem encapsulamento (Req. 4.12, 8.8, 7.11).

Nenhuma credencial é configurada: o gateway recebe um cliente simulado via
injeção; em produção o boto3 resolve credenciais pela IAM Role da EC2
(Req. 12.1, 12.2).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from app.acesso.sqs_gateway import (
    ATRIBUTO_APPROXIMATE_RECEIVE_COUNT,
    MAX_NUMBER_OF_MESSAGES_MAX,
    MAX_NUMBER_OF_MESSAGES_MIN,
    WAIT_TIME_SECONDS_MAX,
    WAIT_TIME_SECONDS_MIN,
    MensagemRecebida,
    SqsGateway,
    e_erro_infra_sqs,
    e_receipt_handle_expirado,
)
from app.dominio import constantes as c


class _ErroAwsSimulado(Exception):
    """Erro que imita uma exceção do botocore (ex.: ClientError)."""


def _cliente_simulado(**overrides: Any) -> MagicMock:
    """Cria um cliente SQS simulado com respostas padrão coerentes."""
    cliente = MagicMock()
    cliente.get_queue_url.return_value = {
        "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/lab-sqs-standard"
    }
    cliente.send_message.return_value = {"MessageId": "msg-001"}
    cliente.receive_message.return_value = {"Messages": []}
    cliente.delete_message.return_value = {}
    cliente.get_queue_attributes.return_value = {"Attributes": {}}
    for nome, valor in overrides.items():
        getattr(cliente, nome).return_value = valor
    return cliente


# --- Envio -------------------------------------------------------------------


def test_send_message_standard_passa_parametros_basicos() -> None:
    cliente = _cliente_simulado()
    gw = SqsGateway(cliente_sqs=cliente)

    gw.send_message(c.FILA_STANDARD, "corpo-da-mensagem")

    cliente.send_message.assert_called_once()
    kwargs = cliente.send_message.call_args.kwargs
    assert kwargs["MessageBody"] == "corpo-da-mensagem"
    assert kwargs["QueueUrl"].endswith("lab-sqs-standard")
    # Standard não deve enviar parâmetros de FIFO.
    assert "MessageGroupId" not in kwargs
    assert "MessageDeduplicationId" not in kwargs


def test_send_message_fifo_passa_group_id_e_dedup_id() -> None:
    cliente = _cliente_simulado()
    gw = SqsGateway(cliente_sqs=cliente)

    gw.send_message(
        c.FILA_FIFO,
        "corpo",
        message_group_id="grupo-A",
        message_deduplication_id="dedup-1",
    )

    kwargs = cliente.send_message.call_args.kwargs
    assert kwargs["MessageGroupId"] == "grupo-A"
    assert kwargs["MessageDeduplicationId"] == "dedup-1"


def test_send_message_repassa_erro_original_da_aws() -> None:
    cliente = _cliente_simulado()
    cliente.send_message.side_effect = _ErroAwsSimulado("falha no SendMessage")
    gw = SqsGateway(cliente_sqs=cliente)

    with pytest.raises(_ErroAwsSimulado, match="falha no SendMessage"):
        gw.send_message(c.FILA_STANDARD, "corpo")


# --- Recebimento (Long_Polling) ----------------------------------------------


def test_receive_messages_usa_long_polling_e_pede_atributos() -> None:
    cliente = _cliente_simulado()
    gw = SqsGateway(cliente_sqs=cliente)

    gw.receive_messages(
        c.FILA_STANDARD, max_mensagens=10, wait_time_seconds=c.LONG_POLLING_SEGUNDOS
    )

    kwargs = cliente.receive_message.call_args.kwargs
    assert kwargs["WaitTimeSeconds"] == c.LONG_POLLING_SEGUNDOS
    assert kwargs["MaxNumberOfMessages"] == 10
    # Deve solicitar os atributos de sistema (que incluem ApproximateReceiveCount).
    assert kwargs["AttributeNames"] == ["All"]
    assert kwargs["MessageAttributeNames"] == ["All"]


def test_receive_messages_le_approximate_receive_count_como_inteiro() -> None:
    resposta = {
        "Messages": [
            {
                "MessageId": "m1",
                "ReceiptHandle": "rh1",
                "Body": "corpo-1",
                "Attributes": {ATRIBUTO_APPROXIMATE_RECEIVE_COUNT: "2"},
            }
        ]
    }
    cliente = _cliente_simulado(receive_message=resposta)
    gw = SqsGateway(cliente_sqs=cliente)

    mensagens = gw.receive_messages(c.FILA_STANDARD, wait_time_seconds=20)

    assert len(mensagens) == 1
    msg = mensagens[0]
    assert isinstance(msg, MensagemRecebida)
    assert msg.approximate_receive_count == 2
    assert isinstance(msg.approximate_receive_count, int)
    assert msg.receipt_handle == "rh1"
    assert msg.body == "corpo-1"


def test_receive_messages_receive_count_ausente_assume_zero() -> None:
    resposta = {
        "Messages": [
            {"MessageId": "m1", "ReceiptHandle": "rh1", "Body": "c", "Attributes": {}}
        ]
    }
    cliente = _cliente_simulado(receive_message=resposta)
    gw = SqsGateway(cliente_sqs=cliente)

    mensagens = gw.receive_messages(c.FILA_STANDARD, wait_time_seconds=5)
    assert mensagens[0].approximate_receive_count == 0


def test_receive_messages_fifo_expoe_group_id_e_sequence() -> None:
    resposta = {
        "Messages": [
            {
                "MessageId": "m1",
                "ReceiptHandle": "rh1",
                "Body": "c",
                "Attributes": {
                    ATRIBUTO_APPROXIMATE_RECEIVE_COUNT: "1",
                    "MessageGroupId": "grupo-A",
                    "SequenceNumber": "18849496460467532",
                },
            }
        ]
    }
    cliente = _cliente_simulado(receive_message=resposta)
    gw = SqsGateway(cliente_sqs=cliente)

    msg = gw.receive_messages(c.FILA_FIFO, wait_time_seconds=20)[0]
    assert msg.message_group_id == "grupo-A"
    assert msg.sequence_number == "18849496460467532"


def test_receive_messages_vazio_retorna_lista_vazia_sem_erro() -> None:
    cliente = _cliente_simulado(receive_message={"Messages": []})
    gw = SqsGateway(cliente_sqs=cliente)

    assert gw.receive_messages(c.FILA_STANDARD, wait_time_seconds=20) == []


def test_receive_messages_sem_chave_messages_retorna_lista_vazia() -> None:
    cliente = _cliente_simulado(receive_message={})
    gw = SqsGateway(cliente_sqs=cliente)

    assert gw.receive_messages(c.FILA_STANDARD, wait_time_seconds=20) == []


@pytest.mark.parametrize("wait", [WAIT_TIME_SECONDS_MIN, 10, WAIT_TIME_SECONDS_MAX])
def test_receive_messages_aceita_wait_time_nos_limites(wait: int) -> None:
    cliente = _cliente_simulado()
    gw = SqsGateway(cliente_sqs=cliente)
    gw.receive_messages(c.FILA_STANDARD, wait_time_seconds=wait)
    assert cliente.receive_message.call_args.kwargs["WaitTimeSeconds"] == wait


@pytest.mark.parametrize("wait", [WAIT_TIME_SECONDS_MIN - 1, WAIT_TIME_SECONDS_MAX + 1, 0, 21])
def test_receive_messages_rejeita_wait_time_fora_da_faixa(wait: int) -> None:
    cliente = _cliente_simulado()
    gw = SqsGateway(cliente_sqs=cliente)
    with pytest.raises(ValueError):
        gw.receive_messages(c.FILA_STANDARD, wait_time_seconds=wait)
    cliente.receive_message.assert_not_called()


@pytest.mark.parametrize(
    "lote", [MAX_NUMBER_OF_MESSAGES_MIN - 1, MAX_NUMBER_OF_MESSAGES_MAX + 1]
)
def test_receive_messages_rejeita_lote_fora_da_faixa(lote: int) -> None:
    cliente = _cliente_simulado()
    gw = SqsGateway(cliente_sqs=cliente)
    with pytest.raises(ValueError):
        gw.receive_messages(c.FILA_STANDARD, max_mensagens=lote, wait_time_seconds=20)
    cliente.receive_message.assert_not_called()


def test_receive_messages_repassa_erro_original_da_aws() -> None:
    cliente = _cliente_simulado()
    cliente.receive_message.side_effect = _ErroAwsSimulado("falha no ReceiveMessage")
    gw = SqsGateway(cliente_sqs=cliente)
    with pytest.raises(_ErroAwsSimulado, match="falha no ReceiveMessage"):
        gw.receive_messages(c.FILA_STANDARD, wait_time_seconds=20)


# --- Exclusão ----------------------------------------------------------------


def test_delete_message_passa_receipt_handle() -> None:
    cliente = _cliente_simulado()
    gw = SqsGateway(cliente_sqs=cliente)

    gw.delete_message(c.FILA_STANDARD, "rh-xyz")

    kwargs = cliente.delete_message.call_args.kwargs
    assert kwargs["ReceiptHandle"] == "rh-xyz"
    assert kwargs["QueueUrl"].endswith("lab-sqs-standard")


def test_delete_message_repassa_erro_original_da_aws() -> None:
    cliente = _cliente_simulado()
    cliente.delete_message.side_effect = _ErroAwsSimulado("falha no DeleteMessage")
    gw = SqsGateway(cliente_sqs=cliente)
    with pytest.raises(_ErroAwsSimulado, match="falha no DeleteMessage"):
        gw.delete_message(c.FILA_STANDARD, "rh")


# --- Extensão do Visibility_Timeout (ChangeMessageVisibility) ----------------


def test_change_message_visibility_passa_parametros() -> None:
    cliente = _cliente_simulado()
    cliente.change_message_visibility.return_value = {}
    gw = SqsGateway(cliente_sqs=cliente)

    gw.change_message_visibility(c.FILA_FIFO, "rh-abc", 60)

    kwargs = cliente.change_message_visibility.call_args.kwargs
    assert kwargs["ReceiptHandle"] == "rh-abc"
    assert kwargs["VisibilityTimeout"] == 60
    assert kwargs["QueueUrl"].endswith("lab-sqs-standard")


def test_change_message_visibility_repassa_erro_original_da_aws() -> None:
    cliente = _cliente_simulado()
    cliente.change_message_visibility.side_effect = _ErroAwsSimulado(
        "falha no ChangeMessageVisibility"
    )
    gw = SqsGateway(cliente_sqs=cliente)
    with pytest.raises(_ErroAwsSimulado, match="falha no ChangeMessageVisibility"):
        gw.change_message_visibility(c.FILA_FIFO, "rh", 60)


# --- Classificadores de erro do DeleteMessage (helpers puros) ----------------


def _client_error(codigo: str, mensagem: str = "") -> Exception:
    """Cria uma exceção que imita um botocore.ClientError (com ``.response``)."""

    class _ClientErrorSimulado(Exception):
        pass

    erro = _ClientErrorSimulado(mensagem or codigo)
    erro.response = {"Error": {"Code": codigo, "Message": mensagem}}  # type: ignore[attr-defined]
    return erro


def test_e_receipt_handle_expirado_para_invalid_parameter_value_com_mensagem() -> None:
    erro = _client_error(
        "InvalidParameterValue", "The receipt handle has expired."
    )
    assert e_receipt_handle_expirado(erro) is True


def test_e_receipt_handle_expirado_para_receipt_handle_is_invalid() -> None:
    erro = _client_error("ReceiptHandleIsInvalid", "qualquer mensagem")
    assert e_receipt_handle_expirado(erro) is True


def test_e_receipt_handle_expirado_falso_para_invalid_parameter_sem_mensagem() -> None:
    # InvalidParameterValue sem "receipt handle has expired" NÃO deve ser tratado
    # como expiração (para não mascarar outros usos do código).
    erro = _client_error("InvalidParameterValue", "Outro problema de parâmetro.")
    assert e_receipt_handle_expirado(erro) is False


def test_e_receipt_handle_expirado_falso_sem_response() -> None:
    assert e_receipt_handle_expirado(_ErroAwsSimulado("sem response")) is False


def test_e_erro_infra_sqs_true_quando_tem_response() -> None:
    assert e_erro_infra_sqs(_client_error("ServiceUnavailable")) is True


def test_e_erro_infra_sqs_true_por_mro() -> None:
    class BotoCoreError(Exception):
        pass

    class EndpointConnectionError(BotoCoreError):
        pass

    assert e_erro_infra_sqs(EndpointConnectionError("timeout")) is True


def test_e_erro_infra_sqs_falso_para_erro_generico() -> None:
    assert e_erro_infra_sqs(ValueError("erro de domínio")) is False


# --- Atributos de fila -------------------------------------------------------


def test_get_queue_attributes_retorna_dicionario_de_atributos() -> None:
    cliente = _cliente_simulado(
        get_queue_attributes={
            "Attributes": {
                "SqsManagedSseEnabled": "true",
                "RedrivePolicy": '{"maxReceiveCount":"2"}',
            }
        }
    )
    gw = SqsGateway(cliente_sqs=cliente)

    atributos = gw.get_queue_attributes(c.FILA_STANDARD)

    assert atributos["SqsManagedSseEnabled"] == "true"
    assert cliente.get_queue_attributes.call_args.kwargs["AttributeNames"] == ["All"]


def test_get_queue_attributes_com_lista_especifica() -> None:
    cliente = _cliente_simulado(
        get_queue_attributes={"Attributes": {"VisibilityTimeout": "30"}}
    )
    gw = SqsGateway(cliente_sqs=cliente)

    gw.get_queue_attributes(c.FILA_STANDARD, atributos=["VisibilityTimeout"])
    kwargs = cliente.get_queue_attributes.call_args.kwargs
    assert kwargs["AttributeNames"] == ["VisibilityTimeout"]


def test_get_queue_attributes_repassa_erro_original_da_aws() -> None:
    cliente = _cliente_simulado()
    cliente.get_queue_attributes.side_effect = _ErroAwsSimulado("falha nos atributos")
    gw = SqsGateway(cliente_sqs=cliente)
    with pytest.raises(_ErroAwsSimulado, match="falha nos atributos"):
        gw.get_queue_attributes(c.FILA_STANDARD)


# --- Reprocessamento (redrive) -----------------------------------------------


def test_redrive_from_dlq_reenvia_e_exclui_da_dlq() -> None:
    resposta_recebimento = {
        "Messages": [
            {
                "MessageId": "m1",
                "ReceiptHandle": "rh1",
                "Body": "pedido-1",
                "Attributes": {ATRIBUTO_APPROXIMATE_RECEIVE_COUNT: "2"},
            },
            {
                "MessageId": "m2",
                "ReceiptHandle": "rh2",
                "Body": "pedido-2",
                "Attributes": {ATRIBUTO_APPROXIMATE_RECEIVE_COUNT: "2"},
            },
        ]
    }
    cliente = _cliente_simulado(receive_message=resposta_recebimento)
    cliente.send_message.side_effect = [
        {"MessageId": "novo-1"},
        {"MessageId": "novo-2"},
    ]
    gw = SqsGateway(cliente_sqs=cliente)

    reenviadas = gw.redrive_from_dlq(c.FILA_STANDARD_DLQ, c.FILA_STANDARD)

    assert reenviadas == ["novo-1", "novo-2"]
    # Reenvia à fila principal e exclui ambas da DLQ.
    assert cliente.send_message.call_count == 2
    assert cliente.delete_message.call_count == 2


def test_redrive_from_dlq_fifo_preserva_message_group_id() -> None:
    resposta_recebimento = {
        "Messages": [
            {
                "MessageId": "m1",
                "ReceiptHandle": "rh1",
                "Body": "pedido-1",
                "Attributes": {
                    ATRIBUTO_APPROXIMATE_RECEIVE_COUNT: "2",
                    "MessageGroupId": "grupo-A",
                },
            }
        ]
    }
    cliente = _cliente_simulado(receive_message=resposta_recebimento)
    cliente.send_message.return_value = {"MessageId": "novo-1"}
    gw = SqsGateway(cliente_sqs=cliente)

    gw.redrive_from_dlq(c.FILA_FIFO_DLQ, c.FILA_FIFO)

    kwargs = cliente.send_message.call_args.kwargs
    assert kwargs["MessageGroupId"] == "grupo-A"


def test_redrive_from_dlq_sem_mensagens_retorna_vazio() -> None:
    cliente = _cliente_simulado(receive_message={"Messages": []})
    gw = SqsGateway(cliente_sqs=cliente)

    assert gw.redrive_from_dlq(c.FILA_STANDARD_DLQ, c.FILA_STANDARD) == []
    cliente.send_message.assert_not_called()
    cliente.delete_message.assert_not_called()


def test_redrive_falha_de_reenvio_preserva_mensagem_na_dlq() -> None:
    resposta_recebimento = {
        "Messages": [
            {
                "MessageId": "m1",
                "ReceiptHandle": "rh1",
                "Body": "pedido-1",
                "Attributes": {ATRIBUTO_APPROXIMATE_RECEIVE_COUNT: "2"},
            }
        ]
    }
    cliente = _cliente_simulado(receive_message=resposta_recebimento)
    cliente.send_message.side_effect = _ErroAwsSimulado("falha no reenvio")
    gw = SqsGateway(cliente_sqs=cliente)

    with pytest.raises(_ErroAwsSimulado, match="falha no reenvio"):
        gw.redrive_from_dlq(c.FILA_STANDARD_DLQ, c.FILA_STANDARD)
    # Como o reenvio falhou antes da exclusão, a mensagem não é removida da DLQ.
    cliente.delete_message.assert_not_called()


# --- Resolução de URL de fila ------------------------------------------------


def test_obter_url_fila_e_memoizado() -> None:
    cliente = _cliente_simulado()
    gw = SqsGateway(cliente_sqs=cliente)

    gw.obter_url_fila(c.FILA_STANDARD)
    gw.obter_url_fila(c.FILA_STANDARD)

    # GetQueueUrl é chamado apenas uma vez por fila (cache).
    cliente.get_queue_url.assert_called_once()


def test_obter_url_fila_repassa_erro_original_da_aws() -> None:
    cliente = _cliente_simulado()
    cliente.get_queue_url.side_effect = _ErroAwsSimulado("fila inexistente")
    gw = SqsGateway(cliente_sqs=cliente)
    with pytest.raises(_ErroAwsSimulado, match="fila inexistente"):
        gw.obter_url_fila("fila-que-nao-existe")
