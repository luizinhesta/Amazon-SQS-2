"""Camada de acesso ao Amazon SQS (SqsGateway).

Encapsula o cliente ``boto3`` do SQS e centraliza as operações usadas pela
API_Backend e pelos consumidores: envio, recebimento com Long_Polling,
exclusão, leitura de atributos de fila e reprocessamento (redrive) a partir de
uma DLQ.

Decisões de design relevantes a esta camada (ver ``design.md``):

- **Credenciais exclusivamente via IAM_Role_EC2.** O cliente ``boto3`` é criado
  sem qualquer Access Key fixa (``aws_access_key_id``/``aws_secret_access_key``).
  O ``boto3`` resolve as credenciais temporárias automaticamente pela cadeia de
  provedores padrão, que, na instância EC2 do laboratório, obtém as credenciais
  da IAM Role anexada via metadata service (IMDSv2). Nenhuma Access Key é lida de
  código, configuração ou variável de ambiente (Req. 12.1, 12.2).
- **Repasse do erro original da AWS.** Nenhuma operação captura nem encapsula
  exceções do ``botocore`` (por exemplo, ``ClientError``). Qualquer erro retornado
  pela AWS propaga intacto ao chamador, que decide como registrá-lo e exibi-lo
  (Req. 1.8, 4.12, 7.11, 8.8).
- **Long_Polling configurável.** ``receive_messages`` aceita ``WaitTimeSeconds``
  entre 1 e 20 segundos (Req. 8.10, 14.5, 14.6); valores fora da faixa são
  rejeitados com ``ValueError`` antes de qualquer chamada à AWS.
- **Leitura de ApproximateReceiveCount.** ``receive_messages`` solicita o
  atributo de sistema ``ApproximateReceiveCount`` e o expõe como inteiro em cada
  mensagem retornada (Req. 8.1, 8.2, 8.4, 5.2).

Referências de requisitos: 4.10, 7.7, 8.1, 8.2, 8.10, 8.11, 12.1, 12.2, 14.5, 14.6.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

import boto3

from app.dominio import constantes as c

# Atributo de sistema do SQS que carrega a contagem aproximada de recebimentos
# de uma mensagem (ApproximateReceiveCount). Deve ser solicitado explicitamente
# em ReceiveMessage para ser retornado pela AWS (Req. 5.2, 8.4).
ATRIBUTO_APPROXIMATE_RECEIVE_COUNT: str = "ApproximateReceiveCount"

# Limites do WaitTimeSeconds de Long_Polling aceitos pelo gateway (Req. 8.10).
WAIT_TIME_SECONDS_MIN: int = 1
WAIT_TIME_SECONDS_MAX: int = 20

# Limites de MaxNumberOfMessages por chamada ReceiveMessage, conforme o SQS
# (lotes de 1 a 10 mensagens) (Req. 8.1, 8.2).
MAX_NUMBER_OF_MESSAGES_MIN: int = 1
MAX_NUMBER_OF_MESSAGES_MAX: int = 10


@dataclass(frozen=True)
class MensagemRecebida:
    """Representa uma mensagem recebida do SQS, com os campos de interesse.

    Mantém o ``receipt_handle`` necessário para excluir a mensagem e expõe o
    ``ApproximateReceiveCount`` já convertido para inteiro, além dos atributos e
    atributos de mensagem brutos para uso pelos consumidores.
    """

    message_id: str
    receipt_handle: str
    body: str
    approximate_receive_count: int
    attributes: Mapping[str, str] = field(default_factory=dict)
    message_attributes: Mapping[str, Any] = field(default_factory=dict)
    message_group_id: Optional[str] = None
    sequence_number: Optional[str] = None

    @classmethod
    def de_resposta_sqs(cls, mensagem: Mapping[str, Any]) -> "MensagemRecebida":
        """Constrói a partir de um item do campo ``Messages`` do ReceiveMessage."""
        atributos = dict(mensagem.get("Attributes", {}) or {})
        receive_count_bruto = atributos.get(ATRIBUTO_APPROXIMATE_RECEIVE_COUNT, "0")
        try:
            approximate_receive_count = int(receive_count_bruto)
        except (TypeError, ValueError):
            # O SQS envia o atributo como string numérica; se vier ausente ou
            # inesperado, assume 0 para manter o contrato de inteiro ≥ 0 (Req. 5.2).
            approximate_receive_count = 0

        return cls(
            message_id=mensagem.get("MessageId", ""),
            receipt_handle=mensagem.get("ReceiptHandle", ""),
            body=mensagem.get("Body", ""),
            approximate_receive_count=approximate_receive_count,
            attributes=atributos,
            message_attributes=dict(mensagem.get("MessageAttributes", {}) or {}),
            message_group_id=atributos.get("MessageGroupId"),
            sequence_number=atributos.get("SequenceNumber"),
        )


class SqsGateway:
    """Fachada sobre o cliente ``boto3`` do SQS.

    O gateway é criado uma única vez e reutilizado pela aplicação. O cliente
    ``boto3`` é construído sem Access Keys: a resolução de credenciais fica
    inteiramente a cargo da cadeia padrão do ``boto3`` (IAM Role da EC2), conforme
    Req. 12.1 e 12.2.
    """

    def __init__(
        self,
        cliente_sqs: Optional[Any] = None,
        regiao: str = c.REGIAO_AWS,
    ) -> None:
        """Inicializa o gateway.

        Args:
            cliente_sqs: cliente SQS já construído. Use APENAS em testes, para
                injetar um cliente simulado (mock/stub). Em produção, deixe
                ``None`` para que o gateway crie o cliente ``boto3`` padrão.
            regiao: região AWS do laboratório (padrão ``us-east-1``).
        """
        # Importante: NÃO passamos aws_access_key_id/aws_secret_access_key.
        # O boto3 resolve as credenciais temporárias pela IAM Role da EC2
        # através da cadeia de provedores padrão (Req. 12.1, 12.2).
        self._sqs = cliente_sqs if cliente_sqs is not None else boto3.client(
            "sqs", region_name=regiao
        )
        self._regiao = regiao
        # Cache de URLs de fila por nome, para evitar chamadas GetQueueUrl repetidas.
        self._url_por_fila: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Resolução de URL de fila
    # ------------------------------------------------------------------
    def obter_url_fila(self, nome_fila: str) -> str:
        """Resolve e memoiza a URL de uma fila a partir do seu nome.

        Repassa qualquer erro original da AWS (por exemplo, fila inexistente)
        sem encapsular (Req. 1.8, 7.11).
        """
        if nome_fila in self._url_por_fila:
            return self._url_por_fila[nome_fila]
        resposta = self._sqs.get_queue_url(QueueName=nome_fila)
        url = resposta["QueueUrl"]
        self._url_por_fila[nome_fila] = url
        return url

    # ------------------------------------------------------------------
    # Envio
    # ------------------------------------------------------------------
    def send_message(
        self,
        nome_fila: str,
        corpo: str,
        *,
        message_group_id: Optional[str] = None,
        message_deduplication_id: Optional[str] = None,
        message_attributes: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        """Envia uma mensagem à fila indicada (SendMessage).

        Para filas FIFO, informe ``message_group_id`` e, quando a deduplicação
        por conteúdo estiver desabilitada, ``message_deduplication_id``
        (Req. 4.6, 4.7). Para filas Standard, deixe ambos como ``None``.

        O erro original da AWS é repassado sem encapsulamento (Req. 4.10, 4.12).
        """
        url = self.obter_url_fila(nome_fila)
        parametros: dict[str, Any] = {"QueueUrl": url, "MessageBody": corpo}
        if message_group_id is not None:
            parametros["MessageGroupId"] = message_group_id
        if message_deduplication_id is not None:
            parametros["MessageDeduplicationId"] = message_deduplication_id
        if message_attributes:
            parametros["MessageAttributes"] = dict(message_attributes)
        return self._sqs.send_message(**parametros)

    # ------------------------------------------------------------------
    # Recebimento (Long_Polling)
    # ------------------------------------------------------------------
    def receive_messages(
        self,
        nome_fila: str,
        *,
        max_mensagens: int = MAX_NUMBER_OF_MESSAGES_MAX,
        wait_time_seconds: int = c.LONG_POLLING_SEGUNDOS,
        visibility_timeout: Optional[int] = None,
    ) -> list[MensagemRecebida]:
        """Recebe mensagens com Long_Polling (ReceiveMessage).

        Args:
            nome_fila: nome da fila de origem.
            max_mensagens: tamanho do lote, de 1 a 10 (Req. 8.1, 8.2).
            wait_time_seconds: WaitTimeSeconds do Long_Polling, de 1 a 20
                segundos (Req. 8.10, 14.5, 14.6).
            visibility_timeout: Visibility_Timeout específico desta chamada, em
                segundos; quando ``None``, usa o configurado na fila.

        Sempre solicita o atributo de sistema ``ApproximateReceiveCount`` e o
        expõe convertido em cada mensagem (Req. 5.2, 8.4). Um recebimento vazio
        retorna lista vazia, sem erro, para que o consumidor repita a tentativa
        (Req. 8.11).

        O erro original da AWS é repassado sem encapsulamento (Req. 8.8).
        """
        if not (WAIT_TIME_SECONDS_MIN <= wait_time_seconds <= WAIT_TIME_SECONDS_MAX):
            raise ValueError(
                "WaitTimeSeconds deve estar entre "
                f"{WAIT_TIME_SECONDS_MIN} e {WAIT_TIME_SECONDS_MAX} segundos "
                f"(valor informado: {wait_time_seconds})."
            )
        if not (MAX_NUMBER_OF_MESSAGES_MIN <= max_mensagens <= MAX_NUMBER_OF_MESSAGES_MAX):
            raise ValueError(
                "O lote (MaxNumberOfMessages) deve estar entre "
                f"{MAX_NUMBER_OF_MESSAGES_MIN} e {MAX_NUMBER_OF_MESSAGES_MAX} "
                f"mensagens (valor informado: {max_mensagens})."
            )

        url = self.obter_url_fila(nome_fila)
        parametros: dict[str, Any] = {
            "QueueUrl": url,
            "MaxNumberOfMessages": max_mensagens,
            "WaitTimeSeconds": wait_time_seconds,
            # Solicita o ApproximateReceiveCount e demais atributos de sistema,
            # além de todos os atributos de mensagem definidos pelo remetente.
            "AttributeNames": ["All"],
            "MessageAttributeNames": ["All"],
        }
        if visibility_timeout is not None:
            parametros["VisibilityTimeout"] = visibility_timeout

        resposta = self._sqs.receive_message(**parametros)
        mensagens = resposta.get("Messages", []) or []
        return [MensagemRecebida.de_resposta_sqs(m) for m in mensagens]

    # ------------------------------------------------------------------
    # Exclusão
    # ------------------------------------------------------------------
    def delete_message(self, nome_fila: str, receipt_handle: str) -> Mapping[str, Any]:
        """Exclui uma mensagem da fila (DeleteMessage).

        Deve ser chamado somente quando o processamento concluir com sucesso
        (Req. 8.6, 8.7). O erro original da AWS é repassado sem encapsulamento.
        """
        url = self.obter_url_fila(nome_fila)
        return self._sqs.delete_message(QueueUrl=url, ReceiptHandle=receipt_handle)

    # ------------------------------------------------------------------
    # Atributos de fila
    # ------------------------------------------------------------------
    def get_queue_attributes(
        self,
        nome_fila: str,
        atributos: Optional[Sequence[str]] = None,
    ) -> Mapping[str, str]:
        """Lê atributos de uma fila (GetQueueAttributes).

        Útil para a verificação de inicialização (existência/acessibilidade e
        confirmação de SSE_SQS) e para inspeção da Redrive_Policy (Req. 1.8,
        7.10, 12.8). Quando ``atributos`` é ``None``, solicita ``All``.

        Retorna o dicionário ``Attributes`` da resposta. O erro original da AWS
        é repassado sem encapsulamento.
        """
        url = self.obter_url_fila(nome_fila)
        nomes = list(atributos) if atributos else ["All"]
        resposta = self._sqs.get_queue_attributes(QueueUrl=url, AttributeNames=nomes)
        return dict(resposta.get("Attributes", {}) or {})

    # ------------------------------------------------------------------
    # Reprocessamento a partir da DLQ (redrive)
    # ------------------------------------------------------------------
    def redrive_from_dlq(
        self,
        nome_dlq: str,
        nome_fila_destino: str,
        *,
        max_mensagens_por_lote: int = MAX_NUMBER_OF_MESSAGES_MAX,
        wait_time_seconds: int = WAIT_TIME_SECONDS_MIN,
    ) -> list[str]:
        """Move mensagens da DLQ de volta à fila principal (Req. 7.7).

        Implementa o reprocessamento recebendo mensagens da ``nome_dlq``,
        reenviando-as à ``nome_fila_destino`` e, somente após o reenvio
        confirmado, excluindo-as da DLQ. Preserva a ordem/agrupamento de filas
        FIFO repassando ``MessageGroupId`` e ``MessageDeduplicationId`` quando
        presentes.

        Processa um único lote por chamada; o chamador decide quando repetir.
        Retorna a lista de ``MessageId`` reenviados com sucesso.

        O erro original da AWS é repassado sem encapsulamento (Req. 7.11). Como o
        reenvio ocorre antes da exclusão, uma falha de reenvio preserva a
        mensagem na DLQ (não há perda de mensagem).
        """
        e_fila_fifo_destino = nome_fila_destino.endswith(".fifo")
        mensagens = self.receive_messages(
            nome_dlq,
            max_mensagens=max_mensagens_por_lote,
            wait_time_seconds=wait_time_seconds,
        )

        reenviadas: list[str] = []
        for mensagem in mensagens:
            parametros_envio: dict[str, Any] = {}
            if e_fila_fifo_destino:
                # Preserva o agrupamento FIFO. Se o grupo não veio nos atributos,
                # usa um grupo padrão para não violar a exigência da fila FIFO.
                parametros_envio["message_group_id"] = (
                    mensagem.message_group_id or "redrive"
                )
                dedup_id = mensagem.attributes.get("MessageDeduplicationId")
                if dedup_id:
                    parametros_envio["message_deduplication_id"] = dedup_id
            if mensagem.message_attributes:
                parametros_envio["message_attributes"] = mensagem.message_attributes

            resposta_envio = self.send_message(
                nome_fila_destino, mensagem.body, **parametros_envio
            )
            # Só exclui da DLQ após o reenvio ser confirmado pela AWS.
            self.delete_message(nome_dlq, mensagem.receipt_handle)
            reenviadas.append(resposta_envio.get("MessageId", mensagem.message_id))

        return reenviadas
