"""Modelo de evento da Tabela_Historico (DynamoDB).

A Tabela_Historico é um log de eventos *append-only*: cada ação do ciclo de vida de
uma mensagem gera um item de evento. Este módulo define:

- :class:`Evento`: a estrutura do item de evento, com os atributos descritos na
  seção *Data Models* do design.md.
- A construção das chaves de acesso (``PK``, ``SK``, ``GSI1PK``, ``GSI1SK``).
- A serialização para item DynamoDB (:meth:`Evento.para_item`) e a desserialização
  a partir de um item DynamoDB (:meth:`Evento.de_item`).

Formato das chaves (ver design.md):

- ``PK``     = ``PEDIDO#<order_id>`` — agrupa todos os eventos de um mesmo pedido.
- ``SK``     = ``EVENTO#<app_time_ms>#<seq>`` — ordena eventos cronologicamente;
  ``seq`` é o desempate estável entre eventos de mesmo horário (Req. 6.1, 11.6).
- ``GSI1PK`` = ``FILA#<queue_name>`` — habilita a consulta por fila (Req. 11.4).
- ``GSI1SK`` = ``<app_time_ms>#<order_id>`` — ordena cronologicamente dentro da fila.

O Estado da mensagem NUNCA é persistido como campo mutável: ele é derivado da
sequência de eventos pelo StateDeriver (tarefa posterior).

Referências de requisitos: 11.1, 11.3, 11.4, 11.6, 6.1.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.dominio.tipos import EventSource, EventType

# ---------------------------------------------------------------------------
# Prefixos e separadores das chaves
# ---------------------------------------------------------------------------
# Mantidos como constantes para evitar divergência entre construção e parsing.
PREFIXO_PK: str = "PEDIDO#"
PREFIXO_SK: str = "EVENTO#"
PREFIXO_GSI1PK: str = "FILA#"
SEPARADOR: str = "#"

# Largura do campo `seq` na SK (zero-padding). Os exemplos do design usam 4 dígitos
# (ex.: "EVENTO#1719322800123#0001"), o que mantém a ordenação lexicográfica
# coerente com a ordem numérica para até 9999 eventos de mesmo horário.
LARGURA_SEQ: int = 4


def construir_pk(order_id: str) -> str:
    """Constrói a Partition Key: ``PEDIDO#<order_id>`` (Req. 11.3)."""
    return f"{PREFIXO_PK}{order_id}"


def construir_sk(app_time_ms: int, seq: int) -> str:
    """Constrói a Sort Key: ``EVENTO#<app_time_ms>#<seq>`` (Req. 6.1, 11.6).

    O ``seq`` recebe zero-padding (:data:`LARGURA_SEQ` dígitos) para que a ordenação
    lexicográfica da SK coincida com a ordem de registro entre eventos de mesmo
    ``app_time_ms`` (desempate estável).
    """
    return f"{PREFIXO_SK}{app_time_ms}{SEPARADOR}{seq:0{LARGURA_SEQ}d}"


def construir_gsi1pk(queue_name: str) -> str:
    """Constrói a GSI1 Partition Key: ``FILA#<queue_name>`` (Req. 11.4)."""
    return f"{PREFIXO_GSI1PK}{queue_name}"


def construir_gsi1sk(app_time_ms: int, order_id: str) -> str:
    """Constrói a GSI1 Sort Key: ``<app_time_ms>#<order_id>``.

    Ordena cronologicamente dentro da fila e usa ``order_id`` como desempate.
    """
    return f"{app_time_ms}{SEPARADOR}{order_id}"


@dataclass(frozen=True)
class Evento:
    """Item de evento do ciclo de vida de uma mensagem na Tabela_Historico.

    Atributos obrigatórios identificam a mensagem, o tipo e a origem do evento e o
    horário real da aplicação. Os demais atributos são opcionais e só aparecem no
    item DynamoDB quando têm valor, espelhando os exemplos JSON do design (um evento
    de ``enviada`` não carrega ``consumer_id`` nem ``receive_count``, por exemplo).

    Campos:
        order_id: Identificador do pedido (1–80 caracteres).
        queue_name: Nome da fila (``lab-sqs-standard``, ``lab-sqs-fifo.fifo`` etc.).
        event_type: Tipo do evento (:class:`EventType`).
        event_source: Origem do evento, OBSERVADO ou INFERIDO (:class:`EventSource`).
        app_time: Horário real da aplicação em ISO-8601 UTC com milissegundos.
        app_time_ms: Epoch em milissegundos (usado na SK e na ordenação).
        seq: Sequência de desempate entre eventos de mesmo ``app_time_ms`` (Req. 6.1).
        consumer_id: Identificador do consumidor responsável (quando aplicável).
        receive_count: Contagem de recebimentos (``ApproximateReceiveCount``) ≥ 0.
        processing_seconds: Tempo_Processamento_Simulado da mensagem.
        message_group_id: ``MessageGroupId`` (apenas FIFO).
        sqs_message_id: ``MessageId`` retornado pelo SQS.
        error_detail: Erro original retornado pela AWS/aplicação, quando houver.
    """

    order_id: str
    queue_name: str
    event_type: EventType
    event_source: EventSource
    app_time: str
    app_time_ms: int
    seq: int = 1
    consumer_id: str | None = None
    receive_count: int | None = None
    processing_seconds: int | None = None
    message_group_id: str | None = None
    sqs_message_id: str | None = None
    error_detail: str | None = None

    # -- Chaves derivadas ---------------------------------------------------

    @property
    def pk(self) -> str:
        """Partition Key da tabela base: ``PEDIDO#<order_id>``."""
        return construir_pk(self.order_id)

    @property
    def sk(self) -> str:
        """Sort Key da tabela base: ``EVENTO#<app_time_ms>#<seq>``."""
        return construir_sk(self.app_time_ms, self.seq)

    @property
    def gsi1pk(self) -> str:
        """Partition Key do GSI1: ``FILA#<queue_name>``."""
        return construir_gsi1pk(self.queue_name)

    @property
    def gsi1sk(self) -> str:
        """Sort Key do GSI1: ``<app_time_ms>#<order_id>``."""
        return construir_gsi1sk(self.app_time_ms, self.order_id)

    # -- Serialização -------------------------------------------------------

    def para_item(self) -> dict[str, Any]:
        """Serializa o evento no formato de item da Tabela_Historico (DynamoDB).

        Os atributos ``event_type`` e ``event_source`` são gravados como strings
        (o ``.value`` do enum). Atributos opcionais sem valor são omitidos do item,
        reproduzindo o formato dos exemplos JSON do design e evitando gravar ``None``
        no DynamoDB.
        """
        item: dict[str, Any] = {
            "PK": self.pk,
            "SK": self.sk,
            "order_id": self.order_id,
            "queue_name": self.queue_name,
            "event_type": self.event_type.value,
            "event_source": self.event_source.value,
            "app_time": self.app_time,
            "app_time_ms": self.app_time_ms,
            "GSI1PK": self.gsi1pk,
            "GSI1SK": self.gsi1sk,
        }

        # Atributos opcionais: incluídos somente quando têm valor.
        if self.consumer_id is not None:
            item["consumer_id"] = self.consumer_id
        if self.receive_count is not None:
            item["receive_count"] = self.receive_count
        if self.processing_seconds is not None:
            item["processing_seconds"] = self.processing_seconds
        if self.message_group_id is not None:
            item["message_group_id"] = self.message_group_id
        if self.sqs_message_id is not None:
            item["sqs_message_id"] = self.sqs_message_id
        if self.error_detail is not None:
            item["error_detail"] = self.error_detail

        return item

    @classmethod
    def de_item(cls, item: dict[str, Any]) -> "Evento":
        """Reconstrói um :class:`Evento` a partir de um item da Tabela_Historico.

        O ``seq`` é recuperado da SK (``EVENTO#<app_time_ms>#<seq>``) para preservar
        o desempate estável; os atributos opcionais ausentes retornam ``None``.

        Args:
            item: Item retornado pelo DynamoDB (atributos como tipos nativos Python).

        Returns:
            Uma instância de :class:`Evento` equivalente ao item.

        Raises:
            KeyError: se faltar um atributo obrigatório.
            ValueError: se a SK não estiver no formato esperado ou se os valores de
                ``event_type``/``event_source`` não forem válidos.
        """
        app_time_ms = int(item["app_time_ms"])
        seq = _extrair_seq_da_sk(str(item["SK"]), app_time_ms)

        receive_count = item.get("receive_count")
        processing_seconds = item.get("processing_seconds")

        return cls(
            order_id=str(item["order_id"]),
            queue_name=str(item["queue_name"]),
            event_type=EventType(item["event_type"]),
            event_source=EventSource(item["event_source"]),
            app_time=str(item["app_time"]),
            app_time_ms=app_time_ms,
            seq=seq,
            consumer_id=_como_str_opcional(item.get("consumer_id")),
            receive_count=int(receive_count) if receive_count is not None else None,
            processing_seconds=(
                int(processing_seconds) if processing_seconds is not None else None
            ),
            message_group_id=_como_str_opcional(item.get("message_group_id")),
            sqs_message_id=_como_str_opcional(item.get("sqs_message_id")),
            error_detail=_como_str_opcional(item.get("error_detail")),
        )


def _como_str_opcional(valor: Any) -> str | None:
    """Converte um atributo opcional para ``str``, preservando ``None``."""
    return None if valor is None else str(valor)


def _extrair_seq_da_sk(sk: str, app_time_ms: int) -> int:
    """Extrai o ``seq`` de uma SK no formato ``EVENTO#<app_time_ms>#<seq>``.

    Args:
        sk: A Sort Key completa.
        app_time_ms: O epoch em milissegundos esperado na SK (validação de coerência).

    Returns:
        O valor inteiro de ``seq``.

    Raises:
        ValueError: se a SK não seguir o formato esperado.
    """
    if not sk.startswith(PREFIXO_SK):
        raise ValueError(f"SK fora do formato esperado (prefixo ausente): {sk!r}")

    corpo = sk[len(PREFIXO_SK):]
    partes = corpo.split(SEPARADOR)
    if len(partes) != 2:
        raise ValueError(f"SK fora do formato esperado: {sk!r}")

    ms_na_sk, seq_str = partes
    if ms_na_sk != str(app_time_ms):
        raise ValueError(
            "SK inconsistente com app_time_ms: "
            f"SK contém {ms_na_sk!r}, esperado {app_time_ms}"
        )

    return int(seq_str)
