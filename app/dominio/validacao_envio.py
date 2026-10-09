"""Validador do payload de envio de mensagens (Tarefa 3.3).

Valida o payload do envio de 1 a 10 mensagens para a fila selecionada
(Fila_Principal_Standard ou Fila_Principal_FIFO) antes de o Sistema acionar o
SQS. A validação é lógica pura, sem efeitos colaterais, e produz um resultado
que indica se o payload é válido, a lista de campos inválidos e as mensagens em
pt-BR, sempre preservando os valores informados pelo usuário.

Regras (ver critérios de aceitação):
- quantidade de mensagens em [1, 10] (Req. 4.2);
- `order_id` com 1 a 80 caracteres (Req. 4.3);
- `tempo_processamento_simulado` em [0, 300] segundos (Req. 4.4);
- quando a fila é FIFO, `message_group_id` com 1 a 128 caracteres (Req. 4.6);
- quando a fila é FIFO e a Deduplicacao_Por_Conteudo está desabilitada,
  `message_deduplication_id` com 1 a 128 caracteres (Req. 4.7);
- ao rejeitar, identificar ao menos um campo inválido e preservar os valores
  informados, com mensagem em pt-BR (Req. 4.9).

Este módulo apenas valida; o envio efetivo ao SQS é feito em tarefa posterior.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from app.dominio import constantes as c


class TipoFila(str, Enum):
    """Tipo da fila de destino do envio (Req. 4.1)."""

    STANDARD = "standard"
    FIFO = "fifo"


# Nomes de campo usados na lista de campos inválidos. Mantidos como constantes
# para evitar divergência entre a implementação e os testes/consumidores.
CAMPO_TIPO_FILA = "tipo_fila"
CAMPO_QUANTIDADE = "quantidade"
CAMPO_ORDER_ID = "order_id"
CAMPO_TEMPO_PROCESSAMENTO = "tempo_processamento_simulado"
CAMPO_MESSAGE_GROUP_ID = "message_group_id"
CAMPO_MESSAGE_DEDUPLICATION_ID = "message_deduplication_id"


@dataclass(frozen=True)
class PayloadEnvio:
    """Payload de envio informado pelo usuário.

    Os valores são mantidos exatamente como informados (incluindo tipos
    possivelmente inválidos), para que o resultado da validação possa
    preservá-los conforme o Req. 4.9. A normalização/validação é feita pela
    função `validar_payload_envio`.
    """

    tipo_fila: Any
    quantidade: Any
    order_id: Any
    tempo_processamento_simulado: Any
    # Campos específicos de FIFO. Nos envios Standard devem permanecer None.
    message_group_id: Any = None
    # Deduplicacao_Por_Conteudo habilitada por padrão (Config_Aplicada FIFO,
    # Req. 2.7); quando False, o MessageDeduplicationId passa a ser obrigatório.
    deduplicacao_por_conteudo: bool = True
    message_deduplication_id: Any = None


@dataclass(frozen=True)
class ResultadoValidacao:
    """Resultado da validação do payload de envio.

    - `valido`: True se e somente se todos os campos aplicáveis são válidos.
    - `campos_invalidos`: lista com os nomes dos campos reprovados (ao menos um
      quando `valido` é False), sem duplicatas e em ordem estável.
    - `mensagens`: mensagens em pt-BR por campo inválido (chave = nome do campo).
    - `valores_informados`: eco dos valores recebidos, preservados (Req. 4.9).
    """

    valido: bool
    campos_invalidos: list[str] = field(default_factory=list)
    mensagens: dict[str, str] = field(default_factory=dict)
    valores_informados: dict[str, Any] = field(default_factory=dict)


def _eh_inteiro(valor: Any) -> bool:
    """Indica se o valor representa um inteiro (aceita bool? não).

    `bool` é subtipo de `int` em Python; aqui tratamos booleanos como inválidos
    para campos numéricos, pois não representam quantidade/tempo legítimos.
    """

    return isinstance(valor, int) and not isinstance(valor, bool)


def _tamanho_texto_valido(valor: Any, minimo: int, maximo: int) -> bool:
    """Valida um campo textual pelo número de caracteres em [minimo, maximo]."""

    return isinstance(valor, str) and minimo <= len(valor) <= maximo


def _normalizar_tipo_fila(valor: Any) -> TipoFila | None:
    """Converte o valor informado para TipoFila, ou None se inválido."""

    if isinstance(valor, TipoFila):
        return valor
    if isinstance(valor, str):
        chave = valor.strip().lower()
        for tipo in TipoFila:
            if tipo.value == chave:
                return tipo
    return None


def validar_payload_envio(payload: PayloadEnvio) -> ResultadoValidacao:
    """Valida o payload de envio e retorna o resultado detalhado.

    Aceita o payload se e somente se todos os campos aplicáveis estiverem dentro
    das faixas definidas. Ao rejeitar, identifica ao menos um campo inválido e
    preserva os valores informados, com mensagens em pt-BR (Req. 4.2, 4.3, 4.4,
    4.6, 4.7, 4.9).
    """

    campos_invalidos: list[str] = []
    mensagens: dict[str, str] = {}

    valores_informados: dict[str, Any] = {
        CAMPO_TIPO_FILA: payload.tipo_fila,
        CAMPO_QUANTIDADE: payload.quantidade,
        CAMPO_ORDER_ID: payload.order_id,
        CAMPO_TEMPO_PROCESSAMENTO: payload.tempo_processamento_simulado,
        CAMPO_MESSAGE_GROUP_ID: payload.message_group_id,
        "deduplicacao_por_conteudo": payload.deduplicacao_por_conteudo,
        CAMPO_MESSAGE_DEDUPLICATION_ID: payload.message_deduplication_id,
    }

    def reprovar(campo: str, mensagem: str) -> None:
        if campo not in mensagens:
            campos_invalidos.append(campo)
            mensagens[campo] = mensagem

    # --- Tipo da fila (Req. 4.1) ---------------------------------------------
    tipo_fila = _normalizar_tipo_fila(payload.tipo_fila)
    if tipo_fila is None:
        reprovar(
            CAMPO_TIPO_FILA,
            "A fila de destino deve ser 'standard' ou 'fifo'.",
        )

    # --- Quantidade de mensagens [1, 10] (Req. 4.2) --------------------------
    q_min = c.QUANTIDADE_MENSAGENS_MIN
    q_max = c.QUANTIDADE_MENSAGENS_MAX
    if not _eh_inteiro(payload.quantidade) or not (
        q_min <= payload.quantidade <= q_max
    ):
        reprovar(
            CAMPO_QUANTIDADE,
            f"A quantidade de mensagens deve ser um número inteiro entre "
            f"{q_min} e {q_max}.",
        )

    # --- order_id [1, 80] caracteres (Req. 4.3) ------------------------------
    oid_min = c.ORDER_ID_TAMANHO_MIN
    oid_max = c.ORDER_ID_TAMANHO_MAX
    if not _tamanho_texto_valido(payload.order_id, oid_min, oid_max):
        reprovar(
            CAMPO_ORDER_ID,
            f"O identificador do pedido deve ter de {oid_min} a {oid_max} "
            f"caracteres.",
        )

    # --- Tempo_Processamento_Simulado [0, 300] (Req. 4.4) --------------------
    t_min = c.TEMPO_PROCESSAMENTO_SIMULADO_MIN
    t_max = c.TEMPO_PROCESSAMENTO_SIMULADO_MAX
    if not _eh_inteiro(payload.tempo_processamento_simulado) or not (
        t_min <= payload.tempo_processamento_simulado <= t_max
    ):
        reprovar(
            CAMPO_TEMPO_PROCESSAMENTO,
            f"O tempo de processamento simulado deve ser um número inteiro de "
            f"{t_min} a {t_max} segundos.",
        )

    # --- Campos específicos de FIFO (Req. 4.6, 4.7) --------------------------
    if tipo_fila is TipoFila.FIFO:
        mg_min = c.MESSAGE_GROUP_ID_TAMANHO_MIN
        mg_max = c.MESSAGE_GROUP_ID_TAMANHO_MAX
        if not _tamanho_texto_valido(payload.message_group_id, mg_min, mg_max):
            reprovar(
                CAMPO_MESSAGE_GROUP_ID,
                f"Para filas FIFO, o MessageGroupId deve ter de {mg_min} a "
                f"{mg_max} caracteres.",
            )

        # Quando a Deduplicacao_Por_Conteudo está desabilitada, o
        # MessageDeduplicationId passa a ser obrigatório (Req. 4.7).
        if not payload.deduplicacao_por_conteudo:
            md_min = c.MESSAGE_DEDUPLICATION_ID_TAMANHO_MIN
            md_max = c.MESSAGE_DEDUPLICATION_ID_TAMANHO_MAX
            if not _tamanho_texto_valido(
                payload.message_deduplication_id, md_min, md_max
            ):
                reprovar(
                    CAMPO_MESSAGE_DEDUPLICATION_ID,
                    f"Com a deduplicação por conteúdo desabilitada, o "
                    f"MessageDeduplicationId deve ter de {md_min} a {md_max} "
                    f"caracteres.",
                )

    valido = len(campos_invalidos) == 0
    return ResultadoValidacao(
        valido=valido,
        campos_invalidos=campos_invalidos,
        mensagens=mensagens,
        valores_informados=valores_informados,
    )
