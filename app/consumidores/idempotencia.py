"""Verificação de idempotência por identificador de pedido (``order_id``).

Este módulo implementa a lógica de **idempotência** do consumidor descrita na
seção "Idempotência por identificador de pedido" do design.md (Req. 9). Antes de
executar os efeitos colaterais do processamento de uma mensagem, o consumidor
precisa decidir, de forma **testável e sem efeitos colaterais de negócio**, o que
fazer com o recebimento atual. Esta decisão é modelada por
:class:`DecisaoIdempotencia` e produzida por :func:`verificar_idempotencia`.

Regras (Req. 9.1 a 9.6):

- **``order_id`` ausente ou inválido** (``None``, vazio ou com mais de
  :data:`~app.dominio.constantes.ORDER_ID_TAMANHO_MAX` caracteres): o consumidor
  rejeita o processamento, registra um Evento_Observado
  :attr:`~app.dominio.tipos.EventType.ERRO_VALIDACAO_ORDER_ID` e **preserva a
  mensagem** para reprocessamento. Decisão: :attr:`DecisaoIdempotencia.REJEITAR_VALIDACAO`
  (Req. 9.3).
- **Falha de leitura/escrita na Tabela_Historico** durante a verificação: o
  consumidor **não** executa efeitos colaterais, **preserva a mensagem** e sinaliza
  a falha com um Evento_Observado
  :attr:`~app.dominio.tipos.EventType.ERRO_ACESSO_HISTORICO`. Decisão:
  :attr:`DecisaoIdempotencia.ERRO_HISTORICO` (Req. 9.5).
- **Pedido já processado com sucesso** (há evento
  :attr:`~app.dominio.tipos.EventType.PROCESSADA` para o ``order_id``, ou seja, o
  Estado derivado é :attr:`~app.dominio.tipos.Estado.PROCESSADA`): o consumidor
  **não** reexecuta os efeitos colaterais e registra um Evento_Observado
  :attr:`~app.dominio.tipos.EventType.RECEBIMENTO_REPETIDO` com o ``order_id`` e o
  instante da detecção. Decisão: :attr:`DecisaoIdempotencia.JA_PROCESSADO`
  (Req. 9.1, 9.4, 9.6).
- **Caso contrário** (pedido ainda não processado): o consumidor pode seguir com o
  processamento. Decisão: :attr:`DecisaoIdempotencia.PROCESSAR`.

O resultado é exatamente **um** processamento concluído por ``order_id``,
independentemente do número de recebimentos (Req. 9.6): o primeiro recebimento
resulta em ``PROCESSAR`` e, após o sucesso (evento ``processada`` registrado),
todos os recebimentos seguintes resultam em ``JA_PROCESSADO``.

Este módulo é intencionalmente **puro quanto aos efeitos de negócio**: ele não
exclui mensagens nem simula processamento. Seu único efeito externo é registrar os
eventos de auditoria exigidos (``recebimento_repetido``, ``erro_validacao_order_id``,
``erro_acesso_historico``) na Tabela_Historico via :class:`HistoricoRepository`. A
decisão retornada orienta o loop do consumidor (tarefa 10.4 / 12.1 / 13.1).

Referências de requisitos: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from app.acesso.historico_repository import (
    FalhaRegistroEvento,
    HistoricoRepository,
    agora_utc_ms,
)
from app.dominio.constantes import (
    MAX_RECEIVE_COUNT,
    ORDER_ID_TAMANHO_MAX,
    ORDER_ID_TAMANHO_MIN,
)
from app.dominio.evento import Evento
from app.dominio.state_deriver import derivar_estado
from app.dominio.tipos import Estado, EventSource, EventType


class DecisaoIdempotencia(str, Enum):
    """Decisão produzida pela verificação de idempotência de um recebimento.

    A decisão orienta o loop do consumidor sobre o que fazer com a mensagem recebida,
    sem acoplar a lógica de idempotência à execução do processamento. Os valores são
    strings legíveis para facilitar logging e asserções em testes.

    - ``PROCESSAR``: o pedido ainda não foi processado com sucesso; o consumidor pode
      prosseguir com os efeitos colaterais do processamento.
    - ``JA_PROCESSADO``: já existe processamento concluído com sucesso para o
      ``order_id``; o consumidor NÃO deve reexecutar efeitos colaterais. Um
      Evento_Observado ``recebimento_repetido`` foi registrado (Req. 9.1, 9.4, 9.6).
    - ``REJEITAR_VALIDACAO``: o ``order_id`` está ausente ou é inválido; o
      processamento é rejeitado, a mensagem é preservada e um Evento_Observado
      ``erro_validacao_order_id`` foi registrado (Req. 9.3).
    - ``ERRO_HISTORICO``: a leitura/escrita na Tabela_Historico falhou durante a
      verificação; o consumidor NÃO deve executar efeitos colaterais e deve preservar
      a mensagem. Um Evento_Observado ``erro_acesso_historico`` foi registrado quando
      possível (Req. 9.5).
    """

    PROCESSAR = "PROCESSAR"
    JA_PROCESSADO = "JA_PROCESSADO"
    REJEITAR_VALIDACAO = "REJEITAR_VALIDACAO"
    ERRO_HISTORICO = "ERRO_HISTORICO"


@dataclass(frozen=True)
class ResultadoIdempotencia:
    """Resultado detalhado da verificação de idempotência.

    Além da :attr:`decisao` (que orienta o loop do consumidor), expõe campos de
    diagnóstico úteis para o chamador e para os testes, sem exigir nova consulta à
    Tabela_Historico.

    Atributos:
        decisao: A :class:`DecisaoIdempotencia` resultante.
        order_id: O ``order_id`` avaliado (``None`` quando ausente na mensagem).
        detalhe: Mensagem em pt-BR explicando a decisão (campo inválido, causa da
            falha de histórico, etc.). ``None`` quando não há detalhe relevante.
        efeitos_permitidos: ``True`` somente quando ``decisao`` é ``PROCESSAR`` — ou
            seja, quando o consumidor está autorizado a executar os efeitos colaterais
            do processamento. Para todas as demais decisões é ``False``, garantindo
            exatamente um processamento por ``order_id`` (Req. 9.6).
    """

    decisao: DecisaoIdempotencia
    order_id: Optional[str]
    detalhe: Optional[str] = None

    @property
    def efeitos_permitidos(self) -> bool:
        """Indica se o consumidor pode executar os efeitos colaterais do processamento."""
        return self.decisao is DecisaoIdempotencia.PROCESSAR


def order_id_valido(order_id: Optional[str]) -> bool:
    """Indica se um ``order_id`` é válido para processamento.

    Um ``order_id`` é válido quando é uma string **não vazia** com tamanho dentro da
    faixa ``[ORDER_ID_TAMANHO_MIN, ORDER_ID_TAMANHO_MAX]`` (1 a 80 caracteres por
    padrão). São considerados **inválidos** (Req. 9.3):

    - ``None`` (identificador ausente);
    - string vazia (ou que não seja ``str``);
    - string com mais de :data:`ORDER_ID_TAMANHO_MAX` caracteres.

    Args:
        order_id: O identificador do pedido a validar.

    Returns:
        ``True`` se o ``order_id`` for válido; ``False`` caso contrário.
    """
    if not isinstance(order_id, str):
        return False
    tamanho = len(order_id)
    return ORDER_ID_TAMANHO_MIN <= tamanho <= ORDER_ID_TAMANHO_MAX


def _pedido_ja_processado(
    eventos: list[Evento],
    *,
    max_receive_count: int,
) -> bool:
    """Indica se o pedido já tem processamento concluído com sucesso.

    O Estado é sempre **derivado** dos eventos pelo StateDeriver (nunca lido de um
    campo mutável), preservando as regras de precedência do design. Um pedido é
    considerado já processado quando o Estado derivado é
    :attr:`~app.dominio.tipos.Estado.PROCESSADA` — isto é, há um evento
    ``processada`` registrado (Req. 9.1, 9.2).

    Args:
        eventos: Eventos do pedido lidos da Tabela_Historico.
        max_receive_count: Limite da Redrive_Policy repassado ao StateDeriver.

    Returns:
        ``True`` se o pedido já foi processado com sucesso; ``False`` caso contrário.
    """
    if not eventos:
        return False
    return derivar_estado(eventos, max_receive_count=max_receive_count) is Estado.PROCESSADA


def verificar_idempotencia(
    repositorio: HistoricoRepository,
    *,
    order_id: Optional[str],
    queue_name: str,
    consumer_id: Optional[str] = None,
    receive_count: Optional[int] = None,
    message_group_id: Optional[str] = None,
    sqs_message_id: Optional[str] = None,
    max_receive_count: int = MAX_RECEIVE_COUNT,
) -> ResultadoIdempotencia:
    """Verifica a idempotência de um recebimento e retorna a decisão de processamento.

    Esta é a entrada principal do módulo. A ordem de avaliação segue as regras do
    design (Req. 9):

    1. **Validação do ``order_id``** (Req. 9.3). Se ausente/inválido, registra um
       Evento_Observado ``erro_validacao_order_id`` e retorna
       :attr:`DecisaoIdempotencia.REJEITAR_VALIDACAO`, preservando a mensagem. A
       falha ao registrar o evento de validação é tratada como erro de acesso ao
       histórico (ver passo 2), também preservando a mensagem.
    2. **Consulta por ``order_id``** na Tabela_Historico (Req. 9.2). Se a leitura
       falhar, tenta registrar um Evento_Observado ``erro_acesso_historico`` e
       retorna :attr:`DecisaoIdempotencia.ERRO_HISTORICO`, sem executar efeitos e
       preservando a mensagem (Req. 9.5). Se a própria escrita do evento de erro
       também falhar, ainda assim retorna ``ERRO_HISTORICO`` (a mensagem permanece
       na fila e será reavaliada em um recebimento futuro).
    3. **Pedido já processado** (Req. 9.1, 9.4, 9.6). Se o Estado derivado for
       ``Processada``, registra um Evento_Observado ``recebimento_repetido`` com o
       ``order_id`` e o instante da detecção e retorna
       :attr:`DecisaoIdempotencia.JA_PROCESSADO`, sem reexecutar efeitos.
    4. **Pedido ainda não processado**. Retorna :attr:`DecisaoIdempotencia.PROCESSAR`,
       autorizando o consumidor a executar os efeitos colaterais exatamente uma vez.

    A função só produz efeitos de **auditoria** (registro de eventos). Ela nunca
    exclui mensagens nem executa o processamento de negócio — isso cabe ao loop do
    consumidor conforme a decisão retornada.

    Args:
        repositorio: Repositório da Tabela_Historico (consulta e registro de eventos).
        order_id: Identificador do pedido da mensagem recebida (pode ser ``None``).
        queue_name: Nome da fila de onde a mensagem foi recebida (usado nos eventos).
        consumer_id: Identificador do consumidor que recebeu a mensagem (opcional).
        receive_count: Contagem de recebimentos da mensagem (``ApproximateReceiveCount``).
        message_group_id: ``MessageGroupId`` da mensagem (apenas FIFO; opcional).
        sqs_message_id: ``MessageId`` retornado pelo SQS (opcional).
        max_receive_count: Limite da Redrive_Policy repassado ao StateDeriver.

    Returns:
        Um :class:`ResultadoIdempotencia` com a decisão e os detalhes de diagnóstico.
    """
    # 1) Validação do order_id (Req. 9.3). order_id ausente/inválido => rejeitar,
    #    registrar erro_validacao_order_id e preservar a mensagem.
    if not order_id_valido(order_id):
        detalhe = _detalhe_order_id_invalido(order_id)
        # Para o registro do evento de erro usamos um identificador seguro: quando o
        # order_id é inválido, não há PK de pedido confiável; usamos um marcador
        # determinístico para não perder o rastro do erro de validação.
        order_id_para_registro = order_id if isinstance(order_id, str) and order_id else "<order_id_invalido>"
        registrado = _tentar_registrar_evento(
            repositorio,
            order_id=order_id_para_registro,
            queue_name=queue_name,
            event_type=EventType.ERRO_VALIDACAO_ORDER_ID,
            consumer_id=consumer_id,
            receive_count=receive_count,
            message_group_id=message_group_id,
            sqs_message_id=sqs_message_id,
            error_detail=detalhe,
        )
        if not registrado:
            # Não foi possível nem registrar o erro de validação: tratamos como
            # falha de acesso ao histórico, preservando a mensagem (Req. 9.5).
            return ResultadoIdempotencia(
                decisao=DecisaoIdempotencia.ERRO_HISTORICO,
                order_id=order_id if isinstance(order_id, str) else None,
                detalhe=(
                    "Falha ao registrar o erro de validação do identificador de pedido "
                    "na Tabela_Historico; a mensagem foi preservada."
                ),
            )
        return ResultadoIdempotencia(
            decisao=DecisaoIdempotencia.REJEITAR_VALIDACAO,
            order_id=order_id if isinstance(order_id, str) else None,
            detalhe=detalhe,
        )

    # A partir daqui, order_id é uma str válida.
    assert isinstance(order_id, str)

    # 2) Consulta por order_id na Tabela_Historico (Req. 9.2). Se a leitura falhar,
    #    não executar efeitos, preservar a mensagem e registrar erro_acesso_historico
    #    (Req. 9.5).
    try:
        eventos = repositorio.consultar_por_pedido(order_id)
    except Exception as erro:  # noqa: BLE001 — qualquer falha de leitura preserva a mensagem
        detalhe = (
            "Falha ao consultar a Tabela_Historico durante a verificação de "
            f"idempotência do pedido {order_id!r}: {erro!r}"
        )
        _tentar_registrar_evento(
            repositorio,
            order_id=order_id,
            queue_name=queue_name,
            event_type=EventType.ERRO_ACESSO_HISTORICO,
            consumer_id=consumer_id,
            receive_count=receive_count,
            message_group_id=message_group_id,
            sqs_message_id=sqs_message_id,
            error_detail=detalhe,
        )
        return ResultadoIdempotencia(
            decisao=DecisaoIdempotencia.ERRO_HISTORICO,
            order_id=order_id,
            detalhe=detalhe,
        )

    # 3) Pedido já processado com sucesso (Req. 9.1, 9.4, 9.6). Registra
    #    recebimento_repetido com order_id e instante e NÃO reexecuta efeitos.
    if _pedido_ja_processado(eventos, max_receive_count=max_receive_count):
        detalhe = (
            f"Recebimento repetido detectado para o pedido {order_id!r}; o "
            "processamento já foi concluído com sucesso e não será reexecutado."
        )
        registrado = _tentar_registrar_evento(
            repositorio,
            order_id=order_id,
            queue_name=queue_name,
            event_type=EventType.RECEBIMENTO_REPETIDO,
            consumer_id=consumer_id,
            receive_count=receive_count,
            message_group_id=message_group_id,
            sqs_message_id=sqs_message_id,
            error_detail=None,
        )
        if not registrado:
            # Falhou ao registrar o recebimento repetido: isso é uma falha de acesso
            # ao histórico. Não executamos efeitos e preservamos a mensagem (Req. 9.5).
            return ResultadoIdempotencia(
                decisao=DecisaoIdempotencia.ERRO_HISTORICO,
                order_id=order_id,
                detalhe=(
                    "Falha ao registrar o recebimento repetido na Tabela_Historico; "
                    "a mensagem foi preservada e os efeitos não foram executados."
                ),
            )
        return ResultadoIdempotencia(
            decisao=DecisaoIdempotencia.JA_PROCESSADO,
            order_id=order_id,
            detalhe=detalhe,
        )

    # 4) Pedido ainda não processado: autoriza o processamento (efeitos exatamente
    #    uma vez por order_id).
    return ResultadoIdempotencia(
        decisao=DecisaoIdempotencia.PROCESSAR,
        order_id=order_id,
        detalhe=None,
    )


def _detalhe_order_id_invalido(order_id: Optional[str]) -> str:
    """Produz a mensagem em pt-BR que descreve por que o ``order_id`` é inválido."""
    if order_id is None:
        return "Identificador de pedido ausente (order_id é obrigatório)."
    if not isinstance(order_id, str) or order_id == "":
        return "Identificador de pedido vazio (order_id deve ter de 1 a 80 caracteres)."
    return (
        "Identificador de pedido inválido: possui "
        f"{len(order_id)} caracteres, acima do máximo de {ORDER_ID_TAMANHO_MAX}."
    )


def _tentar_registrar_evento(
    repositorio: HistoricoRepository,
    *,
    order_id: str,
    queue_name: str,
    event_type: EventType,
    consumer_id: Optional[str],
    receive_count: Optional[int],
    message_group_id: Optional[str],
    sqs_message_id: Optional[str],
    error_detail: Optional[str],
) -> bool:
    """Registra um Evento_Observado de auditoria, tolerando falhas de escrita.

    Monta um :class:`Evento` com o instante atual (``app_time``/``app_time_ms`` via
    :func:`agora_utc_ms`) e o persiste pelo :class:`HistoricoRepository`. Todos os
    eventos registrados por esta verificação são **Evento_Observado** (resultam de
    uma constatação concreta da aplicação): ``recebimento_repetido``,
    ``erro_validacao_order_id`` e ``erro_acesso_historico`` (Req. 9.3, 9.4, 9.5).

    A escrita pode falhar (``FalhaRegistroEvento`` após esgotar as tentativas do
    repositório, ou qualquer outra exceção). Como esta função é usada justamente nos
    caminhos de erro/auditoria, a falha de escrita **não** deve interromper a decisão
    do consumidor com uma exceção: ela é sinalizada pelo valor de retorno para que o
    chamador decida preservar a mensagem (Req. 9.5).

    Returns:
        ``True`` se o evento foi registrado com sucesso; ``False`` se a escrita falhou.
    """
    app_time, app_time_ms = agora_utc_ms()
    evento = Evento(
        order_id=order_id,
        queue_name=queue_name,
        event_type=event_type,
        event_source=EventSource.OBSERVADO,
        app_time=app_time,
        app_time_ms=app_time_ms,
        consumer_id=consumer_id,
        receive_count=receive_count,
        message_group_id=message_group_id,
        sqs_message_id=sqs_message_id,
        error_detail=error_detail,
    )
    try:
        repositorio.registrar_evento(evento)
        return True
    except (FalhaRegistroEvento, Exception):  # noqa: BLE001 — falha de escrita não propaga
        return False
