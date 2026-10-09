"""Lógica de processamento e exclusão de mensagens sob Cenario_Falha (Tarefa 10.4).

Este módulo implementa o núcleo de processamento de uma mensagem recebida por um
consumidor (Standard ou FIFO). A sequência de passos reproduz o fluxo descrito na
seção *Fluxo Standard — caminho feliz* do ``design.md`` e nos critérios do Req. 8:

1. **Registrar o recebimento** (``recebida``, Evento_Observado) com o
   ``consumer_id`` do consumidor responsável e a contagem de recebimentos
   (``receive_count``/``ApproximateReceiveCount``) — Req. 8.3, 8.4, 10.4.
2. **Registrar o início do processamento** (``processamento_iniciado``).
3. **Aguardar** um período equivalente ao Tempo_Processamento_Simulado da mensagem
   antes de concluir — Req. 8.5. A espera é feita por uma função ``sleep``
   **injetável**, de modo que os testes executem sem espera real.
4. **Aplicar o Cenario_Falha** da mensagem, determinando o desfecho do
   processamento (ver :class:`CenarioFalha`).
5. **Excluir a mensagem** (DeleteMessage) **se e somente se** o processamento
   concluir com sucesso — Req. 8.6, 8.7, 8.9. Em qualquer outro desfecho (exceção,
   interrupção, cenário que força a expiração do Visibility_Timeout) a mensagem é
   **preservada** na fila para reprocessamento e o erro original é registrado na
   Tabela_Historico — Req. 8.8, 8.9.

Dependências injetáveis:
- :class:`~app.acesso.sqs_gateway.SqsGateway` para o ``delete_message``.
- :class:`~app.acesso.historico_repository.HistoricoRepository` para
  ``registrar_evento``.
- Uma função ``sleep`` (``Callable[[float], None]``), com padrão ``time.sleep``,
  substituível nos testes para evitar espera real.

Este módulo trata apenas da etapa de processamento/exclusão. A verificação de
idempotência por ``order_id`` (Req. 9) é implementada separadamente (tarefa 10.1) e
ocorre antes de chamar :func:`processar_mensagem`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional

from app.acesso.historico_repository import HistoricoRepository, agora_utc_ms
from app.acesso.sqs_gateway import MensagemRecebida, SqsGateway
from app.dominio.evento import Evento
from app.dominio.tipos import EventSource, EventType

# Tipo da função de espera injetável. Recebe a duração em segundos e não retorna
# nada. Em produção é ``time.sleep``; nos testes é substituída por um dublê que
# apenas registra a chamada, sem esperar de verdade.
FuncaoSleep = Callable[[float], None]


class CenarioFalha(str, Enum):
    """Cenários de falha simuláveis no processamento de uma mensagem (Req. 7, 8).

    O Cenario_Falha determina o comportamento do consumidor ao processar a
    mensagem. Apenas :data:`CenarioFalha.SUCESSO` conclui o processamento com
    êxito e, portanto, leva à exclusão da mensagem (Req. 8.6, 8.7, 8.9). Todos os
    demais cenários preservam a mensagem na fila.

    Valores:
        SUCESSO: processamento conclui normalmente; a mensagem é excluída
            (DeleteMessage) e registra-se ``processada`` e ``excluida``
            (Req. 7.1, 8.6).
        EXCECAO_PROPOSITAL: o consumidor lança uma exceção proposital durante o
            processamento; a mensagem fica em estado ``Falha`` e o erro é
            registrado, sem exclusão (Req. 7.3, 8.8).
        INTERRUPCAO: o consumidor é interrompido durante o processamento (sem
            confirmação); a mensagem não é excluída e volta a ficar visível após
            o Visibility_Timeout (Req. 7.4, 8.9).
        DURACAO_SUPERIOR_VISIBILITY: o processamento dura mais que o
            Visibility_Timeout; a mensagem volta a ficar visível e a contagem de
            recebimentos é incrementada em uma reentrega futura (Req. 7.2, 8.9).
            Não há DeleteMessage.
        RECEBIMENTO_REPETIDO: simula o recebimento repetido da mesma mensagem; o
            consumidor abstém-se de excluir para que a mensagem seja reentregue
            ao menos uma vez (Req. 7.5). Não há DeleteMessage.
        ENCAMINHAR_DLQ: a mensagem falha repetidamente para que, ao atingir o
            maxReceiveCount, o SQS a mova para a DLQ (Req. 7.6). Não há
            DeleteMessage.
    """

    SUCESSO = "sucesso"
    EXCECAO_PROPOSITAL = "excecao_proposital"
    INTERRUPCAO = "interrupcao"
    DURACAO_SUPERIOR_VISIBILITY = "duracao_superior_visibility"
    RECEBIMENTO_REPETIDO = "recebimento_repetido"
    ENCAMINHAR_DLQ = "encaminhar_dlq"


# Conjunto de cenários cujo desfecho é "sucesso" (processamento concluído e
# mensagem excluída). Mantê-lo explícito deixa a regra do Req. 8.6/8.7/8.9 óbvia:
# a exclusão ocorre se e somente se o cenário for de sucesso.
CENARIOS_DE_SUCESSO: frozenset[CenarioFalha] = frozenset({CenarioFalha.SUCESSO})


class InterrupcaoProcessamento(BaseException):
    """Interrupção do consumidor durante o processamento (CenarioFalha.INTERRUPCAO).

    Herda de :class:`BaseException` (não de :class:`Exception`) para modelar uma
    interrupção abrupta do processo/thread, análoga a ``KeyboardInterrupt``. O
    processamento é abortado sem concluir e, portanto, **sem** DeleteMessage; a
    mensagem é preservada e volta a ficar visível após o Visibility_Timeout
    (Req. 7.4, 8.9).
    """


class ErroProcessamentoSimulado(Exception):
    """Exceção proposital lançada no cenário :data:`CenarioFalha.EXCECAO_PROPOSITAL`.

    Representa a falha de processamento controlada do laboratório. A mensagem é
    preservada na fila e o erro original é registrado na Tabela_Historico
    (Req. 7.3, 8.8).
    """


@dataclass(frozen=True)
class ResultadoProcessamento:
    """Resultado de :func:`processar_mensagem`.

    Atributos:
        order_id: Identificador do pedido processado.
        cenario: O :class:`CenarioFalha` aplicado.
        sucesso: ``True`` se o processamento concluiu com sucesso (e a mensagem
            foi excluída); ``False`` em qualquer outro desfecho.
        excluida: ``True`` se o DeleteMessage foi executado. Por construção,
            ``excluida == sucesso`` (a exclusão ocorre se e somente se há sucesso)
            (Req. 8.6, 8.7, 8.9).
        erro_detalhe: Mensagem do erro original quando o processamento não
            concluiu com sucesso; ``None`` em caso de sucesso (Req. 8.8).
    """

    order_id: str
    cenario: CenarioFalha
    sucesso: bool
    excluida: bool
    erro_detalhe: Optional[str] = None


def _novo_evento(
    *,
    order_id: str,
    queue_name: str,
    event_type: EventType,
    event_source: EventSource,
    seq: int,
    consumer_id: Optional[str] = None,
    receive_count: Optional[int] = None,
    processing_seconds: Optional[int] = None,
    message_group_id: Optional[str] = None,
    sqs_message_id: Optional[str] = None,
    error_detail: Optional[str] = None,
) -> Evento:
    """Cria um :class:`Evento` carimbando o horário real da aplicação (UTC, ms).

    Centraliza a obtenção do par ``(app_time, app_time_ms)`` via
    :func:`app.acesso.historico_repository.agora_utc_ms`, garantindo precisão de
    milissegundos em todos os eventos registrados (Req. 8.3, 11.1).
    """
    app_time, app_time_ms = agora_utc_ms()
    return Evento(
        order_id=order_id,
        queue_name=queue_name,
        event_type=event_type,
        event_source=event_source,
        app_time=app_time,
        app_time_ms=app_time_ms,
        seq=seq,
        consumer_id=consumer_id,
        receive_count=receive_count,
        processing_seconds=processing_seconds,
        message_group_id=message_group_id,
        sqs_message_id=sqs_message_id,
        error_detail=error_detail,
    )


def _aplicar_cenario(cenario: CenarioFalha) -> None:
    """Aplica o efeito do Cenario_Falha após a espera de processamento.

    - :data:`CenarioFalha.SUCESSO`: retorna normalmente (processamento concluído).
    - :data:`CenarioFalha.EXCECAO_PROPOSITAL`: levanta
      :class:`ErroProcessamentoSimulado`.
    - :data:`CenarioFalha.INTERRUPCAO`: levanta :class:`InterrupcaoProcessamento`.
    - :data:`CenarioFalha.DURACAO_SUPERIOR_VISIBILITY`,
      :data:`CenarioFalha.RECEBIMENTO_REPETIDO`,
      :data:`CenarioFalha.ENCAMINHAR_DLQ`: retornam sem concluir o processamento
      com "sucesso" — nestes cenários o consumidor **abstém-se** de excluir a
      mensagem para que o Visibility_Timeout expire e ela seja reentregue
      (Req. 8.9). A ausência de exclusão é decidida em :func:`processar_mensagem`
      a partir do cenário, não aqui.

    Esta função só sinaliza desfechos que interrompem o fluxo via exceção. Os
    cenários "sem confirmação" não lançam exceção: eles simplesmente não são de
    sucesso, e por isso não levam ao DeleteMessage.
    """
    if cenario is CenarioFalha.EXCECAO_PROPOSITAL:
        raise ErroProcessamentoSimulado(
            "Exceção proposital no consumidor durante o processamento "
            "(CenarioFalha.EXCECAO_PROPOSITAL)."
        )
    if cenario is CenarioFalha.INTERRUPCAO:
        raise InterrupcaoProcessamento(
            "Consumidor interrompido durante o processamento "
            "(CenarioFalha.INTERRUPCAO)."
        )
    # Demais cenários (SUCESSO e os 'sem confirmação') não lançam exceção aqui.


def processar_mensagem(
    mensagem: MensagemRecebida,
    *,
    nome_fila: str,
    consumer_id: str,
    cenario: CenarioFalha,
    processing_seconds: int,
    sqs: SqsGateway,
    historico: HistoricoRepository,
    sleep: FuncaoSleep = time.sleep,
    message_group_id: Optional[str] = None,
    order_id: Optional[str] = None,
) -> ResultadoProcessamento:
    """Processa uma mensagem recebida aplicando o Cenario_Falha (Req. 8.3–8.9).

    Executa, nesta ordem:

    1. Registra o Evento_Observado ``recebida`` com ``consumer_id`` e
       ``receive_count`` (``ApproximateReceiveCount`` da mensagem) — Req. 8.3, 8.4,
       10.4.
    2. Registra o Evento_Observado ``processamento_iniciado``.
    3. Aguarda ``processing_seconds`` via a função ``sleep`` injetável — Req. 8.5.
    4. Aplica o ``cenario``.
    5. **Se, e somente se, o processamento concluir com sucesso** (``cenario`` em
       :data:`CENARIOS_DE_SUCESSO`), executa ``delete_message`` e registra
       ``processada`` e ``excluida`` — Req. 8.6, 8.7, 8.9. Em qualquer outro
       desfecho, **não** exclui a mensagem (ela é preservada para reprocessamento)
       e registra ``falha`` com o **erro original** em ``error_detail`` — Req. 8.8,
       8.9.

    A função nunca propaga as exceções dos cenários de falha
    (:class:`ErroProcessamentoSimulado`, :class:`InterrupcaoProcessamento`): ela as
    captura, registra o erro original e retorna um :class:`ResultadoProcessamento`
    com ``sucesso=False``. Assim, o consumidor que a chama em laço continua
    operando. Caso o próprio ``delete_message`` falhe no caminho de sucesso, o erro
    original da AWS é propagado ao chamador (Req. 8.8), pois trata-se de uma falha
    de infraestrutura, não de um cenário simulado.

    Args:
        mensagem: A mensagem recebida do SQS (carrega ``receipt_handle``,
            ``approximate_receive_count`` e ``message_group_id``).
        nome_fila: Nome da fila de origem (usado no registro dos eventos e na
            exclusão).
        consumer_id: Identificador único do consumidor responsável (Req. 8.4,
            10.4).
        cenario: O :class:`CenarioFalha` a aplicar.
        processing_seconds: Tempo_Processamento_Simulado da mensagem, em segundos
            (Req. 8.5). Usado como argumento da espera e registrado nos eventos.
        sqs: :class:`SqsGateway` para o DeleteMessage.
        historico: :class:`HistoricoRepository` para registrar os eventos.
        sleep: Função de espera injetável (padrão ``time.sleep``). Nos testes,
            injeta-se um dublê que não espera de verdade.
        message_group_id: ``MessageGroupId`` da mensagem (apenas FIFO). Quando
            ``None``, usa o da própria ``mensagem`` se disponível.
        order_id: Identificador do pedido já validado pela verificação de
            idempotência (tarefa 10.1). Quando ``None``, usa o corpo da mensagem
            como ``order_id`` (conveniência do laboratório).

    Returns:
        Um :class:`ResultadoProcessamento` descrevendo o desfecho. ``excluida`` é
        ``True`` se e somente se ``sucesso`` é ``True``.

    Raises:
        Exception: o erro original da AWS, caso o ``delete_message`` falhe no
            caminho de sucesso (Req. 8.8). Exceções dos cenários simulados NÃO são
            propagadas.
    """
    # O order_id canônico vem da verificação de idempotência (tarefa 10.1).
    # Consumidores reais passam o order_id já validado; quando ausente, usamos o
    # corpo da mensagem (no laboratório, o corpo é o próprio order_id simples).
    order_id_efetivo = order_id if order_id is not None else mensagem.body

    grupo = (
        message_group_id
        if message_group_id is not None
        else mensagem.message_group_id
    )

    # (1) Registrar o recebimento (Evento_Observado) com consumidor e tentativas.
    historico.registrar_evento(
        _novo_evento(
            order_id=order_id_efetivo,
            queue_name=nome_fila,
            event_type=EventType.RECEBIDA,
            event_source=EventSource.OBSERVADO,
            seq=1,
            consumer_id=consumer_id,
            receive_count=mensagem.approximate_receive_count,
            processing_seconds=processing_seconds,
            message_group_id=grupo,
            sqs_message_id=mensagem.message_id,
        )
    )

    # (2) Registrar o início do processamento (Evento_Observado).
    historico.registrar_evento(
        _novo_evento(
            order_id=order_id_efetivo,
            queue_name=nome_fila,
            event_type=EventType.PROCESSAMENTO_INICIADO,
            event_source=EventSource.OBSERVADO,
            seq=2,
            consumer_id=consumer_id,
            receive_count=mensagem.approximate_receive_count,
            processing_seconds=processing_seconds,
            message_group_id=grupo,
            sqs_message_id=mensagem.message_id,
        )
    )

    # (3) Aguardar o Tempo_Processamento_Simulado via sleep injetável (Req. 8.5).
    sleep(processing_seconds)

    # (4) Aplicar o cenário. Exceções simuladas são tratadas abaixo.
    try:
        _aplicar_cenario(cenario)
        concluiu_sem_excecao = True
        erro_original: Optional[BaseException] = None
    except (ErroProcessamentoSimulado, InterrupcaoProcessamento) as erro:
        concluiu_sem_excecao = False
        erro_original = erro

    # Desfecho de sucesso = cenário de sucesso E processamento sem exceção.
    # A exclusão ocorre SE E SOMENTE SE o processamento concluir com sucesso
    # (Req. 8.6, 8.7, 8.9).
    sucesso = concluiu_sem_excecao and cenario in CENARIOS_DE_SUCESSO

    if sucesso:
        # (5a) Caminho de sucesso: excluir a mensagem e registrar processada +
        # excluida. Uma falha do próprio DeleteMessage é de infraestrutura e seu
        # erro original é propagado ao chamador (Req. 8.8).
        historico.registrar_evento(
            _novo_evento(
                order_id=order_id_efetivo,
                queue_name=nome_fila,
                event_type=EventType.PROCESSADA,
                event_source=EventSource.OBSERVADO,
                seq=3,
                consumer_id=consumer_id,
                receive_count=mensagem.approximate_receive_count,
                processing_seconds=processing_seconds,
                message_group_id=grupo,
                sqs_message_id=mensagem.message_id,
            )
        )
        sqs.delete_message(nome_fila, mensagem.receipt_handle)
        historico.registrar_evento(
            _novo_evento(
                order_id=order_id_efetivo,
                queue_name=nome_fila,
                event_type=EventType.EXCLUIDA,
                event_source=EventSource.OBSERVADO,
                seq=4,
                consumer_id=consumer_id,
                receive_count=mensagem.approximate_receive_count,
                processing_seconds=processing_seconds,
                message_group_id=grupo,
                sqs_message_id=mensagem.message_id,
            )
        )
        return ResultadoProcessamento(
            order_id=order_id_efetivo,
            cenario=cenario,
            sucesso=True,
            excluida=True,
            erro_detalhe=None,
        )

    # (5b) Qualquer outro desfecho: preservar a mensagem (NÃO excluir) e registrar
    # o erro original na Tabela_Historico (Req. 8.8, 8.9).
    erro_detalhe = _descrever_erro(cenario, erro_original)
    historico.registrar_evento(
        _novo_evento(
            order_id=order_id_efetivo,
            queue_name=nome_fila,
            event_type=EventType.FALHA,
            event_source=EventSource.OBSERVADO,
            seq=3,
            consumer_id=consumer_id,
            receive_count=mensagem.approximate_receive_count,
            processing_seconds=processing_seconds,
            message_group_id=grupo,
            sqs_message_id=mensagem.message_id,
            error_detail=erro_detalhe,
        )
    )
    return ResultadoProcessamento(
        order_id=order_id_efetivo,
        cenario=cenario,
        sucesso=False,
        excluida=False,
        erro_detalhe=erro_detalhe,
    )


def _descrever_erro(
    cenario: CenarioFalha, erro_original: Optional[BaseException]
) -> str:
    """Produz a descrição do erro original para o campo ``error_detail``.

    Quando o cenário levantou uma exceção simulada, usa a representação do erro
    original (Req. 8.8). Para os cenários "sem confirmação" (que não lançam
    exceção, mas também não concluem com sucesso), descreve a razão pela qual a
    mensagem foi preservada, deixando o histórico explícito sobre o motivo da não
    exclusão (Req. 8.9).
    """
    if erro_original is not None:
        return repr(erro_original)

    razoes_sem_confirmacao = {
        CenarioFalha.DURACAO_SUPERIOR_VISIBILITY: (
            "Processamento com duração superior ao Visibility_Timeout: mensagem "
            "preservada para reentrega (sem DeleteMessage)."
        ),
        CenarioFalha.RECEBIMENTO_REPETIDO: (
            "Cenário de recebimento repetido: mensagem preservada para ser "
            "reentregue ao menos uma vez (sem DeleteMessage)."
        ),
        CenarioFalha.ENCAMINHAR_DLQ: (
            "Cenário de encaminhamento para DLQ: mensagem preservada para falhar "
            "até atingir o maxReceiveCount (sem DeleteMessage)."
        ),
    }
    return razoes_sem_confirmacao.get(
        cenario, "Processamento não concluído com sucesso: mensagem preservada."
    )
