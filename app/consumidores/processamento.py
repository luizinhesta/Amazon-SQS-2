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

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional

from app.acesso.historico_repository import (
    FalhaRegistroEvento,
    HistoricoRepository,
    agora_utc_ms,
)
from app.acesso.sqs_gateway import (
    MensagemRecebida,
    SqsGateway,
    e_erro_infra_sqs,
    e_receipt_handle_expirado,
)
from app.dominio.constantes import HEARTBEAT_VISIBILITY_SEGUNDOS
from app.dominio.evento import Evento
from app.dominio.tipos import EventSource, EventType
from app.observabilidade.log_estruturado import log_evento

# Faixa máxima do Visibility_Timeout aceita pelo SQS (12 horas, em segundos). Usada
# apenas pela validação da extensão opt-in (frente C).
VISIBILITY_TIMEOUT_MAX_SEGUNDOS: int = 43200

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
        exclusao_nao_confirmada: ``True`` quando o processamento concluiu o
            negócio com sucesso, mas o ``DeleteMessage`` falhou por um erro de
            infraestrutura reconhecido (tipicamente ``ReceiptHandle`` expirado).
            Nesse caso ``sucesso`` e ``excluida`` são ``False`` (a mensagem segue
            na fila), mas a falha não é de processamento: um evento
            ``exclusao_nao_confirmada`` é registrado e o consumidor segue vivo.
    """

    order_id: str
    cenario: CenarioFalha
    sucesso: bool
    excluida: bool
    erro_detalhe: Optional[str] = None
    exclusao_nao_confirmada: bool = False


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


def validar_extensao_visibility(
    estender: bool, valor: Optional[int]
) -> Optional[int]:
    """Valida a extensão opt-in do Visibility_Timeout (frente C) — owner único.

    Esta é a **única** autoridade de validação da extensão: o
    :class:`~app.acesso.sqs_gateway.SqsGateway` apenas repassa o valor. A
    validação ocorre **antes** de qualquer chamada à AWS, de modo que uma
    configuração inválida vire ``ValueError`` sem efeito colateral.

    Regras:
      - Com ``estender`` desligado (padrão), nada é validado e devolve-se ``None``
        (o comportamento atual é 100% preservado).
      - Com ``estender`` ligado, ``valor`` é **obrigatório**, inteiro e deve estar
        na faixa ``[HEARTBEAT_VISIBILITY_SEGUNDOS, 43200]`` (o teto do SQS). Fora
        da faixa, ausente ou de tipo inválido → :class:`ValueError`.

    Returns:
        O ``valor`` validado quando ``estender`` é ``True``; ``None`` caso
        contrário.

    Raises:
        ValueError: quando ``estender`` é ``True`` e ``valor`` é ausente, não
            inteiro ou fora da faixa permitida.
    """
    if not estender:
        return None
    # ``bool`` é subclasse de ``int``: rejeitamos explicitamente para não aceitar
    # True/False como se fossem o número de segundos.
    if valor is None or isinstance(valor, bool) or not isinstance(valor, int):
        raise ValueError(
            "visibility_timeout_estendido é obrigatório e deve ser inteiro quando "
            "estender_visibility=True "
            f"(valor informado: {valor!r})."
        )
    if not (HEARTBEAT_VISIBILITY_SEGUNDOS <= valor <= VISIBILITY_TIMEOUT_MAX_SEGUNDOS):
        raise ValueError(
            "visibility_timeout_estendido deve estar entre "
            f"{HEARTBEAT_VISIBILITY_SEGUNDOS} e {VISIBILITY_TIMEOUT_MAX_SEGUNDOS} "
            f"segundos (valor informado: {valor})."
        )
    return valor


def _aguardar_com_heartbeat(
    *,
    processing_seconds: int,
    visibility_timeout_estendido: int,
    nome_fila: str,
    receipt_handle: str,
    consumer_id: str,
    order_id: str,
    sqs: SqsGateway,
    sleep: FuncaoSleep,
) -> None:
    """Aguarda ``processing_seconds`` renovando o Visibility_Timeout por fatias.

    Em vez de um ``sleep`` único, dorme em fatias de
    :data:`HEARTBEAT_VISIBILITY_SEGUNDOS` e, entre as fatias, chama
    ``change_message_visibility`` renovando o prazo da mensagem (frente C). A
    função ``sleep`` injetável continua sendo usada, de modo que os testes
    controlam o avanço sem espera real.

    Uma falha de ``change_message_visibility`` é **recuperável**: registra um log
    ``WARNING`` e segue o processamento sem renovar (a mensagem pode expirar e cair
    no fluxo normal de reentrega). Nunca aborta o processamento.
    """
    restante = processing_seconds
    while restante > 0:
        fatia = min(HEARTBEAT_VISIBILITY_SEGUNDOS, restante)
        sleep(fatia)
        restante -= fatia
        if restante <= 0:
            break
        try:
            sqs.change_message_visibility(
                nome_fila, receipt_handle, visibility_timeout_estendido
            )
        except Exception as erro:  # noqa: BLE001 — renovação é best-effort
            log_evento(
                logging.WARNING,
                "change_message_visibility_falhou",
                order_id=order_id,
                fila=nome_fila,
                consumer_id=consumer_id,
                erro=repr(erro),
            )
            # Sem renovar: continua dormindo o restante; a mensagem pode expirar
            # e seguir o fluxo normal de reentrega.
            sleep(restante)
            return


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
    estender_visibility: bool = False,
    visibility_timeout_estendido: Optional[int] = None,
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
    operando. Caso o próprio ``delete_message`` falhe no caminho de sucesso por um
    erro de infraestrutura reconhecido (``ReceiptHandle`` expirado ou outro erro de
    ``ClientError``/``BotoCoreError``), a função também **não** propaga: registra o
    evento ``exclusao_nao_confirmada`` com o erro original e retorna
    ``sucesso=False``/``excluida=False``/``exclusao_nao_confirmada=True`` — isso
    impede que a thread do consumidor morra. Apenas um erro genuinamente inesperado
    (não reconhecido como infraestrutura) é re-levantado, para ser tratado pela
    guarda por iteração do consumidor.

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
        ValueError: quando ``estender_visibility`` é ``True`` e
            ``visibility_timeout_estendido`` é inválido (ausente ou fora da faixa),
            levantado **antes** de qualquer chamada à AWS.
        Exception: o erro original, caso o ``delete_message`` falhe no caminho de
            sucesso com um erro **não** reconhecido como infraestrutura. Erros de
            infraestrutura reconhecidos e as exceções dos cenários simulados NÃO
            são propagados.
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
    # Frente C (opt-in): quando a extensão está ligada e o cenário é de SUCESSO,
    # dorme em fatias renovando o Visibility_Timeout entre elas. A validação da
    # faixa ocorre ANTES de qualquer chamada à AWS (owner único). O cenário
    # DURACAO_SUPERIOR_VISIBILITY ignora a extensão (deixa expirar de propósito).
    valor_estendido = validar_extensao_visibility(
        estender_visibility, visibility_timeout_estendido
    )
    deve_estender = (
        valor_estendido is not None
        and cenario is CenarioFalha.SUCESSO
    )
    if deve_estender:
        _aguardar_com_heartbeat(
            processing_seconds=processing_seconds,
            visibility_timeout_estendido=valor_estendido,  # type: ignore[arg-type]
            nome_fila=nome_fila,
            receipt_handle=mensagem.receipt_handle,
            consumer_id=consumer_id,
            order_id=order_id_efetivo,
            sqs=sqs,
            sleep=sleep,
        )
    else:
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
        # (5a) Caminho de sucesso. Correção crítica: o DeleteMessage é tentado
        # ANTES de registrar `processada`. Assim, `excluida` nunca é registrado
        # sem confirmação e `processada` (que dispara a idempotência de "já
        # processado") só existe quando a mensagem foi de fato removida da fila.
        try:
            sqs.delete_message(nome_fila, mensagem.receipt_handle)
        except Exception as erro:  # noqa: BLE001 — classificamos abaixo
            # Falha de infraestrutura reconhecida (tipicamente ReceiptHandle
            # expirado): NÃO mata o consumidor. Registra o erro original e o
            # evento `exclusao_nao_confirmada`, preservando a mensagem na fila.
            if e_receipt_handle_expirado(erro) or e_erro_infra_sqs(erro):
                log_evento(
                    logging.ERROR,
                    "delete_message_falhou",
                    order_id=order_id_efetivo,
                    fila=nome_fila,
                    consumer_id=consumer_id,
                    erro=repr(erro),
                )
                historico.registrar_evento(
                    _novo_evento(
                        order_id=order_id_efetivo,
                        queue_name=nome_fila,
                        event_type=EventType.EXCLUSAO_NAO_CONFIRMADA,
                        event_source=EventSource.OBSERVADO,
                        seq=3,
                        consumer_id=consumer_id,
                        receive_count=mensagem.approximate_receive_count,
                        processing_seconds=processing_seconds,
                        message_group_id=grupo,
                        sqs_message_id=mensagem.message_id,
                        error_detail=repr(erro),
                    )
                )
                return ResultadoProcessamento(
                    order_id=order_id_efetivo,
                    cenario=cenario,
                    sucesso=False,
                    excluida=False,
                    erro_detalhe=repr(erro),
                    exclusao_nao_confirmada=True,
                )
            # Erro inesperado (não reconhecido como infra): propaga para a guarda
            # por iteração do consumidor (frente B), que impede que o laço morra.
            raise

        # Delete confirmado: janela crítica pós-delete. A mensagem JÁ saiu da
        # fila; registrar `processada` (seq=3) e `excluida` (seq=4) é apenas
        # auditoria. Se o DynamoDB falhar persistentemente aqui, NÃO re-levantamos
        # (re-levantar não traz a mensagem de volta e confundiria o contador de
        # erros de iteração): logamos `historico_pos_delete_falhou` e ainda
        # retornamos sucesso=True/excluida=True, pois o delete de fato ocorreu.
        try:
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
        except (FalhaRegistroEvento, Exception) as erro:  # noqa: BLE001
            log_evento(
                logging.ERROR,
                "historico_pos_delete_falhou",
                order_id=order_id_efetivo,
                fila=nome_fila,
                consumer_id=consumer_id,
                evento_pendente="processada",
                erro=repr(erro),
            )
            return ResultadoProcessamento(
                order_id=order_id_efetivo,
                cenario=cenario,
                sucesso=True,
                excluida=True,
                erro_detalhe=None,
            )
        try:
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
        except (FalhaRegistroEvento, Exception) as erro:  # noqa: BLE001
            log_evento(
                logging.ERROR,
                "historico_pos_delete_falhou",
                order_id=order_id_efetivo,
                fila=nome_fila,
                consumer_id=consumer_id,
                evento_pendente="excluida",
                erro=repr(erro),
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
