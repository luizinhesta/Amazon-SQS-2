"""Loop do Consumidor_FIFO com um único MessageGroupId por instância por vez (Tarefa 13.1).

Este módulo implementa o laço de consumo da **Fila_Principal_FIFO**
(:data:`~app.dominio.constantes.FILA_FIFO`). Diferentemente do Consumidor_Standard,
o Consumidor_FIFO precisa respeitar a semântica de ordenação do Amazon SQS FIFO: as
mensagens de um mesmo ``MessageGroupId`` são entregues e devem ser processadas **na
ordem de recebimento**, e uma instância do consumidor processa **um único
MessageGroupId por vez** antes de passar a outro (Req. 10.5, 10.6).

Regra central (Req. 10.5, 10.6):

- A cada iteração, o consumidor recebe um lote de 1 a 10 mensagens da fila FIFO com
  Long_Polling (``WaitTimeSeconds`` = :data:`~app.dominio.constantes.LONG_POLLING_SEGUNDOS`
  = 20) — Req. 8.2, 14.6.
- Se o lote contiver mensagens de **múltiplos** ``MessageGroupId``, o consumidor
  **seleciona um único grupo** e processa **todas** as mensagens desse grupo, na
  **ordem de recebimento**, antes de considerar qualquer outro grupo. As mensagens
  dos demais grupos não são processadas nesta iteração: elas permanecem na fila
  (não são excluídas) e voltam a ficar visíveis após o Visibility_Timeout, podendo
  ser recebidas em iterações futuras. Isso garante que, em cada momento, a instância
  esteja dedicada a um único grupo (Req. 10.6).
- Dentro do grupo selecionado, a ordem de recebimento é preservada: as mensagens são
  processadas sequencialmente na ordem em que o SQS as entregou no lote (Req. 10.5).

Para cada mensagem do grupo selecionado o consumidor aplica a verificação de
idempotência (:func:`~app.consumidores.idempotencia.verificar_idempotencia`,
Req. 9) e, quando autorizado, o processamento
(:func:`~app.consumidores.processamento.processar_mensagem`, Req. 8.3–8.9).

Envio para a fila FIFO (Req. 4.6, 4.7): :func:`enviar_mensagem_fifo` encapsula o
``send_message`` do :class:`~app.acesso.sqs_gateway.SqsGateway` respeitando os dois
modos de deduplicação de uma fila FIFO:

- **Deduplicacao_Por_Conteudo habilitada** (``ContentBasedDeduplication`` na fila):
  o remetente **não** informa ``MessageDeduplicationId``; o próprio SQS calcula o
  identificador de deduplicação a partir do conteúdo (Req. 4.7).
- **Deduplicacao_Por_Conteudo desabilitada**: o remetente **deve** informar um
  ``MessageDeduplicationId`` explícito (Req. 4.7). A ausência do identificador neste
  modo é rejeitada com ``ValueError`` antes de qualquer chamada à AWS.

Todo o módulo é **testável por injeção de dependências**: o loop recebe o
``SqsGateway``, o ``HistoricoRepository``, as funções de idempotência/processamento e
um **critério de parada** (``Callable[[], bool]``), de modo que os testes executem um
número determinístico de iterações sem laço infinito nem espera real.

Referências de requisitos: 8.2, 10.5, 10.6, 14.6, 4.6, 4.7.
"""

from __future__ import annotations

import itertools
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence

from app.acesso.historico_repository import HistoricoRepository
from app.acesso.sqs_gateway import (
    MAX_NUMBER_OF_MESSAGES_MAX,
    MAX_NUMBER_OF_MESSAGES_MIN,
    MensagemRecebida,
    SqsGateway,
)
from app.consumidores.idempotencia import (
    DecisaoIdempotencia,
    ResultadoIdempotencia,
    verificar_idempotencia,
)
from app.consumidores.payload_mensagem import extrair_payload
from app.consumidores.processamento import (
    CenarioFalha,
    FuncaoSleep,
    ResultadoProcessamento,
    processar_mensagem,
)
from app.dominio import constantes as c
from app.dominio.tipos import EventType


# ---------------------------------------------------------------------------
# Tipos das dependências injetáveis (Protocols) — permitem fakes nos testes
# ---------------------------------------------------------------------------
class FuncaoVerificarIdempotencia(Protocol):
    """Assinatura da função de idempotência injetável (ver verificar_idempotencia)."""

    def __call__(
        self,
        repositorio: HistoricoRepository,
        *,
        order_id: Optional[str],
        queue_name: str,
        consumer_id: Optional[str] = ...,
        receive_count: Optional[int] = ...,
        message_group_id: Optional[str] = ...,
        sqs_message_id: Optional[str] = ...,
        max_receive_count: int = ...,
    ) -> ResultadoIdempotencia:
        ...


class FuncaoProcessarMensagem(Protocol):
    """Assinatura da função de processamento injetável (ver processar_mensagem)."""

    def __call__(
        self,
        mensagem: MensagemRecebida,
        *,
        nome_fila: str,
        consumer_id: str,
        cenario: CenarioFalha,
        processing_seconds: int,
        sqs: SqsGateway,
        historico: HistoricoRepository,
        sleep: FuncaoSleep = ...,
        message_group_id: Optional[str] = ...,
        order_id: Optional[str] = ...,
    ) -> ResultadoProcessamento:
        ...


# ---------------------------------------------------------------------------
# Seleção de grupo e ordenação dentro do grupo (núcleo da regra FIFO)
# ---------------------------------------------------------------------------
def agrupar_por_message_group_id(
    mensagens: Sequence[MensagemRecebida],
) -> dict[Optional[str], list[MensagemRecebida]]:
    """Agrupa as mensagens de um lote pelo seu ``MessageGroupId``.

    A **ordem de recebimento** (a ordem em que as mensagens aparecem no lote
    retornado pelo SQS) é preservada dentro de cada grupo, pois percorremos o lote
    uma única vez e anexamos cada mensagem ao final da lista do seu grupo. Esta
    preservação é a base para o processamento ordenado exigido pelo Req. 10.5.

    O dicionário resultante também preserva a ordem de **primeira aparição** de cada
    grupo (``dict`` do Python mantém a ordem de inserção), o que torna determinística
    a seleção do primeiro grupo do lote.

    Args:
        mensagens: Mensagens recebidas em um lote da fila FIFO.

    Returns:
        Um dicionário ``MessageGroupId -> [mensagens na ordem de recebimento]``. A
        chave pode ser ``None`` apenas em cenários degenerados (mensagem sem
        ``MessageGroupId``), tratados pelo chamador.
    """
    grupos: dict[Optional[str], list[MensagemRecebida]] = {}
    for mensagem in mensagens:
        grupos.setdefault(mensagem.message_group_id, []).append(mensagem)
    return grupos


def selecionar_grupo(
    mensagens: Sequence[MensagemRecebida],
) -> tuple[Optional[str], list[MensagemRecebida]]:
    """Seleciona um único ``MessageGroupId`` do lote e devolve suas mensagens ordenadas.

    Implementa a regra central do Consumidor_FIFO (Req. 10.6): processar **um único
    grupo por vez**. A estratégia de seleção é determinística — escolhe-se o
    **primeiro grupo** na ordem de recebimento do lote (ou seja, o grupo da primeira
    mensagem do lote). As mensagens devolvidas estão na **ordem de recebimento**
    dentro do grupo (Req. 10.5).

    Args:
        mensagens: Mensagens recebidas em um lote da fila FIFO (ordem de recebimento).

    Returns:
        Uma tupla ``(message_group_id, mensagens_do_grupo)``. Quando o lote está
        vazio, retorna ``(None, [])``.
    """
    if not mensagens:
        return None, []
    grupos = agrupar_por_message_group_id(mensagens)
    # Primeiro grupo na ordem de recebimento (primeira chave inserida no dict).
    grupo_selecionado = next(iter(grupos))
    return grupo_selecionado, grupos[grupo_selecionado]


# ---------------------------------------------------------------------------
# Envio para a fila FIFO (Deduplicacao_Por_Conteudo on/off) — Req. 4.6, 4.7
# ---------------------------------------------------------------------------
def enviar_mensagem_fifo(
    sqs: SqsGateway,
    *,
    corpo: str,
    message_group_id: str,
    deduplicacao_por_conteudo: bool,
    message_deduplication_id: Optional[str] = None,
    nome_fila: str = c.FILA_FIFO,
    message_attributes: Optional[Mapping[str, Any]] = None,
) -> Mapping[str, Any]:
    """Envia uma mensagem à Fila_Principal_FIFO respeitando o modo de deduplicação.

    Em uma fila FIFO o ``MessageGroupId`` é **obrigatório** (Req. 4.6) e o
    identificador de deduplicação é definido de uma destas duas formas (Req. 4.7):

    - ``deduplicacao_por_conteudo=True``: a fila tem ``ContentBasedDeduplication``
      habilitada; o ``MessageDeduplicationId`` **não** é enviado — o SQS o calcula a
      partir do conteúdo. Se um ``message_deduplication_id`` for informado neste
      modo, ele é ignorado (o conteúdo prevalece).
    - ``deduplicacao_por_conteudo=False``: o ``MessageDeduplicationId`` é
      **obrigatório** e deve ser informado explicitamente pelo remetente.

    Args:
        sqs: Gateway do SQS usado para o ``send_message``.
        corpo: Corpo (payload) da mensagem.
        message_group_id: ``MessageGroupId`` da mensagem (1 a 128 caracteres,
            validado pela camada de validação de envio).
        deduplicacao_por_conteudo: ``True`` se a fila usa Deduplicacao_Por_Conteudo.
        message_deduplication_id: ``MessageDeduplicationId`` explícito; obrigatório
            quando ``deduplicacao_por_conteudo=False``.
        nome_fila: Nome da fila FIFO de destino (padrão: Fila_Principal_FIFO).
        message_attributes: Atributos de mensagem opcionais.

    Returns:
        A resposta do ``send_message`` do SQS (contém ``MessageId`` e
        ``SequenceNumber``).

    Raises:
        ValueError: se ``message_group_id`` for vazio, ou se
            ``deduplicacao_por_conteudo=False`` e nenhum ``message_deduplication_id``
            for informado (Req. 4.6, 4.7). A validação ocorre antes de qualquer
            chamada à AWS.
    """
    if not message_group_id:
        raise ValueError(
            "MessageGroupId é obrigatório para envio em fila FIFO (Req. 4.6)."
        )

    if deduplicacao_por_conteudo:
        # A fila calcula o identificador de deduplicação pelo conteúdo; não enviamos
        # MessageDeduplicationId (Req. 4.7).
        dedup_id: Optional[str] = None
    else:
        # Deduplicação por conteúdo desabilitada: o MessageDeduplicationId é
        # obrigatório (Req. 4.7).
        if not message_deduplication_id:
            raise ValueError(
                "MessageDeduplicationId é obrigatório quando a Deduplicacao_Por_Conteudo "
                "está desabilitada na fila FIFO (Req. 4.7)."
            )
        dedup_id = message_deduplication_id

    return sqs.send_message(
        nome_fila,
        corpo,
        message_group_id=message_group_id,
        message_deduplication_id=dedup_id,
        message_attributes=message_attributes,
    )


# ---------------------------------------------------------------------------
# Resultado observável de uma iteração e do loop completo
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ResultadoIteracaoFifo:
    """Resultado observável de uma única iteração do loop do Consumidor_FIFO.

    Atributos:
        recebidas: Número de mensagens recebidas no lote (0 em recebimento vazio).
        grupo_selecionado: ``MessageGroupId`` escolhido para processamento nesta
            iteração (``None`` quando o lote veio vazio).
        processadas_do_grupo: Lista, na ordem de processamento, dos ``order_id`` das
            mensagens do grupo selecionado que tiveram o processamento executado
            (decisão de idempotência ``PROCESSAR``).
        ordem_recebimento_do_grupo: Lista, na ordem de recebimento, dos ``order_id``
            de todas as mensagens do grupo selecionado (independentemente da decisão
            de idempotência). Útil para verificar a preservação de ordem (Req. 10.5).
        decisoes: Decisões de idempotência tomadas, na ordem de recebimento do grupo.
    """

    recebidas: int
    grupo_selecionado: Optional[str]
    processadas_do_grupo: list[str] = field(default_factory=list)
    ordem_recebimento_do_grupo: list[str] = field(default_factory=list)
    decisoes: list[DecisaoIdempotencia] = field(default_factory=list)


@dataclass(frozen=True)
class ResultadoConsumoFifo:
    """Resumo observável da execução completa do loop do Consumidor_FIFO.

    Atributos:
        consumer_id: Identificador único atribuído à instância do consumidor.
        iteracoes: Resultado de cada iteração executada, em ordem.
        total_recebidas: Soma das mensagens recebidas em todas as iterações.
        total_processadas: Soma das mensagens efetivamente processadas.
    """

    consumer_id: str
    iteracoes: list[ResultadoIteracaoFifo] = field(default_factory=list)

    @property
    def total_recebidas(self) -> int:
        return sum(it.recebidas for it in self.iteracoes)

    @property
    def total_processadas(self) -> int:
        return sum(len(it.processadas_do_grupo) for it in self.iteracoes)


def _novo_consumer_id() -> str:
    """Gera um ``consumer_id`` único para a instância do Consumidor_FIFO (Req. 8.4, 10.4)."""
    return f"consumidor-fifo-{uuid.uuid4().hex[:12]}"


class ConsumidorFifo:
    """Instância do Consumidor_FIFO que processa um único MessageGroupId por vez.

    O consumidor é criado com suas dependências injetadas (gateway do SQS,
    repositório de histórico e, opcionalmente, as funções de idempotência e
    processamento e a função ``sleep``). Cada instância recebe um ``consumer_id``
    único (Req. 8.4, 10.4), usado no registro dos eventos de recebimento/processamento.

    O método :meth:`executar` roda o laço de consumo até que o **critério de parada**
    retorne ``True``. O critério é um ``Callable[[], bool]`` injetado, o que torna o
    loop determinístico e testável (sem laço infinito nem espera real).
    """

    def __init__(
        self,
        *,
        sqs: SqsGateway,
        historico: HistoricoRepository,
        consumer_id: Optional[str] = None,
        nome_fila: str = c.FILA_FIFO,
        max_mensagens: int = MAX_NUMBER_OF_MESSAGES_MAX,
        wait_time_seconds: int = c.LONG_POLLING_SEGUNDOS,
        max_receive_count: int = c.MAX_RECEIVE_COUNT,
        cenario_padrao: CenarioFalha = CenarioFalha.SUCESSO,
        processing_seconds_padrao: int = c.TEMPO_PROCESSAMENTO_SIMULADO_SEGUNDOS,
        verificar_idempotencia_fn: FuncaoVerificarIdempotencia = verificar_idempotencia,
        processar_mensagem_fn: FuncaoProcessarMensagem = processar_mensagem,
        sleep: FuncaoSleep = time.sleep,
    ) -> None:
        """Inicializa a instância do Consumidor_FIFO.

        Args:
            sqs: Gateway do SQS (recebimento/exclusão). Injete um fake nos testes.
            historico: Repositório da Tabela_Historico. Injete um fake nos testes.
            consumer_id: Identificador único da instância; se ``None``, é gerado.
            nome_fila: Fila FIFO de origem (padrão: Fila_Principal_FIFO).
            max_mensagens: Tamanho do lote por recebimento, de 1 a 10 (Req. 8.2).
            wait_time_seconds: WaitTimeSeconds do Long_Polling (padrão 20) (Req. 8.2,
                14.6).
            max_receive_count: Limite da Redrive_Policy repassado à idempotência.
            cenario_padrao: Cenario_Falha aplicado quando a mensagem não especifica
                um (no laboratório, o cenário costuma vir do payload/atributos).
            processing_seconds_padrao: Tempo_Processamento_Simulado padrão.
            verificar_idempotencia_fn: Função de idempotência (injetável para testes).
            processar_mensagem_fn: Função de processamento (injetável para testes).
            sleep: Função de espera injetável, repassada ao processamento.

        Raises:
            ValueError: se ``max_mensagens`` estiver fora de 1 a 10 (Req. 8.2).
        """
        if not (MAX_NUMBER_OF_MESSAGES_MIN <= max_mensagens <= MAX_NUMBER_OF_MESSAGES_MAX):
            raise ValueError(
                "O lote por recebimento deve estar entre "
                f"{MAX_NUMBER_OF_MESSAGES_MIN} e {MAX_NUMBER_OF_MESSAGES_MAX} "
                f"mensagens (valor informado: {max_mensagens})."
            )

        self.consumer_id = consumer_id or _novo_consumer_id()
        self._sqs = sqs
        self._historico = historico
        self._nome_fila = nome_fila
        self._max_mensagens = max_mensagens
        self._wait_time_seconds = wait_time_seconds
        self._max_receive_count = max_receive_count
        self._cenario_padrao = cenario_padrao
        self._processing_seconds_padrao = processing_seconds_padrao
        self._verificar_idempotencia = verificar_idempotencia_fn
        self._processar_mensagem = processar_mensagem_fn
        self._sleep = sleep

    # ------------------------------------------------------------------
    # Uma iteração do loop
    # ------------------------------------------------------------------
    def processar_uma_iteracao(self) -> ResultadoIteracaoFifo:
        """Executa uma única iteração: recebe um lote, seleciona um grupo e o processa.

        Passos:

        1. Recebe um lote de 1 a 10 mensagens com Long_Polling (Req. 8.2, 14.6).
           Um recebimento vazio retorna um resultado com ``recebidas=0`` e nenhum
           grupo selecionado — o loop simplesmente repete sem erro (Req. 8.11).
        2. Seleciona **um único** ``MessageGroupId`` (o primeiro na ordem de
           recebimento) e obtém suas mensagens **na ordem de recebimento**
           (Req. 10.5, 10.6).
        3. Para cada mensagem do grupo, **em ordem**, aplica a idempotência e, quando
           autorizada (``PROCESSAR``), o processamento. As mensagens dos demais
           grupos **não** são tocadas nesta iteração.

        Returns:
            Um :class:`ResultadoIteracaoFifo` descrevendo o que ocorreu.
        """
        mensagens = self._sqs.receive_messages(
            self._nome_fila,
            max_mensagens=self._max_mensagens,
            wait_time_seconds=self._wait_time_seconds,
        )

        if not mensagens:
            # Recebimento vazio: repetir sem erro (Req. 8.11).
            return ResultadoIteracaoFifo(recebidas=0, grupo_selecionado=None)

        grupo, mensagens_do_grupo = selecionar_grupo(mensagens)

        processadas: list[str] = []
        ordem_recebimento: list[str] = []
        decisoes: list[DecisaoIdempotencia] = []

        # Processa SOMENTE o grupo selecionado, preservando a ordem de recebimento
        # dentro do grupo (Req. 10.5, 10.6). As mensagens dos demais grupos ficam na
        # fila e voltam a ficar visíveis após o Visibility_Timeout.
        for mensagem in mensagens_do_grupo:
            order_id = self._order_id_da_mensagem(mensagem)
            ordem_recebimento.append(order_id)

            resultado_idem = self._verificar_idempotencia(
                self._historico,
                order_id=order_id,
                queue_name=self._nome_fila,
                consumer_id=self.consumer_id,
                receive_count=mensagem.approximate_receive_count,
                message_group_id=mensagem.message_group_id,
                sqs_message_id=mensagem.message_id,
                max_receive_count=self._max_receive_count,
            )
            decisoes.append(resultado_idem.decisao)

            if not resultado_idem.efeitos_permitidos:
                # JA_PROCESSADO, REJEITAR_VALIDACAO ou ERRO_HISTORICO: não executa
                # efeitos; a mensagem é preservada conforme a decisão (Req. 9).
                continue

            self._processar_mensagem(
                mensagem,
                nome_fila=self._nome_fila,
                consumer_id=self.consumer_id,
                cenario=self._cenario_da_mensagem(mensagem),
                processing_seconds=self._processing_seconds_da_mensagem(mensagem),
                sqs=self._sqs,
                historico=self._historico,
                sleep=self._sleep,
                message_group_id=mensagem.message_group_id,
                order_id=order_id,
            )
            processadas.append(order_id)

        return ResultadoIteracaoFifo(
            recebidas=len(mensagens),
            grupo_selecionado=grupo,
            processadas_do_grupo=processadas,
            ordem_recebimento_do_grupo=ordem_recebimento,
            decisoes=decisoes,
        )

    # ------------------------------------------------------------------
    # O loop completo (com critério de parada injetável)
    # ------------------------------------------------------------------
    def executar(
        self,
        *,
        criterio_parada: Callable[[], bool],
        max_iteracoes: Optional[int] = None,
    ) -> ResultadoConsumoFifo:
        """Executa o laço de consumo até o critério de parada retornar ``True``.

        O critério de parada é avaliado **antes** de cada iteração. Isso torna o
        laço determinístico e testável: por exemplo, um critério baseado em contador
        executa exatamente ``N`` iterações. Opcionalmente, ``max_iteracoes`` impõe um
        teto adicional de segurança para garantir término mesmo que o critério nunca
        retorne ``True``.

        Args:
            criterio_parada: Função sem argumentos que retorna ``True`` quando o loop
                deve parar. Avaliada antes de cada iteração.
            max_iteracoes: Teto opcional de iterações; quando atingido, o loop para
                mesmo que o critério ainda não tenha sinalizado parada.

        Returns:
            Um :class:`ResultadoConsumoFifo` com o ``consumer_id`` e o resultado de
            cada iteração executada.
        """
        iteracoes: list[ResultadoIteracaoFifo] = []
        contador = itertools.count()
        for n in contador:
            if max_iteracoes is not None and n >= max_iteracoes:
                break
            if criterio_parada():
                break
            iteracoes.append(self.processar_uma_iteracao())
        return ResultadoConsumoFifo(consumer_id=self.consumer_id, iteracoes=iteracoes)

    # ------------------------------------------------------------------
    # Extração de parâmetros da mensagem (order_id, cenário, tempo)
    # ------------------------------------------------------------------
    def _order_id_da_mensagem(self, mensagem: MensagemRecebida) -> Optional[str]:
        """Obtém o ``order_id`` da mensagem.

        Precedência: (1) corpo JSON do produtor (contrato — ver
        :func:`~app.consumidores.payload_mensagem.extrair_payload`); (2) atributo
        de mensagem ``order_id``; (3) corpo cru como fallback. Mensagens sem corpo
        nem atributo resultam em ``None``, que a verificação de idempotência
        rejeita (Req. 9.3).

        Como ``extrair_payload`` já aplica o fallback (corpo cru vira ``order_id``
        quando o corpo não é JSON do contrato), o atributo ``order_id`` só prevalece
        quando o corpo veio sem o campo no JSON (payload sem ``order_id``).
        """
        payload = extrair_payload(mensagem.body)
        if payload.order_id is not None:
            return payload.order_id
        atributo = self._valor_atributo(mensagem, "order_id")
        if atributo is not None:
            return atributo
        return mensagem.body if mensagem.body else None

    def _cenario_da_mensagem(self, mensagem: MensagemRecebida) -> CenarioFalha:
        """Obtém o Cenario_Falha da mensagem.

        Precedência: (1) corpo JSON do produtor; (2) atributo de mensagem
        ``cenario``; (3) padrão da instância.
        """
        payload = extrair_payload(mensagem.body)
        if payload.cenario is not None:
            return payload.cenario
        bruto = self._valor_atributo(mensagem, "cenario")
        if bruto is None:
            return self._cenario_padrao
        try:
            return CenarioFalha(bruto)
        except ValueError:
            return self._cenario_padrao

    def _processing_seconds_da_mensagem(self, mensagem: MensagemRecebida) -> int:
        """Obtém o Tempo_Processamento_Simulado da mensagem.

        Precedência: (1) corpo JSON do produtor; (2) atributo de mensagem
        ``processing_seconds``; (3) padrão da instância.
        """
        payload = extrair_payload(mensagem.body)
        if payload.processing_seconds is not None:
            return payload.processing_seconds
        bruto = self._valor_atributo(mensagem, "processing_seconds")
        if bruto is None:
            return self._processing_seconds_padrao
        try:
            return int(bruto)
        except (TypeError, ValueError):
            return self._processing_seconds_padrao

    @staticmethod
    def _valor_atributo(mensagem: MensagemRecebida, nome: str) -> Optional[str]:
        """Lê o valor string de um MessageAttribute, tolerando ausência/formato."""
        atributos = mensagem.message_attributes or {}
        entrada = atributos.get(nome)
        if not isinstance(entrada, Mapping):
            return None
        valor = entrada.get("StringValue")
        return valor if isinstance(valor, str) else None
