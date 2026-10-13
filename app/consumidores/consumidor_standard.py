"""Loop do Consumidor_Standard (Tarefa 12.1).

Este módulo implementa o laço de consumo da **Fila_Principal_Standard** descrito
na seção *Fluxo Standard* do ``design.md``. Cada iteração do consumidor:

1. **Recebe** um lote de 1 a 10 mensagens com **Long_Polling**
   (``WaitTimeSeconds`` = :data:`~app.dominio.constantes.LONG_POLLING_SEGUNDOS`,
   ou seja, 20 s) a partir de ``FILA_STANDARD`` (Req. 8.1, 8.11, 14.5).
2. Para cada mensagem recebida, aplica a **verificação de idempotência** por
   ``order_id`` (Req. 9, tarefa 10.1) e, somente quando autorizado, executa o
   **processamento e a exclusão** sob o Cenario_Falha (Req. 8.3–8.9, tarefa 10.4).
3. **Repete** o laço. Um recebimento vazio (nenhuma mensagem disponível) **não é
   erro**: o consumidor simplesmente volta a aguardar na próxima iteração
   (Req. 8.11).

Cada instância de consumidor recebe um ``consumer_id`` único (ex.:
``"consumidor-standard-<n>"`` ou um UUID), registrado em todos os eventos de
recebimento/processamento para permitir contabilizar *recebidas por consumidor*
(Req. 10.4).

Testabilidade
-------------
O laço é desenhado para ser testável **sem AWS real** e **sem laço infinito**:

- Todas as dependências são **injetáveis**: o :class:`~app.acesso.sqs_gateway.SqsGateway`,
  o :class:`~app.acesso.historico_repository.HistoricoRepository`, a função
  ``sleep`` do processamento e os *callables* de idempotência/processamento (que
  por padrão apontam para as implementações reais, mas podem ser substituídos por
  dublês nos testes).
- A **parada** é controlada por três mecanismos combináveis:
  - ``max_iteracoes``: número máximo de iterações do laço (``None`` = ilimitado);
  - ``continuar``: um ``Callable[[], bool]`` consultado antes de cada iteração
    (padrão: sempre ``True``);
  - :meth:`ConsumidorStandard.parar`: sinaliza a interrupção cooperativa do laço.

- O **Cenario_Falha** de cada mensagem pode ser decidido por um *callable*
  injetável (:data:`SeletorCenario`) a partir do corpo/atributos da mensagem,
  permitindo que os testes forcem desfechos específicos. O padrão trata toda
  mensagem como :data:`~app.consumidores.processamento.CenarioFalha.SUCESSO`.

Este módulo NÃO cria o cliente boto3 nem acessa a AWS diretamente: ele apenas
orquestra as camadas já existentes. A composição real (API + consumidores) é feita
na tarefa 18.1.

Referências de requisitos: 8.1, 8.11, 10.4, 14.5.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from app.acesso.historico_repository import HistoricoRepository
from app.acesso.sqs_gateway import MensagemRecebida, SqsGateway
from app.consumidores.idempotencia import (
    DecisaoIdempotencia,
    ResultadoIdempotencia,
    verificar_idempotencia,
)
from app.consumidores.payload_mensagem import PayloadMensagem, extrair_payload
from app.consumidores.processamento import (
    CenarioFalha,
    FuncaoSleep,
    ResultadoProcessamento,
    processar_mensagem,
)
from app.dominio import constantes as c
from app.observabilidade.log_estruturado import log_evento

# ---------------------------------------------------------------------------
# Tipos dos callables injetáveis
# ---------------------------------------------------------------------------

# Decide o Cenario_Falha de uma mensagem a partir dela própria (corpo/atributos).
# Permite que os testes injetem desfechos específicos. Quando NÃO é fornecido
# (``None``), o cenário é derivado do corpo JSON do produtor (contrato) e, na
# ausência do campo, usa-se SUCESSO como padrão.
SeletorCenario = Callable[[MensagemRecebida], CenarioFalha]

# Deriva o Tempo_Processamento_Simulado (em segundos) de uma mensagem. O padrão
# usa a constante do laboratório para todas as mensagens.
SeletorProcessingSeconds = Callable[[MensagemRecebida], int]

# Assinaturas dos callables de idempotência e processamento, para substituição em
# testes. Por padrão apontam para as implementações reais dos módulos irmãos.
FuncaoVerificarIdempotencia = Callable[..., ResultadoIdempotencia]
FuncaoProcessarMensagem = Callable[..., ResultadoProcessamento]

# Chave de atributo de mensagem usada (opcionalmente) para transportar o cenário.
ATRIBUTO_CENARIO: str = "cenario_falha"


def cenario_sucesso(_mensagem: MensagemRecebida) -> CenarioFalha:
    """Seletor padrão de cenário: trata toda mensagem como SUCESSO.

    Mantém o comportamento do caminho feliz quando o chamador não fornece um
    seletor específico. Os testes de falha podem injetar outro seletor.
    """
    return CenarioFalha.SUCESSO


def cenario_do_atributo(
    mensagem: MensagemRecebida,
    *,
    padrao: CenarioFalha = CenarioFalha.SUCESSO,
) -> CenarioFalha:
    """Deriva o Cenario_Falha a partir do atributo de mensagem ``cenario_falha``.

    Lê o valor textual do atributo de mensagem :data:`ATRIBUTO_CENARIO` (quando
    presente) e o converte para um :class:`CenarioFalha`. Se o atributo estiver
    ausente ou com valor desconhecido, usa ``padrao`` (SUCESSO). Útil para derivar
    o cenário do payload enviado pela API, conforme o laboratório.
    """
    atributo = mensagem.message_attributes.get(ATRIBUTO_CENARIO)
    valor: Optional[str] = None
    if isinstance(atributo, dict):
        # boto3 entrega atributos de mensagem como {"StringValue": ..., "DataType": ...}.
        valor = atributo.get("StringValue")
    elif isinstance(atributo, str):
        valor = atributo
    if not valor:
        return padrao
    try:
        return CenarioFalha(valor)
    except ValueError:
        return padrao


@dataclass
class EstatisticasConsumo:
    """Resumo acumulado da atividade do consumidor (útil em testes e observabilidade).

    Todos os contadores são inteiros ``>= 0`` e refletem o que ocorreu ao longo das
    iterações executadas.
    """

    iteracoes: int = 0
    recebimentos_vazios: int = 0
    mensagens_recebidas: int = 0
    processadas_com_sucesso: int = 0
    puladas_por_idempotencia: int = 0
    rejeitadas_por_validacao: int = 0
    erros_historico: int = 0
    falhas_processamento: int = 0
    # Número de iterações do laço em que uma exceção pontual foi capturada pela
    # guarda por iteração (uma falha de SQS/DynamoDB não encerra a thread).
    iteracoes_com_erro: int = 0
    # Número de mensagens cujo processamento terminou, mas o DeleteMessage não foi
    # confirmado (ReceiptHandle expirado / falha de infra). Contado separadamente
    # das falhas de processamento (o negócio foi executado, só a exclusão não).
    exclusoes_nao_confirmadas: int = 0


@dataclass
class ConsumidorStandard:
    """Consumidor da Fila_Principal_Standard com dependências injetáveis.

    A instância encapsula o ``consumer_id`` único e as dependências necessárias ao
    laço. O método :meth:`executar` roda o laço até o critério de parada; os testes
    controlam o número de iterações e podem injetar dublês de todas as dependências.

    Atributos:
        sqs: Gateway de acesso ao SQS (recebimento e exclusão).
        historico: Repositório da Tabela_Historico (consulta e registro de eventos).
        consumer_id: Identificador único desta instância de consumidor (Req. 10.4).
        nome_fila: Nome da fila de origem (padrão: ``FILA_STANDARD``).
        max_mensagens: Tamanho do lote por recebimento, de 1 a 10 (Req. 8.1).
        wait_time_seconds: WaitTimeSeconds do Long_Polling (padrão: 20 s, Req. 14.5).
        seletor_cenario: Decide o Cenario_Falha de cada mensagem. Quando ``None``
            (padrão), o cenário é derivado do corpo JSON do produtor; se o corpo
            não trouxer o campo, usa SUCESSO.
        seletor_processing_seconds: Decide o Tempo_Processamento_Simulado por
            mensagem. Quando ``None`` (padrão), é derivado do corpo JSON do
            produtor; se o corpo não trouxer o campo, usa a constante do
            laboratório.
        sleep: Função de espera injetável repassada ao processamento (padrão:
            ``time.sleep``; nos testes, um dublê sem espera real).
        verificar_idempotencia_fn: Callable de idempotência (padrão: implementação
            real do módulo ``idempotencia``).
        processar_mensagem_fn: Callable de processamento (padrão: implementação real
            do módulo ``processamento``).
        max_receive_count: maxReceiveCount repassado à verificação de idempotência.
    """

    sqs: SqsGateway
    historico: HistoricoRepository
    consumer_id: str
    nome_fila: str = c.FILA_STANDARD
    max_mensagens: int = c.QUANTIDADE_MENSAGENS_MAX
    wait_time_seconds: int = c.LONG_POLLING_SEGUNDOS
    seletor_cenario: Optional[SeletorCenario] = None
    seletor_processing_seconds: Optional[SeletorProcessingSeconds] = None
    sleep: FuncaoSleep = time.sleep
    verificar_idempotencia_fn: FuncaoVerificarIdempotencia = verificar_idempotencia
    processar_mensagem_fn: FuncaoProcessarMensagem = processar_mensagem
    max_receive_count: int = c.MAX_RECEIVE_COUNT
    estatisticas: EstatisticasConsumo = field(default_factory=EstatisticasConsumo)
    _parar: bool = field(default=False, init=False, repr=False)

    def parar(self) -> None:
        """Sinaliza a interrupção cooperativa do laço na próxima verificação."""
        self._parar = True

    # ------------------------------------------------------------------
    # Laço principal
    # ------------------------------------------------------------------
    def executar(
        self,
        *,
        max_iteracoes: Optional[int] = None,
        continuar: Optional[Callable[[], bool]] = None,
    ) -> EstatisticasConsumo:
        """Executa o laço de consumo até o critério de parada.

        O laço repete, a cada iteração: recebe um lote com Long_Polling, trata cada
        mensagem (idempotência + processamento) e segue adiante. Um recebimento
        vazio é tratado sem erro (apenas contabilizado) e o laço continua
        (Req. 8.11).

        A parada ocorre quando qualquer um destes for verdadeiro, verificado no
        início de cada iteração:

        - ``max_iteracoes`` iterações já foram executadas (quando não é ``None``);
        - ``continuar()`` retorna ``False`` (quando fornecido);
        - :meth:`parar` foi chamado.

        Args:
            max_iteracoes: Número máximo de iterações (``None`` = ilimitado). Em
                produção usa-se ``None``; nos testes, um valor pequeno garante
                terminação.
            continuar: Predicado consultado antes de cada iteração; o laço para
                quando ele retornar ``False``.

        Returns:
            As :class:`EstatisticasConsumo` acumuladas ao longo das iterações.
        """
        iteracao = 0
        while True:
            if self._parar:
                break
            if max_iteracoes is not None and iteracao >= max_iteracoes:
                break
            if continuar is not None and not continuar():
                break

            iteracao += 1
            self.estatisticas.iteracoes += 1
            try:
                self._executar_iteracao()
            except Exception as e:  # NÃO captura BaseException (parada cooperativa).
                # Uma falha pontual de SQS/DynamoDB não deve encerrar a thread do
                # consumidor: registramos de forma estruturada e seguimos o laço.
                self.estatisticas.iteracoes_com_erro += 1
                log_evento(
                    logging.WARNING,
                    "iteracao_consumidor_falhou",
                    consumer_id=self.consumer_id,
                    fila=self.nome_fila,
                    erro=repr(e),
                )

        return self.estatisticas

    def _executar_iteracao(self) -> None:
        """Executa uma única iteração: recebe um lote e trata cada mensagem.

        O recebimento usa Long_Polling (``wait_time_seconds``) e um lote de
        ``max_mensagens`` (1 a 10). Um recebimento vazio apenas incrementa o
        contador de recebimentos vazios e retorna sem erro (Req. 8.11).
        """
        mensagens = self.sqs.receive_messages(
            self.nome_fila,
            max_mensagens=self.max_mensagens,
            wait_time_seconds=self.wait_time_seconds,
        )

        if not mensagens:
            # Recebimento vazio: NÃO é erro; o laço repete na próxima iteração.
            self.estatisticas.recebimentos_vazios += 1
            return

        for mensagem in mensagens:
            self.estatisticas.mensagens_recebidas += 1
            self._tratar_mensagem(mensagem)

    def _tratar_mensagem(self, mensagem: MensagemRecebida) -> None:
        """Aplica idempotência e, quando autorizado, processa e exclui a mensagem.

        O corpo da mensagem segue o contrato do produtor (JSON com ``order_id``,
        ``processing_seconds`` e ``cenario``); é interpretado por
        :func:`~app.consumidores.payload_mensagem.extrair_payload`, com fallback
        seguro que trata o corpo cru como ``order_id`` quando não for JSON válido.
        O ``order_id`` extraído é usado na idempotência e no processamento. A
        verificação de idempotência decide se o processamento pode ocorrer; apenas
        na decisão
        :attr:`~app.consumidores.idempotencia.DecisaoIdempotencia.PROCESSAR` o
        processamento e a eventual exclusão são executados (Req. 9, 8.6).
        """
        payload = extrair_payload(mensagem.body)
        order_id = payload.order_id

        resultado_idem = self.verificar_idempotencia_fn(
            self.historico,
            order_id=order_id,
            queue_name=self.nome_fila,
            consumer_id=self.consumer_id,
            receive_count=mensagem.approximate_receive_count,
            message_group_id=mensagem.message_group_id,
            sqs_message_id=mensagem.message_id,
            max_receive_count=self.max_receive_count,
        )

        if resultado_idem.decisao is DecisaoIdempotencia.JA_PROCESSADO:
            self.estatisticas.puladas_por_idempotencia += 1
            return
        if resultado_idem.decisao is DecisaoIdempotencia.REJEITAR_VALIDACAO:
            self.estatisticas.rejeitadas_por_validacao += 1
            return
        if resultado_idem.decisao is DecisaoIdempotencia.ERRO_HISTORICO:
            self.estatisticas.erros_historico += 1
            return

        # DecisaoIdempotencia.PROCESSAR: segue com o processamento (tarefa 10.4).
        cenario = self._cenario(mensagem, payload)
        processing_seconds = self._processing_seconds(mensagem, payload)

        resultado = self.processar_mensagem_fn(
            mensagem,
            nome_fila=self.nome_fila,
            consumer_id=self.consumer_id,
            cenario=cenario,
            processing_seconds=processing_seconds,
            sqs=self.sqs,
            historico=self.historico,
            sleep=self.sleep,
            message_group_id=mensagem.message_group_id,
            order_id=resultado_idem.order_id or order_id,
        )

        if resultado.sucesso:
            self.estatisticas.processadas_com_sucesso += 1
        elif resultado.exclusao_nao_confirmada:
            # O processamento de negócio terminou, mas o DeleteMessage não foi
            # confirmado (ReceiptHandle expirado / falha de infra). NÃO é uma falha
            # de processamento: contabiliza em contador dedicado.
            self.estatisticas.exclusoes_nao_confirmadas += 1
        else:
            self.estatisticas.falhas_processamento += 1

    def _cenario(
        self, mensagem: MensagemRecebida, payload: PayloadMensagem
    ) -> CenarioFalha:
        """Resolve o Cenario_Falha a aplicar na mensagem.

        Precedência: (1) seletor injetado explicitamente; (2) cenário vindo do
        corpo JSON do produtor; (3) padrão SUCESSO.
        """
        if self.seletor_cenario is not None:
            return self.seletor_cenario(mensagem)
        if payload.cenario is not None:
            return payload.cenario
        return CenarioFalha.SUCESSO

    def _processing_seconds(
        self, mensagem: MensagemRecebida, payload: PayloadMensagem
    ) -> int:
        """Resolve o Tempo_Processamento_Simulado da mensagem (segundos).

        Precedência: (1) seletor injetado explicitamente; (2) valor vindo do corpo
        JSON do produtor; (3) constante padrão do laboratório.
        """
        if self.seletor_processing_seconds is not None:
            return self.seletor_processing_seconds(mensagem)
        if payload.processing_seconds is not None:
            return payload.processing_seconds
        return c.TEMPO_PROCESSAMENTO_SIMULADO_SEGUNDOS


_contador_instancias = 0


def gerar_consumer_id(prefixo: str = "consumidor-standard") -> str:
    """Gera um ``consumer_id`` único por instância (Req. 10.4).

    Usa um contador de processo combinado a um sufixo curto derivado de UUID, de
    modo que instâncias iniciadas no mesmo processo (ou em processos distintos)
    recebam identificadores distintos. O formato ``"<prefixo>-<n>-<sufixo>"`` é
    legível em logs e no histórico (ex.: ``consumidor-standard-1-a1b2c3``).
    """
    global _contador_instancias
    import uuid

    _contador_instancias += 1
    sufixo = uuid.uuid4().hex[:6]
    return f"{prefixo}-{_contador_instancias}-{sufixo}"


def criar_consumidor_standard(
    sqs: SqsGateway,
    historico: HistoricoRepository,
    *,
    consumer_id: Optional[str] = None,
    **kwargs,
) -> ConsumidorStandard:
    """Fábrica de conveniência que atribui um ``consumer_id`` único por instância.

    Quando ``consumer_id`` não é informado, gera um identificador único via
    :func:`gerar_consumer_id` (Req. 10.4). Os demais parâmetros são repassados ao
    :class:`ConsumidorStandard`.
    """
    return ConsumidorStandard(
        sqs=sqs,
        historico=historico,
        consumer_id=consumer_id or gerar_consumer_id(),
        **kwargs,
    )
