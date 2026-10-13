"""Serviço de estatísticas e monitoramento da API_Backend (Tarefa 15.5).

Encapsula a lógica dos endpoints ``GET /api/estatisticas`` e
``GET /api/monitoramento``, mantendo o roteador FastAPI fino. Reúne os eventos da
Tabela_Historico (via :class:`~app.acesso.historico_repository.HistoricoRepository`)
e deriva:

- **Estatísticas agregadas por fluxo** (Standard/FIFO), reutilizando
  :func:`app.dominio.agregador_estatisticas.agregar_estatisticas` (Req. 3.1-3.8,
  3.12, 5.2).
- **Tabela de monitoramento**, uma linha por mensagem, com o Estado derivado pelo
  :func:`app.dominio.state_deriver.derivar_estado` e as colunas ID, Fila,
  Consumidor, Tentativas, Processamento, Estado e Último evento (Req. 5.1-5.5,
  5.9, 5.10).

Decisões de design relevantes:

- **Fonte de verdade é o log de eventos.** O SQS não expõe o estado individual das
  mensagens; tudo é derivado dos eventos registrados (Req. 5.10). As duas filas
  principais e suas DLQs são consultadas por ``GSI1`` (``consultar_por_fila``) e os
  eventos combinados são agregados por ``order_id``.
- **Estado vazio tratado explicitamente.** Sem nenhuma mensagem registrada, o
  monitoramento devolve ``vazio=True`` e uma mensagem em pt-BR (Req. 5.8).
- **Timeout determinístico e injetável (Req. 14.1-14.4).** O cálculo pesado roda
  dentro de :func:`executar_com_timeout`, que aplica um limite de tempo
  configurável (padrão 10 s). Ao exceder, devolve uma indicação clara de timeout
  (campo booleano + mensagem pt-BR) em vez de erro bruto — o Dashboard preserva os
  últimos dados válidos. O limite e o executor são injetáveis, de modo que os
  testes forcem o caminho de timeout sem esperar 3 s reais (basta injetar um
  ``timeout_segundos`` muito pequeno e uma função de cálculo lenta, ou simular a
  expiração).

Referências de requisitos: 3.1-3.8, 3.12, 5.1-5.5, 5.9, 5.10, 14.1, 14.2, 14.3,
14.4.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturoTimeout
from typing import Callable, Optional, Protocol, TypeVar

from app.api.esquemas import (
    EstatisticasFluxoSaida,
    LinhaMonitoramento,
    MENSAGEM_SEM_MENSAGENS_MONITORADAS,
    MENSAGEM_TIMEOUT_ESTATISTICAS,
    MENSAGEM_TIMEOUT_MONITORAMENTO,
    RecebidasPorConsumidor,
    RespostaEstatisticas,
    RespostaMonitoramento,
)
from app.dominio import constantes as c
from app.dominio.agregador_estatisticas import (
    EstatisticasFluxo,
    FLUXO_FIFO,
    FLUXO_STANDARD,
    agregar_estatisticas,
    classificar_fluxo,
)
from app.dominio.evento import Evento
from app.dominio.ordenacao import ordenar_cronologicamente
from app.dominio.state_deriver import derivar_estado
from app.dominio.tipos import EventType

# Limite de tempo padrão (em segundos) dos endpoints de estatísticas/monitoramento
# (Req. 14.1, 14.2). Mantido como constante local do serviço; os endpoints o aceitam
# como parâmetro injetável para permitir que os testes forcem o timeout rapidamente.
#
# Elevado de 3 s para 10 s por ser mais tolerante ao laboratório: quando há
# processamento concorrente (consumidores + monitor de DLQ + múltiplas consultas ao
# histórico), o cálculo pode passar de 3 s e disparar timeouts espúrios que apareciam
# ao usuário como falha de atualização. Os testes continuam injetando um
# `timeout_segundos` pequeno explicitamente, portanto não dependem deste valor.
TIMEOUT_PADRAO_SEGUNDOS: float = 10.0

# Filas principais consultadas para montar estatísticas/monitoramento. As DLQs são
# contabilizadas no mesmo fluxo da fila principal pelo prefixo (ver
# ``classificar_fluxo``), por isso basta consultar as quatro filas e combinar.
_FILAS_CONSULTADAS: tuple[str, ...] = c.FILAS

R = TypeVar("R")


class HistoricoConsultaFila(Protocol):
    """Contrato mínimo do repositório usado por este serviço.

    Apenas :meth:`consultar_por_fila` é necessária para reunir os eventos de todas
    as filas; declarar o mínimo mantém o acoplamento baixo e permite fakes em
    memória nos testes.
    """

    def consultar_por_fila(self, queue_name: str) -> list[Evento]:
        """Retorna os eventos registrados para a fila informada (vazio se nenhum)."""
        ...


# ---------------------------------------------------------------------------
# Timeout determinístico e injetável
# ---------------------------------------------------------------------------
def executar_com_timeout(
    funcao: Callable[[], R],
    *,
    timeout_segundos: float,
    executor: Optional[ThreadPoolExecutor] = None,
) -> tuple[bool, Optional[R]]:
    """Executa ``funcao`` com um limite de tempo, sem levantar erro bruto no estouro.

    Roda ``funcao`` em uma thread de trabalho e aguarda no máximo
    ``timeout_segundos``. O retorno é uma tupla ``(expirou, resultado)``:

    - ``(False, resultado)`` quando a função conclui dentro do limite;
    - ``(True, None)`` quando o limite é excedido (Req. 14.3, 14.4). Neste caso, a
      tarefa de fundo pode continuar executando, mas seu resultado é descartado — o
      chamador devolve a indicação de timeout e o Dashboard preserva os últimos
      dados válidos.

    O executor é injetável para que os testes controlem o paralelismo; quando não
    informado, cria-se um ``ThreadPoolExecutor`` efêmero com um único worker. Um
    ``timeout_segundos <= 0`` força imediatamente o caminho de timeout, útil para os
    testes forçarem o estouro sem esperar o tempo real.

    Args:
        funcao: Callable sem argumentos a executar (o cálculo pesado).
        timeout_segundos: Limite de tempo em segundos.
        executor: ``ThreadPoolExecutor`` opcional a reutilizar.

    Returns:
        Tupla ``(expirou, resultado)`` conforme descrito acima.
    """
    proprio_executor = executor is None
    pool = executor or ThreadPoolExecutor(max_workers=1)
    try:
        futuro = pool.submit(funcao)
        try:
            resultado = futuro.result(timeout=max(timeout_segundos, 0.0))
            return False, resultado
        except FuturoTimeout:
            # Não cancelamos agressivamente: a tarefa termina sozinha e seu
            # resultado é descartado. Devolvemos a indicação de timeout (Req. 14.3/4).
            return True, None
    finally:
        if proprio_executor:
            # Não esperamos a tarefa pendente concluir (shutdown sem wait) para não
            # anular o efeito do timeout; o worker é liberado quando a função retorna.
            pool.shutdown(wait=False)


# ---------------------------------------------------------------------------
# Coleta e agrupamento dos eventos
# ---------------------------------------------------------------------------
def _coletar_eventos(historico: HistoricoConsultaFila) -> list[Evento]:
    """Reúne os eventos de todas as filas do laboratório (principais e DLQs).

    Consulta cada fila por ``GSI1`` e concatena os eventos. Como as estatísticas e o
    monitoramento derivam o Estado agregando por ``order_id``, a combinação das
    quatro filas garante que um encaminhamento para a DLQ seja considerado junto do
    histórico da fila principal correspondente.
    """
    eventos: list[Evento] = []
    for fila in _FILAS_CONSULTADAS:
        eventos.extend(historico.consultar_por_fila(fila))
    return eventos


def _consumer_id(evento: Evento) -> Optional[str]:
    """Extrai um ``consumer_id`` não-vazio de um evento, ou ``None``."""
    valor = evento.consumer_id
    if valor is None:
        return None
    texto = str(valor).strip()
    return texto or None


# ---------------------------------------------------------------------------
# Estatísticas (GET /api/estatisticas)
# ---------------------------------------------------------------------------
def _fluxo_para_saida(fluxo: str, estat: EstatisticasFluxo) -> EstatisticasFluxoSaida:
    """Converte um :class:`EstatisticasFluxo` no esquema de saída da API."""
    recebidas = [
        RecebidasPorConsumidor(consumidor=consumidor, recebidas=quantidade)
        for consumidor, quantidade in sorted(estat.recebidas_por_consumidor.items())
    ]
    return EstatisticasFluxoSaida(
        fluxo=fluxo,
        enviadas=estat.enviadas,
        disponiveis=estat.disponiveis,
        em_processamento=estat.em_processamento,
        processadas=estat.processadas,
        com_falha=estat.com_falha,
        encaminhadas_dlq=estat.encaminhadas_dlq,
        exclusao_nao_confirmada=estat.exclusao_nao_confirmada,
        recebidas_por_consumidor=recebidas,
    )


def _calcular_estatisticas(
    historico: HistoricoConsultaFila,
    *,
    max_receive_count: int,
) -> RespostaEstatisticas:
    """Cálculo (potencialmente pesado) das estatísticas por fluxo.

    Executado dentro de :func:`executar_com_timeout`. Reúne os eventos, delega ao
    agregador puro e converte o resultado para o esquema de saída, separado e
    rotulado por fluxo (Req. 3.8).
    """
    eventos = _coletar_eventos(historico)
    agregadas = agregar_estatisticas(eventos, max_receive_count=max_receive_count)
    return RespostaEstatisticas(
        timeout=False,
        mensagem="",
        standard=_fluxo_para_saida(FLUXO_STANDARD, agregadas.standard),
        fifo=_fluxo_para_saida(FLUXO_FIFO, agregadas.fifo),
    )


def obter_estatisticas(
    historico: HistoricoConsultaFila,
    *,
    timeout_segundos: float = TIMEOUT_PADRAO_SEGUNDOS,
    max_receive_count: int = c.MAX_RECEIVE_COUNT,
    executor: Optional[ThreadPoolExecutor] = None,
    calcular: Optional[Callable[[], RespostaEstatisticas]] = None,
) -> RespostaEstatisticas:
    """Monta a resposta de ``GET /api/estatisticas`` com timeout (Req. 3, 14.1, 14.3).

    O cálculo roda sob um limite de tempo configurável (padrão 10 s). Ao exceder,
    devolve uma :class:`RespostaEstatisticas` com ``timeout=True`` e mensagem em
    pt-BR, sem levantar erro bruto; o Dashboard preserva os últimos dados válidos.

    Args:
        historico: Repositório da Tabela_Historico (injetável; fake nos testes).
        timeout_segundos: Limite de tempo do cálculo (Req. 14.1).
        max_receive_count: Limite da Redrive_Policy repassado ao StateDeriver.
        executor: ``ThreadPoolExecutor`` opcional (injetável nos testes).
        calcular: Função de cálculo opcional, usada apenas nos testes para injetar
            um cálculo lento/determinístico e forçar o caminho de timeout sem
            esperar o tempo real. Em produção, deixe ``None``.

    Returns:
        :class:`RespostaEstatisticas` com os dados por fluxo, ou a indicação de
        timeout.
    """
    funcao = calcular or (
        lambda: _calcular_estatisticas(historico, max_receive_count=max_receive_count)
    )
    expirou, resultado = executar_com_timeout(
        funcao, timeout_segundos=timeout_segundos, executor=executor
    )
    if expirou or resultado is None:
        return RespostaEstatisticas(
            timeout=True,
            mensagem=MENSAGEM_TIMEOUT_ESTATISTICAS.format(
                segundos=_formatar_segundos(timeout_segundos)
            ),
            standard=None,
            fifo=None,
        )
    return resultado


# ---------------------------------------------------------------------------
# Monitoramento (GET /api/monitoramento)
# ---------------------------------------------------------------------------
# Processing_seconds de um pedido vem preferencialmente do evento 'enviada'; a
# contagem de tentativas vem do maior receive_count entre os eventos 'recebida'.
def _linha_de_mensagem(
    order_id: str,
    eventos_do_pedido: list[Evento],
    *,
    max_receive_count: int,
) -> LinhaMonitoramento:
    """Monta uma linha da tabela de monitoramento para um pedido (Req. 5.1-5.5)."""
    ordenados = ordenar_cronologicamente(eventos_do_pedido)

    estado = derivar_estado(ordenados, max_receive_count=max_receive_count)

    # Coluna Tentativas (Req. 5.2): maior receive_count entre eventos 'recebida'.
    tentativas = max(
        (
            ev.receive_count
            for ev in ordenados
            if ev.event_type is EventType.RECEBIDA
            and ev.receive_count is not None
            and ev.receive_count >= 0
        ),
        default=0,
    )

    # Coluna Processamento (Req. 5.3): Tempo_Processamento_Simulado da mensagem; o
    # valor mais confiável é o do envio, com fallback para qualquer evento que o
    # carregue.
    processamento = _processamento_segundos(ordenados)

    # Coluna Consumidor: o consumidor do evento mais recente que possui um.
    consumidor = _consumidor_responsavel(ordenados)

    # Coluna Fila: a fila do evento mais recente (reflete a localização atual).
    fila = ordenados[-1].queue_name if ordenados else ""

    # Coluna Último evento (Req. 5.5): horário do evento mais recente (app_time).
    ultimo_evento = ordenados[-1].app_time if ordenados else None

    return LinhaMonitoramento(
        id=order_id,
        fila=fila,
        consumidor=consumidor,
        tentativas=tentativas,
        processamento_segundos=processamento,
        estado=estado.value,
        ultimo_evento=ultimo_evento,
    )


def _processamento_segundos(ordenados: list[Evento]) -> Optional[int]:
    """Deriva o Tempo_Processamento_Simulado da mensagem (coluna Processamento)."""
    # Preferência: evento 'enviada' (fonte original do parâmetro de simulação).
    for ev in ordenados:
        if ev.event_type is EventType.ENVIADA and ev.processing_seconds is not None:
            return ev.processing_seconds
    # Fallback: qualquer evento que carregue processing_seconds (ex.: recebida).
    for ev in ordenados:
        if ev.processing_seconds is not None:
            return ev.processing_seconds
    return None


def _consumidor_responsavel(ordenados: list[Evento]) -> Optional[str]:
    """Retorna o consumidor do evento mais recente que possui um ``consumer_id``."""
    for ev in reversed(ordenados):
        consumidor = _consumer_id(ev)
        if consumidor is not None:
            return consumidor
    return None


def _calcular_monitoramento(
    historico: HistoricoConsultaFila,
    *,
    max_receive_count: int,
) -> RespostaMonitoramento:
    """Cálculo (potencialmente pesado) da tabela de monitoramento.

    Executado dentro de :func:`executar_com_timeout`. Reúne os eventos, agrupa por
    ``order_id`` preservando a ordem de primeira aparição e monta uma linha por
    mensagem com o Estado derivado (Req. 5.9). Trata o estado vazio (Req. 5.8).
    """
    eventos = _coletar_eventos(historico)

    # Agrupa por pedido, ignorando o fluxo (o monitoramento lista todas as mensagens);
    # eventos sem order_id não representam uma mensagem monitorável e são ignorados.
    eventos_por_pedido: dict[str, list[Evento]] = {}
    for evento in eventos:
        if classificar_fluxo(evento.queue_name) is None:
            continue
        order_id = evento.order_id
        if not order_id:
            continue
        eventos_por_pedido.setdefault(order_id, []).append(evento)

    if not eventos_por_pedido:
        # Estado vazio: sem mensagens registradas (Req. 5.8).
        return RespostaMonitoramento(
            timeout=False,
            mensagem="",
            linhas=[],
            vazio=True,
            mensagem_vazia=MENSAGEM_SEM_MENSAGENS_MONITORADAS,
        )

    linhas = [
        _linha_de_mensagem(
            order_id, eventos_do_pedido, max_receive_count=max_receive_count
        )
        for order_id, eventos_do_pedido in eventos_por_pedido.items()
    ]

    return RespostaMonitoramento(
        timeout=False,
        mensagem="",
        linhas=linhas,
        vazio=False,
        mensagem_vazia=None,
    )


def obter_monitoramento(
    historico: HistoricoConsultaFila,
    *,
    timeout_segundos: float = TIMEOUT_PADRAO_SEGUNDOS,
    max_receive_count: int = c.MAX_RECEIVE_COUNT,
    executor: Optional[ThreadPoolExecutor] = None,
    calcular: Optional[Callable[[], RespostaMonitoramento]] = None,
) -> RespostaMonitoramento:
    """Monta a resposta de ``GET /api/monitoramento`` com timeout (Req. 5, 14.2, 14.4).

    O cálculo roda sob um limite de tempo configurável (padrão 10 s). Ao exceder,
    devolve uma :class:`RespostaMonitoramento` com ``timeout=True`` e mensagem em
    pt-BR, sem levantar erro bruto; o Dashboard preserva os últimos dados válidos.

    Args:
        historico: Repositório da Tabela_Historico (injetável; fake nos testes).
        timeout_segundos: Limite de tempo do cálculo (Req. 14.2).
        max_receive_count: Limite da Redrive_Policy repassado ao StateDeriver.
        executor: ``ThreadPoolExecutor`` opcional (injetável nos testes).
        calcular: Função de cálculo opcional, usada apenas nos testes para forçar o
            caminho de timeout sem esperar o tempo real. Em produção, deixe ``None``.

    Returns:
        :class:`RespostaMonitoramento` com as linhas, o estado vazio ou a indicação
        de timeout.
    """
    funcao = calcular or (
        lambda: _calcular_monitoramento(historico, max_receive_count=max_receive_count)
    )
    expirou, resultado = executar_com_timeout(
        funcao, timeout_segundos=timeout_segundos, executor=executor
    )
    if expirou or resultado is None:
        return RespostaMonitoramento(
            timeout=True,
            mensagem=MENSAGEM_TIMEOUT_MONITORAMENTO.format(
                segundos=_formatar_segundos(timeout_segundos)
            ),
            linhas=[],
            vazio=False,
            mensagem_vazia=None,
        )
    return resultado


def _formatar_segundos(segundos: float) -> str:
    """Formata o limite de tempo para a mensagem (inteiro quando não há fração)."""
    if float(segundos).is_integer():
        return str(int(segundos))
    return f"{segundos:g}"
