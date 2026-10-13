"""Agregador de estatísticas por fluxo (Standard/FIFO).

Este módulo implementa o agregador de estatísticas descrito no design.md
(endpoint ``GET /api/estatisticas``). A partir do log de eventos *append-only*
da Tabela_Historico, ele deriva, por fluxo (Standard vs FIFO), os contadores
exibidos na área de visão geral do Dashboard (Req. 3.1 a 3.8, 3.12) e a
contagem de tentativas por mensagem (Req. 5.2, 11.7).

Princípios de design:

- **Fonte de verdade é o log de eventos.** O SQS não expõe o estado individual
  das mensagens; cada contador é derivado dos eventos (Req. 5.10). O Estado de
  cada mensagem é obtido agregando os eventos por ``order_id`` e aplicando o
  :func:`app.dominio.state_deriver.derivar_estado`.
- **Separação por fluxo pela ``queue_name``.** Eventos de filas cujo nome começa
  com ``lab-sqs-standard`` pertencem ao fluxo Standard; os que começam com
  ``lab-sqs-fifo`` pertencem ao fluxo FIFO. Isso cobre tanto a fila principal
  quanto a respectiva DLQ (``lab-sqs-standard-dlq``, ``lab-sqs-fifo-dlq.fifo``),
  de modo que um encaminhamento para a DLQ é contabilizado no fluxo de origem
  (Req. 3.6, 3.8).
- **Função pura.** Não realiza E/S e não altera a entrada. Recebe qualquer
  objeto compatível com :class:`EventoLike` (dataclass, item desserializado da
  Tabela_Historico ou mock de teste), permitindo testes sem AWS.
- **Invariantes numéricas.** Todos os contadores e a contagem de tentativas são
  inteiros maiores ou iguais a 0; um log vazio produz 0 em todos eles
  (Req. 3.1 a 3.7, 3.12).

Referências de requisitos: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.12,
5.2, 11.7.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable, Optional

from app.dominio.constantes import (
    FILA_FIFO,
    FILA_STANDARD,
    MAX_RECEIVE_COUNT,
)
from app.dominio.state_deriver import derivar_estado
from app.dominio.tipos import Estado, EventType

# ---------------------------------------------------------------------------
# Classificação de fluxo pela queue_name
# ---------------------------------------------------------------------------
# As quatro filas do laboratório têm nomes com prefixos estáveis:
#   - Standard: "lab-sqs-standard" e "lab-sqs-standard-dlq"
#   - FIFO:     "lab-sqs-fifo.fifo" e "lab-sqs-fifo-dlq.fifo"
# Classificar pelo prefixo garante que a DLQ seja contabilizada no mesmo fluxo da
# fila principal correspondente (Req. 3.8).
PREFIXO_FLUXO_STANDARD: str = "lab-sqs-standard"
PREFIXO_FLUXO_FIFO: str = "lab-sqs-fifo"

# Rótulos dos dois fluxos (usados como chave do resultado agregado).
FLUXO_STANDARD: str = "standard"
FLUXO_FIFO: str = "fifo"


@dataclass
class EstatisticasFluxo:
    """Contadores agregados de um único fluxo (Standard ou FIFO).

    Todos os campos são inteiros maiores ou iguais a 0 (Req. 3.1 a 3.7, 3.12).
    Um fluxo sem eventos mantém todos os contadores em 0.

    Campos:
        enviadas: total de mensagens enviadas no fluxo (Req. 3.1).
        disponiveis: mensagens disponíveis na fila — estados Aguardando, Recebida
            e Aguardando nova tentativa (Req. 3.2).
        em_processamento: mensagens em processamento (Req. 3.3).
        processadas: mensagens processadas com sucesso (Req. 3.4).
        com_falha: mensagens no estado Falha (Req. 3.5).
        encaminhadas_dlq: mensagens encaminhadas para a DLQ (Req. 3.6).
        exclusao_nao_confirmada: mensagens cujo processamento terminou mas cuja
            exclusão (DeleteMessage) não foi confirmada. Não contam como
            processadas nem como com_falha.
        recebidas_por_consumidor: recebimentos por ``consumer_id`` (Req. 3.7).
        tentativas_por_mensagem: contagem de tentativas (recebimentos) por
            ``order_id`` (Req. 5.2, 11.7).
    """

    enviadas: int = 0
    disponiveis: int = 0
    em_processamento: int = 0
    processadas: int = 0
    com_falha: int = 0
    encaminhadas_dlq: int = 0
    exclusao_nao_confirmada: int = 0
    recebidas_por_consumidor: dict[str, int] = field(default_factory=dict)
    tentativas_por_mensagem: dict[str, int] = field(default_factory=dict)


@dataclass
class EstatisticasAgregadas:
    """Estatísticas separadas por fluxo (Req. 3.8).

    Mantém um :class:`EstatisticasFluxo` para cada fluxo. Um log vazio produz dois
    conjuntos com todos os contadores em 0.
    """

    standard: EstatisticasFluxo = field(default_factory=EstatisticasFluxo)
    fifo: EstatisticasFluxo = field(default_factory=EstatisticasFluxo)


# Estados que representam uma mensagem "disponível" na fila para o Dashboard
# (ainda não concluída, não falha definitiva e não em processamento). Agrupar
# Aguardando, Recebida e Aguardando nova tentativa reflete mensagens que ainda
# podem voltar a ser entregues a um consumidor (Req. 3.2).
_ESTADOS_DISPONIVEIS: frozenset[Estado] = frozenset(
    {
        Estado.AGUARDANDO,
        Estado.RECEBIDA,
        Estado.AGUARDANDO_NOVA_TENTATIVA,
    }
)


def classificar_fluxo(queue_name: Optional[str]) -> Optional[str]:
    """Classifica uma ``queue_name`` no fluxo Standard ou FIFO.

    A ordem de verificação importa: o prefixo FIFO é testado primeiro apenas por
    clareza, mas os prefixos são mutuamente exclusivos (``lab-sqs-standard`` nunca
    começa com ``lab-sqs-fifo`` e vice-versa).

    Args:
        queue_name: nome da fila do evento (ex.: ``lab-sqs-standard-dlq``).

    Returns:
        :data:`FLUXO_STANDARD`, :data:`FLUXO_FIFO` ou ``None`` quando o nome não
        corresponde a nenhum fluxo conhecido (evento é então ignorado).
    """
    if not queue_name:
        return None
    if queue_name.startswith(PREFIXO_FLUXO_FIFO):
        return FLUXO_FIFO
    if queue_name.startswith(PREFIXO_FLUXO_STANDARD):
        return FLUXO_STANDARD
    return None


def _event_type(evento: object) -> Optional[EventType]:
    """Normaliza ``event_type`` para :class:`EventType` (aceita enum ou string)."""
    valor = getattr(evento, "event_type", None)
    if isinstance(valor, EventType):
        return valor
    if isinstance(valor, str):
        try:
            return EventType(valor)
        except ValueError:
            return None
    return None


def _receive_count(evento: object) -> int:
    """Extrai ``receive_count`` como inteiro não-negativo (ausente/None vira 0)."""
    valor = getattr(evento, "receive_count", None)
    try:
        numero = int(valor) if valor is not None else 0
    except (TypeError, ValueError):
        return 0
    return numero if numero >= 0 else 0


def _consumer_id(evento: object) -> Optional[str]:
    """Extrai ``consumer_id`` como string não-vazia, ou ``None`` se ausente."""
    valor = getattr(evento, "consumer_id", None)
    if valor is None:
        return None
    texto = str(valor).strip()
    return texto or None


def _order_id(evento: object) -> Optional[str]:
    """Extrai ``order_id`` como string não-vazia, ou ``None`` se ausente."""
    valor = getattr(evento, "order_id", None)
    if valor is None:
        return None
    texto = str(valor)
    return texto or None


def agregar_estatisticas(
    eventos: Iterable[object],
    *,
    max_receive_count: int = MAX_RECEIVE_COUNT,
) -> EstatisticasAgregadas:
    """Agrega as estatísticas por fluxo a partir do log de eventos.

    Função pura: não realiza E/S e não altera a entrada. Produz um
    :class:`EstatisticasAgregadas` com um :class:`EstatisticasFluxo` por fluxo.

    Algoritmo:

    1. Classifica cada evento no fluxo Standard ou FIFO pela ``queue_name``;
       eventos de filas desconhecidas são ignorados.
    2. Dentro de cada fluxo, agrupa os eventos por ``order_id`` (cada ``order_id``
       é uma mensagem). Eventos sem ``order_id`` são ignorados para fins de
       derivação de Estado, mas seus recebimentos ainda contam para
       ``recebidas_por_consumidor`` quando possuem ``consumer_id``.
    3. Para cada mensagem, deriva o Estado com
       :func:`app.dominio.state_deriver.derivar_estado` e incrementa o contador
       correspondente (disponíveis, em processamento, processadas, falha ou DLQ).
    4. ``enviadas`` conta eventos ``enviada`` por fluxo (Req. 3.1).
    5. ``recebidas_por_consumidor`` conta eventos ``recebida`` por ``consumer_id``
       (Req. 3.7).
    6. ``tentativas_por_mensagem`` é o maior ``receive_count`` observado nos
       eventos ``recebida`` de cada ``order_id`` (Req. 5.2, 11.7).

    Args:
        eventos: iterável de eventos (log append-only). A ordem fornecida deve ser
            a ordem cronológica de registro, pois o StateDeriver depende dela.
        max_receive_count: limite da Redrive_Policy repassado ao StateDeriver;
            padrão :data:`MAX_RECEIVE_COUNT`.

    Returns:
        :class:`EstatisticasAgregadas` com todos os contadores inteiros ≥ 0; um log
        vazio retorna dois fluxos com todos os contadores em 0 (Req. 3.12).
    """
    lista = list(eventos)

    # Eventos agrupados por (fluxo, order_id) para derivar o Estado de cada mensagem.
    eventos_por_mensagem: dict[tuple[str, str], list[object]] = defaultdict(list)

    # Contadores diretos acumulados por fluxo durante a varredura.
    enviadas: dict[str, int] = defaultdict(int)
    recebidas_por_consumidor: dict[str, dict[str, int]] = {
        FLUXO_STANDARD: defaultdict(int),
        FLUXO_FIFO: defaultdict(int),
    }
    tentativas: dict[str, dict[str, int]] = {
        FLUXO_STANDARD: defaultdict(int),
        FLUXO_FIFO: defaultdict(int),
    }

    for evento in lista:
        fluxo = classificar_fluxo(getattr(evento, "queue_name", None))
        if fluxo is None:
            continue

        tipo = _event_type(evento)
        order_id = _order_id(evento)

        # Agrupa por mensagem para posterior derivação de Estado.
        if order_id is not None:
            eventos_por_mensagem[(fluxo, order_id)].append(evento)

        if tipo is EventType.ENVIADA:
            enviadas[fluxo] += 1

        if tipo is EventType.RECEBIDA:
            consumidor = _consumer_id(evento)
            if consumidor is not None:
                recebidas_por_consumidor[fluxo][consumidor] += 1
            if order_id is not None:
                # Tentativas = maior ApproximateReceiveCount visto para a mensagem.
                atual = tentativas[fluxo][order_id]
                tentativas[fluxo][order_id] = max(atual, _receive_count(evento))

    # Monta o resultado por fluxo, derivando o Estado de cada mensagem.
    resultado = EstatisticasAgregadas()
    por_fluxo: dict[str, EstatisticasFluxo] = {
        FLUXO_STANDARD: resultado.standard,
        FLUXO_FIFO: resultado.fifo,
    }

    for (fluxo, _order_id_chave), eventos_msg in eventos_por_mensagem.items():
        estado = derivar_estado(eventos_msg, max_receive_count=max_receive_count)
        destino = por_fluxo[fluxo]

        if estado in _ESTADOS_DISPONIVEIS:
            destino.disponiveis += 1
        elif estado is Estado.EM_PROCESSAMENTO:
            destino.em_processamento += 1
        elif estado is Estado.PROCESSADA:
            destino.processadas += 1
        elif estado is Estado.FALHA:
            destino.com_falha += 1
        elif estado is Estado.ENCAMINHADA_DLQ:
            destino.encaminhadas_dlq += 1
        elif estado is Estado.EXCLUSAO_NAO_CONFIRMADA:
            destino.exclusao_nao_confirmada += 1

    # Transfere os contadores diretos para o resultado (convertendo defaultdicts
    # em dicts simples e inteiros não-negativos).
    for fluxo, destino in por_fluxo.items():
        destino.enviadas = enviadas[fluxo]
        destino.recebidas_por_consumidor = dict(recebidas_por_consumidor[fluxo])
        destino.tentativas_por_mensagem = dict(tentativas[fluxo])

    return resultado
