"""Serviço do histórico detalhado da API_Backend (Tarefa 15.6).

Encapsula a lógica do endpoint ``GET /api/mensagens/{order_id}/historico``,
mantendo o roteador FastAPI fino. Monta a **linha do tempo** cronológica dos
eventos de uma mensagem a partir da Tabela_Historico
(:meth:`~app.acesso.historico_repository.HistoricoRepository.listar_eventos_cronologico`),
convertendo cada evento no esquema de saída com:

- a ordem cronológica crescente, com desempate estável por ``seq`` (Req. 6.1),
  garantida pelo próprio repositório (que aplica
  :func:`app.dominio.ordenacao.ordenar_cronologicamente`);
- o indicador textual de origem **Observado/Inferido** por evento, sem exigir
  interação do usuário (Req. 6.4), classificado por
  :func:`app.dominio.classificador_origem.classificar_origem` — mantendo a
  consistência com a tabela do design (ex.: ``provavel_expiracao_visibility`` é
  INFERIDO; ``encaminhada_dlq`` é OBSERVADO — Req. 6.5, 6.6);
- o horário real da aplicação com identificação de fuso (Req. 6.3), tal como
  registrado (ISO-8601 UTC com sufixo ``Z``).

Tratamento de estados especiais (Req. 6.7, 6.8):

- **Sem eventos:** a mensagem existe mas não tem eventos; devolve ``vazio=True`` e
  a indicação em pt-BR (Req. 6.8).
- **Consulta indisponível:** qualquer falha ao consultar a Tabela_Historico é
  capturada e convertida em ``indisponivel=True`` com a mensagem em pt-BR, de modo
  que o Dashboard preserve a tela anterior (Req. 6.7). O erro bruto não vaza para o
  cliente.

Referências de requisitos: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8.
"""

from __future__ import annotations

from typing import Protocol

from app.api.esquemas import (
    EventoHistorico,
    MENSAGEM_HISTORICO_INDISPONIVEL,
    MENSAGEM_HISTORICO_SEM_EVENTOS,
    ROTULO_EVENTO_INFERIDO,
    ROTULO_EVENTO_OBSERVADO,
    RespostaHistorico,
)
from app.dominio.classificador_origem import classificar_origem
from app.dominio.evento import Evento
from app.dominio.tipos import EventSource


class HistoricoLinhaDoTempo(Protocol):
    """Contrato mínimo do repositório usado por este serviço.

    Apenas :meth:`listar_eventos_cronologico` é necessária; declarar o mínimo
    mantém o acoplamento baixo e permite fakes em memória nos testes.
    """

    def listar_eventos_cronologico(self, order_id: str) -> list[Evento]:
        """Retorna os eventos do pedido em ordem cronológica crescente."""
        ...


def _para_evento_saida(ordem: int, evento: Evento) -> EventoHistorico:
    """Converte um :class:`Evento` no item de saída da linha do tempo (Req. 6.2-6.6).

    A origem é sempre **classificada** a partir do ``event_type`` pela regra do
    design, garantindo o indicador correto mesmo que o item persistido traga uma
    origem divergente. O horário é repassado como registrado (ISO-8601 UTC com
    sufixo ``Z``, identificando o fuso — Req. 6.3).
    """
    origem = classificar_origem(evento.event_type)
    observado = origem is EventSource.OBSERVADO
    indicador = ROTULO_EVENTO_OBSERVADO if observado else ROTULO_EVENTO_INFERIDO

    return EventoHistorico(
        ordem=ordem,
        event_type=evento.event_type.value,
        event_source=origem.value,
        indicador_origem=indicador,
        observado=observado,
        app_time=evento.app_time,
        fila=evento.queue_name,
        consumidor=evento.consumer_id,
        tentativas=evento.receive_count,
        processamento_segundos=evento.processing_seconds,
        message_group_id=evento.message_group_id,
        detalhe_erro=evento.error_detail,
    )


def obter_historico(
    historico: HistoricoLinhaDoTempo, order_id: str
) -> RespostaHistorico:
    """Monta a resposta de ``GET /api/mensagens/{order_id}/historico`` (Req. 6.1-6.8).

    Fluxo:

    1. Consulta os eventos do pedido em ordem cronológica crescente. Qualquer falha
       de consulta é capturada e convertida em ``indisponivel=True`` com mensagem em
       pt-BR (Req. 6.7) — o erro bruto não vaza ao cliente.
    2. Se não há eventos, devolve ``vazio=True`` com a indicação em pt-BR (Req. 6.8).
    3. Caso contrário, converte cada evento no item de saída, com o indicador de
       origem (Observado/Inferido) e o horário com fuso (Req. 6.3, 6.4).

    Args:
        historico: Repositório da Tabela_Historico (injetável; fake nos testes).
        order_id: Identificador do pedido cuja linha do tempo será montada.

    Returns:
        :class:`RespostaHistorico` com a linha do tempo, o estado vazio ou a
        indicação de indisponibilidade.
    """
    try:
        eventos = historico.listar_eventos_cronologico(order_id)
    except Exception:  # noqa: BLE001 — convertemos em indicação de indisponível (Req. 6.7)
        return RespostaHistorico(
            order_id=order_id,
            indisponivel=True,
            vazio=False,
            mensagem=MENSAGEM_HISTORICO_INDISPONIVEL,
            eventos=[],
        )

    if not eventos:
        return RespostaHistorico(
            order_id=order_id,
            indisponivel=False,
            vazio=True,
            mensagem=MENSAGEM_HISTORICO_SEM_EVENTOS,
            eventos=[],
        )

    eventos_saida = [
        _para_evento_saida(ordem, evento) for ordem, evento in enumerate(eventos)
    ]
    return RespostaHistorico(
        order_id=order_id,
        indisponivel=False,
        vazio=False,
        mensagem="",
        eventos=eventos_saida,
    )
