"""Monitor de DLQ (MonitorDlq) — registra o evento ``encaminhada_dlq`` observado.

Problema que este módulo resolve
--------------------------------
No laboratório, o Amazon SQS move automaticamente para a DLQ as mensagens que
excedem o ``maxReceiveCount`` da Redrive_Policy. Porém, **nenhum** componente da
aplicação lia as DLQs: os consumidores só consomem as filas principais. Como o
``encaminhada_dlq`` só pode ser registrado como **Evento_Observado** depois de
CONFIRMAR a presença física da mensagem na DLQ (Req. 6.6, 7.9, 7.10), o evento
nunca era gravado — o Histórico parava em "falha" e o Monitoramento não exibia
"Encaminhada para DLQ".

O :class:`MonitorDlq` fecha essa lacuna **observando** as DLQs: a cada varredura ele
recebe mensagens das DLQs (sem excluí-las — Req. de não apagar mensagens) e, para
cada uma, registra um evento ``encaminhada_dlq`` OBSERVADO associado à **fila
principal** correspondente, de modo que o Histórico (consulta por ``order_id``) e o
Monitoramento (agrupa por ``order_id`` e deriva o Estado) passem a enxergar o
encaminhamento e o StateDeriver derive :attr:`~app.dominio.tipos.Estado.ENCAMINHADA_DLQ`.

Decisões de design relevantes
-----------------------------
- **Correção acima de tudo (Req. 6.6, 7.9, 7.10).** O evento só é registrado APÓS a
  mensagem ser efetivamente recebida da DLQ (presença física confirmada). Nunca se
  registra ``encaminhada_dlq`` apenas por contagem de falhas.
- **Não apagar mensagens.** A mensagem NÃO é excluída da DLQ. Logo, após o
  Visibility_Timeout da DLQ ela reaparece em varreduras futuras. Para não duplicar o
  evento a cada varredura, o registro é **idempotente**: antes de gravar, consulta-se
  o histórico do ``order_id`` e só se registra quando ainda não existe um evento
  ``encaminhada_dlq`` para aquele pedido.
- **Associação à fila principal.** O evento recebe ``queue_name`` da fila PRINCIPAL
  (não da DLQ), via o mapa inverso :data:`FILA_PRINCIPAL_POR_DLQ`. Assim o evento é
  agregado junto dos demais eventos do pedido tanto no Histórico quanto no
  Monitoramento.
- **``receive_count`` que satisfaz o StateDeriver.** O StateDeriver exige
  ``receive_count >= maxReceiveCount`` para confirmar a DLQ. Usamos
  ``max(approximate_receive_count da DLQ, MAX_RECEIVE_COUNT)`` para garantir a
  condição mesmo que o contador da DLQ reinicie.
- **Tolerância a falhas por fila.** Se a leitura de uma DLQ falhar, a varredura
  captura o erro e segue para as demais DLQs, sem derrubar o monitor.
- **Dependências injetáveis.** Recebe ``SqsGateway`` e ``HistoricoRepository`` (ou
  compatíveis), permitindo fakes em memória nos testes, sem AWS real.

Referências de requisitos: 6.6, 7.9, 7.10, 8.1, 11.3.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Optional

from app.acesso.historico_repository import HistoricoRepository, agora_utc_ms
from app.acesso.sqs_gateway import MensagemRecebida, SqsGateway
from app.consumidores.payload_mensagem import extrair_payload
from app.dominio import constantes as c
from app.dominio.classificador_origem import classificar_origem
from app.dominio.evento import Evento
from app.dominio.tipos import EventType

_log = logging.getLogger(__name__)

# Mapa inverso DLQ -> fila principal, derivado de DLQ_POR_FILA_PRINCIPAL. Usado para
# associar o evento `encaminhada_dlq` à fila PRINCIPAL correspondente (e não à DLQ),
# de modo que o evento seja agregado junto dos demais eventos do pedido.
FILA_PRINCIPAL_POR_DLQ: dict[str, str] = {
    dlq: principal for principal, dlq in c.DLQ_POR_FILA_PRINCIPAL.items()
}

# consumer_id usado nos eventos registrados pelo monitor (identifica a origem).
CONSUMER_ID_MONITOR_DLQ: str = "monitor-dlq"

# Parâmetros padrão da leitura das DLQs. Lote de até 10 mensagens; Long_Polling curto
# para a varredura não ficar presa esperando (o loop periódico cuida da cadência).
MAX_MENSAGENS_POR_VARREDURA: int = 10
WAIT_TIME_SECONDS_VARREDURA: int = 1

# Intervalo padrão (em segundos) entre varreduras no loop de segundo plano.
INTERVALO_VARREDURA_PADRAO_SEGUNDOS: float = 5.0


@dataclass
class MonitorDlq:
    """Observa as DLQs e registra ``encaminhada_dlq`` de forma idempotente.

    Atributos:
        sqs: Gateway de acesso ao SQS (usado para receber mensagens das DLQs).
        historico: Repositório da Tabela_Historico (consulta por pedido e registro).
        filas_dlq: Conjunto de DLQs a varrer (padrão: :data:`c.FILAS_DLQ`).
        max_mensagens: Tamanho do lote por recebimento em cada DLQ (1 a 10).
        wait_time_seconds: WaitTimeSeconds do Long_Polling na leitura da DLQ.
        max_receive_count: Limite da Redrive_Policy; usado como piso do
            ``receive_count`` para satisfazer o StateDeriver.
    """

    sqs: SqsGateway
    historico: HistoricoRepository
    filas_dlq: tuple[str, ...] = c.FILAS_DLQ
    max_mensagens: int = MAX_MENSAGENS_POR_VARREDURA
    wait_time_seconds: int = WAIT_TIME_SECONDS_VARREDURA
    max_receive_count: int = c.MAX_RECEIVE_COUNT

    def varrer_uma_vez(self) -> int:
        """Varre todas as DLQs uma vez e registra os eventos ainda não gravados.

        Para cada DLQ, recebe um lote de mensagens (sem excluí-las) e, para cada
        mensagem, registra o ``encaminhada_dlq`` quando ainda não existir para o
        pedido (idempotência). A leitura de cada DLQ é isolada: um erro em uma DLQ é
        capturado e registrado, sem impedir a varredura das demais.

        Returns:
            A quantidade de eventos ``encaminhada_dlq`` efetivamente registrados
            nesta varredura (0 quando nada novo foi observado).
        """
        total_registrados = 0
        for dlq in self.filas_dlq:
            total_registrados += self._varrer_dlq(dlq)
        return total_registrados

    def _varrer_dlq(self, dlq: str) -> int:
        """Varre uma única DLQ, tolerando falhas de leitura (não derruba as demais)."""
        try:
            mensagens = self.sqs.receive_messages(
                dlq,
                max_mensagens=self.max_mensagens,
                wait_time_seconds=self.wait_time_seconds,
            )
        except Exception:  # noqa: BLE001 — isola a falha desta DLQ e segue adiante.
            _log.exception("Falha ao receber mensagens da DLQ %r; seguindo adiante.", dlq)
            return 0

        registrados = 0
        for mensagem in mensagens:
            try:
                if self._registrar_se_novo(dlq, mensagem):
                    registrados += 1
            except Exception:  # noqa: BLE001 — não deixa uma mensagem derrubar a varredura.
                _log.exception(
                    "Falha ao registrar encaminhada_dlq da DLQ %r (msg_id=%r).",
                    dlq,
                    mensagem.message_id,
                )
        return registrados

    def _registrar_se_novo(self, dlq: str, mensagem: MensagemRecebida) -> bool:
        """Registra o ``encaminhada_dlq`` do pedido se ainda não existir.

        Extrai o ``order_id`` do corpo (contrato do produtor; fallback para o corpo
        cru) e consulta o histórico do pedido. Só registra quando não há evento
        ``encaminhada_dlq`` para aquele ``order_id`` (idempotência), já que a
        mensagem permanece na DLQ e reaparecerá em varreduras futuras.

        Returns:
            ``True`` quando um novo evento foi registrado; ``False`` caso contrário.
        """
        payload = extrair_payload(mensagem.body)
        order_id = payload.order_id or mensagem.body
        if not order_id:
            # Sem order_id identificável: nada a registrar.
            return False

        if self._ja_registrado(order_id):
            return False

        fila_principal = FILA_PRINCIPAL_POR_DLQ.get(dlq, dlq)
        app_time, app_time_ms = agora_utc_ms()
        # Garante receive_count >= maxReceiveCount para satisfazer o StateDeriver
        # (confirmação de DLQ), mesmo que o contador na DLQ venha menor.
        receive_count = max(mensagem.approximate_receive_count, self.max_receive_count)

        evento = Evento(
            order_id=order_id,
            queue_name=fila_principal,
            event_type=EventType.ENCAMINHADA_DLQ,
            # Usa o classificador para manter a consistência com a tabela do design
            # (encaminhada_dlq é sempre OBSERVADO).
            event_source=classificar_origem(EventType.ENCAMINHADA_DLQ),
            app_time=app_time,
            app_time_ms=app_time_ms,
            consumer_id=CONSUMER_ID_MONITOR_DLQ,
            receive_count=receive_count,
            processing_seconds=payload.processing_seconds,
            message_group_id=mensagem.message_group_id,
            sqs_message_id=mensagem.message_id or None,
        )
        self.historico.registrar_evento(evento)
        return True

    def _ja_registrado(self, order_id: str) -> bool:
        """Indica se já existe um evento ``encaminhada_dlq`` para o pedido.

        Consulta o histórico do pedido (``consultar_por_pedido``). Garante a
        idempotência do registro frente à reentrega da mesma mensagem na DLQ.
        """
        eventos = self.historico.consultar_por_pedido(order_id)
        return any(
            evento.event_type is EventType.ENCAMINHADA_DLQ for evento in eventos
        )


@dataclass
class GerenciadorMonitorDlq:
    """Executa o :class:`MonitorDlq` em uma thread daemon de segundo plano.

    Segue o mesmo padrão de parada cooperativa dos consumidores: um
    ``threading.Event`` sinaliza a parada e o loop aguarda o intervalo via
    ``Event.wait`` (que acorda imediatamente no shutdown). As dependências são
    injetáveis para testes.

    Atributos:
        monitor: O :class:`MonitorDlq` a executar periodicamente.
        intervalo_segundos: Pausa entre varreduras (via ``Event.wait``).
    """

    monitor: MonitorDlq
    intervalo_segundos: float = INTERVALO_VARREDURA_PADRAO_SEGUNDOS
    _parada: threading.Event = field(default_factory=threading.Event, init=False)
    _thread: Optional[threading.Thread] = field(default=None, init=False)

    def iniciar(self) -> None:
        """Inicia a thread daemon do monitor (idempotente)."""
        if self._thread is not None:
            return
        self._parada.clear()
        self._thread = threading.Thread(
            target=self._executar,
            name="monitor-dlq",
            daemon=True,
        )
        self._thread.start()

    def _executar(self) -> None:
        """Loop de segundo plano: varre as DLQs e aguarda o intervalo até parar."""
        while not self._parada.is_set():
            try:
                self.monitor.varrer_uma_vez()
            except Exception:  # noqa: BLE001 — o loop nunca deve morrer por uma varredura.
                _log.exception("Falha inesperada na varredura de DLQ; continuando.")
            # Espera cooperativa: acorda imediatamente quando a parada é sinalizada.
            self._parada.wait(self.intervalo_segundos)

    def parar(self, *, timeout: Optional[float] = 5.0) -> None:
        """Sinaliza a parada e aguarda o término da thread (chamado no shutdown)."""
        self._parada.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
        self._thread = None

    @property
    def ativo(self) -> bool:
        """Indica se a thread do monitor está em execução (útil em testes)."""
        return self._thread is not None and self._thread.is_alive()
