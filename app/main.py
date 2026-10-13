"""Composição da aplicação na EC2 (Tarefa 18.1).

Este módulo é o **ponto de entrada** do laboratório Amazon SQS executado na
instância EC2. Ele compõe, em um único processo, os três elementos descritos no
``design.md`` (seção *Architecture*):

1. a **API_Backend** (FastAPI), criada por :func:`app.api.app.create_app`, que
   expõe os endpoints ``/api/*`` consumidos pelo Dashboard;
2. o **Dashboard** estático (``web/``), servido pela própria aplicação via
   ``starlette.staticfiles.StaticFiles`` (Req. 8.1, 8.2 — a aplicação é a mesma que
   hospeda a API);
3. os processos **Consumidor_Standard** e **Consumidor_FIFO**, iniciados como
   *threads* de segundo plano junto com a aplicação (Req. 8.1, 8.2, 10.1, 10.2).

Regra central desta tarefa (Req. 1.6, 1.8): a disponibilização do Dashboard é
**condicionada** à verificação de inicialização dos recursos AWS. Antes de servir o
Dashboard, :func:`compor_aplicacao` chama :func:`app.verificacao_inicializacao.verificar_inicializacao`.
Se a verificação **falha**, a aplicação **não** serve o Dashboard normalmente:
qualquer requisição às páginas estáticas recebe uma resposta de indisponibilidade
(HTTP 503) com a **causa em pt-BR**, identificando o recurso ausente/inacessível por
nome e tipo. Os consumidores também não são iniciados nesse caso, pois dependem dos
mesmos recursos. O endpoint ``GET /api/saude`` continua respondendo (com
``saudavel=False``) para que a causa seja inspecionável.

Decisões de design relevantes:

- **Reutiliza a fábrica existente.** ``create_app`` permanece intacta (os testes das
  Tarefas 15.x dependem do seu comportamento atual). A composição apenas a chama e
  acrescenta, por cima, o *mount* dos estáticos, o *gating* do Dashboard e o ciclo de
  vida dos consumidores. Dependências continuam injetáveis para testes sem AWS real.
- **Consumidores em threads daemon.** Em uma única EC2, iniciar os consumidores como
  *threads* é suficiente e simples (o SDK boto3 libera o GIL em chamadas de rede). O
  número de instâncias de cada tipo vem do :class:`~app.api.estado_instancias.EstadoInstanciasConsumidores`
  (Req. 10.1, 10.2). As threads são *daemon* e param de forma cooperativa no
  *shutdown* da aplicação.
- **Credenciais só pela IAM Role da EC2.** Nenhuma Access Key é usada; o boto3 resolve
  as credenciais temporárias da IAM_Role_EC2 (Req. 12.1, 12.2). Em produção,
  ``create_app`` cria o ``SqsGateway`` e o ``HistoricoRepository`` reais.

Como iniciar a aplicação (resumo; detalhes em ``IMPLANTACAO.md`` — Tarefa 20.2)::

    uvicorn app.main:app --host 0.0.0.0 --port 8000

O objeto ``app`` deste módulo é a instância FastAPI composta e pronta para servir.

Referências de requisitos: 1.6, 1.8, 8.1, 8.2, 10.1, 10.2, 12.1, 12.2.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.staticfiles import StaticFiles

from app.acesso.historico_repository import HistoricoRepository
from app.acesso.sqs_gateway import SqsGateway
from app.api.app import create_app
from app.api.esquemas import TIPO_CONSUMIDOR_FIFO, TIPO_CONSUMIDOR_STANDARD
from app.api.estado_instancias import EstadoInstanciasConsumidores
from app.api.servico_autenticacao import CredenciaisDashboard
from app.consumidores.consumidor_fifo import ConsumidorFifo
from app.consumidores.consumidor_standard import (
    ConsumidorStandard,
    criar_consumidor_standard,
    gerar_consumer_id,
)
from app.consumidores.monitor_dlq import GerenciadorMonitorDlq, MonitorDlq
from app.dominio import constantes as c
from app.observabilidade.log_estruturado import log_evento
from app.seguranca.autenticacao import ControladorAutenticacao
from app.verificacao_inicializacao import (
    ResultadoVerificacao,
    VerificadorTabela,
    verificar_inicializacao,
)

# Nomes das variáveis de ambiente que configuram a autenticação do Dashboard em
# produção (EC2). Nenhum segredo fica no código: os valores são lidos do ambiente
# (ex.: via ``Environment=`` no serviço systemd) e nunca de Access Keys da AWS.
ENV_ORIGENS_AUTORIZADAS: str = "DASHBOARD_ORIGENS_AUTORIZADAS"
ENV_LIBERAR_TODAS_ORIGENS: str = "DASHBOARD_LIBERAR_TODAS_ORIGENS"
ENV_USUARIO: str = "DASHBOARD_USUARIO"
ENV_SENHA: str = "DASHBOARD_SENHA"

# Credenciais de laboratório usadas apenas quando as envs não são definidas, para
# não quebrar o uso local. Em produção, defina DASHBOARD_USUARIO/DASHBOARD_SENHA.
USUARIO_PADRAO_LABORATORIO: str = "admin"
SENHA_PADRAO_LABORATORIO: str = "trocar-no-console"

# Diretório do Dashboard estático (``web/``), relativo à raiz do projeto. Mantido
# configurável para testes e para implantações que posicionem o frontend em outro
# caminho.
_RAIZ_PROJETO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIRETORIO_DASHBOARD_PADRAO: str = os.path.join(_RAIZ_PROJETO, "web")


# ---------------------------------------------------------------------------
# Gerenciamento do ciclo de vida dos consumidores (threads de segundo plano)
# ---------------------------------------------------------------------------
# Rótulos de tipo de consumidor usados internamente pelos slots.
_TIPO_STANDARD: str = TIPO_CONSUMIDOR_STANDARD
_TIPO_FIFO: str = TIPO_CONSUMIDOR_FIFO


@dataclass
class _SlotConsumidor:
    """Posição gerenciada por uma única instância de consumidor e sua thread.

    Um *slot* representa uma vaga lógica (ex.: "o 2º consumidor FIFO"). Em cada
    momento há **no máximo uma** thread viva por slot. Quando a thread morre
    inesperadamente, o supervisor cria um novo consumidor+thread **no mesmo slot**
    (nova instância com novo ``consumer_id``), de modo que não há duplicação: o slot
    continua representando uma única vaga.

    Atributos:
        tipo: ``"standard"`` ou ``"fifo"``.
        consumidor: a instância de consumidor atualmente associada ao slot.
        thread: a thread *daemon* que roda o laço do consumidor.
        reinicios: total de reinícios já realizados para este slot (acumulado).
        marcos_reinicio: *timestamps* (``time.monotonic``) dos reinícios recentes,
            usados para aplicar o teto por janela deslizante.
        degradado: ``True`` quando o slot excedeu o teto de reinícios na janela e
            deixou de ser reiniciado automaticamente (falha persistente).
    """

    tipo: str
    consumidor: object
    thread: threading.Thread
    reinicios: int = 0
    marcos_reinicio: list[float] = field(default_factory=list)
    degradado: bool = False


@dataclass
class EstadoSlotConsumidor:
    """Visão imutável do estado de um slot, exposta ao endpoint de saúde (FEAT-004).

    Atributos:
        tipo: ``"standard"`` ou ``"fifo"``.
        consumer_id: identificador da instância atualmente associada ao slot.
        vivo: ``True`` se a thread do slot está viva (``thread.is_alive()``).
        degradado: ``True`` se o slot foi marcado como falha persistente.
        reinicios: total de reinícios já realizados para o slot.
    """

    tipo: str
    consumer_id: str
    vivo: bool
    degradado: bool
    reinicios: int


@dataclass
class GerenciadorConsumidores:
    """Inicia, supervisiona e encerra os consumidores Standard e FIFO.

    O número de instâncias de cada tipo é lido do
    :class:`~app.api.estado_instancias.EstadoInstanciasConsumidores` no momento do
    início (Req. 10.1, 10.2). Cada instância ocupa um *slot* e roda seu laço em uma
    thread *daemon*, parando de forma cooperativa quando :meth:`parar` é chamado (no
    *shutdown* da aplicação).

    Resiliência (frente B): uma thread *supervisora* dedicada verifica
    periodicamente se algum slot morreu inesperadamente e, nesse caso, recria o
    consumidor **no mesmo slot** (nova instância, novo ``consumer_id``), sem duplicar
    consumidores. Uma política de reinício com teto por janela deslizante evita laços
    de reinício em falhas persistentes: ao exceder
    :data:`~app.dominio.constantes.MAX_REINICIOS_JANELA` reinícios dentro de
    :data:`~app.dominio.constantes.JANELA_REINICIOS_SEGUNDOS`, o slot é marcado como
    ``degradado`` e deixa de ser reiniciado.

    As dependências (``SqsGateway`` e ``HistoricoRepository``) são injetadas para que
    os testes substituam a AWS por dublês. O laço de cada consumidor e o supervisor
    são interrompidos por um ``threading.Event`` compartilhado.
    """

    gateway: SqsGateway
    historico: HistoricoRepository
    estado_instancias: EstadoInstanciasConsumidores
    _parada: threading.Event = field(default_factory=threading.Event, init=False)
    _slots: list[_SlotConsumidor] = field(default_factory=list, init=False)
    _supervisor: Optional[threading.Thread] = field(default=None, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def _deve_parar(self) -> bool:
        """Critério de parada cooperativa compartilhado pelos laços dos consumidores."""
        return self._parada.is_set()

    # ------------------------------------------------------------------
    # Criação de consumidor + thread por tipo
    # ------------------------------------------------------------------
    def _criar_consumidor(self, tipo: str) -> object:
        """Cria uma nova instância de consumidor do ``tipo`` com ``consumer_id`` novo."""
        if tipo == _TIPO_STANDARD:
            return criar_consumidor_standard(self.gateway, self.historico)
        return ConsumidorFifo(
            sqs=self.gateway,
            historico=self.historico,
            consumer_id=gerar_consumer_id("consumidor-fifo"),
        )

    def _criar_thread(self, tipo: str, consumidor: object) -> threading.Thread:
        """Cria a thread *daemon* que roda o laço do ``consumidor`` do ``tipo``."""
        if tipo == _TIPO_STANDARD:
            alvo: Callable[[], None] = lambda: self._executar_standard(consumidor)  # type: ignore[arg-type]
            nome = f"standard-{consumidor.consumer_id}"  # type: ignore[attr-defined]
        else:
            alvo = lambda: self._executar_fifo(consumidor)  # type: ignore[arg-type]
            nome = f"fifo-{consumidor.consumer_id}"  # type: ignore[attr-defined]
        return threading.Thread(target=alvo, name=nome, daemon=True)

    def iniciar(self) -> None:
        """Inicia os slots dos consumidores e a thread supervisora (Req. 8.1, 8.2).

        Lê o número de instâncias de cada tipo do estado em memória (Req. 10.1, 10.2)
        e cria um slot (consumidor + thread *daemon*) por instância. Idempotente: se
        já há slots, retorna sem recriar (não duplica consumidores).
        """
        if self._slots:
            return
        self._parada.clear()

        total_standard = self.estado_instancias.instancias(TIPO_CONSUMIDOR_STANDARD)
        total_fifo = self.estado_instancias.instancias(TIPO_CONSUMIDOR_FIFO)

        for tipo, total in ((_TIPO_STANDARD, total_standard), (_TIPO_FIFO, total_fifo)):
            for _ in range(total):
                consumidor = self._criar_consumidor(tipo)
                thread = self._criar_thread(tipo, consumidor)
                self._slots.append(
                    _SlotConsumidor(tipo=tipo, consumidor=consumidor, thread=thread)
                )
                thread.start()

        # Thread supervisora: recria slots mortos respeitando o critério de parada.
        self._supervisor = threading.Thread(
            target=self.supervisionar,
            name="supervisor-consumidores",
            daemon=True,
        )
        self._supervisor.start()

    def _executar_standard(self, consumidor: ConsumidorStandard) -> None:
        """Roda o laço do Consumidor_Standard até o sinal de parada (Req. 8.1).

        Uma guarda externa garante que, mesmo que o laço escape com uma exceção
        inesperada (que não seja a parada cooperativa), a thread termine
        normalmente — sem propagar a exceção ao runtime — e registre o encerramento
        de forma estruturada para que o supervisor possa recriar o slot.
        """
        try:
            consumidor.executar(continuar=lambda: not self._deve_parar())
        except Exception as e:  # NÃO captura BaseException (parada cooperativa).
            log_evento(
                logging.ERROR,
                "consumidor_encerrou_por_erro",
                tipo=_TIPO_STANDARD,
                consumer_id=consumidor.consumer_id,
                erro=repr(e),
            )
            return

    def _executar_fifo(self, consumidor: ConsumidorFifo) -> None:
        """Roda o laço do Consumidor_FIFO até o sinal de parada (Req. 8.2).

        Mesma guarda externa do Standard: a thread encerra normalmente mesmo diante
        de uma exceção inesperada, registrando o encerramento para o supervisor.
        """
        try:
            consumidor.executar(criterio_parada=self._deve_parar)
        except Exception as e:  # NÃO captura BaseException (parada cooperativa).
            log_evento(
                logging.ERROR,
                "consumidor_encerrou_por_erro",
                tipo=_TIPO_FIFO,
                consumer_id=consumidor.consumer_id,
                erro=repr(e),
            )
            return

    # ------------------------------------------------------------------
    # Supervisão e recriação de slots
    # ------------------------------------------------------------------
    def supervisionar(self) -> None:
        """Laço da thread supervisora: recria slots mortos até o sinal de parada.

        A cada :data:`~app.dominio.constantes.INTERVALO_SUPERVISAO_SEGUNDOS` o laço
        acorda (via ``Event.wait`` com *timeout*, para despertar imediatamente no
        *shutdown*) e percorre os slots. Para cada slot cuja thread morreu, que não
        está em parada e que ainda não foi marcado como ``degradado``, chama
        :meth:`_recriar_slot`. Nunca cria uma segunda thread para um slot vivo (uma
        thread viva por slot).
        """
        while not self._deve_parar():
            # Dorme o intervalo, mas acorda imediatamente se a parada for sinalizada.
            if self._parada.wait(timeout=c.INTERVALO_SUPERVISAO_SEGUNDOS):
                break
            with self._lock:
                if self._deve_parar():
                    break
                for slot in self._slots:
                    if (
                        not slot.thread.is_alive()
                        and not self._deve_parar()
                        and not slot.degradado
                    ):
                        self._recriar_slot(slot)

    def _recriar_slot(self, slot: _SlotConsumidor) -> None:
        """Recria o consumidor+thread de um slot morto, aplicando o teto de reinícios.

        Cria uma nova instância (novo ``consumer_id``) e thread no mesmo slot,
        registra o *timestamp* do reinício, descarta os marcos fora da janela de
        observação e incrementa o contador. Se o número de reinícios dentro de
        :data:`~app.dominio.constantes.JANELA_REINICIOS_SEGUNDOS` exceder
        :data:`~app.dominio.constantes.MAX_REINICIOS_JANELA`, marca o slot como
        ``degradado`` e para de reiniciá-lo (falha persistente).

        Deve ser chamado com :attr:`_lock` já adquirido.
        """
        agora = time.monotonic()
        # Mantém apenas os marcos dentro da janela deslizante de observação.
        slot.marcos_reinicio = [
            t for t in slot.marcos_reinicio if agora - t <= c.JANELA_REINICIOS_SEGUNDOS
        ]
        slot.marcos_reinicio.append(agora)
        slot.reinicios += 1

        if len(slot.marcos_reinicio) > c.MAX_REINICIOS_JANELA:
            slot.degradado = True
            log_evento(
                logging.ERROR,
                "consumidor_em_falha_persistente",
                tipo=slot.tipo,
                consumer_id=getattr(slot.consumidor, "consumer_id", None),
                reinicios=slot.reinicios,
                janela_segundos=c.JANELA_REINICIOS_SEGUNDOS,
            )
            return

        consumidor = self._criar_consumidor(slot.tipo)
        thread = self._criar_thread(slot.tipo, consumidor)
        slot.consumidor = consumidor
        slot.thread = thread
        log_evento(
            logging.WARNING,
            "consumidor_reiniciado",
            tipo=slot.tipo,
            consumer_id=consumidor.consumer_id,  # type: ignore[attr-defined]
            reinicios=slot.reinicios,
        )
        thread.start()

    def parar(self, *, timeout: Optional[float] = 5.0) -> None:
        """Sinaliza a parada cooperativa e aguarda o término das threads.

        Ordem de encerramento (determinística):

        1. sinaliza o ``Event`` de parada;
        2. chama ``parar()`` em cada :class:`ConsumidorStandard`;
        3. encerra e faz *join* da thread supervisora **antes** dos slots (para que
           ela não recrie nenhum slot durante o *shutdown*);
        4. faz *join* (com ``timeout``) em cada thread de slot;
        5. limpa a lista de slots.
        """
        self._parada.set()

        for slot in self._slots:
            if isinstance(slot.consumidor, ConsumidorStandard):
                slot.consumidor.parar()

        # (3) A supervisora é encerrada ANTES dos slots: sem recriação no shutdown.
        if self._supervisor is not None:
            self._supervisor.join(timeout=timeout)
            self._supervisor = None

        for slot in self._slots:
            slot.thread.join(timeout=timeout)

        self._slots.clear()

    # ------------------------------------------------------------------
    # Exposição de estado (consumido pelo endpoint de saúde — FEAT-004)
    # ------------------------------------------------------------------
    def estado_slots(self) -> list[EstadoSlotConsumidor]:
        """Devolve uma visão do estado atual de cada slot (tipo, id, vivo, etc.).

        Método público e seguro para o endpoint de saúde iterar os slots sem acessar
        a estrutura interna. Reflete, em cada chamada, se a thread está viva, se o
        slot está degradado e quantos reinícios já ocorreram.
        """
        with self._lock:
            return [
                EstadoSlotConsumidor(
                    tipo=slot.tipo,
                    consumer_id=getattr(slot.consumidor, "consumer_id", ""),
                    vivo=slot.thread.is_alive(),
                    degradado=slot.degradado,
                    reinicios=slot.reinicios,
                )
                for slot in self._slots
            ]

    @property
    def quantidade_threads(self) -> int:
        """Número de threads de consumidor (slots) atualmente gerenciadas.

        Conta os slots gerenciados (uma thread de consumidor por slot). Após
        :meth:`parar` a lista de slots é esvaziada, então volta a 0. Não inclui a
        thread supervisora.
        """
        return len(self._slots)


# ---------------------------------------------------------------------------
# Mount do Dashboard estático condicionado à verificação (Req. 1.6, 1.8)
# ---------------------------------------------------------------------------
def _montar_dashboard_disponivel(app: FastAPI, diretorio_dashboard: str) -> None:
    """Serve o Dashboard estático a partir de ``diretorio_dashboard`` (Req. 8.1).

    Usa ``StaticFiles`` com ``html=True`` para que ``/`` sirva ``index.html`` e os
    demais arquivos (CSS/JS) sejam servidos pelos seus caminhos. O *mount* é feito na
    raiz, **após** os roteadores ``/api/*`` já registrados por ``create_app``, de modo
    que a API tem precedência sobre o conteúdo estático.
    """
    app.mount(
        "/",
        StaticFiles(directory=diretorio_dashboard, html=True),
        name="dashboard",
    )


def _montar_dashboard_indisponivel(app: FastAPI, resultado: ResultadoVerificacao) -> None:
    """Impede a disponibilização do Dashboard e expõe a causa em pt-BR (Req. 1.8).

    Quando a verificação de inicialização falha, o Dashboard **não** fica disponível
    normalmente: toda requisição que não seja de ``/api/*`` recebe HTTP 503 com uma
    mensagem em pt-BR que identifica o recurso indisponível por nome e tipo. O
    endpoint ``GET /api/saude`` continua respondendo (com ``saudavel=False``) para
    inspeção da causa.
    """

    @app.get("/", include_in_schema=False)
    @app.get("/{caminho:path}", include_in_schema=False)
    def dashboard_indisponivel(request: Request, caminho: str = "") -> JSONResponse:
        # Deixa a API responder normalmente; apenas o conteúdo estático é bloqueado.
        if request.url.path.startswith("/api"):
            return JSONResponse(
                status_code=404,
                content={"detail": "Rota de API não encontrada."},
            )
        return JSONResponse(
            status_code=503,
            content={
                "detail": (
                    "O Dashboard está indisponível porque a verificação de "
                    "inicialização dos recursos AWS falhou. "
                    + resultado.mensagem_pt_br
                ),
                "recurso_indisponivel": (
                    {
                        "nome": resultado.recurso_indisponivel.nome,
                        "tipo": resultado.recurso_indisponivel.tipo.value,
                    }
                    if resultado.recurso_indisponivel is not None
                    else None
                ),
            },
        )


# ---------------------------------------------------------------------------
# Configuração de autenticação a partir de variáveis de ambiente (Req. 12.5, 12.7)
# ---------------------------------------------------------------------------
def _str_para_booleano(valor: Optional[str]) -> bool:
    """Interpreta uma env textual como booleano (``"true"``/``"1"`` → ``True``)."""
    if valor is None:
        return False
    return valor.strip().lower() in {"true", "1", "sim", "yes"}


def _construir_controlador_autenticacao(
    ambiente: dict[str, str],
    *,
    emitir_aviso: Callable[[str], None],
) -> ControladorAutenticacao:
    """Constrói o :class:`ControladorAutenticacao` a partir do ambiente (Req. 12.7).

    Regras de configuração:

    - Se ``DASHBOARD_ORIGENS_AUTORIZADAS`` contém uma lista separada por vírgulas,
      cada origem (IP) é autorizada explicitamente.
    - Se o valor for ``"*"`` ou ``DASHBOARD_LIBERAR_TODAS_ORIGENS`` for verdadeiro
      (``"true"``/``"1"``), ativa o modo laboratório aberto
      (``permitir_todas_origens=True``), autorizando qualquer origem.
    - Se nenhuma origem é configurada e a liberação total não é solicitada, registra
      um aviso em pt-BR explicando que o login ficará bloqueado até configurar as
      envs. Não libera todas as origens silenciosamente (seria inseguro).
    """
    valor_origens = (ambiente.get(ENV_ORIGENS_AUTORIZADAS) or "").strip()
    liberar_todas_flag = _str_para_booleano(ambiente.get(ENV_LIBERAR_TODAS_ORIGENS))
    liberar_todas = liberar_todas_flag or valor_origens == "*"

    if liberar_todas:
        return ControladorAutenticacao(permitir_todas_origens=True)

    origens = [
        origem.strip() for origem in valor_origens.split(",") if origem.strip()
    ]

    if not origens:
        emitir_aviso(
            "Aviso de segurança: nenhuma origem foi autorizada para o Dashboard. "
            f"O login ficará bloqueado até configurar a variável de ambiente "
            f"'{ENV_ORIGENS_AUTORIZADAS}' (lista de IPs separados por vírgula) ou "
            f"'{ENV_LIBERAR_TODAS_ORIGENS}=true' (modo laboratório aberto)."
        )

    return ControladorAutenticacao(origens)


def _construir_credenciais_dashboard(ambiente: dict[str, str]) -> CredenciaisDashboard:
    """Monta as :class:`CredenciaisDashboard` a partir do ambiente (Req. 12.5).

    Usa ``DASHBOARD_USUARIO``/``DASHBOARD_SENHA`` quando definidas; caso contrário,
    mantém o par de laboratório padrão para não quebrar o uso local. Nenhum segredo
    é fixado no código em produção: os valores vêm do ambiente.
    """
    usuario = ambiente.get(ENV_USUARIO) or USUARIO_PADRAO_LABORATORIO
    senha = ambiente.get(ENV_SENHA) or SENHA_PADRAO_LABORATORIO
    return CredenciaisDashboard(usuario=usuario, senha=senha)


# ---------------------------------------------------------------------------
# Composição da aplicação
# ---------------------------------------------------------------------------
def compor_aplicacao(
    *,
    gateway: Optional[SqsGateway] = None,
    verificador_tabela: Optional[VerificadorTabela] = None,
    historico: Optional[HistoricoRepository] = None,
    estado_instancias: Optional[EstadoInstanciasConsumidores] = None,
    diretorio_dashboard: str = DIRETORIO_DASHBOARD_PADRAO,
    iniciar_consumidores: bool = True,
    fabrica_gerenciador: Optional[
        Callable[[SqsGateway, HistoricoRepository, EstadoInstanciasConsumidores], GerenciadorConsumidores]
    ] = None,
    fabrica_monitor_dlq: Optional[
        Callable[[SqsGateway, HistoricoRepository], GerenciadorMonitorDlq]
    ] = None,
    **kwargs: Any,
) -> FastAPI:
    """Compõe a aplicação completa da EC2 (API + Dashboard + consumidores).

    Fluxo da composição:

    1. Cria a API_Backend com :func:`app.api.app.create_app`, injetando as mesmas
       dependências (``gateway``, ``verificador_tabela``, ``historico`` etc.). Em
       produção, deixe-as como ``None`` para que a fábrica crie os recursos boto3
       reais (credenciais pela IAM Role da EC2 — Req. 12.1, 12.2).
    2. Executa a **verificação de inicialização** dos recursos AWS em modo não
       interruptivo (Req. 1.6). O resultado condiciona os passos seguintes.
    3. Se a verificação **tiver sucesso**: serve o Dashboard estático (Req. 8.1) e
       agenda o início dos consumidores Standard e FIFO no *startup* da aplicação
       (Req. 8.1, 8.2, 10.1, 10.2). Se **falhar**: não serve o Dashboard normalmente
       — expõe a causa em pt-BR (HTTP 503) — e não inicia os consumidores (Req. 1.8).

    Args:
        gateway, verificador_tabela, historico, estado_instancias: Dependências
            injetáveis repassadas a ``create_app`` (e aos consumidores). Em testes,
            injete fakes para evitar AWS real.
        diretorio_dashboard: Caminho do Dashboard estático (padrão: ``web/``).
        iniciar_consumidores: Quando ``False``, não inicia as threads dos consumidores
            mesmo com a verificação bem-sucedida (útil em testes de composição que não
            querem threads vivas). O Dashboard continua sendo servido.
        fabrica_gerenciador: Fábrica opcional do :class:`GerenciadorConsumidores`
            (injetável nos testes). Por padrão, usa a implementação real.
        fabrica_monitor_dlq: Fábrica opcional do :class:`GerenciadorMonitorDlq`
            (injetável nos testes). Por padrão, usa a implementação real. O monitor
            de DLQ só é iniciado quando a verificação tem sucesso e
            ``iniciar_consumidores`` é ``True``, seguindo a mesma condição dos
            consumidores.
        **kwargs: Repassados a ``create_app`` (ex.: ``estado_simulacao``,
            ``controlador_autenticacao``, ``credenciais_dashboard``,
            ``cliente_dynamodb``).

    Returns:
        A instância :class:`FastAPI` composta, com o resultado da verificação em
        ``app.state.resultado_verificacao`` e, quando aplicável, o
        :class:`GerenciadorConsumidores` em ``app.state.gerenciador_consumidores``.
    """
    estado_instancias = estado_instancias or EstadoInstanciasConsumidores()

    # Configura a autenticação a partir das variáveis de ambiente quando o chamador
    # (ex.: testes) não injeta explicitamente o controlador/credenciais. Assim, em
    # produção o login funciona com origens autorizadas via env, sem segredos no
    # código (Req. 12.5, 12.7). A injeção por kwargs tem precedência e não é tocada.
    if "controlador_autenticacao" not in kwargs:
        kwargs["controlador_autenticacao"] = _construir_controlador_autenticacao(
            dict(os.environ),
            emitir_aviso=lambda mensagem: print(mensagem, file=sys.stderr),
        )
    if "credenciais_dashboard" not in kwargs:
        kwargs["credenciais_dashboard"] = _construir_credenciais_dashboard(
            dict(os.environ)
        )

    app = create_app(
        gateway=gateway,
        verificador_tabela=verificador_tabela,
        historico=historico,
        estado_instancias=estado_instancias,
        **kwargs,
    )

    # Dependências efetivas resolvidas por create_app (reais em produção).
    gateway_efetivo: SqsGateway = app.state.gateway
    historico_efetivo: HistoricoRepository = app.state.historico
    verificador_efetivo: VerificadorTabela = app.state.verificador_tabela

    # Passo 2: verificação de inicialização em modo não interruptivo (Req. 1.6).
    resultado = verificar_inicializacao(
        gateway_efetivo,
        verificador_efetivo,
        interromper_em_falha=False,
    )
    app.state.resultado_verificacao = resultado
    app.state.gerenciador_consumidores = None
    app.state.gerenciador_monitor_dlq = None

    if not resultado.sucesso:
        # Passo 3b: verificação falhou — Dashboard indisponível, sem consumidores
        # nem monitor de DLQ (todos dependem dos mesmos recursos AWS).
        _montar_dashboard_indisponivel(app, resultado)
        return app

    # Passo 3a: verificação bem-sucedida — serve o Dashboard e inicia os consumidores
    # e o monitor de DLQ (ambos condicionados à verificação OK).
    if iniciar_consumidores:
        criar = fabrica_gerenciador or (
            lambda g, h, e: GerenciadorConsumidores(
                gateway=g, historico=h, estado_instancias=e
            )
        )
        gerenciador = criar(gateway_efetivo, historico_efetivo, estado_instancias)
        app.state.gerenciador_consumidores = gerenciador

        # Monitor de DLQ: observa as DLQs e registra `encaminhada_dlq` (OBSERVADO)
        # de forma idempotente, para que Histórico e Monitoramento reflitam o
        # encaminhamento (Req. 6.6, 7.9, 7.10).
        criar_monitor = fabrica_monitor_dlq or (
            lambda g, h: GerenciadorMonitorDlq(
                monitor=MonitorDlq(sqs=g, historico=h)
            )
        )
        gerenciador_monitor = criar_monitor(gateway_efetivo, historico_efetivo)
        app.state.gerenciador_monitor_dlq = gerenciador_monitor

        @app.on_event("startup")
        def _iniciar_processos_fundo() -> None:
            gerenciador.iniciar()
            gerenciador_monitor.iniciar()

        @app.on_event("shutdown")
        def _parar_processos_fundo() -> None:
            gerenciador.parar()
            gerenciador_monitor.parar()

    # O mount dos estáticos vem por último, para a API ter precedência sobre ``/``.
    _montar_dashboard_disponivel(app, diretorio_dashboard)
    return app


def criar_app_padrao() -> FastAPI:
    """Cria a aplicação padrão (produção) para servir via servidor ASGI.

    As dependências ficam como ``None`` e são criadas por ``create_app`` com
    credenciais pela IAM Role da EC2 (sem Access Keys — Req. 12.1, 12.2). Use esta
    fábrica com o modo *factory* do servidor ASGI, ou acesse o atributo ``app`` deste
    módulo (resolvido sob demanda — ver :func:`__getattr__`).
    """
    return compor_aplicacao()


def __getattr__(nome: str) -> FastAPI:
    """Resolve ``app.main.app`` sob demanda (PEP 562), sem avaliar no import.

    Permite iniciar com ``uvicorn app.main:app`` enquanto mantém o import do módulo
    livre de efeitos colaterais: a aplicação padrão (que cria clientes boto3 reais) só
    é construída quando o servidor ASGI realmente acessa o atributo ``app``. Assim, os
    testes podem importar :func:`compor_aplicacao` e injetar fakes sem acionar a AWS.
    """
    if nome == "app":
        aplicacao = criar_app_padrao()
        globals()["app"] = aplicacao  # memoiza para acessos subsequentes.
        return aplicacao
    raise AttributeError(f"module {__name__!r} has no attribute {nome!r}")
