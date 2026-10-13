"""Aplicação FastAPI (API_Backend) do laboratório Amazon SQS.

Este módulo estabelece a **fundação** da API_Backend sobre a qual os demais
endpoints (Tarefas 15.2, 15.5, 15.6) serão adicionados de forma coesa. A função
:func:`create_app` constrói a instância FastAPI, injeta as dependências
(``SqsGateway``, verificador da Tabela_Historico e o estado de Config_Simulacao)
em ``app.state`` e registra os roteadores.

A Tarefa 15.1 implementa três endpoints:

- ``GET /api/saude`` — verificação de inicialização dos recursos AWS, reutilizando
  :mod:`app.verificacao_inicializacao` (Req. 1.6, 1.8).
- ``GET /api/config`` — retorna Config_Aplicada e Config_Simulacao com rótulos
  textuais distintos (Req. 2.1-2.13).
- ``PUT /api/config/simulacao`` — ajusta parâmetros de Config_Simulacao dentro das
  faixas, reutilizando o validador de faixa (Req. 2.11, 2.12).

Decisões de design relevantes:

- **Injeção de dependências via fábrica.** ``create_app`` recebe ``gateway``,
  ``verificador_tabela`` e ``estado_simulacao`` opcionais. Em produção, cria o
  ``SqsGateway`` e o verificador boto3 reais (credenciais pela IAM Role da EC2,
  sem Access Keys — Req. 12.1, 12.2). Em testes, injeta fakes/mocks, de modo que
  os testes não dependam da AWS real.
- **Extensibilidade.** As rotas são agrupadas em ``APIRouter`` por área. Novos
  endpoints acrescentam roteadores ou rotas sem reescrever a fundação.
- **pt-BR em todas as mensagens ao usuário.** Mensagens, rótulos e descrições são
  em português do Brasil; nomes técnicos permanecem em inglês quando apropriado.

Referências de requisitos: 1.6, 1.8, 2.1-2.7, 2.10, 2.11, 2.12, 2.13, 12.1, 12.2.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, FastAPI, Request

from fastapi import HTTPException

from app.acesso.historico_repository import HistoricoRepository
from app.acesso.sqs_gateway import SqsGateway
from app.api.esquemas import (
    AjusteConfigSimulacao,
    ParametroConfig,
    RecursoSaude,
    RequisicaoAutenticacao,
    RequisicaoCenario,
    RequisicaoEnvioMensagens,
    RequisicaoInstancias,
    RequisicaoReprocessarDlq,
    RespostaAjusteConfigSimulacao,
    RespostaAutenticacao,
    RespostaCenario,
    RespostaConfig,
    RespostaEnvioMensagens,
    RespostaEstatisticas,
    RespostaHistorico,
    RespostaInstancias,
    RespostaMonitoramento,
    RespostaReprocessarDlq,
    RespostaSaude,
    RespostaSaudeConsumidores,
    ResultadoAjusteParametro,
    ROTULO_CONFIG_APLICADA,
    ROTULO_CONFIG_SIMULACAO,
)
from app.api.estado_instancias import (
    EstadoInstanciasConsumidores,
    normalizar_tipo_consumidor,
)
from app.api.estado_simulacao import EstadoConfigSimulacao
from app.api.servico_autenticacao import CredenciaisDashboard, autenticar
from app.api.servico_envio import enviar_mensagens
from app.api.servico_historico import obter_historico
from app.api.servico_laboratorio import executar_cenario, reprocessar_dlq
from app.api.servico_monitoramento import (
    obter_estatisticas,
    obter_monitoramento,
)
from app.api.servico_saude_consumidores import obter_saude_consumidores
from app.dominio import constantes as c
from app.seguranca.autenticacao import ControladorAutenticacao
from app.verificacao_inicializacao import (
    VerificadorTabela,
    descrever_tabela_boto3,
    verificar_inicializacao,
)

# Orientação em pt-BR exibida sempre que o usuário consulta a configuração ou tenta
# ajustar um parâmetro de Config_Aplicada: ela só muda no Console AWS (Req. 2.13).
ORIENTACAO_CONFIG_APLICADA: str = (
    "Os parâmetros de Config_Aplicada refletem o que está configurado nos recursos "
    "SQS e só podem ser alterados manualmente no Console AWS (região "
    f"{c.REGIAO_AWS}). A aplicação não cria, modifica nem exclui recursos AWS."
)


# ---------------------------------------------------------------------------
# Helpers de acesso às dependências em app.state
# ---------------------------------------------------------------------------
def _gateway(request: Request) -> SqsGateway:
    return request.app.state.gateway


def _verificador_tabela(request: Request) -> VerificadorTabela:
    return request.app.state.verificador_tabela


def _estado_simulacao(request: Request) -> EstadoConfigSimulacao:
    return request.app.state.estado_simulacao


def _historico(request: Request) -> HistoricoRepository:
    return request.app.state.historico


def _estado_instancias(request: Request) -> EstadoInstanciasConsumidores:
    return request.app.state.estado_instancias


def _controlador_autenticacao(request: Request) -> ControladorAutenticacao:
    return request.app.state.controlador_autenticacao


def _credenciais_dashboard(request: Request) -> CredenciaisDashboard:
    return request.app.state.credenciais_dashboard


def _gerenciador_consumidores(request: Request) -> Optional[object]:
    # Definido na composição da aplicação (app.main.compor_aplicacao); pode ser None
    # quando os consumidores não foram iniciados ou nem o atributo existe.
    return getattr(request.app.state, "gerenciador_consumidores", None)


# ---------------------------------------------------------------------------
# Montagem das respostas de configuração
# ---------------------------------------------------------------------------
def _parametros_config_aplicada() -> list[ParametroConfig]:
    """Monta a lista de parâmetros de Config_Aplicada (Req. 2.1-2.7, 2.10, 2.13).

    Todos recebem o rótulo distinto ``Config_Aplicada``, ``ajustavel=False`` e a
    orientação de que a mudança só ocorre no Console AWS.
    """

    def aplicada(nome: str, valor: object, unidade: Optional[str] = None) -> ParametroConfig:
        return ParametroConfig(
            nome=nome,
            valor=valor,
            unidade=unidade,
            rotulo=ROTULO_CONFIG_APLICADA,
            ajustavel=False,
            orientacao=ORIENTACAO_CONFIG_APLICADA,
        )

    return [
        aplicada("Visibility Timeout", c.VISIBILITY_TIMEOUT_SEGUNDOS, "segundos"),
        aplicada("maxReceiveCount da Redrive Policy", c.MAX_RECEIVE_COUNT),
        aplicada(
            "Retenção das filas principais", c.RETENCAO_FILA_PRINCIPAL_DIAS, "dias"
        ),
        aplicada("Retenção das DLQs", c.RETENCAO_DLQ_DIAS, "dias"),
        aplicada("Long Polling (WaitTimeSeconds)", c.LONG_POLLING_SEGUNDOS, "segundos"),
        aplicada("Criptografia em repouso (SSE-SQS)", "Habilitada em todas as filas"),
        aplicada(
            "Deduplicação por conteúdo (filas FIFO)", "Habilitada nas filas FIFO"
        ),
    ]


def _parametros_config_simulacao(estado: EstadoConfigSimulacao) -> list[ParametroConfig]:
    """Monta a lista de parâmetros de Config_Simulacao (Req. 2.8, 2.9, 2.10, 2.11).

    Todos recebem o rótulo distinto ``Config_Simulacao``, ``ajustavel=True`` e a
    faixa válida que o Dashboard deve respeitar.
    """
    valores = estado.valores()

    def simulacao(
        nome: str, valor: int, minimo: int, maximo: int, unidade: Optional[str]
    ) -> ParametroConfig:
        return ParametroConfig(
            nome=nome,
            valor=valor,
            unidade=unidade,
            rotulo=ROTULO_CONFIG_SIMULACAO,
            ajustavel=True,
            faixa_minima=minimo,
            faixa_maxima=maximo,
        )

    return [
        simulacao(
            "Tempo de processamento simulado",
            valores.tempo_processamento_simulado_segundos,
            c.TEMPO_PROCESSAMENTO_SIMULADO_MIN,
            c.TEMPO_PROCESSAMENTO_SIMULADO_MAX,
            "segundos",
        ),
        simulacao(
            "Quantidade de mensagens por envio",
            valores.quantidade_mensagens,
            c.QUANTIDADE_MENSAGENS_MIN,
            c.QUANTIDADE_MENSAGENS_MAX,
            "mensagens",
        ),
        simulacao(
            "Intervalo de atualização",
            valores.intervalo_atualizacao_segundos,
            c.INTERVALO_ATUALIZACAO_MIN,
            c.INTERVALO_ATUALIZACAO_MAX,
            "segundos",
        ),
    ]


# ---------------------------------------------------------------------------
# Roteador de configuração e saúde (Tarefa 15.1)
# ---------------------------------------------------------------------------
def criar_roteador_config_saude() -> APIRouter:
    """Cria o APIRouter com os endpoints de saúde e configuração (Tarefa 15.1)."""
    roteador = APIRouter(prefix="/api", tags=["config-saude"])

    @roteador.get("/saude", response_model=RespostaSaude, summary="Verificação de inicialização")
    def obter_saude(request: Request) -> RespostaSaude:
        """Verifica a existência e a acessibilidade dos recursos AWS (Req. 1.6, 1.8).

        Reutiliza ``verificar_inicializacao`` em modo não interruptivo para sempre
        responder (com ``saudavel=False`` quando algum recurso está indisponível),
        preservando o erro original da AWS no detalhe de cada recurso.
        """
        resultado = verificar_inicializacao(
            _gateway(request),
            _verificador_tabela(request),
            interromper_em_falha=False,
        )
        recursos = [
            RecursoSaude(
                nome=r.nome,
                tipo=r.tipo.value,
                disponivel=r.disponivel,
                detalhe_erro=(repr(r.erro_original) if r.erro_original else None),
            )
            for r in resultado.recursos_verificados
        ]
        return RespostaSaude(
            saudavel=resultado.sucesso,
            mensagem=resultado.mensagem_pt_br,
            recursos=recursos,
        )

    @roteador.get("/config", response_model=RespostaConfig, summary="Configuração aplicada e de simulação")
    def obter_config(request: Request) -> RespostaConfig:
        """Retorna Config_Aplicada e Config_Simulacao com rótulos distintos (Req. 2.1-2.13)."""
        return RespostaConfig(
            config_aplicada=_parametros_config_aplicada(),
            config_simulacao=_parametros_config_simulacao(_estado_simulacao(request)),
            orientacao_config_aplicada=ORIENTACAO_CONFIG_APLICADA,
        )

    @roteador.put(
        "/config/simulacao",
        response_model=RespostaAjusteConfigSimulacao,
        summary="Ajuste de parâmetros de Config_Simulacao",
    )
    def ajustar_config_simulacao(
        ajuste: AjusteConfigSimulacao, request: Request
    ) -> RespostaAjusteConfigSimulacao:
        """Ajusta parâmetros de Config_Simulacao dentro das faixas (Req. 2.11, 2.12).

        Cada parâmetro informado é validado pela faixa correspondente: aceito só
        se estiver dentro do intervalo; ao rejeitar, o valor anterior é preservado
        e a mensagem em pt-BR indica a faixa válida. Parâmetros não informados não
        são alterados.
        """
        estado = _estado_simulacao(request)
        ajustes: list[ResultadoAjusteParametro] = []

        if ajuste.tempo_processamento_simulado_segundos is not None:
            resultado = estado.ajustar_tempo_processamento(
                ajuste.tempo_processamento_simulado_segundos
            )
            ajustes.append(
                ResultadoAjusteParametro(
                    parametro="Tempo de processamento simulado",
                    aceito=resultado.aceito,
                    valor_efetivo=resultado.valor_efetivo,
                    mensagem=resultado.mensagem,
                )
            )

        if ajuste.quantidade_mensagens is not None:
            resultado = estado.ajustar_quantidade_mensagens(ajuste.quantidade_mensagens)
            ajustes.append(
                ResultadoAjusteParametro(
                    parametro="Quantidade de mensagens por envio",
                    aceito=resultado.aceito,
                    valor_efetivo=resultado.valor_efetivo,
                    mensagem=resultado.mensagem,
                )
            )

        if ajuste.intervalo_atualizacao_segundos is not None:
            resultado = estado.ajustar_intervalo_atualizacao(
                ajuste.intervalo_atualizacao_segundos
            )
            ajustes.append(
                ResultadoAjusteParametro(
                    parametro="Intervalo de atualização",
                    aceito=resultado.aceito,
                    valor_efetivo=resultado.valor_efetivo,
                    mensagem=resultado.mensagem,
                )
            )

        return RespostaAjusteConfigSimulacao(
            aceito=all(a.aceito for a in ajustes),
            ajustes=ajustes,
            config_simulacao=_parametros_config_simulacao(estado),
        )

    return roteador


# ---------------------------------------------------------------------------
# Roteador de envio de mensagens (Tarefa 15.2)
# ---------------------------------------------------------------------------
def criar_roteador_mensagens() -> APIRouter:
    """Cria o APIRouter com o endpoint de envio de mensagens (Tarefa 15.2)."""
    roteador = APIRouter(prefix="/api", tags=["mensagens"])

    @roteador.post(
        "/mensagens",
        response_model=RespostaEnvioMensagens,
        summary="Envio de 1 a 10 mensagens à fila selecionada",
    )
    def enviar(
        requisicao: RequisicaoEnvioMensagens, request: Request
    ) -> RespostaEnvioMensagens:
        """Envia um lote de 1 a 10 mensagens à fila selecionada (Req. 4.1-4.12).

        Valida o payload (rejeitando com os campos inválidos em pt-BR quando algum
        campo está fora das faixas — Req. 4.9), envia as mensagens válidas via
        ``SqsGateway`` (Req. 4.10) e registra exatamente um Evento_Observado
        ``enviada`` por mensagem enviada (Req. 4.11). Em falha parcial, preserva os
        envios anteriores e o erro original da AWS (Req. 4.12). A lógica fica em
        :func:`app.api.servico_envio.enviar_mensagens` para manter a rota fina.
        """
        return enviar_mensagens(
            requisicao,
            gateway=_gateway(request),
            historico=_historico(request),
            estado_simulacao=_estado_simulacao(request),
        )

    return roteador


# ---------------------------------------------------------------------------
# Roteador de estatísticas e monitoramento (Tarefa 15.5)
# ---------------------------------------------------------------------------
def criar_roteador_estatisticas_monitoramento() -> APIRouter:
    """Cria o APIRouter com os endpoints de estatísticas e monitoramento (Tarefa 15.5).

    Ambos derivam o Estado das mensagens a partir do log de eventos da
    Tabela_Historico (Req. 5.9, 5.10) e aplicam um timeout de 10 s, retornando uma
    indicação clara de timeout ao exceder (Req. 14.1-14.4). A lógica fica em
    :mod:`app.api.servico_monitoramento` para manter as rotas finas.
    """
    roteador = APIRouter(prefix="/api", tags=["estatisticas-monitoramento"])

    @roteador.get(
        "/estatisticas",
        response_model=RespostaEstatisticas,
        summary="Estatísticas agregadas por fluxo (Standard/FIFO)",
    )
    def obter_estatisticas_endpoint(request: Request) -> RespostaEstatisticas:
        """Retorna as estatísticas agregadas por fluxo (Req. 3.1-3.12, 14.1, 14.3).

        Deriva os contadores (enviadas, disponíveis, em processamento, processadas,
        com falha, encaminhadas para DLQ e recebidas por consumidor) separados e
        rotulados por fluxo. Ao exceder o limite de 10 s, devolve ``timeout=True`` e
        uma mensagem em pt-BR, sem erro bruto.
        """
        return obter_estatisticas(_historico(request))

    @roteador.get(
        "/monitoramento",
        response_model=RespostaMonitoramento,
        summary="Tabela de monitoramento com o Estado derivado por mensagem",
    )
    def obter_monitoramento_endpoint(request: Request) -> RespostaMonitoramento:
        """Retorna a tabela de monitoramento (Req. 5.1-5.10, 14.2, 14.4).

        Uma linha por mensagem com as colunas ID, Fila, Consumidor, Tentativas,
        Processamento, Estado e Último evento; o Estado é derivado dos eventos. Sem
        mensagens registradas, devolve ``vazio=True`` com indicação em pt-BR
        (Req. 5.8). Ao exceder o limite de 10 s, devolve ``timeout=True``.
        """
        return obter_monitoramento(_historico(request))

    return roteador


# ---------------------------------------------------------------------------
# Roteador de histórico detalhado (Tarefa 15.6)
# ---------------------------------------------------------------------------
def criar_roteador_historico() -> APIRouter:
    """Cria o APIRouter com o endpoint de histórico detalhado (Tarefa 15.6)."""
    roteador = APIRouter(prefix="/api", tags=["historico"])

    @roteador.get(
        "/mensagens/{order_id}/historico",
        response_model=RespostaHistorico,
        summary="Linha do tempo cronológica dos eventos de uma mensagem",
    )
    def obter_historico_endpoint(order_id: str, request: Request) -> RespostaHistorico:
        """Retorna a linha do tempo cronológica de uma mensagem (Req. 6.1-6.8).

        Lista os eventos em ordem cronológica crescente (desempate estável), cada um
        com o indicador Observado/Inferido e o horário com fuso (Req. 6.1, 6.3, 6.4).
        Mensagem sem eventos devolve indicação em pt-BR (Req. 6.8); falha de consulta
        devolve indicação de histórico indisponível (Req. 6.7). A lógica fica em
        :func:`app.api.servico_historico.obter_historico` para manter a rota fina.
        """
        return obter_historico(_historico(request), order_id)

    return roteador


# ---------------------------------------------------------------------------
# Roteador do laboratório de falhas (Tarefa 15.6)
# ---------------------------------------------------------------------------
def criar_roteador_laboratorio() -> APIRouter:
    """Cria o APIRouter com os endpoints do laboratório de falhas (Tarefa 15.6)."""
    roteador = APIRouter(prefix="/api/laboratorio", tags=["laboratorio"])

    @roteador.post(
        "/cenario",
        response_model=RespostaCenario,
        summary="Executa um cenário de falha controlado",
    )
    def executar_cenario_endpoint(
        requisicao: RequisicaoCenario, request: Request
    ) -> RespostaCenario:
        """Dispara um cenário de falha controlado (Req. 7.1-7.11).

        Interrompe com causa em pt-BR quando a fila está indisponível ou a
        autenticação/autorização falha (Req. 7.11). A lógica fica em
        :func:`app.api.servico_laboratorio.executar_cenario`.
        """
        return executar_cenario(
            requisicao,
            gateway=_gateway(request),
            historico=_historico(request),
            estado_simulacao=_estado_simulacao(request),
            controlador_autenticacao=_controlador_autenticacao(request),
        )

    @roteador.post(
        "/reprocessar-dlq",
        response_model=RespostaReprocessarDlq,
        summary="Move mensagens da DLQ de volta à fila principal",
    )
    def reprocessar_dlq_endpoint(
        requisicao: RequisicaoReprocessarDlq, request: Request
    ) -> RespostaReprocessarDlq:
        """Reprocessa a DLQ do fluxo informado (Req. 7.7, 7.11).

        Interrompe com causa em pt-BR quando a fila está indisponível ou a
        autenticação/autorização falha (Req. 7.11). A lógica fica em
        :func:`app.api.servico_laboratorio.reprocessar_dlq`.
        """
        return reprocessar_dlq(
            requisicao,
            gateway=_gateway(request),
            controlador_autenticacao=_controlador_autenticacao(request),
        )

    return roteador


# ---------------------------------------------------------------------------
# Roteador de ajuste de instâncias de consumidores (Tarefa 15.6)
# ---------------------------------------------------------------------------
def criar_roteador_consumidores() -> APIRouter:
    """Cria o APIRouter com o endpoint de ajuste de instâncias (Tarefa 15.6)."""
    roteador = APIRouter(prefix="/api/consumidores", tags=["consumidores"])

    # Rota literal registrada ANTES da rota variável '/{tipo}/instancias' para que
    # 'saude' não seja capturado como um valor de {tipo}. Como os caminhos são
    # distintos ('/saude' vs '/{tipo}/instancias'), não há conflito de rota.
    @roteador.get(
        "/saude",
        response_model=RespostaSaudeConsumidores,
        summary="Saúde dos consumidores supervisionados (sem tocar na AWS)",
    )
    def obter_saude_consumidores_endpoint(request: Request) -> RespostaSaudeConsumidores:
        """Retorna a saúde dos consumidores lida do estado em memória (frente D).

        Reflete, por slot, se a thread está viva, se o slot está degradado e quantos
        reinícios ocorreram, além dos agregados ``algum_morto``/``algum_degradado``.
        Quando os consumidores não foram iniciados (gerenciador ``None``), devolve
        ``iniciados=False`` e lista vazia. Nunca chama a AWS e nunca retorna 500.
        """
        return obter_saude_consumidores(_gerenciador_consumidores(request))

    @roteador.put(
        "/{tipo}/instancias",
        response_model=RespostaInstancias,
        summary="Ajusta o número de instâncias (1 a 10) de um tipo de consumidor",
    )
    def ajustar_instancias_endpoint(
        tipo: str, requisicao: RequisicaoInstancias, request: Request
    ) -> RespostaInstancias:
        """Ajusta o número de instâncias do consumidor dentro de [1, 10] (Req. 10.1-10.3).

        Quando fora da faixa, rejeita o ajuste, preserva o número anterior e indica o
        intervalo válido em pt-BR (Req. 10.3). Tipo diferente de ``standard``/``fifo``
        resulta em 404 com mensagem em pt-BR.
        """
        tipo_normalizado = normalizar_tipo_consumidor(tipo)
        if tipo_normalizado is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Tipo de consumidor inválido: '{tipo}'. Use 'standard' ou "
                    "'fifo'."
                ),
            )
        resultado = _estado_instancias(request).ajustar(
            tipo_normalizado, requisicao.instancias
        )
        return RespostaInstancias(
            tipo=tipo_normalizado,
            aceito=resultado.aceito,
            instancias=resultado.valor_efetivo,
            mensagem=resultado.mensagem,
        )

    return roteador


# ---------------------------------------------------------------------------
# Roteador de autenticação do Dashboard (Tarefa 15.6)
# ---------------------------------------------------------------------------
def criar_roteador_autenticacao() -> APIRouter:
    """Cria o APIRouter com o endpoint de autenticação do Dashboard (Tarefa 15.6)."""
    roteador = APIRouter(prefix="/api", tags=["autenticacao"])

    @roteador.post(
        "/autenticacao",
        response_model=RespostaAutenticacao,
        summary="Login do Dashboard com bloqueio por origem após 5 falhas",
    )
    def autenticar_endpoint(
        requisicao: RequisicaoAutenticacao, request: Request
    ) -> RespostaAutenticacao:
        """Avalia o login do Dashboard (Req. 12.5, 12.6, 12.7).

        Bloqueia a origem após 5 falhas consecutivas por 300 s; um sucesso antes do
        limite reinicia a contagem. A lógica fica em
        :func:`app.api.servico_autenticacao.autenticar`.
        """
        origem_requisicao = request.client.host if request.client else "desconhecida"
        return autenticar(
            requisicao,
            controlador=_controlador_autenticacao(request),
            credenciais=_credenciais_dashboard(request),
            origem_requisicao=origem_requisicao,
        )

    return roteador


# ---------------------------------------------------------------------------
# Fábrica da aplicação
# ---------------------------------------------------------------------------
def create_app(
    *,
    gateway: Optional[SqsGateway] = None,
    verificador_tabela: Optional[VerificadorTabela] = None,
    estado_simulacao: Optional[EstadoConfigSimulacao] = None,
    historico: Optional[HistoricoRepository] = None,
    estado_instancias: Optional[EstadoInstanciasConsumidores] = None,
    controlador_autenticacao: Optional[ControladorAutenticacao] = None,
    credenciais_dashboard: Optional[CredenciaisDashboard] = None,
    cliente_dynamodb: Optional[Any] = None,
) -> FastAPI:
    """Constrói e configura a aplicação FastAPI (API_Backend).

    Esta é a fundação sobre a qual os endpoints das Tarefas 15.2, 15.5 e 15.6 serão
    adicionados. As dependências são injetáveis para permitir testes sem AWS real.

    Args:
        gateway: :class:`SqsGateway` a usar. Em produção, deixe ``None`` para que a
            fábrica crie um gateway padrão (credenciais pela IAM Role da EC2, sem
            Access Keys — Req. 12.1, 12.2).
        verificador_tabela: Callable que faz o *describe* da Tabela_Historico. Em
            produção, deixe ``None``; a fábrica cria um verificador boto3 a partir
            de ``cliente_dynamodb`` (ou de um cliente boto3 recém-criado).
        estado_simulacao: Estado de Config_Simulacao a usar. Se ``None``, cria um
            com os valores iniciais do laboratório.
        historico: :class:`HistoricoRepository` a usar para registrar os eventos de
            envio (Tarefa 15.2). Em testes, injete um fake em memória. Em produção,
            deixe ``None``; a fábrica cria um repositório boto3 a partir de
            ``cliente_dynamodb`` (ou de um recurso boto3 recém-criado), com
            credenciais pela IAM Role da EC2 (sem Access Keys — Req. 12.1, 12.2).
        cliente_dynamodb: Cliente boto3 do DynamoDB opcional, usado apenas para
            construir o ``verificador_tabela`` padrão quando este não é informado.

    Returns:
        Instância :class:`FastAPI` pronta para servir, com dependências em
        ``app.state``.
    """
    app = FastAPI(
        title="API_Backend — Laboratório Amazon SQS",
        description=(
            "API do laboratório didático de Amazon SQS (certificação AWS SAA). "
            "A aplicação apenas usa recursos AWS criados manualmente; nunca os "
            "cria, modifica ou exclui."
        ),
        version="0.1.0",
    )

    app.state.gateway = gateway if gateway is not None else SqsGateway()

    if verificador_tabela is not None:
        app.state.verificador_tabela = verificador_tabela
    else:
        cliente = cliente_dynamodb
        if cliente is None:
            import boto3  # import local para não exigir credenciais em import-time

            cliente = boto3.client("dynamodb", region_name=c.REGIAO_AWS)
        app.state.verificador_tabela = descrever_tabela_boto3(cliente)

    app.state.estado_simulacao = (
        estado_simulacao if estado_simulacao is not None else EstadoConfigSimulacao()
    )

    if historico is not None:
        app.state.historico = historico
    else:
        import boto3  # import local para não exigir credenciais em import-time

        # Credenciais pela IAM Role da EC2 (sem Access Keys — Req. 12.1, 12.2). O
        # recurso DynamoDB expõe .Table(nome), que o HistoricoRepository consome.
        recurso = boto3.resource("dynamodb", region_name=c.REGIAO_AWS)
        app.state.historico = HistoricoRepository(recurso.Table(c.TABELA_HISTORICO))

    # Estado em memória do número de instâncias por tipo de consumidor (Req. 10.1-10.3).
    app.state.estado_instancias = (
        estado_instancias
        if estado_instancias is not None
        else EstadoInstanciasConsumidores()
    )

    # Controlador de autenticação com bloqueio por origem (Req. 12.5, 12.6). Em
    # produção, deixe None para a fábrica criar um controlador padrão; as origens
    # autorizadas são configuradas na composição da aplicação (Tarefa 18.1).
    app.state.controlador_autenticacao = (
        controlador_autenticacao
        if controlador_autenticacao is not None
        else ControladorAutenticacao()
    )

    # Credenciais esperadas do Dashboard (Req. 12.5). Injetáveis para testes; em
    # produção vêm de uma fonte segura, nunca de Access Keys da AWS.
    app.state.credenciais_dashboard = (
        credenciais_dashboard
        if credenciais_dashboard is not None
        else CredenciaisDashboard(usuario="admin", senha="trocar-no-console")
    )

    app.include_router(criar_roteador_config_saude())
    app.include_router(criar_roteador_mensagens())
    app.include_router(criar_roteador_estatisticas_monitoramento())
    app.include_router(criar_roteador_historico())
    app.include_router(criar_roteador_laboratorio())
    app.include_router(criar_roteador_consumidores())
    app.include_router(criar_roteador_autenticacao())
    return app
