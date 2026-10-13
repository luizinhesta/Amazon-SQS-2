"""Esquemas (modelos Pydantic) de entrada e saída da API_Backend.

Centraliza os contratos HTTP dos endpoints de configuração e saúde (Tarefa 15.1).
Todos os campos e descrições estão em pt-BR, pois alimentam o Dashboard e a
documentação automática da API. Os nomes técnicos (atributos, siglas AWS) são
mantidos quando apropriado.

Referências de requisitos: 1.6, 1.8, 2.1-2.7, 2.10, 2.11, 2.12, 2.13.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Saúde / verificação de inicialização
# ---------------------------------------------------------------------------
class RecursoSaude(BaseModel):
    """Situação de um único recurso AWS na verificação de inicialização."""

    nome: str = Field(..., description="Nome do recurso (ex.: lab-sqs-standard).")
    tipo: str = Field(..., description="Tipo do recurso em pt-BR (fila SQS, tabela DynamoDB).")
    disponivel: bool = Field(..., description="Se o recurso existe e está acessível.")
    detalhe_erro: Optional[str] = Field(
        None,
        description="Erro original da AWS quando o recurso está indisponível.",
    )


class RespostaSaude(BaseModel):
    """Resultado da verificação de inicialização dos recursos AWS (Req. 1.6, 1.8)."""

    saudavel: bool = Field(
        ..., description="True somente se todos os recursos esperados estão disponíveis."
    )
    mensagem: str = Field(..., description="Mensagem em pt-BR descrevendo o resultado.")
    recursos: list[RecursoSaude] = Field(
        default_factory=list,
        description="Recursos verificados, na ordem de verificação.",
    )


# ---------------------------------------------------------------------------
# Configuração (Config_Aplicada e Config_Simulacao)
# ---------------------------------------------------------------------------
# Rótulos textuais distintos exigidos pelo Req. 2.10.
ROTULO_CONFIG_APLICADA: str = "Config_Aplicada"
ROTULO_CONFIG_SIMULACAO: str = "Config_Simulacao"


class ParametroConfig(BaseModel):
    """Um parâmetro de configuração com rótulo, valor e descrição em pt-BR.

    O ``rotulo`` identifica textualmente se o parâmetro é Config_Aplicada ou
    Config_Simulacao (Req. 2.10). ``ajustavel`` indica se pode ser alterado pelo
    Dashboard; para Config_Aplicada é sempre ``False`` e ``orientacao`` explica
    que a mudança só ocorre no Console AWS (Req. 2.13).
    """

    nome: str = Field(..., description="Nome do parâmetro em pt-BR.")
    valor: object = Field(..., description="Valor atual do parâmetro.")
    unidade: Optional[str] = Field(None, description="Unidade do valor, quando houver.")
    rotulo: str = Field(
        ..., description="Rótulo textual distinto: Config_Aplicada ou Config_Simulacao."
    )
    ajustavel: bool = Field(
        ..., description="Se o parâmetro pode ser ajustado pelo Dashboard."
    )
    faixa_minima: Optional[int] = Field(
        None, description="Limite inferior aceito, para parâmetros ajustáveis."
    )
    faixa_maxima: Optional[int] = Field(
        None, description="Limite superior aceito, para parâmetros ajustáveis."
    )
    orientacao: Optional[str] = Field(
        None,
        description="Orientação em pt-BR (ex.: alteração apenas no Console AWS).",
    )


class RespostaConfig(BaseModel):
    """Config_Aplicada e Config_Simulacao com rótulos distintos (Req. 2.1-2.13)."""

    config_aplicada: list[ParametroConfig] = Field(
        default_factory=list,
        description="Parâmetros efetivamente configurados nos recursos SQS.",
    )
    config_simulacao: list[ParametroConfig] = Field(
        default_factory=list,
        description="Parâmetros usados apenas pela lógica de simulação da aplicação.",
    )
    orientacao_config_aplicada: str = Field(
        ...,
        description=(
            "Orientação em pt-BR de que a Config_Aplicada só muda no Console AWS."
        ),
    )


class AjusteConfigSimulacao(BaseModel):
    """Payload de ajuste de parâmetros de Config_Simulacao (Req. 2.11, 2.12).

    Todos os campos são opcionais; apenas os informados são ajustados. Cada ajuste
    é validado pela faixa correspondente (Req. 2.12).
    """

    tempo_processamento_simulado_segundos: Optional[int] = Field(
        None, description="Tempo_Processamento_Simulado em segundos [0, 300]."
    )
    quantidade_mensagens: Optional[int] = Field(
        None, description="Quantidade de mensagens por envio [1, 10]."
    )
    intervalo_atualizacao_segundos: Optional[int] = Field(
        None, description="Intervalo de atualização periódica em segundos [5, 300]."
    )


class ResultadoAjusteParametro(BaseModel):
    """Resultado do ajuste de um único parâmetro de Config_Simulacao."""

    parametro: str = Field(..., description="Nome do parâmetro ajustado.")
    aceito: bool = Field(..., description="Se o valor informado foi aceito.")
    valor_efetivo: int = Field(
        ..., description="Valor em vigor (novo se aceito, anterior se rejeitado)."
    )
    mensagem: str = Field(
        "", description="Mensagem em pt-BR; indica a faixa válida quando rejeitado."
    )


class RespostaAjusteConfigSimulacao(BaseModel):
    """Resposta do PUT /api/config/simulacao com o resultado de cada ajuste."""

    aceito: bool = Field(
        ..., description="True somente se todos os parâmetros informados foram aceitos."
    )
    ajustes: list[ResultadoAjusteParametro] = Field(
        default_factory=list, description="Resultado de cada parâmetro informado."
    )
    config_simulacao: list[ParametroConfig] = Field(
        default_factory=list,
        description="Config_Simulacao resultante após os ajustes.",
    )


# ---------------------------------------------------------------------------
# Envio de mensagens (Tarefa 15.2)
# ---------------------------------------------------------------------------
# Rótulos de tipo de fila expostos na API, coerentes com ``TipoFila`` do validador
# de envio (app.dominio.validacao_envio). Mantidos como literais em inglês técnico
# ("standard"/"fifo") por serem nomes de recurso/configuração do SQS.
TIPO_FILA_STANDARD: str = "standard"
TIPO_FILA_FIFO: str = "fifo"


class ItemEnvioMensagem(BaseModel):
    """Dados de uma única mensagem a enviar dentro de um lote (Req. 4.3, 4.4, 4.5).

    Cada item identifica um pedido (``order_id``), o Tempo_Processamento_Simulado e
    o Cenario_Falha a embutir na mensagem; para filas FIFO, pode trazer o
    ``MessageGroupId`` e, quando a deduplicação por conteúdo está desabilitada, o
    ``MessageDeduplicationId``. A validação de faixas é feita pelo validador de
    envio (``app.dominio.validacao_envio``), não pelo Pydantic, para produzir
    mensagens de erro em pt-BR apontando o campo inválido (Req. 4.9).
    """

    order_id: str = Field(..., description="Identificador do pedido (1 a 80 caracteres).")
    tempo_processamento_simulado_segundos: Optional[int] = Field(
        None,
        description=(
            "Tempo_Processamento_Simulado desta mensagem em segundos [0, 300]. "
            "Quando omitido, usa o valor de Config_Simulacao."
        ),
    )
    cenario_falha: Optional[str] = Field(
        None,
        description=(
            "Cenario_Falha a embutir na mensagem (ex.: 'sucesso', 'encaminhar_dlq'). "
            "Quando omitido, usa 'sucesso'."
        ),
    )
    message_group_id: Optional[str] = Field(
        None, description="MessageGroupId (1 a 128 caracteres) — apenas filas FIFO."
    )
    message_deduplication_id: Optional[str] = Field(
        None,
        description=(
            "MessageDeduplicationId (1 a 128 caracteres) — exigido em FIFO quando a "
            "Deduplicacao_Por_Conteudo está desabilitada."
        ),
    )


class RequisicaoEnvioMensagens(BaseModel):
    """Payload do POST /api/mensagens (Req. 4.1, 4.2, 4.5, 4.6, 4.7).

    O lote contém de 1 a 10 itens, um por mensagem; a quantidade é derivada do
    tamanho da lista ``mensagens``. ``tipo_fila`` seleciona a fila de destino
    (Standard ou FIFO). ``deduplicacao_por_conteudo`` indica o modo de deduplicação
    da fila FIFO (habilitada por padrão, conforme Config_Aplicada — Req. 2.7).
    """

    tipo_fila: str = Field(
        ..., description="Fila de destino: 'standard' ou 'fifo' (Req. 4.1)."
    )
    mensagens: list[ItemEnvioMensagem] = Field(
        ..., description="Mensagens do lote, de 1 a 10 (Req. 4.2)."
    )
    deduplicacao_por_conteudo: bool = Field(
        True,
        description=(
            "Para filas FIFO, indica se a Deduplicacao_Por_Conteudo está habilitada. "
            "Quando False, o MessageDeduplicationId passa a ser obrigatório (Req. 4.7)."
        ),
    )


class CampoInvalido(BaseModel):
    """Descrição de um campo de envio reprovado na validação (Req. 4.9)."""

    campo: str = Field(..., description="Nome do campo inválido.")
    mensagem: str = Field(..., description="Mensagem em pt-BR explicando a rejeição.")
    indice_mensagem: Optional[int] = Field(
        None,
        description=(
            "Índice (base 0) da mensagem do lote com o campo inválido; nulo quando o "
            "campo é do lote como um todo (ex.: tipo da fila ou quantidade)."
        ),
    )


class ResultadoEnvioItem(BaseModel):
    """Resultado do envio de uma mensagem do lote (Req. 4.11, 4.12)."""

    indice: int = Field(..., description="Índice (base 0) da mensagem no lote.")
    order_id: str = Field(..., description="Identificador do pedido enviado.")
    enviada: bool = Field(..., description="Se a mensagem foi enviada com sucesso.")
    sqs_message_id: Optional[str] = Field(
        None, description="MessageId retornado pelo SQS quando enviada."
    )
    detalhe_erro: Optional[str] = Field(
        None,
        description="Erro original da AWS preservado quando o envio falhou (Req. 4.12).",
    )


class RespostaEnvioMensagens(BaseModel):
    """Resposta do POST /api/mensagens (Req. 4.9, 4.11, 4.12).

    Quando o payload é inválido, ``aceito`` é ``False``, ``campos_invalidos`` lista
    os campos reprovados em pt-BR e nenhum envio é realizado. Quando o payload é
    válido, ``resultados`` traz o desfecho de cada mensagem; em falha parcial, os
    itens anteriores permanecem ``enviada=True`` e o erro original é preservado no
    item que falhou (Req. 4.12).
    """

    aceito: bool = Field(
        ...,
        description="True se o payload foi aceito para envio (validação aprovada).",
    )
    mensagem: str = Field(..., description="Resumo em pt-BR do resultado da operação.")
    tipo_fila: str = Field(..., description="Fila de destino do envio.")
    nome_fila: Optional[str] = Field(
        None, description="Nome da fila SQS de destino, quando o envio é tentado."
    )
    total_solicitadas: int = Field(
        ..., description="Quantidade de mensagens do lote informado."
    )
    total_enviadas: int = Field(
        ..., description="Quantidade de mensagens efetivamente enviadas."
    )
    houve_falha: bool = Field(
        ..., description="True se ao menos uma mensagem do lote falhou ao enviar."
    )
    campos_invalidos: list[CampoInvalido] = Field(
        default_factory=list,
        description="Campos reprovados na validação (vazio quando aceito).",
    )
    resultados: list[ResultadoEnvioItem] = Field(
        default_factory=list,
        description="Desfecho de cada mensagem do lote (vazio quando rejeitado).",
    )


# ---------------------------------------------------------------------------
# Estatísticas e monitoramento (Tarefa 15.5)
# ---------------------------------------------------------------------------
# Mensagem exibida quando a tabela de monitoramento não tem nenhuma mensagem
# registrada na Tabela_Historico (estado vazio — Req. 5.8). Em pt-BR.
MENSAGEM_SEM_MENSAGENS_MONITORADAS: str = (
    "Nenhuma mensagem monitorada: ainda não há mensagens registradas na "
    "Tabela_Historico. Envie mensagens para acompanhar o ciclo de vida aqui."
)

# Indicação de timeout retornada quando a agregação/derivação excede o limite de
# tempo configurado (Req. 14.3, 14.4). O Dashboard, ao recebê-la, preserva os
# últimos dados válidos exibidos.
MENSAGEM_TIMEOUT_ESTATISTICAS: str = (
    "Tempo limite excedido ao calcular as estatísticas (acima de {segundos} s). "
    "Os últimos dados válidos foram preservados; tente novamente."
)
MENSAGEM_TIMEOUT_MONITORAMENTO: str = (
    "Tempo limite excedido ao montar a tabela de monitoramento (acima de "
    "{segundos} s). Os últimos dados válidos foram preservados; tente novamente."
)


class RecebidasPorConsumidor(BaseModel):
    """Contagem de recebimentos de um único consumidor (Req. 3.7)."""

    consumidor: str = Field(..., description="Identificador do consumidor (consumer_id).")
    recebidas: int = Field(
        ..., ge=0, description="Mensagens recebidas por este consumidor (inteiro ≥ 0)."
    )


class EstatisticasFluxoSaida(BaseModel):
    """Estatísticas agregadas de um único fluxo (Standard ou FIFO) (Req. 3.1-3.8, 3.12).

    Todos os contadores são inteiros maiores ou iguais a 0; quando não há dados,
    valem 0 (Req. 3.12).
    """

    fluxo: str = Field(..., description="Rótulo do fluxo: 'standard' ou 'fifo' (Req. 3.8).")
    enviadas: int = Field(..., ge=0, description="Total de mensagens enviadas (Req. 3.1).")
    disponiveis: int = Field(
        ..., ge=0, description="Mensagens disponíveis na fila (Req. 3.2)."
    )
    em_processamento: int = Field(
        ..., ge=0, description="Mensagens em processamento (Req. 3.3)."
    )
    processadas: int = Field(
        ..., ge=0, description="Mensagens processadas com sucesso (Req. 3.4)."
    )
    com_falha: int = Field(..., ge=0, description="Mensagens com falha (Req. 3.5).")
    encaminhadas_dlq: int = Field(
        ..., ge=0, description="Mensagens encaminhadas para a DLQ (Req. 3.6)."
    )
    exclusao_nao_confirmada: int = Field(
        0,
        ge=0,
        description=(
            "Mensagens cujo processamento terminou, mas o DeleteMessage não foi "
            "confirmado (ex.: ReceiptHandle expirado). Default 0 para não quebrar "
            "serializações existentes."
        ),
    )
    recebidas_por_consumidor: list[RecebidasPorConsumidor] = Field(
        default_factory=list,
        description="Recebimentos por consumidor, identificando cada um (Req. 3.7).",
    )


class RespostaEstatisticas(BaseModel):
    """Resposta do GET /api/estatisticas (Req. 3.1-3.12, 14.1, 14.3).

    Traz as estatísticas separadas e rotuladas por fluxo (Standard e FIFO) e os
    campos de indicação de timeout. Quando ``timeout`` é ``True``, os conjuntos por
    fluxo não refletem um cálculo concluído e o Dashboard deve preservar os últimos
    dados válidos (Req. 14.3).
    """

    timeout: bool = Field(
        False,
        description="True se o cálculo excedeu o tempo limite (Req. 14.3).",
    )
    mensagem: str = Field(
        "", description="Mensagem em pt-BR; descreve o timeout quando ocorre."
    )
    standard: Optional[EstatisticasFluxoSaida] = Field(
        None, description="Estatísticas do fluxo Standard (nulo em timeout)."
    )
    fifo: Optional[EstatisticasFluxoSaida] = Field(
        None, description="Estatísticas do fluxo FIFO (nulo em timeout)."
    )


class LinhaMonitoramento(BaseModel):
    """Uma linha da tabela de monitoramento (uma mensagem) (Req. 5.1-5.5).

    As colunas seguem o Req. 5.1: ID, Fila, Consumidor, Tentativas, Processamento,
    Estado e Último evento. A coluna Ações é responsabilidade do Dashboard e não
    faz parte do contrato de dados.
    """

    id: str = Field(..., description="Identificador do pedido (order_id) — coluna ID.")
    fila: str = Field(..., description="Nome da fila da mensagem — coluna Fila.")
    consumidor: Optional[str] = Field(
        None, description="Consumidor responsável — coluna Consumidor (nulo se nenhum)."
    )
    tentativas: int = Field(
        ..., ge=0, description="Contagem de recebimentos — coluna Tentativas (Req. 5.2)."
    )
    processamento_segundos: Optional[int] = Field(
        None,
        description=(
            "Tempo_Processamento_Simulado em segundos — coluna Processamento (Req. 5.3)."
        ),
    )
    estado: str = Field(
        ..., description="Estado derivado da mensagem — coluna Estado (Req. 5.4)."
    )
    ultimo_evento: Optional[str] = Field(
        None,
        description=(
            "Horário do evento mais recente (ISO-8601 UTC) — coluna Último evento "
            "(Req. 5.5). Nulo se a mensagem não tem eventos com horário."
        ),
    )


class RespostaMonitoramento(BaseModel):
    """Resposta do GET /api/monitoramento (Req. 5.1-5.10, 14.2, 14.4).

    Lista as linhas da tabela de monitoramento, uma por mensagem, com o Estado
    derivado dos eventos (Req. 5.9). Quando não há mensagens registradas, ``linhas``
    é vazia e ``mensagem_vazia`` traz a indicação em pt-BR (Req. 5.8). Quando o
    cálculo excede o tempo limite, ``timeout`` é ``True`` e o Dashboard preserva os
    últimos dados válidos (Req. 14.4).
    """

    timeout: bool = Field(
        False, description="True se o cálculo excedeu o tempo limite (Req. 14.4)."
    )
    mensagem: str = Field(
        "", description="Mensagem em pt-BR; descreve o timeout quando ocorre."
    )
    colunas: list[str] = Field(
        default_factory=lambda: [
            "ID",
            "Fila",
            "Consumidor",
            "Tentativas",
            "Processamento",
            "Estado",
            "Último evento",
        ],
        description="Rótulos das colunas da tabela de monitoramento (Req. 5.1).",
    )
    linhas: list[LinhaMonitoramento] = Field(
        default_factory=list,
        description="Uma linha por mensagem monitorada (vazia em estado vazio/timeout).",
    )
    vazio: bool = Field(
        False,
        description="True quando não há nenhuma mensagem registrada (Req. 5.8).",
    )
    mensagem_vazia: Optional[str] = Field(
        None,
        description="Indicação em pt-BR de ausência de mensagens, quando vazio (Req. 5.8).",
    )

# ---------------------------------------------------------------------------
# Histórico detalhado — linha do tempo (Tarefa 15.6)
# ---------------------------------------------------------------------------
# Mensagens em pt-BR para os estados especiais da linha do tempo (Req. 6.7, 6.8).
MENSAGEM_HISTORICO_SEM_EVENTOS: str = (
    "Não há eventos registrados para esta mensagem na Tabela_Historico."
)
MENSAGEM_HISTORICO_INDISPONIVEL: str = (
    "Histórico indisponível: não foi possível consultar os eventos desta "
    "mensagem na Tabela_Historico. A tela anterior foi preservada; tente "
    "novamente."
)

# Rótulos textuais distintos do indicador de origem exibido por evento, sem exigir
# interação do usuário (Req. 6.4). Mantêm o valor técnico do enum EventSource.
ROTULO_EVENTO_OBSERVADO: str = "Observado"
ROTULO_EVENTO_INFERIDO: str = "Inferido"


class EventoHistorico(BaseModel):
    """Um evento da linha do tempo do histórico detalhado (Req. 6.1-6.6).

    Cada evento traz o tipo, a origem (OBSERVADO/INFERIDO) com rótulo em pt-BR, o
    horário real da aplicação (ISO-8601 UTC com milissegundos e identificação de
    fuso) e os metadados relevantes para a didática (consumidor, tentativas,
    tempo de processamento, grupo FIFO e detalhe de erro).
    """

    ordem: int = Field(
        ..., description="Posição do evento na linha do tempo (base 0, crescente)."
    )
    event_type: str = Field(..., description="Tipo do evento (ex.: 'enviada').")
    event_source: str = Field(
        ..., description="Origem técnica do evento: OBSERVADO ou INFERIDO (Req. 6.4)."
    )
    indicador_origem: str = Field(
        ...,
        description=(
            "Rótulo em pt-BR do indicador de origem (Observado/Inferido), exibido "
            "sem exigir interação do usuário (Req. 6.4)."
        ),
    )
    observado: bool = Field(
        ..., description="True se o evento é Evento_Observado; False se Inferido."
    )
    app_time: str = Field(
        ...,
        description=(
            "Horário real da aplicação em ISO-8601 UTC com milissegundos e "
            "identificação de fuso (sufixo Z) — Req. 6.3."
        ),
    )
    fila: str = Field(..., description="Nome da fila associada ao evento.")
    consumidor: Optional[str] = Field(
        None, description="Consumidor responsável, quando aplicável."
    )
    tentativas: Optional[int] = Field(
        None, description="Contagem de recebimentos (receive_count), quando houver."
    )
    processamento_segundos: Optional[int] = Field(
        None, description="Tempo_Processamento_Simulado associado, quando houver."
    )
    message_group_id: Optional[str] = Field(
        None, description="MessageGroupId do evento (apenas FIFO), quando houver."
    )
    detalhe_erro: Optional[str] = Field(
        None, description="Erro original registrado no evento, quando houver."
    )


class RespostaHistorico(BaseModel):
    """Resposta do GET /api/mensagens/{order_id}/historico (Req. 6.1-6.8).

    Lista os eventos em ordem cronológica crescente, com desempate estável. Quando
    a mensagem não tem eventos, ``vazio`` é ``True`` e ``mensagem`` traz a indicação
    em pt-BR (Req. 6.8). Quando a consulta falha, ``indisponivel`` é ``True`` e
    ``mensagem`` indica que o histórico está indisponível (Req. 6.7).
    """

    order_id: str = Field(..., description="Identificador do pedido consultado.")
    indisponivel: bool = Field(
        False, description="True se a consulta à Tabela_Historico falhou (Req. 6.7)."
    )
    vazio: bool = Field(
        False, description="True quando a mensagem não tem eventos (Req. 6.8)."
    )
    mensagem: str = Field(
        "",
        description="Mensagem em pt-BR; descreve vazio/indisponível quando aplicável.",
    )
    eventos: list[EventoHistorico] = Field(
        default_factory=list,
        description="Eventos em ordem cronológica crescente (vazio se não houver).",
    )


# ---------------------------------------------------------------------------
# Laboratório de falhas — execução de cenário (Tarefa 15.6)
# ---------------------------------------------------------------------------
class RequisicaoCenario(BaseModel):
    """Payload do POST /api/laboratorio/cenario (Req. 7.1-7.11).

    Executa um cenário de falha controlado, enviando uma ou mais mensagens à fila
    selecionada com o Cenario_Falha embutido, para que os consumidores reproduzam o
    comportamento desejado. A execução é interrompida com causa em pt-BR quando a
    fila está indisponível ou a autenticação/autorização falha (Req. 7.11).
    """

    tipo_fila: str = Field(
        ..., description="Fila de destino do cenário: 'standard' ou 'fifo' (Req. 4.1)."
    )
    cenario: str = Field(
        ...,
        description=(
            "Cenario_Falha a executar (ex.: 'sucesso', 'excecao_proposital', "
            "'encaminhar_dlq', 'duracao_superior_visibility', 'interrupcao', "
            "'recebimento_repetido')."
        ),
    )
    quantidade: int = Field(
        1, description="Quantidade de mensagens do cenário, de 1 a 10 (Req. 4.2)."
    )
    order_id_base: str = Field(
        "cenario",
        description="Prefixo do order_id gerado para cada mensagem do cenário.",
    )
    tempo_processamento_simulado_segundos: Optional[int] = Field(
        None,
        description=(
            "Tempo_Processamento_Simulado das mensagens [0, 300]; quando omitido, "
            "usa o valor de Config_Simulacao."
        ),
    )
    message_group_id: Optional[str] = Field(
        None, description="MessageGroupId do cenário (apenas FIFO)."
    )
    origem: Optional[str] = Field(
        None,
        description=(
            "Origem autenticada do solicitante (ex.: IP). Usada para verificar a "
            "autenticação/autorização antes de executar o cenário (Req. 7.11, 12.5)."
        ),
    )


class RespostaCenario(BaseModel):
    """Resposta do POST /api/laboratorio/cenario (Req. 7.1-7.11).

    Quando o cenário é aceito e disparado, ``executado`` é ``True`` e ``envio`` traz
    o resultado do envio das mensagens. Quando interrompido (fila indisponível ou
    falha de autenticação/autorização), ``executado`` é ``False``, ``interrompido``
    é ``True`` e ``causa`` descreve o motivo em pt-BR (Req. 7.11).
    """

    cenario: str = Field(..., description="Cenario_Falha solicitado.")
    tipo_fila: str = Field(..., description="Fila de destino do cenário.")
    executado: bool = Field(
        ..., description="True se o cenário foi disparado (mensagens enviadas)."
    )
    interrompido: bool = Field(
        False,
        description="True se o cenário foi interrompido antes de concluir (Req. 7.11).",
    )
    causa: Optional[str] = Field(
        None,
        description="Causa da interrupção em pt-BR, quando interrompido (Req. 7.11).",
    )
    mensagem: str = Field(..., description="Resumo em pt-BR do resultado do cenário.")
    envio: Optional[RespostaEnvioMensagens] = Field(
        None, description="Resultado do envio das mensagens do cenário, quando houver."
    )


# ---------------------------------------------------------------------------
# Laboratório de falhas — reprocessamento da DLQ (Tarefa 15.6)
# ---------------------------------------------------------------------------
class RequisicaoReprocessarDlq(BaseModel):
    """Payload do POST /api/laboratorio/reprocessar-dlq (Req. 7.7, 7.11).

    Move mensagens da DLQ de volta à fila principal do fluxo selecionado. A operação
    é interrompida com causa em pt-BR quando a fila está indisponível ou a
    autenticação/autorização falha (Req. 7.11).
    """

    tipo_fila: str = Field(
        ...,
        description=(
            "Fluxo cuja DLQ será reprocessada: 'standard' ou 'fifo'. A aplicação "
            "resolve a DLQ e a fila principal correspondentes (Req. 7.7)."
        ),
    )
    origem: Optional[str] = Field(
        None,
        description=(
            "Origem autenticada do solicitante. Usada para verificar a "
            "autenticação/autorização antes de reprocessar (Req. 7.11, 12.5)."
        ),
    )


class RespostaReprocessarDlq(BaseModel):
    """Resposta do POST /api/laboratorio/reprocessar-dlq (Req. 7.7, 7.11).

    Quando bem-sucedida, ``reprocessado`` é ``True`` e ``mensagens_movidas`` traz os
    identificadores das mensagens movidas da DLQ de volta à fila principal. Quando
    interrompida, ``interrompido`` é ``True`` e ``causa`` descreve o motivo em pt-BR.
    """

    tipo_fila: str = Field(..., description="Fluxo reprocessado.")
    dlq: Optional[str] = Field(None, description="Nome da DLQ de origem.")
    fila_principal: Optional[str] = Field(
        None, description="Nome da fila principal de destino."
    )
    reprocessado: bool = Field(
        ..., description="True se ao menos um lote foi movido com sucesso."
    )
    total_movidas: int = Field(
        0, description="Quantidade de mensagens movidas da DLQ à fila principal."
    )
    mensagens_movidas: list[str] = Field(
        default_factory=list,
        description="Identificadores (MessageId) das mensagens reenviadas.",
    )
    interrompido: bool = Field(
        False, description="True se a operação foi interrompida (Req. 7.11)."
    )
    causa: Optional[str] = Field(
        None, description="Causa da interrupção em pt-BR, quando interrompido."
    )
    mensagem: str = Field(..., description="Resumo em pt-BR do resultado.")


# ---------------------------------------------------------------------------
# Ajuste de instâncias de consumidores (Tarefa 15.6)
# ---------------------------------------------------------------------------
# Rótulos de tipo de consumidor aceitos pelo endpoint de instâncias (Req. 10.1, 10.2).
TIPO_CONSUMIDOR_STANDARD: str = "standard"
TIPO_CONSUMIDOR_FIFO: str = "fifo"


class RequisicaoInstancias(BaseModel):
    """Payload do PUT /api/consumidores/{tipo}/instancias (Req. 10.1-10.3)."""

    instancias: int = Field(
        ..., description="Número desejado de instâncias do consumidor, de 1 a 10."
    )


class RespostaInstancias(BaseModel):
    """Resposta do PUT /api/consumidores/{tipo}/instancias (Req. 10.1-10.3).

    Quando o valor está na faixa [1, 10], ``aceito`` é ``True`` e ``instancias``
    reflete o novo número aplicado. Quando fora da faixa, ``aceito`` é ``False``, o
    número anterior é preservado em ``instancias`` e ``mensagem`` indica o intervalo
    válido em pt-BR (Req. 10.3).
    """

    tipo: str = Field(..., description="Tipo do consumidor: 'standard' ou 'fifo'.")
    aceito: bool = Field(..., description="True se o ajuste foi aceito (dentro da faixa).")
    instancias: int = Field(
        ..., description="Número de instâncias em vigor (novo se aceito, anterior se não)."
    )
    mensagem: str = Field(
        "", description="Mensagem em pt-BR; indica a faixa válida quando rejeitado."
    )


# ---------------------------------------------------------------------------
# Saúde dos consumidores (frente D — supervisão em memória)
# ---------------------------------------------------------------------------
class ConsumidorSaude(BaseModel):
    """Situação de uma única instância (slot) de consumidor, lida da memória.

    Reflete o estado observado pelo :class:`GerenciadorConsumidores` sem tocar na
    AWS: se a thread está viva, se o slot foi marcado como degradado (falha
    persistente) e quantos reinícios já ocorreram.
    """

    tipo: str = Field(..., description="Tipo do consumidor: 'standard' ou 'fifo'.")
    consumer_id: str = Field(
        ..., description="Identificador da instância atualmente associada ao slot."
    )
    vivo: bool = Field(
        ..., description="True se a thread do consumidor está viva no momento."
    )
    degradado: bool = Field(
        ...,
        description="True se o slot foi marcado como falha persistente (parou de reiniciar).",
    )
    reinicios: int = Field(
        ..., ge=0, description="Total de reinícios já realizados para o slot."
    )


class RespostaSaudeConsumidores(BaseModel):
    """Resposta do GET /api/consumidores/saude (frente D).

    Expõe a saúde dos consumidores supervisionados em memória, sem consultar a AWS.
    Quando os consumidores não foram iniciados (ou não há gerenciador), ``iniciados``
    é ``False`` e ``consumidores`` é vazia. Os agregados ``algum_morto`` e
    ``algum_degradado`` resumem o estado para o Dashboard destacar problemas.
    """

    iniciados: bool = Field(
        ...,
        description="True se há um gerenciador de consumidores ativo com slots.",
    )
    algum_morto: bool = Field(
        ...,
        description="True se ao menos um slot não está vivo (thread encerrada).",
    )
    algum_degradado: bool = Field(
        ...,
        description="True se ao menos um slot foi marcado como falha persistente.",
    )
    consumidores: list[ConsumidorSaude] = Field(
        default_factory=list,
        description="Situação de cada slot de consumidor (vazia quando não iniciados).",
    )


# ---------------------------------------------------------------------------
# Autenticação do Dashboard (Tarefa 15.6)
# ---------------------------------------------------------------------------
class RequisicaoAutenticacao(BaseModel):
    """Payload do POST /api/autenticacao (Req. 12.5, 12.6).

    O login do Dashboard exige usuário e senha. A ``origem`` identifica o solicitante
    para o controle de bloqueio por origem após 5 falhas consecutivas (Req. 12.6).
    """

    usuario: str = Field(..., description="Usuário informado no login.")
    senha: str = Field(..., description="Senha informada no login.")
    origem: Optional[str] = Field(
        None,
        description=(
            "Origem do solicitante (ex.: IP). Quando omitida, usa o host do cliente "
            "da requisição. Base do controle de bloqueio por origem (Req. 12.6, 12.7)."
        ),
    )


class RespostaAutenticacao(BaseModel):
    """Resposta do POST /api/autenticacao (Req. 12.5, 12.6).

    ``acesso_concedido`` é ``True`` somente com credenciais válidas. ``bloqueada`` é
    ``True`` quando a origem está em período de bloqueio (após 5 falhas consecutivas)
    e ``segundos_restantes_bloqueio`` indica o tempo restante. ``mensagem`` descreve
    o desfecho em pt-BR.
    """

    acesso_concedido: bool = Field(
        ..., description="True apenas quando a autenticação é bem-sucedida."
    )
    resultado: str = Field(
        ...,
        description=(
            "Desfecho técnico: SUCESSO, FALHA, BLOQUEADA ou ORIGEM_NAO_AUTORIZADA."
        ),
    )
    origem: str = Field(..., description="Origem avaliada.")
    bloqueada: bool = Field(
        ..., description="True se a origem está bloqueada após esta avaliação (Req. 12.6)."
    )
    falhas_consecutivas: int = Field(
        ..., ge=0, description="Falhas consecutivas acumuladas para a origem."
    )
    segundos_restantes_bloqueio: int = Field(
        ..., ge=0, description="Segundos restantes do bloqueio (0 se não bloqueada)."
    )
    mensagem: str = Field(..., description="Mensagem em pt-BR descrevendo o desfecho.")
