"""Constantes do laboratório Amazon SQS.

Centraliza os valores fixos do laboratório — nomes de recursos AWS, região e
parâmetros iniciais de Config_Aplicada e Config_Simulacao. A aplicação NÃO cria,
modifica nem exclui recursos AWS; estas constantes apenas identificam e descrevem
os recursos criados manualmente pelo usuário no Console AWS.

Referências de requisitos: 1.1, 1.2, 1.3, 1.4, 2.1, 2.2, 2.3, 2.4, 2.5.
"""

from __future__ import annotations

from typing import Final

# ---------------------------------------------------------------------------
# Região AWS
# ---------------------------------------------------------------------------
# Todo o laboratório opera na região Norte da Virgínia (Req. 1.1 a 1.4).
REGIAO_AWS: Final[str] = "us-east-1"

# ---------------------------------------------------------------------------
# Nomes das filas SQS (criadas manualmente no Console AWS)
# ---------------------------------------------------------------------------
# Fila_Principal_Standard (Req. 1.1).
FILA_STANDARD: Final[str] = "lab-sqs-standard"
# DLQ_Standard (Req. 1.2).
FILA_STANDARD_DLQ: Final[str] = "lab-sqs-standard-dlq"
# Fila_Principal_FIFO (Req. 1.3).
FILA_FIFO: Final[str] = "lab-sqs-fifo.fifo"
# DLQ_FIFO (Req. 1.4).
FILA_FIFO_DLQ: Final[str] = "lab-sqs-fifo-dlq.fifo"

# Conjunto das quatro filas esperadas pelo laboratório.
FILAS: Final[tuple[str, ...]] = (
    FILA_STANDARD,
    FILA_STANDARD_DLQ,
    FILA_FIFO,
    FILA_FIFO_DLQ,
)

# Filas principais (produção/consumo direto) e DLQs, separadas por papel.
FILAS_PRINCIPAIS: Final[tuple[str, ...]] = (FILA_STANDARD, FILA_FIFO)
FILAS_DLQ: Final[tuple[str, ...]] = (FILA_STANDARD_DLQ, FILA_FIFO_DLQ)

# Associação de cada fila principal à sua DLQ (Redrive_Policy manual).
DLQ_POR_FILA_PRINCIPAL: Final[dict[str, str]] = {
    FILA_STANDARD: FILA_STANDARD_DLQ,
    FILA_FIFO: FILA_FIFO_DLQ,
}

# ---------------------------------------------------------------------------
# Tabela DynamoDB
# ---------------------------------------------------------------------------
# Tabela_Historico: log de eventos append-only do ciclo de vida das mensagens.
TABELA_HISTORICO: Final[str] = "lab-sqs-historico"
# Índice secundário global usado para consulta por fila (GSI1PK = FILA#<nome>).
INDICE_GSI1: Final[str] = "GSI1"

# ---------------------------------------------------------------------------
# Config_Aplicada — parâmetros efetivamente configurados nos recursos SQS
# ---------------------------------------------------------------------------
# Estes valores refletem o que foi configurado manualmente nos recursos AWS.
# Alterá-los exige mudança no Console AWS (Req. 2.13); a aplicação só os exibe.

# Visibility_Timeout inicial das filas principais, em segundos (Req. 2.1).
VISIBILITY_TIMEOUT_SEGUNDOS: Final[int] = 30
# maxReceiveCount inicial da Redrive_Policy (Req. 2.2).
MAX_RECEIVE_COUNT: Final[int] = 2
# Long_Polling inicial (WaitTimeSeconds) das filas principais, em segundos (Req. 2.5).
LONG_POLLING_SEGUNDOS: Final[int] = 20
# Retenção inicial das filas principais, em dias (Req. 2.3).
RETENCAO_FILA_PRINCIPAL_DIAS: Final[int] = 4
# Retenção inicial das DLQs, em dias (Req. 2.4).
RETENCAO_DLQ_DIAS: Final[int] = 14

# ---------------------------------------------------------------------------
# Config_Simulacao — parâmetros usados apenas pela lógica de simulação
# ---------------------------------------------------------------------------
# Ajustáveis pelo usuário no Dashboard, dentro das faixas definidas (Req. 2.11).

# Tempo_Processamento_Simulado inicial, em segundos (Req. 2.8).
TEMPO_PROCESSAMENTO_SIMULADO_SEGUNDOS: Final[int] = 45
# Faixa válida do Tempo_Processamento_Simulado (Req. 2.8, 4.4).
TEMPO_PROCESSAMENTO_SIMULADO_MIN: Final[int] = 0
TEMPO_PROCESSAMENTO_SIMULADO_MAX: Final[int] = 300

# Quantidade inicial de mensagens por fila (Req. 2.9).
QUANTIDADE_MENSAGENS_INICIAL: Final[int] = 5
# Faixa válida da quantidade de mensagens por envio (Req. 2.9, 4.2).
QUANTIDADE_MENSAGENS_MIN: Final[int] = 1
QUANTIDADE_MENSAGENS_MAX: Final[int] = 10

# Faixa válida do número de instâncias de cada tipo de consumidor (Req. 10.1-10.3).
INSTANCIAS_CONSUMIDOR_MIN: Final[int] = 1
INSTANCIAS_CONSUMIDOR_MAX: Final[int] = 10
INSTANCIAS_CONSUMIDOR_INICIAL: Final[int] = 1

# Faixa de caracteres do identificador do pedido (order_id) (Req. 4.3).
ORDER_ID_TAMANHO_MIN: Final[int] = 1
ORDER_ID_TAMANHO_MAX: Final[int] = 80

# Faixa de caracteres do MessageGroupId em filas FIFO (Req. 4.6).
MESSAGE_GROUP_ID_TAMANHO_MIN: Final[int] = 1
MESSAGE_GROUP_ID_TAMANHO_MAX: Final[int] = 128

# Faixa de caracteres do MessageDeduplicationId em filas FIFO (Req. 4.7).
MESSAGE_DEDUPLICATION_ID_TAMANHO_MIN: Final[int] = 1
MESSAGE_DEDUPLICATION_ID_TAMANHO_MAX: Final[int] = 128

# ---------------------------------------------------------------------------
# Intervalos de atualização periódica do Dashboard
# ---------------------------------------------------------------------------
# Visão geral: intervalo configurável 1-60 s, padrão 5 s (Req. 3.9).
INTERVALO_VISAO_GERAL_MIN: Final[int] = 1
INTERVALO_VISAO_GERAL_MAX: Final[int] = 60
INTERVALO_VISAO_GERAL_PADRAO: Final[int] = 5
# Atualização periódica geral: intervalo 5-300 s, padrão 10 s (Req. 13.2).
INTERVALO_ATUALIZACAO_MIN: Final[int] = 5
INTERVALO_ATUALIZACAO_MAX: Final[int] = 300
INTERVALO_ATUALIZACAO_PADRAO: Final[int] = 10

# ---------------------------------------------------------------------------
# Segurança
# ---------------------------------------------------------------------------
# Bloqueio de autenticação após falhas consecutivas por origem (Req. 12.6).
MAX_TENTATIVAS_AUTENTICACAO: Final[int] = 5
BLOQUEIO_AUTENTICACAO_SEGUNDOS: Final[int] = 300

# Número máximo de tentativas de escrita na Tabela_Historico (Req. 11.2).
MAX_TENTATIVAS_ESCRITA_HISTORICO: Final[int] = 3

# ---------------------------------------------------------------------------
# Supervisão e recuperação de consumidores (resiliência)
# ---------------------------------------------------------------------------
# Intervalo entre verificações do supervisor de threads de consumidores, em
# segundos. O supervisor checa periodicamente se alguma thread encerrou de forma
# inesperada e a reinicia, respeitando o critério de parada cooperativa.
INTERVALO_SUPERVISAO_SEGUNDOS: Final[int] = 5
# Número máximo de reinícios automáticos de um consumidor dentro da janela de
# observação, evitando laços de reinício em falhas persistentes.
MAX_REINICIOS_JANELA: Final[int] = 5
# Janela de observação (em segundos) para contabilizar os reinícios automáticos.
JANELA_REINICIOS_SEGUNDOS: Final[int] = 60
# Intervalo (em segundos) entre extensões opcionais do Visibility_Timeout via
# ChangeMessageVisibility (heartbeat) durante processamentos longos. Mantém a
# mensagem invisível sem alterar o Visibility_Timeout de 30s configurado na fila.
HEARTBEAT_VISIBILITY_SEGUNDOS: Final[int] = 20
