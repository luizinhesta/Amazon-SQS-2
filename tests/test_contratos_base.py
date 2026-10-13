"""Testes de sanidade dos contratos base (Tarefa 1).

Verificam que as constantes do laboratório e os tipos de domínio (EventType,
EventSource, Estado) estão definidos conforme os requisitos 1.1-1.4, 2.1-2.5 e 5.4.
São testes unitários simples; a lógica de domínio (StateDeriver, validações,
agregadores, idempotência) é testada em tarefas posteriores.
"""

from __future__ import annotations

from app.dominio import constantes as c
from app.dominio.tipos import (
    ESTADOS_PERMITIDOS,
    ESTADOS_TERMINAIS,
    ORIGEM_OBRIGATORIA_POR_TIPO,
    Estado,
    EventSource,
    EventType,
)


def test_nomes_das_quatro_filas() -> None:
    # Req. 1.1 a 1.4: nomes e composição do conjunto de filas.
    assert c.FILA_STANDARD == "lab-sqs-standard"
    assert c.FILA_STANDARD_DLQ == "lab-sqs-standard-dlq"
    assert c.FILA_FIFO == "lab-sqs-fifo.fifo"
    assert c.FILA_FIFO_DLQ == "lab-sqs-fifo-dlq.fifo"
    assert c.FILAS == (
        "lab-sqs-standard",
        "lab-sqs-standard-dlq",
        "lab-sqs-fifo.fifo",
        "lab-sqs-fifo-dlq.fifo",
    )


def test_regiao_e_dlq_por_fila_principal() -> None:
    assert c.REGIAO_AWS == "us-east-1"
    assert c.DLQ_POR_FILA_PRINCIPAL[c.FILA_STANDARD] == c.FILA_STANDARD_DLQ
    assert c.DLQ_POR_FILA_PRINCIPAL[c.FILA_FIFO] == c.FILA_FIFO_DLQ


def test_config_aplicada_valores_iniciais() -> None:
    # Req. 2.1-2.5: parâmetros iniciais de Config_Aplicada.
    assert c.VISIBILITY_TIMEOUT_SEGUNDOS == 30
    assert c.MAX_RECEIVE_COUNT == 2
    assert c.LONG_POLLING_SEGUNDOS == 20
    assert c.RETENCAO_FILA_PRINCIPAL_DIAS == 4
    assert c.RETENCAO_DLQ_DIAS == 14


def test_config_simulacao_valor_inicial() -> None:
    # Tempo_Processamento_Simulado inicial de 45 s, ajustável entre 0 e 300.
    assert c.TEMPO_PROCESSAMENTO_SIMULADO_SEGUNDOS == 45
    assert c.TEMPO_PROCESSAMENTO_SIMULADO_MIN == 0
    assert c.TEMPO_PROCESSAMENTO_SIMULADO_MAX == 300


def test_conjunto_de_estados_corresponde_ao_requisito_5_4() -> None:
    valores = {estado.value for estado in Estado}
    assert valores == {
        "Aguardando",
        "Recebida",
        "Em processamento",
        "Processada",
        "Falha",
        "Aguardando nova tentativa",
        "Encaminhada para DLQ",
        "Exclusão não confirmada",
    }
    assert ESTADOS_PERMITIDOS == frozenset(Estado)
    assert ESTADOS_TERMINAIS == frozenset({Estado.PROCESSADA, Estado.ENCAMINHADA_DLQ})
    # O estado Exclusão não confirmada é permitido mas NÃO terminal: uma reentrega
    # posterior pode reprocessar/concluir a mensagem.
    assert Estado.EXCLUSAO_NAO_CONFIRMADA in ESTADOS_PERMITIDOS
    assert Estado.EXCLUSAO_NAO_CONFIRMADA not in ESTADOS_TERMINAIS


def test_event_source_tem_apenas_observado_e_inferido() -> None:
    assert {s.value for s in EventSource} == {"OBSERVADO", "INFERIDO"}


def test_event_types_incluem_tipos_chave_do_ciclo_de_vida() -> None:
    tipos = {t.value for t in EventType}
    # Amostra dos tipos essenciais do ciclo de vida (ver design.md).
    for esperado in {
        "enviada",
        "recebida",
        "processada",
        "falha",
        "encaminhada_dlq",
        "provavel_expiracao_visibility",
    }:
        assert esperado in tipos


def test_origem_obrigatoria_por_tipo() -> None:
    # Req. 6.5/6.6/7.9: origens fixas para os eventos de expiração e DLQ.
    assert (
        ORIGEM_OBRIGATORIA_POR_TIPO[EventType.PROVAVEL_EXPIRACAO_VISIBILITY]
        is EventSource.INFERIDO
    )
    assert (
        ORIGEM_OBRIGATORIA_POR_TIPO[EventType.ENCAMINHADA_DLQ]
        is EventSource.OBSERVADO
    )
