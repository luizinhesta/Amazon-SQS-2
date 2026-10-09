"""Testes unitários do HistoricoRepository.registrar_evento (Tarefa 7.1).

Cobrem o retry de escrita (PutItem) com até ``MAX_TENTATIVAS_ESCRITA_HISTORICO``
tentativas em falhas transitórias e, ao esgotar, a preservação da mensagem original
e a sinalização do erro — sem exceder 3 tentativas. Também validam a precisão de
milissegundos do ``app_time`` (Req. 8.3, 11.1).

Os testes usam um *fake* de DynamoDB em memória (sem AWS real). As falhas da AWS são
reproduzidas por exceções equivalentes a ``ClientError`` (atributo ``response`` com
``Error.Code``) e a ``BotoCoreError`` (erros de rede/conexão).

Referências de requisitos: 8.3, 11.1, 11.2.
"""

from __future__ import annotations

import re

import pytest

from app.acesso.historico_repository import (
    FalhaRegistroEvento,
    HistoricoRepository,
    agora_utc_ms,
)
from app.dominio.constantes import MAX_TENTATIVAS_ESCRITA_HISTORICO
from app.dominio.evento import Evento
from app.dominio.tipos import EventSource, EventType


# ---------------------------------------------------------------------------
# Fakes de erro equivalentes às exceções do boto3/botocore
# ---------------------------------------------------------------------------


class ErroClienteFalso(Exception):
    """Equivalente a ``botocore.exceptions.ClientError``.

    Expõe ``response["Error"]["Code"]``, que é como o repositório classifica se o
    erro é transitório.
    """

    def __init__(self, codigo: str, mensagem: str = "erro simulado") -> None:
        self.response = {"Error": {"Code": codigo, "Message": mensagem}}
        super().__init__(f"{codigo}: {mensagem}")


class ErroBotoCoreFalso(Exception):
    """Equivalente a ``botocore.exceptions.BotoCoreError`` (erro de rede/conexão).

    O repositório identifica erros transitórios de rede pelo nome ``BotoCoreError``
    na hierarquia de classes (MRO); por isso a classe tem exatamente esse nome base.
    """


class BotoCoreError(ErroBotoCoreFalso):  # noqa: N818 — nome imita a classe do botocore
    """Nome que coincide com ``BotoCoreError`` para acionar a classificação por MRO."""


# ---------------------------------------------------------------------------
# Fake de DynamoDB em memória
# ---------------------------------------------------------------------------


class TabelaDynamoDBFalsa:
    """Fake de uma tabela DynamoDB (boto3 ``Table``) em memória.

    Guarda os itens escritos por PutItem e permite programar uma sequência de falhas
    para simular falhas transitórias/permanentes antes do sucesso.

    Args:
        falhas: Lista de exceções a levantar nas primeiras chamadas de ``put_item``,
            em ordem. Após consumir a lista, as chamadas seguintes têm sucesso.
    """

    def __init__(self, falhas: list[Exception] | None = None) -> None:
        self._falhas = list(falhas or [])
        self.itens: list[dict] = []
        self.chamadas = 0

    def put_item(self, **kwargs) -> dict:
        self.chamadas += 1
        if self._falhas:
            raise self._falhas.pop(0)
        self.itens.append(kwargs["Item"])
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _evento_exemplo() -> Evento:
    return Evento(
        order_id="pedido-0001",
        queue_name="lab-sqs-standard",
        event_type=EventType.ENVIADA,
        event_source=EventSource.OBSERVADO,
        app_time="2024-06-25T12:00:00.123Z",
        app_time_ms=1719322800123,
        seq=1,
        sqs_message_id="a1b2c3d4-0001",
        processing_seconds=45,
    )


# ---------------------------------------------------------------------------
# Sucesso na primeira tentativa
# ---------------------------------------------------------------------------


def test_registra_evento_com_sucesso_na_primeira_tentativa() -> None:
    tabela = TabelaDynamoDBFalsa()
    repositorio = HistoricoRepository(tabela)
    evento = _evento_exemplo()

    repositorio.registrar_evento(evento)

    assert tabela.chamadas == 1
    assert tabela.itens == [evento.para_item()]


# ---------------------------------------------------------------------------
# Retry em falhas transitórias
# ---------------------------------------------------------------------------


def test_retry_sucede_na_terceira_tentativa_apos_duas_falhas_transitorias() -> None:
    # Duas falhas transitórias seguidas e, na 3ª tentativa, sucesso.
    tabela = TabelaDynamoDBFalsa(
        falhas=[
            ErroClienteFalso("ProvisionedThroughputExceededException"),
            ErroClienteFalso("ThrottlingException"),
        ]
    )
    repositorio = HistoricoRepository(tabela)
    evento = _evento_exemplo()

    repositorio.registrar_evento(evento)

    assert tabela.chamadas == 3  # exatamente MAX_TENTATIVAS
    assert tabela.chamadas == MAX_TENTATIVAS_ESCRITA_HISTORICO
    assert tabela.itens == [evento.para_item()]


def test_erro_de_rede_botocore_e_tratado_como_transitorio() -> None:
    tabela = TabelaDynamoDBFalsa(falhas=[BotoCoreError("conexao perdida")])
    repositorio = HistoricoRepository(tabela)
    evento = _evento_exemplo()

    repositorio.registrar_evento(evento)

    assert tabela.chamadas == 2
    assert tabela.itens == [evento.para_item()]


# ---------------------------------------------------------------------------
# Esgotamento das tentativas: preserva mensagem e erro, sem exceder 3
# ---------------------------------------------------------------------------


def test_esgota_tentativas_preserva_mensagem_e_erro_sem_exceder_tres() -> None:
    erro_final = ErroClienteFalso("InternalServerError", "indisponivel")
    tabela = TabelaDynamoDBFalsa(
        falhas=[
            ErroClienteFalso("ThrottlingException"),
            ErroClienteFalso("ServiceUnavailable"),
            erro_final,
        ]
    )
    repositorio = HistoricoRepository(tabela)
    evento = _evento_exemplo()

    with pytest.raises(FalhaRegistroEvento) as info:
        repositorio.registrar_evento(evento)

    # Nunca excede o máximo de 3 tentativas (Req. 11.2).
    assert tabela.chamadas == MAX_TENTATIVAS_ESCRITA_HISTORICO
    assert info.value.tentativas == MAX_TENTATIVAS_ESCRITA_HISTORICO
    # Preserva a "mensagem" original (o evento) e o erro original da AWS.
    assert info.value.evento is evento
    assert info.value.erro_original is erro_final
    assert info.value.__cause__ is erro_final
    # Nada foi efetivamente gravado.
    assert tabela.itens == []


def test_falhas_transitorias_infinitas_nao_excedem_o_limite() -> None:
    # Mais falhas do que o limite: o repositório deve parar em 3 tentativas.
    tabela = TabelaDynamoDBFalsa(
        falhas=[ErroClienteFalso("ThrottlingException") for _ in range(10)]
    )
    repositorio = HistoricoRepository(tabela)

    with pytest.raises(FalhaRegistroEvento):
        repositorio.registrar_evento(_evento_exemplo())

    assert tabela.chamadas == MAX_TENTATIVAS_ESCRITA_HISTORICO


# ---------------------------------------------------------------------------
# Falha permanente: não repete
# ---------------------------------------------------------------------------


def test_falha_permanente_nao_e_repetida() -> None:
    # Erro de validação é permanente: repetir não resolve. Para na 1ª tentativa.
    erro = ErroClienteFalso("ValidationException", "atributo inválido")
    tabela = TabelaDynamoDBFalsa(falhas=[erro, erro, erro])
    repositorio = HistoricoRepository(tabela)
    evento = _evento_exemplo()

    with pytest.raises(FalhaRegistroEvento) as info:
        repositorio.registrar_evento(evento)

    assert tabela.chamadas == 1  # não repetiu
    assert info.value.tentativas == 1
    assert info.value.erro_original is erro
    assert info.value.evento is evento


def test_acesso_negado_nao_e_repetido() -> None:
    erro = ErroClienteFalso("AccessDeniedException")
    tabela = TabelaDynamoDBFalsa(falhas=[erro, erro, erro])
    repositorio = HistoricoRepository(tabela)

    with pytest.raises(FalhaRegistroEvento):
        repositorio.registrar_evento(_evento_exemplo())

    assert tabela.chamadas == 1


# ---------------------------------------------------------------------------
# Limite configurável de tentativas
# ---------------------------------------------------------------------------


def test_limite_de_tentativas_configuravel() -> None:
    tabela = TabelaDynamoDBFalsa(
        falhas=[ErroClienteFalso("ThrottlingException") for _ in range(10)]
    )
    repositorio = HistoricoRepository(tabela, max_tentativas=2)

    with pytest.raises(FalhaRegistroEvento) as info:
        repositorio.registrar_evento(_evento_exemplo())

    assert tabela.chamadas == 2
    assert info.value.tentativas == 2


# ---------------------------------------------------------------------------
# Precisão de milissegundos do app_time (Req. 8.3, 11.1)
# ---------------------------------------------------------------------------


def test_agora_utc_ms_tem_precisao_de_milissegundos() -> None:
    app_time, app_time_ms = agora_utc_ms()

    # Formato ISO-8601 UTC com milissegundos e sufixo Z (ex.: 2024-06-25T12:00:00.123Z).
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", app_time), (
        f"app_time fora do formato ISO-8601 UTC com ms: {app_time!r}"
    )
    assert isinstance(app_time_ms, int)
    assert app_time_ms > 0

    # app_time_ms e app_time devem referir-se ao mesmo instante (coerência de ms).
    milissegundos_no_texto = app_time.split(".")[1][:3]
    assert milissegundos_no_texto == f"{app_time_ms % 1000:03d}"


def test_registra_evento_cujo_app_time_tem_precisao_de_milissegundos() -> None:
    app_time, app_time_ms = agora_utc_ms()
    evento = Evento(
        order_id="pedido-ms",
        queue_name="lab-sqs-standard",
        event_type=EventType.RECEBIDA,
        event_source=EventSource.OBSERVADO,
        app_time=app_time,
        app_time_ms=app_time_ms,
        seq=1,
        receive_count=1,
    )
    tabela = TabelaDynamoDBFalsa()
    repositorio = HistoricoRepository(tabela)

    repositorio.registrar_evento(evento)

    item_gravado = tabela.itens[0]
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", item_gravado["app_time"]
    )
