"""Camada de acesso ao DynamoDB sobre a Tabela_Historico (HistoricoRepository).

Este módulo encapsula o acesso à **Tabela_Historico** (DynamoDB), modelada como um
log de eventos *append-only* (ver seção *Data Models* do design.md). A classe
:class:`HistoricoRepository` recebe um recurso/tabela boto3 **injetável** para que:

- a produção use ``boto3.resource("dynamodb", region_name=REGIAO_AWS).Table(TABELA_HISTORICO)``;
- os testes injetem um *fake* de DynamoDB em memória, sem depender da AWS real.

A tarefa 7.1 implementou o método :meth:`HistoricoRepository.registrar_evento`
(PutItem com até ``MAX_TENTATIVAS_ESCRITA_HISTORICO`` tentativas em falhas
transitórias). A tarefa 7.3 estende esta mesma classe com as **consultas**:

- :meth:`HistoricoRepository.consultar_por_pedido` — Query na tabela base por
  ``PK = PEDIDO#<order_id>`` (Req. 11.3, 11.5).
- :meth:`HistoricoRepository.consultar_por_fila` — Query no ``GSI1`` por
  ``GSI1PK = FILA#<queue_name>`` (Req. 11.4, 11.5).
- :meth:`HistoricoRepository.listar_eventos_cronologico` — eventos de um pedido em
  ordem cronológica crescente pelo horário real da aplicação (Req. 11.6).
- :meth:`HistoricoRepository.contar_tentativas` — contagem de tentativas como um
  inteiro ≥ 0 (Req. 11.7).
- :meth:`HistoricoRepository.pedidos_em_falha` — identifica os pedidos cujos eventos
  indiquem estado de falha, derivando o Estado pelo StateDeriver (Req. 11.8).

Todas as consultas retornam **resultado vazio sem erro** quando não há registros
(Req. 11.5), paginam de forma transparente (seguindo ``LastEvaluatedKey``) e usam
:meth:`Evento.de_item` para desserializar os itens.

Referências de requisitos:
- 8.3  — registrar o horário real da aplicação com precisão de milissegundos.
- 11.1 — registrar o evento em até 5 s, com ``app_time`` com precisão de ms.
- 11.2 — repetir a escrita até 3 vezes; ao esgotar, preservar a mensagem original e
         sinalizar o erro, sem exceder 3 tentativas.
- 11.3 — consulta por identificador de pedido retorna os eventos associados.
- 11.4 — consulta por fila (GSI1) retorna as mensagens/eventos associados.
- 11.5 — consulta sem registros retorna resultado vazio sem erro.
- 11.6 — eventos listados em ordem cronológica crescente pelo horário da aplicação.
- 11.7 — contagem de tentativas como inteiro ≥ 0.
- 11.8 — identificação das mensagens cujos eventos indiquem estado de falha.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol

from app.dominio.constantes import (
    INDICE_GSI1,
    MAX_RECEIVE_COUNT,
    MAX_TENTATIVAS_ESCRITA_HISTORICO,
    REGIAO_AWS,
    TABELA_HISTORICO,
)
from app.dominio.evento import (
    Evento,
    construir_gsi1pk,
    construir_pk,
)
from app.dominio.ordenacao import ordenar_cronologicamente
from app.dominio.state_deriver import derivar_estado
from app.dominio.tipos import Estado, EventType

# ---------------------------------------------------------------------------
# Classificação de erros transitórios do DynamoDB
# ---------------------------------------------------------------------------
# Em falhas transitórias (throttling, erro interno momentâneo, indisponibilidade),
# repetir a escrita tende a ter sucesso. Em falhas permanentes (validação, recurso
# inexistente, acesso negado), repetir é inútil e só mascara o erro — por isso elas
# NÃO são repetidas. A lista abaixo cobre os códigos transitórios conhecidos do
# DynamoDB (Req. 11.2).
CODIGOS_ERRO_TRANSITORIO: frozenset[str] = frozenset(
    {
        "ProvisionedThroughputExceededException",
        "ThrottlingException",
        "ThrottlingError",
        "RequestLimitExceeded",
        "InternalServerError",
        "ServiceUnavailable",
        "ServiceUnavailableException",
        "TransactionInProgressException",
    }
)


class FalhaRegistroEvento(Exception):
    """Sinaliza que a escrita de um evento na Tabela_Historico não foi concluída.

    Levantada quando o PutItem esgota as ``MAX_TENTATIVAS_ESCRITA_HISTORICO``
    tentativas em falhas transitórias, ou quando ocorre uma falha permanente que não
    deve ser repetida. Em ambos os casos o **erro original** da AWS/boto3 é preservado
    no atributo :attr:`erro_original` e encadeado via ``raise ... from`` (Req. 11.2).

    Atributos:
        evento: O :class:`Evento` cuja escrita falhou (mensagem original preservada).
        tentativas: Quantidade de tentativas efetivamente executadas (nunca excede
            ``MAX_TENTATIVAS_ESCRITA_HISTORICO``).
        erro_original: A exceção original retornada pela AWS/boto3.
    """

    def __init__(
        self, evento: Evento, tentativas: int, erro_original: BaseException
    ) -> None:
        self.evento = evento
        self.tentativas = tentativas
        self.erro_original = erro_original
        super().__init__(
            "Falha ao registrar evento na Tabela_Historico "
            f"(pedido={evento.order_id!r}, tipo={evento.event_type.value!r}) "
            f"após {tentativas} tentativa(s): {erro_original!r}"
        )


class TabelaDynamoDB(Protocol):
    """Contrato mínimo da tabela DynamoDB usado por :class:`HistoricoRepository`.

    Corresponde ao objeto ``Table`` do recurso boto3 do DynamoDB. Declarar apenas os
    métodos usados mantém o acoplamento baixo e permite injetar um *fake* em memória
    nos testes (sem AWS real).
    """

    def put_item(self, **kwargs: Any) -> dict[str, Any]:
        """Escreve um item na tabela (operação PutItem do DynamoDB)."""
        ...

    def query(self, **kwargs: Any) -> dict[str, Any]:
        """Consulta itens por condição de chave (operação Query do DynamoDB).

        Aceita os mesmos parâmetros nomeados do ``Table.query`` do boto3 usados
        pelas consultas desta classe: ``KeyConditionExpression`` (construído com
        ``boto3.dynamodb.conditions.Key``), ``IndexName`` (para consultar o GSI1),
        ``ScanIndexForward`` (ordenação pela Sort Key) e ``ExclusiveStartKey``
        (paginação). O retorno segue o formato do boto3: um ``dict`` com a chave
        ``Items`` (lista de itens) e, havendo mais páginas, ``LastEvaluatedKey``.
        """
        ...


def agora_utc_ms() -> tuple[str, int]:
    """Retorna o instante atual em UTC com precisão de milissegundos.

    Produz o par ``(app_time, app_time_ms)`` usado pelos eventos:

    - ``app_time``: string ISO-8601 em UTC com milissegundos e sufixo ``Z`` (ex.:
      ``"2024-06-25T12:00:00.123Z"``), coerente com os exemplos do design.
    - ``app_time_ms``: epoch em milissegundos (inteiro), usado na SK e na ordenação.

    A precisão é truncada a milissegundos (não microssegundos) para atender aos
    Req. 8.3 e 11.1.

    Returns:
        Tupla ``(app_time, app_time_ms)``.
    """
    agora = datetime.now(timezone.utc)
    app_time_ms = int(agora.timestamp() * 1000)
    # isoformat com milissegundos: usa timespec="milliseconds" e troca +00:00 por Z.
    app_time = agora.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return app_time, app_time_ms


def _e_erro_transitorio(erro: BaseException) -> bool:
    """Indica se um erro de escrita é transitório (vale a pena repetir).

    - ``ClientError`` do DynamoDB: transitório apenas se o código de erro constar em
      :data:`CODIGOS_ERRO_TRANSITORIO` (ex.: throttling, erro interno).
    - Erros de conexão/rede do botocore (``BotoCoreError``, ex.:
      ``EndpointConnectionError``, ``ConnectTimeoutError``): sempre transitórios.
    - Qualquer outra exceção: permanente (não repetir).

    A verificação é feita por nome de classe/estrutura para evitar importar o
    ``botocore`` em tempo de módulo (mantém o módulo testável com *fakes* que
    levantam exceções equivalentes).
    """
    # ClientError expõe response["Error"]["Code"].
    resposta = getattr(erro, "response", None)
    if isinstance(resposta, dict):
        codigo = resposta.get("Error", {}).get("Code")
        if codigo is not None:
            return codigo in CODIGOS_ERRO_TRANSITORIO

    # Erros de conexão/rede do botocore são transitórios por natureza.
    nomes_das_bases = {base.__name__ for base in type(erro).__mro__}
    if "BotoCoreError" in nomes_das_bases:
        return True

    return False


class HistoricoRepository:
    """Repositório de eventos sobre a Tabela_Historico (DynamoDB).

    Encapsula o acesso à Tabela_Historico. A tabela/cliente boto3 é **injetada** no
    construtor, o que torna a classe testável com um *fake* em memória e facilita a
    adição das consultas na tarefa 7.3 (que usarão a mesma tabela e o nome do GSI1).

    Exemplo de uso em produção::

        import boto3
        tabela = boto3.resource("dynamodb", region_name=REGIAO_AWS).Table(TABELA_HISTORICO)
        repositorio = HistoricoRepository(tabela)
        repositorio.registrar_evento(evento)
    """

    def __init__(
        self,
        tabela: TabelaDynamoDB,
        *,
        nome_tabela: str = TABELA_HISTORICO,
        nome_indice_gsi1: str = INDICE_GSI1,
        regiao: str = REGIAO_AWS,
        max_tentativas: int = MAX_TENTATIVAS_ESCRITA_HISTORICO,
    ) -> None:
        """Inicializa o repositório com a tabela DynamoDB injetável.

        Args:
            tabela: Objeto ``Table`` do boto3 (ou *fake* compatível com
                :class:`TabelaDynamoDB`). É a única dependência externa.
            nome_tabela: Nome da Tabela_Historico (guardado para diagnóstico e para as
                consultas da tarefa 7.3).
            nome_indice_gsi1: Nome do índice ``GSI1`` (usado pelas consultas futuras).
            regiao: Região AWS do laboratório (guardada para diagnóstico).
            max_tentativas: Número máximo de tentativas de escrita (Req. 11.2). O
                padrão é ``MAX_TENTATIVAS_ESCRITA_HISTORICO`` (3); nunca é excedido.
        """
        self._tabela = tabela
        self._nome_tabela = nome_tabela
        self._nome_indice_gsi1 = nome_indice_gsi1
        self._regiao = regiao
        self._max_tentativas = max_tentativas

    def registrar_evento(self, evento: Evento) -> None:
        """Registra um evento na Tabela_Historico com retry em falhas transitórias.

        Executa o PutItem com **no máximo** ``max_tentativas`` tentativas (padrão 3,
        de :data:`MAX_TENTATIVAS_ESCRITA_HISTORICO`). O comportamento (Req. 11.2):

        - **Sucesso:** retorna ``None`` assim que o PutItem conclui.
        - **Falha transitória** (throttling, erro interno, falha de rede): repete até
          o limite de tentativas. Ao **esgotar** as tentativas sem sucesso, levanta
          :class:`FalhaRegistroEvento` preservando o evento original (a "mensagem")
          e o **erro original** da AWS, sem exceder o número de tentativas.
        - **Falha permanente** (validação, recurso inexistente, acesso negado):
          **não** repete — levanta :class:`FalhaRegistroEvento` imediatamente,
          também preservando o erro original. Isso evita mascarar erros de
          configuração com tentativas inúteis.

        O item escrito vem de :meth:`Evento.para_item`, cujo ``app_time`` já tem
        precisão de milissegundos (ver :func:`agora_utc_ms`) — Req. 8.3, 11.1.

        Args:
            evento: O :class:`Evento` a persistir.

        Raises:
            FalhaRegistroEvento: quando a escrita não é concluída (falha permanente ou
                esgotamento das tentativas em falhas transitórias). O erro original
                fica em :attr:`FalhaRegistroEvento.erro_original`.
        """
        item = evento.para_item()
        ultimo_erro: BaseException | None = None
        tentativas = 0

        while tentativas < self._max_tentativas:
            tentativas += 1
            try:
                self._tabela.put_item(Item=item)
                return
            except Exception as erro:  # noqa: BLE001 — classificamos a seguir
                ultimo_erro = erro
                # Falha permanente: repetir não resolve. Interrompe de imediato,
                # preservando o erro original (sem gastar tentativas à toa).
                if not _e_erro_transitorio(erro):
                    raise FalhaRegistroEvento(evento, tentativas, erro) from erro
                # Falha transitória: continua o laço até o limite de tentativas.

        # Esgotou as tentativas em falhas transitórias: preserva o erro original e
        # sinaliza a falha, garantindo que nunca houve mais de `max_tentativas` (3).
        assert ultimo_erro is not None  # garantido pelo laço acima
        raise FalhaRegistroEvento(evento, tentativas, ultimo_erro) from ultimo_erro

    # -- Consultas (tarefa 7.3) --------------------------------------------

    def _consultar_itens(
        self, parametros_base: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Executa uma Query paginada e devolve todos os itens.

        O DynamoDB limita o tamanho de cada página de ``Query`` e sinaliza a
        existência de mais dados por meio de ``LastEvaluatedKey``. Este helper
        encapsula a iteração de paginação: repete a Query acrescentando
        ``ExclusiveStartKey`` enquanto houver uma próxima página, acumulando os
        itens de todas as páginas.

        Quando a consulta não encontra registros, o DynamoDB retorna ``Items``
        vazio (ou ausente) e **sem** ``LastEvaluatedKey``; portanto este método
        devolve uma lista vazia naturalmente, sem erro (Req. 11.5).

        Args:
            parametros_base: parâmetros nomeados da Query (ex.:
                ``KeyConditionExpression``, ``IndexName``, ``ScanIndexForward``),
                sem a chave de paginação, que é gerenciada aqui.

        Returns:
            A lista de itens (``dict``) de todas as páginas, na ordem retornada
            pelo DynamoDB.
        """
        itens: list[dict[str, Any]] = []
        chave_inicial: dict[str, Any] | None = None

        while True:
            parametros = dict(parametros_base)
            if chave_inicial is not None:
                parametros["ExclusiveStartKey"] = chave_inicial

            resposta = self._tabela.query(**parametros)
            itens.extend(resposta.get("Items", []) or [])

            chave_inicial = resposta.get("LastEvaluatedKey")
            if not chave_inicial:
                break

        return itens

    def consultar_por_pedido(self, order_id: str) -> list[Evento]:
        """Retorna os eventos de um pedido (Query na tabela base por ``PK``).

        Consulta a tabela base por ``PK = PEDIDO#<order_id>``, retornando todos os
        eventos daquele pedido. A ordenação segue a Sort Key crescente
        (``EVENTO#<app_time_ms>#<seq>``), que coincide com a ordem cronológica.
        Quando não há eventos para o pedido, devolve uma lista vazia **sem erro**
        (Req. 11.3, 11.5).

        Args:
            order_id: Identificador do pedido a consultar.

        Returns:
            Lista de :class:`Evento` do pedido (vazia se não houver registros).
        """
        # Importação adiada do boto3 para não acoplar o módulo à AWS em tempo de
        # import (mantém a camada de domínio e os testes leves).
        from boto3.dynamodb.conditions import Key

        parametros = {
            "KeyConditionExpression": Key("PK").eq(construir_pk(order_id)),
            # Sort Key crescente = ordem cronológica (Req. 6.1, 11.6).
            "ScanIndexForward": True,
        }
        itens = self._consultar_itens(parametros)
        return [Evento.de_item(item) for item in itens]

    def consultar_por_fila(self, queue_name: str) -> list[Evento]:
        """Retorna os eventos de uma fila (Query no ``GSI1`` por ``GSI1PK``).

        Consulta o índice ``GSI1`` por ``GSI1PK = FILA#<queue_name>``, retornando
        os eventos de todas as mensagens associadas àquela fila, ordenados pela
        GSI1 Sort Key crescente (``<app_time_ms>#<order_id>``). Quando a fila não
        tem registros, devolve uma lista vazia **sem erro** (Req. 11.4, 11.5).

        Args:
            queue_name: Nome da fila a consultar (ex.: ``lab-sqs-standard``).

        Returns:
            Lista de :class:`Evento` da fila (vazia se não houver registros).
        """
        from boto3.dynamodb.conditions import Key

        parametros = {
            "IndexName": self._nome_indice_gsi1,
            "KeyConditionExpression": Key("GSI1PK").eq(construir_gsi1pk(queue_name)),
            "ScanIndexForward": True,
        }
        itens = self._consultar_itens(parametros)
        return [Evento.de_item(item) for item in itens]

    def listar_eventos_cronologico(self, order_id: str) -> list[Evento]:
        """Lista os eventos de um pedido em ordem cronológica crescente.

        Reaproveita :meth:`consultar_por_pedido` e aplica
        :func:`app.dominio.ordenacao.ordenar_cronologicamente` para garantir a
        ordenação não-decrescente por ``app_time_ms`` com desempate estável por
        ``seq`` — independentemente da ordem em que o DynamoDB devolveu os itens
        (Req. 11.6). Pedido sem eventos resulta em lista vazia sem erro (Req. 11.5).

        Args:
            order_id: Identificador do pedido a listar.

        Returns:
            Lista de :class:`Evento` ordenada cronologicamente (crescente).
        """
        eventos = self.consultar_por_pedido(order_id)
        return ordenar_cronologicamente(eventos)

    def contar_tentativas(self, order_id: str) -> int:
        """Retorna a contagem de tentativas (recebimentos) de um pedido.

        A contagem corresponde ao maior ``receive_count`` entre os eventos de
        recebimento (``recebida``) do pedido, que reflete o
        ``ApproximateReceiveCount`` acumulado da mensagem no SQS. O resultado é
        sempre um inteiro **maior ou igual a 0** (Req. 11.7): um pedido sem eventos
        de recebimento (ou sem eventos nenhum) retorna 0, sem erro (Req. 11.5).

        Args:
            order_id: Identificador do pedido.

        Returns:
            A contagem de tentativas (inteiro ≥ 0).
        """
        eventos = self.consultar_por_pedido(order_id)
        contagens = [
            evento.receive_count
            for evento in eventos
            if evento.event_type is EventType.RECEBIDA
            and evento.receive_count is not None
            and evento.receive_count >= 0
        ]
        return max(contagens, default=0)

    def pedidos_em_falha(
        self,
        queue_name: str,
        *,
        max_receive_count: int = MAX_RECEIVE_COUNT,
    ) -> list[str]:
        """Identifica os pedidos de uma fila cujo Estado derivado é ``Falha``.

        Consulta os eventos da fila (:meth:`consultar_por_fila`), agrupa-os por
        ``order_id`` e deriva o Estado de cada pedido com o
        :func:`app.dominio.state_deriver.derivar_estado`. Retorna os ``order_id``
        cujo Estado derivado é :attr:`Estado.FALHA` (Req. 11.8).

        O Estado é sempre **derivado** dos eventos, nunca lido de um campo mutável.
        Por isso a identificação de falha delega ao StateDeriver, preservando as
        regras de precedência (ex.: um pedido que falhou mas já foi ``Processada``
        não é considerado em falha). Filas sem registros resultam em lista vazia,
        sem erro (Req. 11.5).

        Args:
            queue_name: Nome da fila a inspecionar.
            max_receive_count: Limite da Redrive_Policy repassado ao StateDeriver
                (padrão :data:`MAX_RECEIVE_COUNT`).

        Returns:
            Lista de ``order_id`` em estado de falha, sem duplicatas, na ordem de
            primeira ocorrência dos pedidos na fila.
        """
        eventos = self.consultar_por_fila(queue_name)

        # Agrupa eventos por pedido preservando a ordem de primeira aparição.
        eventos_por_pedido: dict[str, list[Evento]] = {}
        for evento in eventos:
            eventos_por_pedido.setdefault(evento.order_id, []).append(evento)

        em_falha: list[str] = []
        for order_id, eventos_do_pedido in eventos_por_pedido.items():
            ordenados = ordenar_cronologicamente(eventos_do_pedido)
            estado = derivar_estado(ordenados, max_receive_count=max_receive_count)
            if estado is Estado.FALHA:
                em_falha.append(order_id)
        return em_falha
