"""Verificação de inicialização dos recursos AWS (Tarefa 9.1).

Antes de disponibilizar o **Dashboard**, o Sistema precisa confirmar que todos os
recursos AWS esperados — as quatro filas SQS (``FILAS``) e a **Tabela_Historico**
(``TABELA_HISTORICO``) do DynamoDB — **existem** e estão **acessíveis** pela
IAM_Role_EC2 (Req. 1.6). Este módulo implementa essa verificação como uma função
pura de orquestração, projetada para ser testável sem AWS real.

Princípios de design desta verificação (ver ``requirements.md`` e ``design.md``):

- **Somente leitura; nunca cria/modifica/exclui recursos.** A verificação usa
  apenas operações de leitura: ``get_queue_attributes`` (GetQueueAttributes) para
  as filas e um *describe* para a tabela DynamoDB. Nenhuma operação de escrita,
  criação ou exclusão é executada (Req. 1.5, 1.7, 1.9).
- **Preserva o erro original da AWS.** Quando um recurso está ausente ou
  inacessível, o erro original retornado pela AWS/boto3 é preservado — tanto no
  resultado estruturado quanto encadeado na exceção (``raise ... from``) — para que
  a camada superior possa registrá-lo (Req. 1.8).
- **Interrompe sem alterar recursos.** Ao encontrar o primeiro recurso
  indisponível, a verificação para imediatamente (*fail-fast*) e sinaliza a falha;
  como ela só lê, interromper não deixa nenhum recurso em estado alterado (Req. 1.8).
- **Identifica o recurso por nome e tipo em pt-BR.** O recurso indisponível é
  identificado pelo seu nome (ex.: ``lab-sqs-standard``) e pelo seu tipo em pt-BR
  (``fila SQS`` ou ``tabela DynamoDB``), e uma mensagem pronta para o Dashboard é
  produzida em português (Req. 1.8).
- **Injeção de dependências para testabilidade.** O ``SqsGateway`` e um
  *verificador de tabela DynamoDB* (callable) são injetados, permitindo testar com
  mocks/fakes — recursos presentes e ausentes — sem acessar a AWS.

Referências de requisitos: 1.5, 1.6, 1.7, 1.8, 1.9.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Sequence

from app.acesso.sqs_gateway import SqsGateway
from app.dominio import constantes as c


class TipoRecurso(str, Enum):
    """Tipo de recurso AWS verificado, com rótulo legível em pt-BR.

    O ``value`` é usado para identificar o recurso indisponível por tipo nas
    mensagens exibidas no Dashboard (Req. 1.8).
    """

    FILA_SQS = "fila SQS"
    TABELA_DYNAMODB = "tabela DynamoDB"


@dataclass(frozen=True)
class RecursoVerificado:
    """Resultado da verificação de um único recurso AWS.

    Atributos:
        nome: Nome do recurso (ex.: ``lab-sqs-standard`` ou ``lab-sqs-historico``).
        tipo: :class:`TipoRecurso` do recurso (fila SQS ou tabela DynamoDB).
        disponivel: ``True`` se o recurso existe e está acessível; ``False`` caso
            contrário.
        erro_original: Erro original retornado pela AWS/boto3 quando o recurso está
            indisponível; ``None`` quando disponível (Req. 1.8).
    """

    nome: str
    tipo: TipoRecurso
    disponivel: bool
    erro_original: Optional[BaseException] = None

    @property
    def descricao_pt_br(self) -> str:
        """Descreve o recurso por tipo e nome em pt-BR (ex.: ``fila SQS 'x'``)."""
        return f"{self.tipo.value} '{self.nome}'"


@dataclass(frozen=True)
class ResultadoVerificacao:
    """Resultado agregado da verificação de inicialização.

    Como a verificação é *fail-fast* (interrompe no primeiro recurso indisponível),
    ``recursos_verificados`` contém os recursos checados até o ponto da interrupção,
    terminando no recurso que falhou quando ``sucesso`` é ``False``.

    Atributos:
        sucesso: ``True`` somente se todos os recursos esperados estão disponíveis.
        recursos_verificados: Recursos checados, na ordem de verificação.
        recurso_indisponivel: O primeiro recurso indisponível encontrado, ou
            ``None`` quando todos estão disponíveis.
        mensagem_pt_br: Mensagem pronta para o Dashboard, em pt-BR, identificando o
            recurso indisponível por nome e tipo quando ``sucesso`` é ``False``
            (Req. 1.8).
    """

    sucesso: bool
    recursos_verificados: tuple[RecursoVerificado, ...] = field(default_factory=tuple)
    recurso_indisponivel: Optional[RecursoVerificado] = None
    mensagem_pt_br: str = ""

    @property
    def erro_original(self) -> Optional[BaseException]:
        """Erro original da AWS do recurso indisponível, se houver (Req. 1.8)."""
        if self.recurso_indisponivel is None:
            return None
        return self.recurso_indisponivel.erro_original


class FalhaVerificacaoInicializacao(Exception):
    """Sinaliza que a verificação de inicialização falhou (Req. 1.6, 1.8).

    Levantada por :func:`verificar_inicializacao` quando ``interromper_em_falha`` é
    ``True`` e algum recurso esperado está ausente ou inacessível. Interromper a
    inicialização impede a disponibilização do Dashboard antes de os recursos
    estarem prontos; como a verificação só executa leituras, a interrupção não
    altera nenhum recurso AWS (Req. 1.7, 1.8).

    O erro original da AWS é preservado em :attr:`erro_original` e encadeado via
    ``raise ... from`` para não perder a causa (Req. 1.8).

    Atributos:
        resultado: O :class:`ResultadoVerificacao` que descreve a falha, incluindo o
            recurso indisponível e a mensagem em pt-BR.
    """

    def __init__(self, resultado: ResultadoVerificacao) -> None:
        self.resultado = resultado
        super().__init__(resultado.mensagem_pt_br)

    @property
    def erro_original(self) -> Optional[BaseException]:
        """Atalho para o erro original da AWS do recurso indisponível."""
        return self.resultado.erro_original


# Assinatura do verificador de tabela DynamoDB injetável. Recebe o nome da tabela e
# executa uma leitura (describe) que levanta exceção se a tabela não existir ou não
# estiver acessível. Não deve criar, modificar nem excluir a tabela (Req. 1.7).
VerificadorTabela = Callable[[str], Any]


def _mensagem_indisponivel_pt_br(recurso: RecursoVerificado) -> str:
    """Monta a mensagem em pt-BR para um recurso indisponível (Req. 1.8).

    Identifica o recurso por nome e tipo e inclui o detalhe do erro original da AWS
    quando disponível, para orientar o estudante a corrigir a configuração manual no
    Console AWS.
    """
    base = (
        "Inicialização interrompida: o recurso "
        f"{recurso.descricao_pt_br} está ausente ou inacessível. "
        "Verifique se ele foi criado manualmente no Console AWS (região "
        f"{c.REGIAO_AWS}) e se a IAM Role da EC2 tem permissão de acesso."
    )
    if recurso.erro_original is not None:
        base += f" Detalhe do erro original da AWS: {recurso.erro_original!r}."
    return base


def verificar_fila(gateway: SqsGateway, nome_fila: str) -> RecursoVerificado:
    """Verifica a existência e a acessibilidade de uma fila SQS (somente leitura).

    Usa :meth:`SqsGateway.get_queue_attributes` (GetQueueAttributes), que resolve a
    URL da fila (GetQueueUrl) e lê seus atributos. Ambas são operações de leitura:
    nenhuma criação, modificação ou exclusão ocorre (Req. 1.7). Se a fila não existe
    ou a IAM Role não tem acesso, a AWS levanta uma exceção, que é capturada e
    preservada no resultado — sem ser mascarada (Req. 1.8).

    Args:
        gateway: Fachada do SQS (injetável; em testes, com cliente simulado).
        nome_fila: Nome da fila a verificar (ex.: ``lab-sqs-standard``).

    Returns:
        :class:`RecursoVerificado` indicando disponibilidade e, em falha, o erro
        original da AWS.
    """
    try:
        gateway.get_queue_attributes(nome_fila)
    except Exception as erro:  # noqa: BLE001 — preservamos o erro original (Req. 1.8)
        return RecursoVerificado(
            nome=nome_fila,
            tipo=TipoRecurso.FILA_SQS,
            disponivel=False,
            erro_original=erro,
        )
    return RecursoVerificado(
        nome=nome_fila, tipo=TipoRecurso.FILA_SQS, disponivel=True
    )


def verificar_tabela(
    verificador_tabela: VerificadorTabela, nome_tabela: str
) -> RecursoVerificado:
    """Verifica a existência e a acessibilidade da Tabela_Historico (somente leitura).

    Delega a um ``verificador_tabela`` injetável, que deve executar uma leitura do
    tipo *describe* (ex.: ``DescribeTable``) e levantar exceção quando a tabela não
    existe ou não está acessível. O verificador **não** deve criar, modificar nem
    excluir a tabela (Req. 1.7). O erro original é preservado no resultado (Req. 1.8).

    Args:
        verificador_tabela: Callable que recebe o nome da tabela e faz o describe.
        nome_tabela: Nome da Tabela_Historico (ex.: ``lab-sqs-historico``).

    Returns:
        :class:`RecursoVerificado` indicando disponibilidade e, em falha, o erro
        original da AWS.
    """
    try:
        verificador_tabela(nome_tabela)
    except Exception as erro:  # noqa: BLE001 — preservamos o erro original (Req. 1.8)
        return RecursoVerificado(
            nome=nome_tabela,
            tipo=TipoRecurso.TABELA_DYNAMODB,
            disponivel=False,
            erro_original=erro,
        )
    return RecursoVerificado(
        nome=nome_tabela, tipo=TipoRecurso.TABELA_DYNAMODB, disponivel=True
    )


def descrever_tabela_boto3(cliente_dynamodb: Any) -> VerificadorTabela:
    """Cria um :data:`VerificadorTabela` baseado em um cliente boto3 do DynamoDB.

    Produz um callable que chama ``describe_table`` (operação **somente leitura** do
    DynamoDB) para confirmar a existência e a acessibilidade da tabela. Em produção,
    passe ``boto3.client("dynamodb", region_name=REGIAO_AWS)``; o boto3 resolve as
    credenciais pela IAM Role da EC2, sem Access Keys (Req. 12.1, 12.2). Qualquer
    erro da AWS (ex.: ``ResourceNotFoundException``, acesso negado) propaga intacto
    para ser preservado por :func:`verificar_tabela` (Req. 1.8).

    Args:
        cliente_dynamodb: Cliente boto3 do DynamoDB (ou objeto compatível com
            ``describe_table(TableName=...)``).

    Returns:
        Um callable ``(nome_tabela) -> resposta`` adequado a
        :func:`verificar_inicializacao`.
    """

    def _verificar(nome_tabela: str) -> Any:
        # describe_table é somente leitura: não cria/modifica/exclui (Req. 1.7).
        return cliente_dynamodb.describe_table(TableName=nome_tabela)

    return _verificar


def verificar_inicializacao(
    gateway: SqsGateway,
    verificador_tabela: VerificadorTabela,
    *,
    filas: Sequence[str] = c.FILAS,
    nome_tabela: str = c.TABELA_HISTORICO,
    interromper_em_falha: bool = True,
) -> ResultadoVerificacao:
    """Verifica existência e acessibilidade dos recursos AWS antes do Dashboard.

    Checa, **somente por leitura** e em ordem determinística, as filas em ``filas``
    (via :func:`verificar_fila`) e a Tabela_Historico ``nome_tabela`` (via
    :func:`verificar_tabela`). A verificação é *fail-fast*: para no **primeiro**
    recurso indisponível, pois não há motivo para seguir — e, por só ler, interromper
    não altera nenhum recurso (Req. 1.6, 1.7, 1.8).

    Em caso de recurso indisponível, o resultado identifica o recurso por **nome e
    tipo** (fila SQS / tabela DynamoDB), preserva o **erro original** da AWS e produz
    uma **mensagem em pt-BR** pronta para o Dashboard (Req. 1.8). Quando
    ``interromper_em_falha`` é ``True`` (padrão), levanta
    :class:`FalhaVerificacaoInicializacao` para impedir a disponibilização do
    Dashboard; quando ``False``, apenas devolve o resultado com ``sucesso=False``.

    A aplicação **nunca** cria, modifica ou exclui recursos aqui: todas as operações
    são leituras (Req. 1.5, 1.7, 1.9).

    Args:
        gateway: :class:`SqsGateway` injetável (em testes, com cliente simulado).
        verificador_tabela: Callable que faz o *describe* da tabela DynamoDB
            (injetável; use :func:`descrever_tabela_boto3` em produção).
        filas: Nomes das filas a verificar (padrão: as quatro filas em ``FILAS``).
        nome_tabela: Nome da Tabela_Historico (padrão ``TABELA_HISTORICO``).
        interromper_em_falha: Se ``True`` (padrão), levanta
            :class:`FalhaVerificacaoInicializacao` ao primeiro recurso indisponível;
            se ``False``, retorna o resultado sem levantar exceção.

    Returns:
        :class:`ResultadoVerificacao` com ``sucesso=True`` quando todos os recursos
        estão disponíveis.

    Raises:
        FalhaVerificacaoInicializacao: quando há recurso indisponível e
            ``interromper_em_falha`` é ``True``. O erro original fica em
            :attr:`FalhaVerificacaoInicializacao.erro_original`.
    """
    verificados: list[RecursoVerificado] = []

    def _falha(recurso: RecursoVerificado) -> ResultadoVerificacao:
        """Monta o resultado de falha e, se configurado, interrompe (Req. 1.8)."""
        resultado = ResultadoVerificacao(
            sucesso=False,
            recursos_verificados=tuple(verificados),
            recurso_indisponivel=recurso,
            mensagem_pt_br=_mensagem_indisponivel_pt_br(recurso),
        )
        if interromper_em_falha:
            # Encadeia o erro original da AWS para não perder a causa (Req. 1.8).
            raise FalhaVerificacaoInicializacao(resultado) from recurso.erro_original
        return resultado

    # 1) Filas SQS (as quatro filas esperadas), na ordem de FILAS.
    for nome_fila in filas:
        recurso = verificar_fila(gateway, nome_fila)
        verificados.append(recurso)
        if not recurso.disponivel:
            return _falha(recurso)

    # 2) Tabela_Historico (DynamoDB).
    recurso_tabela = verificar_tabela(verificador_tabela, nome_tabela)
    verificados.append(recurso_tabela)
    if not recurso_tabela.disponivel:
        return _falha(recurso_tabela)

    # Todos os recursos estão disponíveis e acessíveis via IAM Role (Req. 1.6).
    return ResultadoVerificacao(
        sucesso=True,
        recursos_verificados=tuple(verificados),
        recurso_indisponivel=None,
        mensagem_pt_br="Todos os recursos AWS esperados estão disponíveis e acessíveis.",
    )
