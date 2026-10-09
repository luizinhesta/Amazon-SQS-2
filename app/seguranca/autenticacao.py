"""Controle de autenticação do Dashboard com bloqueio por origem (Tarefa 14.1).

Este módulo concentra a lógica de segurança da camada de autenticação do Dashboard
e o tratamento de falhas de autorização via IAM_Role_EC2. Toda a lógica é **pura e
testável**: o controlador de tentativas por origem recebe um **relógio injetável**
(``relogio: Callable[[], float]`` que devolve segundos monotônicos), de modo que a
expiração do bloqueio possa ser verificada em testes sem ``sleep`` real.

Regras implementadas:

- Autenticação exigida quando o Dashboard está exposto à internet; o acesso só é
  concedido mediante usuário/senha válidos (Req. 12.5).
- Bloqueio por origem após ``MAX_TENTATIVAS_AUTENTICACAO`` (5) falhas de autenticação
  **consecutivas**, por ``BLOQUEIO_AUTENTICACAO_SEGUNDOS`` (300) segundos; durante o
  bloqueio, novas tentativas são recusadas com indicação de bloqueio (Req. 12.6).
- Um sucesso **antes** de atingir o limite reinicia a contagem de falhas (Req. 12.6).
- Restrição de origens: conexões de origens não explicitamente autorizadas são
  rejeitadas (Req. 12.7).
- Falha de autorização via IAM_Role_EC2 interrompe a operação, registra o evento e
  expõe erro de autorização **sem persistir dados parciais** (Req. 12.9).

As constantes ``MAX_TENTATIVAS_AUTENTICACAO`` e ``BLOQUEIO_AUTENTICACAO_SEGUNDOS``
são lidas de :mod:`app.dominio.constantes` (não são redefinidas aqui).

Referências de requisitos: 12.5, 12.6, 12.7, 12.9.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from enum import Enum

from app.dominio import constantes as c


# ---------------------------------------------------------------------------
# Resultados da avaliação de autenticação
# ---------------------------------------------------------------------------


class ResultadoAutenticacao(str, Enum):
    """Desfecho possível de uma tentativa de autenticação por uma origem.

    - SUCESSO: credenciais válidas; a contagem de falhas da origem é reiniciada.
    - FALHA: credenciais inválidas; a contagem de falhas da origem é incrementada.
    - BLOQUEADA: a origem está em período de bloqueio e a tentativa foi recusada
      sem avaliar as credenciais.
    - ORIGEM_NAO_AUTORIZADA: a origem não consta entre as origens autorizadas e a
      conexão foi rejeitada (Req. 12.7).
    """

    SUCESSO = "SUCESSO"
    FALHA = "FALHA"
    BLOQUEADA = "BLOQUEADA"
    ORIGEM_NAO_AUTORIZADA = "ORIGEM_NAO_AUTORIZADA"


@dataclass(frozen=True)
class AvaliacaoAutenticacao:
    """Resultado imutável da avaliação de uma tentativa de autenticação.

    Attributes:
        resultado: o desfecho da tentativa (ver :class:`ResultadoAutenticacao`).
        origem: a origem avaliada (ex.: endereço IP ou identificador do cliente).
        falhas_consecutivas: número de falhas consecutivas acumuladas após esta
            avaliação (0 em caso de sucesso).
        bloqueada: ``True`` se, após esta avaliação, a origem está bloqueada.
        segundos_restantes_bloqueio: segundos restantes do bloqueio quando
            ``bloqueada`` é ``True``; 0 caso contrário.
        mensagem: mensagem em pt-BR descrevendo o desfecho para exibição no
            Dashboard.
    """

    resultado: ResultadoAutenticacao
    origem: str
    falhas_consecutivas: int
    bloqueada: bool
    segundos_restantes_bloqueio: int
    mensagem: str

    @property
    def acesso_concedido(self) -> bool:
        """Indica se o acesso foi concedido (apenas no caso de sucesso)."""
        return self.resultado is ResultadoAutenticacao.SUCESSO


@dataclass
class _EstadoOrigem:
    """Estado interno de tentativas de uma origem (uso privado do controlador).

    Attributes:
        falhas_consecutivas: contagem de falhas desde o último sucesso ou expiração.
        bloqueada_ate: instante (no relógio injetado) em que o bloqueio expira;
            ``None`` quando a origem não está bloqueada.
    """

    falhas_consecutivas: int = 0
    bloqueada_ate: float | None = None


class ControladorAutenticacao:
    """Controla tentativas de autenticação por origem com bloqueio temporário.

    A lógica é determinística e sem efeitos colaterais externos: o único acoplamento
    temporal é o ``relogio`` injetado, o que permite testar a expiração do bloqueio
    sem ``sleep`` real. O estado é mantido em memória, por origem.

    A instância restringe o acesso às ``origens_autorizadas`` quando informadas; se o
    conjunto for vazio/omitido, nenhuma origem é considerada autorizada por padrão —
    comportamento condizente com "restringir às origens autorizadas" (Req. 12.7).

    Args:
        origens_autorizadas: coleção de origens explicitamente autorizadas a tentar
            autenticar. Quando omitida, inicia vazia e pode ser preenchida via
            :meth:`autorizar_origem`.
        max_tentativas: número de falhas consecutivas que dispara o bloqueio.
            Padrão: ``MAX_TENTATIVAS_AUTENTICACAO`` (5).
        bloqueio_segundos: duração do bloqueio em segundos.
            Padrão: ``BLOQUEIO_AUTENTICACAO_SEGUNDOS`` (300).
        relogio: função sem argumentos que devolve o tempo atual em segundos
            (monotônico). Padrão: :func:`time.monotonic`. Injete nos testes para
            controlar a passagem do tempo.
    """

    def __init__(
        self,
        origens_autorizadas: Iterable[str] | None = None,
        *,
        max_tentativas: int = c.MAX_TENTATIVAS_AUTENTICACAO,
        bloqueio_segundos: int = c.BLOQUEIO_AUTENTICACAO_SEGUNDOS,
        relogio: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_tentativas < 1:
            raise ValueError("max_tentativas deve ser maior ou igual a 1.")
        if bloqueio_segundos < 0:
            raise ValueError("bloqueio_segundos não pode ser negativo.")

        self._max_tentativas = max_tentativas
        self._bloqueio_segundos = bloqueio_segundos
        self._relogio = relogio
        self._origens_autorizadas: set[str] = set(origens_autorizadas or ())
        self._estado_por_origem: dict[str, _EstadoOrigem] = {}

    # -- Gestão de origens autorizadas (Req. 12.7) --------------------------

    def autorizar_origem(self, origem: str) -> None:
        """Adiciona ``origem`` ao conjunto de origens autorizadas."""
        self._origens_autorizadas.add(origem)

    def revogar_origem(self, origem: str) -> None:
        """Remove ``origem`` do conjunto de origens autorizadas, se presente."""
        self._origens_autorizadas.discard(origem)

    def origem_autorizada(self, origem: str) -> bool:
        """Retorna ``True`` se ``origem`` está entre as origens autorizadas."""
        return origem in self._origens_autorizadas

    # -- Consulta de bloqueio ----------------------------------------------

    def esta_bloqueada(self, origem: str) -> bool:
        """Indica se ``origem`` está atualmente bloqueada (bloqueio não expirado)."""
        return self.segundos_restantes_bloqueio(origem) > 0

    def segundos_restantes_bloqueio(self, origem: str) -> int:
        """Segundos restantes do bloqueio de ``origem`` (0 se não bloqueada).

        O resultado é arredondado para cima em segundos inteiros, de modo que um
        bloqueio ainda ativo nunca seja reportado como 0.
        """
        estado = self._estado_por_origem.get(origem)
        if estado is None or estado.bloqueada_ate is None:
            return 0
        restante = estado.bloqueada_ate - self._relogio()
        if restante <= 0:
            return 0
        # Arredonda para cima: frações de segundo contam como 1 segundo restante.
        return int(restante) + (1 if restante != int(restante) else 0)

    # -- Avaliação de uma tentativa ----------------------------------------

    def avaliar_tentativa(
        self, origem: str, credenciais_validas: bool
    ) -> AvaliacaoAutenticacao:
        """Avalia uma tentativa de autenticação de ``origem``.

        A ordem de verificação é:

        1. Origem autorizada? Caso contrário, rejeita (Req. 12.7).
        2. Origem bloqueada e bloqueio ainda vigente? Recusa sem avaliar as
           credenciais (Req. 12.6).
        3. Bloqueio expirado? Reinicia o estado antes de avaliar.
        4. Credenciais válidas? Sucesso reinicia a contagem (Req. 12.6);
           falha incrementa e, ao atingir ``max_tentativas`` consecutivas,
           ativa o bloqueio por ``bloqueio_segundos`` (Req. 12.6).

        Args:
            origem: identificador da origem (ex.: endereço IP).
            credenciais_validas: ``True`` se usuário/senha informados são válidos.

        Returns:
            AvaliacaoAutenticacao descrevendo o desfecho e o estado resultante.
        """
        # 1) Restrição de origem (Req. 12.7).
        if not self.origem_autorizada(origem):
            return AvaliacaoAutenticacao(
                resultado=ResultadoAutenticacao.ORIGEM_NAO_AUTORIZADA,
                origem=origem,
                falhas_consecutivas=0,
                bloqueada=False,
                segundos_restantes_bloqueio=0,
                mensagem=(
                    f"Origem não autorizada: {origem}. "
                    "A conexão foi rejeitada."
                ),
            )

        estado = self._estado_por_origem.setdefault(origem, _EstadoOrigem())

        # 2) Bloqueio ainda vigente: recusa sem avaliar credenciais (Req. 12.6).
        restante = self.segundos_restantes_bloqueio(origem)
        if restante > 0:
            return AvaliacaoAutenticacao(
                resultado=ResultadoAutenticacao.BLOQUEADA,
                origem=origem,
                falhas_consecutivas=estado.falhas_consecutivas,
                bloqueada=True,
                segundos_restantes_bloqueio=restante,
                mensagem=(
                    "Acesso temporariamente bloqueado para esta origem após "
                    f"{self._max_tentativas} tentativas inválidas consecutivas. "
                    f"Tente novamente em {restante} segundos."
                ),
            )

        # 3) Bloqueio expirado: reinicia o estado antes de avaliar.
        if estado.bloqueada_ate is not None:
            estado.falhas_consecutivas = 0
            estado.bloqueada_ate = None

        # 4) Avalia as credenciais.
        if credenciais_validas:
            # Sucesso reinicia a contagem de falhas (Req. 12.6).
            estado.falhas_consecutivas = 0
            estado.bloqueada_ate = None
            return AvaliacaoAutenticacao(
                resultado=ResultadoAutenticacao.SUCESSO,
                origem=origem,
                falhas_consecutivas=0,
                bloqueada=False,
                segundos_restantes_bloqueio=0,
                mensagem="Autenticação bem-sucedida.",
            )

        # Falha: incrementa a contagem de falhas consecutivas.
        estado.falhas_consecutivas += 1

        if estado.falhas_consecutivas >= self._max_tentativas:
            # Atingiu o limite: bloqueia a origem (Req. 12.6).
            estado.bloqueada_ate = self._relogio() + self._bloqueio_segundos
            return AvaliacaoAutenticacao(
                resultado=ResultadoAutenticacao.BLOQUEADA,
                origem=origem,
                falhas_consecutivas=estado.falhas_consecutivas,
                bloqueada=True,
                segundos_restantes_bloqueio=self._bloqueio_segundos,
                mensagem=(
                    f"Foram registradas {estado.falhas_consecutivas} tentativas "
                    "inválidas consecutivas. O acesso desta origem está bloqueado "
                    f"por {self._bloqueio_segundos} segundos."
                ),
            )

        restantes = self._max_tentativas - estado.falhas_consecutivas
        return AvaliacaoAutenticacao(
            resultado=ResultadoAutenticacao.FALHA,
            origem=origem,
            falhas_consecutivas=estado.falhas_consecutivas,
            bloqueada=False,
            segundos_restantes_bloqueio=0,
            mensagem=(
                "Usuário ou senha inválidos. Restam "
                f"{restantes} tentativa(s) antes do bloqueio da origem."
            ),
        )

    def reiniciar(self, origem: str) -> None:
        """Remove todo o estado de ``origem`` (falhas e bloqueio).

        Útil para testes e para redefinição administrativa do bloqueio.
        """
        self._estado_por_origem.pop(origem, None)


# ---------------------------------------------------------------------------
# Tratamento de falha de autorização via IAM_Role_EC2 (Req. 12.9)
# ---------------------------------------------------------------------------


class FalhaAutorizacaoIAM(Exception):
    """Erro de autorização ao acessar SQS/DynamoDB via IAM_Role_EC2.

    Representa a interrupção de uma operação por falha de autorização. Preserva o
    ``erro_original`` retornado pela AWS e identifica a ``operacao`` e o ``recurso``
    afetados, sem conter qualquer dado parcial da operação (Req. 12.9).
    """

    def __init__(self, operacao: str, recurso: str, erro_original: str) -> None:
        self.operacao = operacao
        self.recurso = recurso
        self.erro_original = erro_original
        super().__init__(
            f"Falha de autorização via IAM_Role_EC2 na operação '{operacao}' "
            f"sobre o recurso '{recurso}': {erro_original}"
        )


@dataclass(frozen=True)
class EventoFalhaAutorizacao:
    """Registro imutável de uma falha de autorização via IAM_Role_EC2 (Req. 12.9).

    Contém apenas metadados do erro — jamais dados parciais da operação —, de modo a
    ser registrado com segurança e exposto como indicação de erro de autorização.

    Attributes:
        operacao: nome lógico da operação interrompida (ex.: "ReceiveMessage").
        recurso: recurso AWS alvo (ex.: nome da fila ou da Tabela_Historico).
        erro_original: mensagem de erro original retornada pela AWS.
        mensagem: mensagem em pt-BR para exibição no Dashboard.
    """

    operacao: str
    recurso: str
    erro_original: str
    mensagem: str


@dataclass
class RegistroFalhasAutorizacao:
    """Coletor em memória dos eventos de falha de autorização (lógica pura).

    Permite registrar e consultar as falhas sem acoplar a camada de persistência. A
    aplicação pode usar este registro para auditoria e para expor a indicação de erro
    sem persistir dados parciais da operação interrompida (Req. 12.9).
    """

    eventos: list[EventoFalhaAutorizacao] = field(default_factory=list)

    def registrar(self, evento: EventoFalhaAutorizacao) -> None:
        """Acrescenta ``evento`` à lista de falhas registradas."""
        self.eventos.append(evento)

    def quantidade(self) -> int:
        """Número de falhas de autorização registradas."""
        return len(self.eventos)


def tratar_falha_autorizacao(
    operacao: str,
    recurso: str,
    erro_original: str,
    registro: RegistroFalhasAutorizacao | None = None,
) -> EventoFalhaAutorizacao:
    """Trata uma falha de autorização via IAM_Role_EC2 (Req. 12.9).

    Constrói o :class:`EventoFalhaAutorizacao` (que **não** carrega dados parciais da
    operação), opcionalmente o registra em ``registro`` para auditoria e o retorna
    para que o chamador interrompa a operação e exponha a indicação de erro.

    O chamador é responsável por **não** persistir resultados parciais da operação
    interrompida; esta função apenas produz/registra o evento de falha.

    Args:
        operacao: nome lógico da operação afetada (ex.: "PutItem", "SendMessage").
        recurso: recurso AWS alvo (ex.: nome da fila ou da Tabela_Historico).
        erro_original: mensagem de erro original retornada pela AWS.
        registro: coletor opcional onde o evento é registrado para auditoria.

    Returns:
        O :class:`EventoFalhaAutorizacao` construído.
    """
    mensagem = (
        "Erro de autorização ao acessar os recursos AWS via IAM_Role_EC2 "
        f"durante a operação '{operacao}' sobre '{recurso}'. A operação foi "
        "interrompida e nenhum dado parcial foi persistido."
    )
    evento = EventoFalhaAutorizacao(
        operacao=operacao,
        recurso=recurso,
        erro_original=erro_original,
        mensagem=mensagem,
    )
    if registro is not None:
        registro.registrar(evento)
    return evento
