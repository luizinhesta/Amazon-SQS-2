"""Validador de faixa genérico para parâmetros de Config_Simulacao e correlatos.

Implementa a regra de aceitação por faixa fechada: um valor é aceito se e somente
se ``min <= valor <= max``. Ao rejeitar, o valor anterior é preservado e uma
mensagem em pt-BR com a faixa válida é produzida.

Este módulo é lógica de domínio pura (sem efeitos colaterais) e consome apenas as
constantes de faixa já definidas em :mod:`app.dominio.constantes`.

Faixas suportadas (Tarefa 3.1):
- Tempo_Processamento_Simulado [0, 300] (Req. 2.8, 4.4).
- quantidade de mensagens [1, 10] (Req. 2.9, 4.2).
- instâncias de consumidor [1, 10] (Req. 10.1-10.3).
- intervalo de atualização [5, 300] (Req. 13.2).

Referências de requisitos: 2.8, 2.9, 2.11, 2.12, 10.3, 13.2, 13.3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from app.dominio import constantes as c


@dataclass(frozen=True)
class Faixa:
    """Faixa fechada ``[minimo, maximo]`` nomeada para mensagens em pt-BR.

    Attributes:
        nome: rótulo em pt-BR do parâmetro (ex.: "Tempo de processamento simulado").
        minimo: limite inferior aceito (inclusivo).
        maximo: limite superior aceito (inclusivo).
        unidade: unidade opcional exibida na mensagem (ex.: "segundos").
    """

    nome: str
    minimo: int
    maximo: int
    unidade: str = ""

    def __post_init__(self) -> None:
        if self.minimo > self.maximo:
            raise ValueError(
                "Faixa inválida: o limite mínimo não pode ser maior que o máximo."
            )

    def contem(self, valor: int) -> bool:
        """Retorna ``True`` se e somente se ``minimo <= valor <= maximo``."""
        return self.minimo <= valor <= self.maximo

    def descricao(self) -> str:
        """Descreve a faixa válida em pt-BR, incluindo a unidade quando houver."""
        sufixo = f" {self.unidade}" if self.unidade else ""
        return f"de {self.minimo} a {self.maximo}{sufixo}"


@dataclass(frozen=True)
class ResultadoValidacaoFaixa:
    """Resultado da validação de faixa.

    Attributes:
        aceito: indica se o valor informado foi aceito.
        valor_efetivo: valor que passa a valer — o novo valor se aceito, ou o valor
            anterior preservado se rejeitado.
        mensagem: mensagem em pt-BR; vazia quando aceito, com a faixa válida quando
            rejeitado.
    """

    aceito: bool
    valor_efetivo: int
    mensagem: str


def validar_faixa(
    faixa: Faixa, valor_informado: int, valor_anterior: int
) -> ResultadoValidacaoFaixa:
    """Valida ``valor_informado`` contra ``faixa``, preservando ``valor_anterior``.

    Aceita o valor se e somente se ele estiver dentro da faixa fechada
    ``[faixa.minimo, faixa.maximo]`` (Req. 2.12, 10.3, 13.3). Ao rejeitar, o
    ``valor_anterior`` é preservado como valor efetivo e a mensagem em pt-BR indica
    a faixa válida.

    Args:
        faixa: a faixa fechada nomeada contra a qual validar.
        valor_informado: o valor que o usuário deseja aplicar.
        valor_anterior: o valor atualmente em vigor, preservado em caso de rejeição.

    Returns:
        ResultadoValidacaoFaixa com ``aceito``, ``valor_efetivo`` e ``mensagem``.
    """
    if faixa.contem(valor_informado):
        return ResultadoValidacaoFaixa(
            aceito=True,
            valor_efetivo=valor_informado,
            mensagem="",
        )

    mensagem = (
        f"Valor inválido para {faixa.nome}: {valor_informado}. "
        f"Informe um valor {faixa.descricao()}. "
        f"O valor anterior ({valor_anterior}) foi preservado."
    )
    return ResultadoValidacaoFaixa(
        aceito=False,
        valor_efetivo=valor_anterior,
        mensagem=mensagem,
    )


# ---------------------------------------------------------------------------
# Faixas pré-definidas do laboratório (usam as constantes já existentes)
# ---------------------------------------------------------------------------

# Tempo_Processamento_Simulado [0, 300] segundos (Req. 2.8, 4.4).
FAIXA_TEMPO_PROCESSAMENTO: Final[Faixa] = Faixa(
    nome="Tempo de processamento simulado",
    minimo=c.TEMPO_PROCESSAMENTO_SIMULADO_MIN,
    maximo=c.TEMPO_PROCESSAMENTO_SIMULADO_MAX,
    unidade="segundos",
)

# Quantidade de mensagens por envio [1, 10] (Req. 2.9, 4.2).
FAIXA_QUANTIDADE_MENSAGENS: Final[Faixa] = Faixa(
    nome="Quantidade de mensagens",
    minimo=c.QUANTIDADE_MENSAGENS_MIN,
    maximo=c.QUANTIDADE_MENSAGENS_MAX,
    unidade="mensagens",
)

# Número de instâncias de cada tipo de consumidor [1, 10] (Req. 10.1-10.3).
FAIXA_INSTANCIAS_CONSUMIDOR: Final[Faixa] = Faixa(
    nome="Número de instâncias do consumidor",
    minimo=c.INSTANCIAS_CONSUMIDOR_MIN,
    maximo=c.INSTANCIAS_CONSUMIDOR_MAX,
    unidade="instâncias",
)

# Intervalo de atualização periódica [5, 300] segundos (Req. 13.2).
FAIXA_INTERVALO_ATUALIZACAO: Final[Faixa] = Faixa(
    nome="Intervalo de atualização",
    minimo=c.INTERVALO_ATUALIZACAO_MIN,
    maximo=c.INTERVALO_ATUALIZACAO_MAX,
    unidade="segundos",
)


def validar_tempo_processamento(
    valor_informado: int, valor_anterior: int
) -> ResultadoValidacaoFaixa:
    """Valida o Tempo_Processamento_Simulado [0, 300] (Req. 2.8, 2.12)."""
    return validar_faixa(FAIXA_TEMPO_PROCESSAMENTO, valor_informado, valor_anterior)


def validar_quantidade_mensagens(
    valor_informado: int, valor_anterior: int
) -> ResultadoValidacaoFaixa:
    """Valida a quantidade de mensagens [1, 10] (Req. 2.9, 2.12)."""
    return validar_faixa(FAIXA_QUANTIDADE_MENSAGENS, valor_informado, valor_anterior)


def validar_instancias_consumidor(
    valor_informado: int, valor_anterior: int
) -> ResultadoValidacaoFaixa:
    """Valida o número de instâncias do consumidor [1, 10] (Req. 10.3)."""
    return validar_faixa(FAIXA_INSTANCIAS_CONSUMIDOR, valor_informado, valor_anterior)


def validar_intervalo_atualizacao(
    valor_informado: int, valor_anterior: int
) -> ResultadoValidacaoFaixa:
    """Valida o intervalo de atualização periódica [5, 300] (Req. 13.2, 13.3)."""
    return validar_faixa(FAIXA_INTERVALO_ATUALIZACAO, valor_informado, valor_anterior)
