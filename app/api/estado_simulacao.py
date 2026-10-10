"""Estado em memória da Config_Simulacao da aplicação (API_Backend).

A **Config_Simulacao** reúne os parâmetros que existem apenas na lógica de
simulação da aplicação e que o usuário pode ajustar pelo Dashboard, dentro de
faixas definidas, sem alterar o código-fonte (Req. 2.8, 2.9, 2.11). Diferente da
**Config_Aplicada** — que reflete o que foi configurado manualmente nos recursos
SQS e só muda no Console AWS (Req. 2.13) — a Config_Simulacao é mutável em tempo
de execução.

Este módulo mantém esse estado em memória, protegido por um ``threading.Lock``,
pois a API_Backend (FastAPI) pode atender requisições concorrentes. Os valores
iniciais vêm das constantes do laboratório (:mod:`app.dominio.constantes`), e todo
ajuste passa pelo validador de faixa (:mod:`app.dominio.validacao_faixa`), que
aceita o valor se e somente se estiver dentro da faixa e, ao rejeitar, preserva o
valor anterior (Req. 2.12).

Referências de requisitos: 2.8, 2.9, 2.11, 2.12.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from app.dominio import constantes as c
from app.dominio.validacao_faixa import (
    ResultadoValidacaoFaixa,
    validar_intervalo_atualizacao,
    validar_quantidade_mensagens,
    validar_tempo_processamento,
)


@dataclass(frozen=True)
class ValoresConfigSimulacao:
    """Snapshot imutável dos parâmetros de Config_Simulacao.

    Attributes:
        tempo_processamento_simulado_segundos: Tempo_Processamento_Simulado em
            segundos, ajustável em [0, 300] (Req. 2.8).
        quantidade_mensagens: Quantidade de mensagens por envio, em [1, 10]
            (Req. 2.9).
        intervalo_atualizacao_segundos: Intervalo de atualização periódica em
            segundos, em [5, 300] (Req. 13.2).
    """

    tempo_processamento_simulado_segundos: int
    quantidade_mensagens: int
    intervalo_atualizacao_segundos: int


class EstadoConfigSimulacao:
    """Guarda e ajusta a Config_Simulacao de forma thread-safe.

    Cada ajuste é validado pela faixa correspondente: aceito somente se dentro do
    intervalo; ao rejeitar, o valor anterior é preservado e uma mensagem em pt-BR
    com a faixa válida é retornada (Req. 2.12). Os valores iniciais vêm das
    constantes do laboratório.
    """

    def __init__(
        self,
        *,
        tempo_processamento_simulado_segundos: int = (
            c.TEMPO_PROCESSAMENTO_SIMULADO_SEGUNDOS
        ),
        quantidade_mensagens: int = c.QUANTIDADE_MENSAGENS_INICIAL,
        intervalo_atualizacao_segundos: int = c.INTERVALO_ATUALIZACAO_PADRAO,
    ) -> None:
        self._lock = threading.Lock()
        self._tempo_processamento = tempo_processamento_simulado_segundos
        self._quantidade_mensagens = quantidade_mensagens
        self._intervalo_atualizacao = intervalo_atualizacao_segundos

    def valores(self) -> ValoresConfigSimulacao:
        """Retorna um snapshot imutável dos valores atuais."""
        with self._lock:
            return ValoresConfigSimulacao(
                tempo_processamento_simulado_segundos=self._tempo_processamento,
                quantidade_mensagens=self._quantidade_mensagens,
                intervalo_atualizacao_segundos=self._intervalo_atualizacao,
            )

    def ajustar_tempo_processamento(
        self, valor_informado: int
    ) -> ResultadoValidacaoFaixa:
        """Ajusta o Tempo_Processamento_Simulado dentro de [0, 300] (Req. 2.8, 2.12)."""
        with self._lock:
            resultado = validar_tempo_processamento(
                valor_informado, self._tempo_processamento
            )
            self._tempo_processamento = resultado.valor_efetivo
            return resultado

    def ajustar_quantidade_mensagens(
        self, valor_informado: int
    ) -> ResultadoValidacaoFaixa:
        """Ajusta a quantidade de mensagens dentro de [1, 10] (Req. 2.9, 2.12)."""
        with self._lock:
            resultado = validar_quantidade_mensagens(
                valor_informado, self._quantidade_mensagens
            )
            self._quantidade_mensagens = resultado.valor_efetivo
            return resultado

    def ajustar_intervalo_atualizacao(
        self, valor_informado: int
    ) -> ResultadoValidacaoFaixa:
        """Ajusta o intervalo de atualização dentro de [5, 300] (Req. 13.2, 13.3)."""
        with self._lock:
            resultado = validar_intervalo_atualizacao(
                valor_informado, self._intervalo_atualizacao
            )
            self._intervalo_atualizacao = resultado.valor_efetivo
            return resultado
