"""Estado em memória do número de instâncias dos consumidores (API_Backend).

O laboratório permite ajustar, pelo Dashboard, a quantidade de instâncias de cada
tipo de consumidor (Standard e FIFO), de 1 a 10, para testar concorrência nas filas
(Req. 10.1, 10.2). Este módulo mantém esse número em memória, por tipo de
consumidor, protegido por um ``threading.Lock`` (a API_Backend atende requisições
concorrentes).

Todo ajuste passa pelo validador de faixa
(:func:`app.dominio.validacao_faixa.validar_instancias_consumidor`): o valor é
aceito se e somente se estiver em [1, 10]; ao rejeitar, o **número anterior é
preservado** e uma mensagem em pt-BR com o intervalo válido é devolvida (Req. 10.3).

Este módulo registra apenas o número desejado de instâncias — a composição real dos
processos/threads dos consumidores (Tarefa 18.1) consome este estado. Mantê-lo
isolado e injetável permite testar o endpoint sem subir consumidores reais.

Referências de requisitos: 10.1, 10.2, 10.3.
"""

from __future__ import annotations

import threading

from app.api.esquemas import TIPO_CONSUMIDOR_FIFO, TIPO_CONSUMIDOR_STANDARD
from app.dominio import constantes as c
from app.dominio.validacao_faixa import (
    ResultadoValidacaoFaixa,
    validar_instancias_consumidor,
)

# Tipos de consumidor válidos para o ajuste de instâncias (Req. 10.1, 10.2).
TIPOS_CONSUMIDOR_VALIDOS: frozenset[str] = frozenset(
    {TIPO_CONSUMIDOR_STANDARD, TIPO_CONSUMIDOR_FIFO}
)


def normalizar_tipo_consumidor(tipo_bruto: str) -> str | None:
    """Normaliza o rótulo do tipo de consumidor, ou ``None`` se inválido.

    Aceita ``standard`` e ``fifo`` sem distinção de caixa; qualquer outro valor é
    inválido (o endpoint responde com erro em pt-BR).
    """
    if isinstance(tipo_bruto, str):
        chave = tipo_bruto.strip().lower()
        if chave in TIPOS_CONSUMIDOR_VALIDOS:
            return chave
    return None


class EstadoInstanciasConsumidores:
    """Guarda e ajusta o número de instâncias por tipo de consumidor (thread-safe).

    Cada ajuste é validado pela faixa [1, 10]: aceito somente se dentro do intervalo;
    ao rejeitar, o número anterior é preservado e uma mensagem em pt-BR com o
    intervalo válido é retornada (Req. 10.3). Os valores iniciais vêm da constante
    :data:`app.dominio.constantes.INSTANCIAS_CONSUMIDOR_INICIAL`.
    """

    def __init__(
        self,
        *,
        standard: int = c.INSTANCIAS_CONSUMIDOR_INICIAL,
        fifo: int = c.INSTANCIAS_CONSUMIDOR_INICIAL,
    ) -> None:
        self._lock = threading.Lock()
        self._instancias: dict[str, int] = {
            TIPO_CONSUMIDOR_STANDARD: standard,
            TIPO_CONSUMIDOR_FIFO: fifo,
        }

    def instancias(self, tipo: str) -> int:
        """Retorna o número de instâncias atualmente em vigor para ``tipo``."""
        with self._lock:
            return self._instancias[tipo]

    def ajustar(self, tipo: str, valor_informado: int) -> ResultadoValidacaoFaixa:
        """Ajusta o número de instâncias de ``tipo`` dentro de [1, 10] (Req. 10.3).

        Args:
            tipo: tipo normalizado do consumidor (``standard`` ou ``fifo``).
            valor_informado: número de instâncias desejado.

        Returns:
            :class:`ResultadoValidacaoFaixa` com ``aceito``, ``valor_efetivo`` (novo
            se aceito, anterior preservado se rejeitado) e ``mensagem`` em pt-BR.

        Raises:
            KeyError: se ``tipo`` não for um tipo de consumidor válido. O endpoint
                normaliza/valida o tipo antes de chamar este método.
        """
        with self._lock:
            anterior = self._instancias[tipo]
            resultado = validar_instancias_consumidor(valor_informado, anterior)
            self._instancias[tipo] = resultado.valor_efetivo
            return resultado
