"""Ordenação cronológica da listagem de eventos (Tarefa 5.1).

Este módulo implementa a função pura que ordena a lista de eventos de uma mensagem
em ordem cronológica crescente, conforme exigido pela linha do tempo do histórico
detalhado e pela listagem da Tabela_Historico.

Regra de ordenação (ver design.md, Property 6):

- Ordenação primária por ``app_time_ms`` de forma **não-decrescente** (crescente,
  permitindo horários iguais).
- Desempate estável por ``seq`` (ordem de registro) entre eventos de mesmo
  ``app_time_ms``, de forma **crescente**.

A função é pura: não possui efeitos colaterais e não modifica a lista recebida —
retorna uma nova lista ordenada. A ordenação usa a chave ``(app_time_ms, seq)``, o
que reproduz exatamente a ordenação lexicográfica da Sort Key
``EVENTO#<app_time_ms>#<seq>`` com ``seq`` em zero-padding (ver
:mod:`app.dominio.evento`). Como o ``sorted`` do Python é estável, a ordem relativa
de eventos com a mesma chave ``(app_time_ms, seq)`` também é preservada.

Referências de requisitos: 6.1, 11.6.
"""

from __future__ import annotations

from collections.abc import Iterable

from app.dominio.evento import Evento


def chave_cronologica(evento: Evento) -> tuple[int, int]:
    """Retorna a chave de ordenação cronológica de um evento.

    A chave ``(app_time_ms, seq)`` ordena primeiro pelo horário real da aplicação em
    milissegundos e, em caso de empate, pela ordem de registro (``seq``). Expor a
    chave facilita o reúso em outras consultas e a verificação em testes.

    Args:
        evento: O evento a ter sua chave de ordenação calculada.

    Returns:
        A tupla ``(app_time_ms, seq)`` usada como chave de ordenação.
    """
    return (evento.app_time_ms, evento.seq)


def ordenar_cronologicamente(eventos: Iterable[Evento]) -> list[Evento]:
    """Ordena eventos em ordem cronológica crescente com desempate estável.

    A ordenação é não-decrescente por ``app_time_ms`` e, entre eventos de mesmo
    horário, crescente por ``seq`` (ordem de registro). A função é pura: não altera a
    coleção de entrada e devolve uma nova lista.

    Args:
        eventos: Coleção de eventos a ordenar (lista, tupla ou qualquer iterável).

    Returns:
        Uma nova lista com os eventos ordenados por ``(app_time_ms, seq)``.
    """
    return sorted(eventos, key=chave_cronologica)
