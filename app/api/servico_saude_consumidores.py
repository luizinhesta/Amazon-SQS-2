"""Serviço de saúde dos consumidores (frente D, backend).

Encapsula a lógica do endpoint ``GET /api/consumidores/saude``, mantendo a rota
fina. A saúde é derivada apenas do estado em memória do
:class:`~app.main.GerenciadorConsumidores` — a função inspeciona os *slots*
(``estado_slots()``) e **nunca** chama a AWS.

Decisões de design relevantes:

- **Sem AWS.** O endpoint reflete a supervisão local das threads (vivo, degradado,
  reinícios). O estado das filas/mensagens continua vindo dos endpoints de
  estatísticas/monitoramento, que derivam do log de eventos.
- **Gerenciador opcional.** Quando os consumidores não foram iniciados (ou a
  composição não criou um gerenciador — ``app.state.gerenciador_consumidores`` é
  ``None``), devolve ``iniciados=False`` e lista vazia, sem erro.
- **Agregados derivados.** ``algum_morto``/``algum_degradado`` resumem a lista de
  slots para o Dashboard destacar problemas sem reprocessar a lista.
"""

from __future__ import annotations

from typing import Optional

from app.api.esquemas import ConsumidorSaude, RespostaSaudeConsumidores


def obter_saude_consumidores(gerenciador: Optional[object]) -> RespostaSaudeConsumidores:
    """Monta a resposta de ``GET /api/consumidores/saude`` a partir dos slots.

    Lê o estado em memória do gerenciador (``estado_slots()``), sem tocar na AWS.

    Args:
        gerenciador: o :class:`GerenciadorConsumidores` ativo (normalmente
            ``request.app.state.gerenciador_consumidores``) ou ``None`` quando os
            consumidores não foram iniciados.

    Returns:
        :class:`RespostaSaudeConsumidores` com a situação de cada slot e os
        agregados ``algum_morto``/``algum_degradado``. Com ``gerenciador=None`` (ou
        sem slots), ``iniciados=False`` e a lista é vazia.
    """
    if gerenciador is None:
        return RespostaSaudeConsumidores(
            iniciados=False,
            algum_morto=False,
            algum_degradado=False,
            consumidores=[],
        )

    slots = gerenciador.estado_slots()
    consumidores = [
        ConsumidorSaude(
            tipo=slot.tipo,
            consumer_id=slot.consumer_id,
            vivo=slot.vivo,
            degradado=slot.degradado,
            reinicios=slot.reinicios,
        )
        for slot in slots
    ]
    return RespostaSaudeConsumidores(
        iniciados=bool(consumidores),
        algum_morto=any(not c.vivo for c in consumidores),
        algum_degradado=any(c.degradado for c in consumidores),
        consumidores=consumidores,
    )
