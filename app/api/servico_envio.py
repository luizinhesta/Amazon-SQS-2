"""Serviço de envio de mensagens da API_Backend (Tarefa 15.2).

Encapsula a lógica do endpoint ``POST /api/mensagens``, mantendo o roteador FastAPI
fino: validar o payload, enviar de 1 a 10 mensagens ao SQS via
:class:`~app.acesso.sqs_gateway.SqsGateway` e registrar **exatamente um**
Evento_Observado ``enviada`` por mensagem efetivamente enviada na Tabela_Historico
via :class:`~app.acesso.historico_repository.HistoricoRepository`.

Decisões de design relevantes (ver ``design.md`` — Property 9 e Property 10):

- **Validação antes de qualquer efeito.** O payload passa pelo validador puro
  :func:`app.dominio.validacao_envio.validar_payload_envio`, aplicado item a item.
  Se qualquer item for inválido, **nenhuma** mensagem é enviada e os campos
  reprovados são devolvidos em pt-BR, apontando o índice da mensagem (Req. 4.9).
- **Um evento ``enviada`` por mensagem enviada (Property 9).** Para cada envio
  bem-sucedido, registra-se um único Evento_Observado ``enviada`` com o horário
  real da aplicação (precisão de milissegundos) e o ``sqs_message_id`` retornado
  pela AWS (Req. 4.11).
- **Falha parcial preserva envios anteriores e o erro original (Property 10).** O
  lote é enviado em ordem; se o envio falhar na posição ``k``, as mensagens das
  posições anteriores permanecem registradas como ``enviada`` e o **erro original**
  da AWS é preservado no resultado da posição ``k``. O processamento do lote é
  interrompido em ``k`` (as mensagens enviadas antes não são desfeitas) — Req. 4.12.
- **Erro original repassado.** O ``SqsGateway`` repassa o erro da AWS sem
  encapsular; aqui ele é capturado apenas para montar a resposta em pt-BR,
  preservando a representação do erro original (``repr``), sem mascará-lo.

Referências de requisitos: 4.1, 4.5, 4.8, 4.10, 4.11, 4.12.
"""

from __future__ import annotations

import json
from typing import Optional

from app.acesso.historico_repository import HistoricoRepository, agora_utc_ms
from app.acesso.sqs_gateway import SqsGateway
from app.api.esquemas import (
    CampoInvalido,
    RequisicaoEnvioMensagens,
    RespostaEnvioMensagens,
    ResultadoEnvioItem,
    TIPO_FILA_FIFO,
)
from app.api.estado_simulacao import EstadoConfigSimulacao
from app.consumidores.processamento import CenarioFalha
from app.dominio import constantes as c
from app.dominio.classificador_origem import classificar_origem
from app.dominio.evento import Evento
from app.dominio.tipos import EventType
from app.dominio.validacao_envio import (
    CAMPO_QUANTIDADE,
    PayloadEnvio,
    TipoFila,
    validar_payload_envio,
)

# Cenario_Falha padrão quando a mensagem não especifica um (envio feliz).
CENARIO_PADRAO: CenarioFalha = CenarioFalha.SUCESSO


def _normalizar_tipo_fila(tipo_fila_bruto: str) -> Optional[TipoFila]:
    """Converte o rótulo informado para :class:`TipoFila`, ou ``None`` se inválido."""
    if isinstance(tipo_fila_bruto, str):
        chave = tipo_fila_bruto.strip().lower()
        for tipo in TipoFila:
            if tipo.value == chave:
                return tipo
    return None


def _nome_fila(tipo_fila: TipoFila) -> str:
    """Resolve o nome da fila SQS de destino a partir do tipo (Req. 4.1)."""
    return c.FILA_FIFO if tipo_fila is TipoFila.FIFO else c.FILA_STANDARD


def _cenario_valido(cenario_bruto: Optional[str]) -> CenarioFalha:
    """Converte o Cenario_Falha informado em :class:`CenarioFalha` (padrão SUCESSO)."""
    if cenario_bruto is None:
        return CENARIO_PADRAO
    try:
        return CenarioFalha(cenario_bruto)
    except ValueError:
        return CENARIO_PADRAO


def _corpo_mensagem(
    order_id: str,
    tempo_processamento: int,
    cenario: CenarioFalha,
) -> str:
    """Monta o corpo JSON da mensagem SQS com o ``order_id`` e parâmetros de simulação.

    O ``order_id`` vai no corpo (lido pelos consumidores), junto do
    Tempo_Processamento_Simulado e do Cenario_Falha a aplicar no processamento.
    """
    return json.dumps(
        {
            "order_id": order_id,
            "processing_seconds": tempo_processamento,
            "cenario": cenario.value,
        },
        ensure_ascii=False,
    )


def enviar_mensagens(
    requisicao: RequisicaoEnvioMensagens,
    *,
    gateway: SqsGateway,
    historico: HistoricoRepository,
    estado_simulacao: EstadoConfigSimulacao,
) -> RespostaEnvioMensagens:
    """Valida, envia o lote de mensagens e registra os eventos ``enviada``.

    Fluxo:

    1. **Validação (Req. 4.9).** Cada item do lote é validado por
       :func:`validar_payload_envio`; a quantidade é o tamanho da lista (1 a 10).
       Se algum item for inválido, nada é enviado e os campos reprovados são
       devolvidos em pt-BR apontando o índice da mensagem.
    2. **Envio em ordem (Req. 4.10).** As mensagens válidas são enviadas uma a uma
       pelo ``SqsGateway``. O ``order_id`` vai no corpo; para FIFO, o
       ``MessageGroupId`` (e o ``MessageDeduplicationId`` quando a deduplicação por
       conteúdo está desabilitada) são repassados.
    3. **Registro (Req. 4.11 / Property 9).** Cada envio bem-sucedido registra
       exatamente um Evento_Observado ``enviada`` com o horário real da aplicação.
    4. **Falha parcial (Req. 4.12 / Property 10).** Se o envio falhar na posição
       ``k``, os envios anteriores permanecem registrados como ``enviada`` e o erro
       original é preservado no resultado da posição ``k``; o lote é interrompido.

    Args:
        requisicao: Payload do POST /api/mensagens.
        gateway: Gateway do SQS (injetável; use um fake nos testes).
        historico: Repositório da Tabela_Historico (injetável; fake nos testes).
        estado_simulacao: Config_Simulacao vigente, usada para o
            Tempo_Processamento_Simulado padrão quando o item não o informa.

    Returns:
        :class:`RespostaEnvioMensagens` descrevendo o resultado da operação.
    """
    tipo_fila = _normalizar_tipo_fila(requisicao.tipo_fila)
    quantidade = len(requisicao.mensagens)
    tempo_padrao = estado_simulacao.valores().tempo_processamento_simulado_segundos

    # --- Etapa 1: validação de todo o lote (nada é enviado se houver inválido) ---
    campos_invalidos = _validar_lote(requisicao, tipo_fila, quantidade, tempo_padrao)
    if campos_invalidos:
        return RespostaEnvioMensagens(
            aceito=False,
            mensagem=(
                "Envio rejeitado: há campos inválidos no payload. Corrija os campos "
                "indicados e tente novamente."
            ),
            tipo_fila=requisicao.tipo_fila,
            nome_fila=None,
            total_solicitadas=quantidade,
            total_enviadas=0,
            houve_falha=False,
            campos_invalidos=campos_invalidos,
            resultados=[],
        )

    # A validação acima garante que tipo_fila não é None aqui.
    assert tipo_fila is not None
    nome_fila = _nome_fila(tipo_fila)
    e_fifo = tipo_fila is TipoFila.FIFO

    # --- Etapa 2-4: envio em ordem, registro e tratamento de falha parcial -------
    resultados: list[ResultadoEnvioItem] = []
    total_enviadas = 0
    houve_falha = False

    for indice, item in enumerate(requisicao.mensagens):
        tempo = (
            item.tempo_processamento_simulado_segundos
            if item.tempo_processamento_simulado_segundos is not None
            else tempo_padrao
        )
        cenario = _cenario_valido(item.cenario_falha)
        corpo = _corpo_mensagem(item.order_id, tempo, cenario)

        try:
            resposta_sqs = gateway.send_message(
                nome_fila,
                corpo,
                message_group_id=item.message_group_id if e_fifo else None,
                message_deduplication_id=(
                    item.message_deduplication_id
                    if e_fifo and not requisicao.deduplicacao_por_conteudo
                    else None
                ),
            )
        except Exception as erro:  # noqa: BLE001 — preservamos o erro original (Req. 4.12)
            # Falha parcial: preserva os envios anteriores (já registrados como
            # 'enviada') e o erro original da AWS; interrompe o lote (Property 10).
            houve_falha = True
            resultados.append(
                ResultadoEnvioItem(
                    indice=indice,
                    order_id=item.order_id,
                    enviada=False,
                    sqs_message_id=None,
                    detalhe_erro=repr(erro),
                )
            )
            break

        sqs_message_id = resposta_sqs.get("MessageId") if resposta_sqs else None

        # Registra exatamente um Evento_Observado 'enviada' por mensagem (Req. 4.11).
        app_time, app_time_ms = agora_utc_ms()
        evento = Evento(
            order_id=item.order_id,
            queue_name=nome_fila,
            event_type=EventType.ENVIADA,
            event_source=classificar_origem(EventType.ENVIADA),
            app_time=app_time,
            app_time_ms=app_time_ms,
            processing_seconds=tempo,
            message_group_id=item.message_group_id if e_fifo else None,
            sqs_message_id=sqs_message_id,
        )
        historico.registrar_evento(evento)

        total_enviadas += 1
        resultados.append(
            ResultadoEnvioItem(
                indice=indice,
                order_id=item.order_id,
                enviada=True,
                sqs_message_id=sqs_message_id,
                detalhe_erro=None,
            )
        )

    mensagem = _resumo(total_enviadas, quantidade, houve_falha)
    return RespostaEnvioMensagens(
        aceito=True,
        mensagem=mensagem,
        tipo_fila=requisicao.tipo_fila,
        nome_fila=nome_fila,
        total_solicitadas=quantidade,
        total_enviadas=total_enviadas,
        houve_falha=houve_falha,
        campos_invalidos=[],
        resultados=resultados,
    )


def _validar_lote(
    requisicao: RequisicaoEnvioMensagens,
    tipo_fila: Optional[TipoFila],
    quantidade: int,
    tempo_padrao: int,
) -> list[CampoInvalido]:
    """Valida o lote inteiro e devolve os campos inválidos (vazio se tudo válido).

    A quantidade do lote (1 a 10) é validada uma única vez, pois é propriedade do
    lote. Cada mensagem é validada individualmente com :func:`validar_payload_envio`,
    reutilizando as faixas de ``order_id``, Tempo_Processamento_Simulado e, para
    FIFO, ``MessageGroupId``/``MessageDeduplicationId`` (Req. 4.2, 4.3, 4.4, 4.6, 4.7).
    """
    campos: list[CampoInvalido] = []

    # Quantidade do lote [1, 10] (Req. 4.2) — propriedade do lote, não da mensagem.
    if not (c.QUANTIDADE_MENSAGENS_MIN <= quantidade <= c.QUANTIDADE_MENSAGENS_MAX):
        campos.append(
            CampoInvalido(
                campo=CAMPO_QUANTIDADE,
                mensagem=(
                    "A quantidade de mensagens do lote deve ser um número inteiro "
                    f"entre {c.QUANTIDADE_MENSAGENS_MIN} e "
                    f"{c.QUANTIDADE_MENSAGENS_MAX}."
                ),
                indice_mensagem=None,
            )
        )

    # Tipo da fila inválido é um erro do lote (Req. 4.1); reporta uma única vez.
    if tipo_fila is None:
        campos.append(
            CampoInvalido(
                campo="tipo_fila",
                mensagem="A fila de destino deve ser 'standard' ou 'fifo'.",
                indice_mensagem=None,
            )
        )

    # Validação item a item. Quando o tipo de fila é desconhecido, assume Standard
    # apenas para validar os demais campos (order_id/tempo) sem exigir campos FIFO.
    tipo_para_validacao = tipo_fila if tipo_fila is not None else TipoFila.STANDARD
    for indice, item in enumerate(requisicao.mensagens):
        tempo = (
            item.tempo_processamento_simulado_segundos
            if item.tempo_processamento_simulado_segundos is not None
            else tempo_padrao
        )
        payload = PayloadEnvio(
            tipo_fila=tipo_para_validacao,
            # A quantidade é validada no nível do lote; aqui usa um valor válido
            # fixo (1) para não duplicar o erro de quantidade por mensagem.
            quantidade=1,
            order_id=item.order_id,
            tempo_processamento_simulado=tempo,
            message_group_id=item.message_group_id,
            deduplicacao_por_conteudo=requisicao.deduplicacao_por_conteudo,
            message_deduplication_id=item.message_deduplication_id,
        )
        resultado = validar_payload_envio(payload)
        if resultado.valido:
            continue
        for campo in resultado.campos_invalidos:
            # O erro de tipo_fila já é tratado no nível do lote; não repetir.
            if campo == "tipo_fila":
                continue
            campos.append(
                CampoInvalido(
                    campo=campo,
                    mensagem=resultado.mensagens.get(campo, "Campo inválido."),
                    indice_mensagem=indice,
                )
            )

    return campos


def _resumo(total_enviadas: int, quantidade: int, houve_falha: bool) -> str:
    """Monta a mensagem-resumo em pt-BR do resultado do envio."""
    if houve_falha:
        return (
            f"Falha parcial no envio: {total_enviadas} de {quantidade} mensagens "
            "foram enviadas antes do erro. As mensagens já enviadas foram "
            "registradas; o erro original da fila foi preservado."
        )
    return (
        f"Envio concluído: {total_enviadas} de {quantidade} mensagens enviadas com "
        "sucesso e registradas na Tabela_Historico."
    )
