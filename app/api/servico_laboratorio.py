"""Serviço do laboratório de falhas da API_Backend (Tarefa 15.6).

Encapsula a lógica dos endpoints ``POST /api/laboratorio/cenario`` e
``POST /api/laboratorio/reprocessar-dlq``, mantendo os roteadores FastAPI finos.

- **Executar cenário (Req. 7.1-7.11).** Dispara um cenário de falha controlado
  enviando de 1 a 10 mensagens à fila selecionada com o Cenario_Falha embutido,
  reutilizando :func:`app.api.servico_envio.enviar_mensagens` (que valida o payload,
  envia via ``SqsGateway`` e registra o Evento_Observado ``enviada``). Os
  consumidores reproduzem o comportamento do cenário ao processar as mensagens.
- **Reprocessar DLQ (Req. 7.7).** Move mensagens da DLQ de volta à fila principal do
  fluxo selecionado, reutilizando
  :meth:`app.acesso.sqs_gateway.SqsGateway.redrive_from_dlq`.

**Interrupção com causa em pt-BR (Req. 7.11).** Antes de qualquer efeito, verifica a
autenticação/autorização da ``origem`` (quando um controlador de autenticação é
injetado): origem não autorizada ou bloqueada interrompe a operação com a causa em
pt-BR. Durante a execução, qualquer erro de acesso ao SQS (fila indisponível, falha
de autorização via IAM_Role_EC2) é capturado e convertido em interrupção com a causa
em pt-BR, preservando o erro original no texto — o erro bruto não vaza como exceção
ao cliente.

Referências de requisitos: 7.1-7.11, 12.5, 12.6.
"""

from __future__ import annotations

from typing import Optional, Protocol

from app.acesso.sqs_gateway import SqsGateway
from app.api.esquemas import (
    RequisicaoCenario,
    RequisicaoReprocessarDlq,
    RequisicaoEnvioMensagens,
    RespostaCenario,
    RespostaReprocessarDlq,
    ItemEnvioMensagem,
)
from app.api.estado_simulacao import EstadoConfigSimulacao
from app.api.servico_envio import enviar_mensagens
from app.dominio import constantes as c


class HistoricoRegistro(Protocol):
    """Contrato mínimo do repositório usado pelo serviço de envio reaproveitado."""

    def registrar_evento(self, evento: object) -> None:  # pragma: no cover - protocolo
        ...


class ControladorBloqueio(Protocol):
    """Contrato mínimo do controlador de autenticação para checagem de acesso.

    Apenas as consultas necessárias para interromper a operação sem conceder acesso
    são declaradas: se a origem está autorizada e se está bloqueada (Req. 7.11,
    12.6, 12.7).
    """

    def origem_autorizada(self, origem: str) -> bool:  # pragma: no cover - protocolo
        ...

    def esta_bloqueada(self, origem: str) -> bool:  # pragma: no cover - protocolo
        ...

    def segundos_restantes_bloqueio(
        self, origem: str
    ) -> int:  # pragma: no cover - protocolo
        ...


# Rótulos técnicos dos tipos de fila aceitos (coerentes com o validador de envio).
_TIPO_STANDARD = "standard"
_TIPO_FIFO = "fifo"


def _verificar_acesso(
    origem: Optional[str], controlador: Optional[ControladorBloqueio]
) -> Optional[str]:
    """Verifica a autenticação/autorização antes de executar a operação (Req. 7.11).

    Quando um ``controlador`` é injetado e uma ``origem`` é informada, interrompe a
    operação se a origem não está autorizada ou está bloqueada, devolvendo a causa
    em pt-BR. Caso contrário (sem controlador ou sem origem), não interrompe —
    retorna ``None``.

    Returns:
        A causa da interrupção em pt-BR, ou ``None`` quando o acesso é permitido.
    """
    if controlador is None or origem is None:
        return None

    if not controlador.origem_autorizada(origem):
        return (
            f"Autenticação falhou: a origem '{origem}' não está autorizada a "
            "executar operações no laboratório. A operação foi interrompida."
        )

    if controlador.esta_bloqueada(origem):
        restante = controlador.segundos_restantes_bloqueio(origem)
        return (
            f"Autenticação falhou: a origem '{origem}' está bloqueada após "
            "tentativas inválidas consecutivas. A operação foi interrompida. "
            f"Tente novamente em {restante} segundos."
        )

    return None


def executar_cenario(
    requisicao: RequisicaoCenario,
    *,
    gateway: SqsGateway,
    historico: HistoricoRegistro,
    estado_simulacao: EstadoConfigSimulacao,
    controlador_autenticacao: Optional[ControladorBloqueio] = None,
) -> RespostaCenario:
    """Executa um cenário de falha controlado (Req. 7.1-7.11).

    Fluxo:

    1. **Checagem de acesso (Req. 7.11).** Se a origem não está autorizada ou está
       bloqueada, interrompe com a causa em pt-BR, sem enviar nada.
    2. **Envio das mensagens do cenário.** Monta o payload do cenário (quantidade,
       order_id gerado por mensagem, Cenario_Falha, Tempo_Processamento_Simulado e,
       para FIFO, o MessageGroupId) e delega a
       :func:`app.api.servico_envio.enviar_mensagens`, que valida e envia via
       ``SqsGateway``. Uma falha de acesso ao SQS (fila indisponível) é tratada por
       :func:`enviar_mensagens` como falha parcial e refletida em ``houve_falha``;
       aqui ela também é sinalizada como interrupção com causa em pt-BR.

    Args:
        requisicao: Payload do cenário.
        gateway: Gateway do SQS (injetável; fake nos testes).
        historico: Repositório da Tabela_Historico (injetável; fake nos testes).
        estado_simulacao: Config_Simulacao vigente (Tempo_Processamento_Simulado
            padrão).
        controlador_autenticacao: Controlador de bloqueio por origem (opcional).

    Returns:
        :class:`RespostaCenario` descrevendo o resultado (executado ou interrompido).
    """
    causa = _verificar_acesso(requisicao.origem, controlador_autenticacao)
    if causa is not None:
        return RespostaCenario(
            cenario=requisicao.cenario,
            tipo_fila=requisicao.tipo_fila,
            executado=False,
            interrompido=True,
            causa=causa,
            mensagem=causa,
            envio=None,
        )

    # Monta o lote do cenário: uma mensagem por order_id gerado.
    quantidade = max(1, int(requisicao.quantidade))
    mensagens = [
        ItemEnvioMensagem(
            order_id=f"{requisicao.order_id_base}-{indice + 1:04d}",
            tempo_processamento_simulado_segundos=(
                requisicao.tempo_processamento_simulado_segundos
            ),
            cenario_falha=requisicao.cenario,
            message_group_id=requisicao.message_group_id,
        )
        for indice in range(quantidade)
    ]
    payload = RequisicaoEnvioMensagens(
        tipo_fila=requisicao.tipo_fila,
        mensagens=mensagens,
    )

    try:
        envio = enviar_mensagens(
            payload,
            gateway=gateway,
            historico=historico,
            estado_simulacao=estado_simulacao,
        )
    except Exception as erro:  # noqa: BLE001 — fila indisponível vira interrupção pt-BR
        causa = (
            "Fila indisponível ao executar o cenário: a operação foi interrompida. "
            f"Erro original: {erro!r}"
        )
        return RespostaCenario(
            cenario=requisicao.cenario,
            tipo_fila=requisicao.tipo_fila,
            executado=False,
            interrompido=True,
            causa=causa,
            mensagem=causa,
            envio=None,
        )

    # Payload inválido (ex.: FIFO sem MessageGroupId) não é interrupção de infra,
    # mas o cenário não foi disparado: reporta como não executado, sem interromper.
    if not envio.aceito:
        return RespostaCenario(
            cenario=requisicao.cenario,
            tipo_fila=requisicao.tipo_fila,
            executado=False,
            interrompido=False,
            causa=None,
            mensagem=(
                "Cenário não disparado: o payload do cenário é inválido. "
                "Corrija os campos indicados e tente novamente."
            ),
            envio=envio,
        )

    # Falha parcial de envio (fila indisponível no meio do lote) interrompe o
    # cenário com causa em pt-BR, preservando os envios já realizados (Req. 7.11).
    if envio.houve_falha:
        detalhe = next(
            (r.detalhe_erro for r in envio.resultados if r.detalhe_erro),
            None,
        )
        causa = (
            "Fila indisponível durante o cenário: a execução foi interrompida após "
            f"{envio.total_enviadas} de {envio.total_solicitadas} mensagens. "
            + (f"Erro original: {detalhe}" if detalhe else "")
        ).strip()
        return RespostaCenario(
            cenario=requisicao.cenario,
            tipo_fila=requisicao.tipo_fila,
            executado=False,
            interrompido=True,
            causa=causa,
            mensagem=causa,
            envio=envio,
        )

    return RespostaCenario(
        cenario=requisicao.cenario,
        tipo_fila=requisicao.tipo_fila,
        executado=True,
        interrompido=False,
        causa=None,
        mensagem=(
            f"Cenário '{requisicao.cenario}' disparado: {envio.total_enviadas} "
            "mensagem(ns) enviada(s). Acompanhe o processamento no monitoramento."
        ),
        envio=envio,
    )


def _resolver_filas_dlq(tipo_bruto: str) -> Optional[tuple[str, str]]:
    """Resolve ``(dlq, fila_principal)`` do fluxo informado, ou ``None`` se inválido."""
    if not isinstance(tipo_bruto, str):
        return None
    chave = tipo_bruto.strip().lower()
    if chave == _TIPO_STANDARD:
        return c.FILA_STANDARD_DLQ, c.FILA_STANDARD
    if chave == _TIPO_FIFO:
        return c.FILA_FIFO_DLQ, c.FILA_FIFO
    return None


def reprocessar_dlq(
    requisicao: RequisicaoReprocessarDlq,
    *,
    gateway: SqsGateway,
    controlador_autenticacao: Optional[ControladorBloqueio] = None,
) -> RespostaReprocessarDlq:
    """Move mensagens da DLQ de volta à fila principal (Req. 7.7, 7.11).

    Fluxo:

    1. **Checagem de acesso (Req. 7.11).** Origem não autorizada ou bloqueada
       interrompe com a causa em pt-BR, sem mover mensagens.
    2. **Resolução das filas.** Determina a DLQ e a fila principal do fluxo
       (``standard`` ou ``fifo``). Tipo inválido interrompe com causa em pt-BR.
    3. **Redrive.** Delega a :meth:`SqsGateway.redrive_from_dlq`, que reenvia à fila
       principal e só então exclui da DLQ (sem perda de mensagem). Qualquer erro de
       acesso ao SQS (fila indisponível, autorização) é convertido em interrupção
       com a causa em pt-BR, preservando o erro original no texto.

    Args:
        requisicao: Payload com o fluxo a reprocessar e a origem.
        gateway: Gateway do SQS (injetável; fake nos testes).
        controlador_autenticacao: Controlador de bloqueio por origem (opcional).

    Returns:
        :class:`RespostaReprocessarDlq` com o resultado (reprocessado ou
        interrompido).
    """
    causa = _verificar_acesso(requisicao.origem, controlador_autenticacao)
    if causa is not None:
        return RespostaReprocessarDlq(
            tipo_fila=requisicao.tipo_fila,
            dlq=None,
            fila_principal=None,
            reprocessado=False,
            total_movidas=0,
            mensagens_movidas=[],
            interrompido=True,
            causa=causa,
            mensagem=causa,
        )

    filas = _resolver_filas_dlq(requisicao.tipo_fila)
    if filas is None:
        causa = (
            "Fluxo inválido para reprocessamento: informe 'standard' ou 'fifo'. "
            "A operação foi interrompida."
        )
        return RespostaReprocessarDlq(
            tipo_fila=requisicao.tipo_fila,
            dlq=None,
            fila_principal=None,
            reprocessado=False,
            total_movidas=0,
            mensagens_movidas=[],
            interrompido=True,
            causa=causa,
            mensagem=causa,
        )

    dlq, fila_principal = filas
    try:
        movidas = gateway.redrive_from_dlq(dlq, fila_principal)
    except Exception as erro:  # noqa: BLE001 — fila indisponível vira interrupção pt-BR
        causa = (
            f"Fila indisponível ao reprocessar a DLQ '{dlq}': a operação foi "
            f"interrompida. Erro original: {erro!r}"
        )
        return RespostaReprocessarDlq(
            tipo_fila=requisicao.tipo_fila,
            dlq=dlq,
            fila_principal=fila_principal,
            reprocessado=False,
            total_movidas=0,
            mensagens_movidas=[],
            interrompido=True,
            causa=causa,
            mensagem=causa,
        )

    total = len(movidas)
    if total == 0:
        mensagem = (
            f"Nenhuma mensagem para reprocessar na DLQ '{dlq}'. A fila principal "
            f"'{fila_principal}' permanece inalterada."
        )
    else:
        mensagem = (
            f"Reprocessamento concluído: {total} mensagem(ns) movida(s) da DLQ "
            f"'{dlq}' para a fila principal '{fila_principal}'."
        )

    return RespostaReprocessarDlq(
        tipo_fila=requisicao.tipo_fila,
        dlq=dlq,
        fila_principal=fila_principal,
        reprocessado=total > 0,
        total_movidas=total,
        mensagens_movidas=list(movidas),
        interrompido=False,
        causa=None,
        mensagem=mensagem,
    )
