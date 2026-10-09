"""Testes unitários do controle de autenticação com bloqueio por origem (Tarefa 14.1).

Verificam o controlador de tentativas por origem (lógica pura, com relógio
injetável) e o tratamento de falha de autorização via IAM_Role_EC2:

- Exigência de usuário/senha válidos para conceder acesso (Req. 12.5).
- Bloqueio da origem após 5 falhas consecutivas por 300 segundos, com expiração
  verificada via relógio injetado, sem ``sleep`` real (Req. 12.6).
- Um sucesso antes do limite reinicia a contagem de falhas (Req. 12.6).
- Restrição às origens autorizadas (Req. 12.7).
- Falha de autorização via IAM interrompendo a operação, registrando o evento e
  expondo erro sem persistir dados parciais (Req. 12.9).

Referências de requisitos: 12.5, 12.6, 12.7, 12.9.
"""

from __future__ import annotations

import pytest

from app.dominio import constantes as c
from app.seguranca.autenticacao import (
    AvaliacaoAutenticacao,
    ControladorAutenticacao,
    EventoFalhaAutorizacao,
    FalhaAutorizacaoIAM,
    RegistroFalhasAutorizacao,
    ResultadoAutenticacao,
    tratar_falha_autorizacao,
)


class RelogioFake:
    """Relógio controlável para testar expiração do bloqueio sem sleep real."""

    def __init__(self, inicial: float = 0.0) -> None:
        self.agora = inicial

    def __call__(self) -> float:
        return self.agora

    def avancar(self, segundos: float) -> None:
        self.agora += segundos


ORIGEM = "203.0.113.10"


def _controlador(relogio: RelogioFake) -> ControladorAutenticacao:
    return ControladorAutenticacao([ORIGEM], relogio=relogio)


# ---------------------------------------------------------------------------
# Exigência de credenciais válidas (Req. 12.5)
# ---------------------------------------------------------------------------

def test_sucesso_concede_acesso() -> None:
    relogio = RelogioFake()
    ctrl = _controlador(relogio)
    aval = ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=True)
    assert aval.resultado is ResultadoAutenticacao.SUCESSO
    assert aval.acesso_concedido is True
    assert aval.bloqueada is False
    assert aval.falhas_consecutivas == 0


def test_falha_nega_acesso() -> None:
    relogio = RelogioFake()
    ctrl = _controlador(relogio)
    aval = ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=False)
    assert aval.resultado is ResultadoAutenticacao.FALHA
    assert aval.acesso_concedido is False
    assert aval.falhas_consecutivas == 1


# ---------------------------------------------------------------------------
# Restrição de origem (Req. 12.7)
# ---------------------------------------------------------------------------

def test_origem_nao_autorizada_e_rejeitada() -> None:
    relogio = RelogioFake()
    ctrl = ControladorAutenticacao([ORIGEM], relogio=relogio)
    aval = ctrl.avaliar_tentativa("198.51.100.7", credenciais_validas=True)
    assert aval.resultado is ResultadoAutenticacao.ORIGEM_NAO_AUTORIZADA
    assert aval.acesso_concedido is False


def test_origem_pode_ser_autorizada_dinamicamente() -> None:
    relogio = RelogioFake()
    ctrl = ControladorAutenticacao(relogio=relogio)
    assert ctrl.origem_autorizada(ORIGEM) is False
    ctrl.autorizar_origem(ORIGEM)
    assert ctrl.origem_autorizada(ORIGEM) is True
    aval = ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=True)
    assert aval.acesso_concedido is True


def test_origem_pode_ser_revogada() -> None:
    relogio = RelogioFake()
    ctrl = ControladorAutenticacao([ORIGEM], relogio=relogio)
    ctrl.revogar_origem(ORIGEM)
    aval = ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=True)
    assert aval.resultado is ResultadoAutenticacao.ORIGEM_NAO_AUTORIZADA


# ---------------------------------------------------------------------------
# Bloqueio após 5 falhas consecutivas por 300 s (Req. 12.6)
# ---------------------------------------------------------------------------

def test_bloqueia_apos_cinco_falhas_consecutivas() -> None:
    relogio = RelogioFake()
    ctrl = _controlador(relogio)
    # As 4 primeiras falhas não bloqueiam.
    for i in range(1, c.MAX_TENTATIVAS_AUTENTICACAO):
        aval = ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=False)
        assert aval.resultado is ResultadoAutenticacao.FALHA
        assert aval.falhas_consecutivas == i
        assert aval.bloqueada is False
    # A 5ª falha dispara o bloqueio.
    aval = ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=False)
    assert aval.resultado is ResultadoAutenticacao.BLOQUEADA
    assert aval.falhas_consecutivas == c.MAX_TENTATIVAS_AUTENTICACAO
    assert aval.bloqueada is True
    assert aval.segundos_restantes_bloqueio == c.BLOQUEIO_AUTENTICACAO_SEGUNDOS
    assert ctrl.esta_bloqueada(ORIGEM) is True


def test_tentativa_durante_bloqueio_e_recusada_sem_avaliar_credenciais() -> None:
    relogio = RelogioFake()
    ctrl = _controlador(relogio)
    for _ in range(c.MAX_TENTATIVAS_AUTENTICACAO):
        ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=False)
    # Mesmo com credenciais válidas, o bloqueio recusa a tentativa.
    aval = ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=True)
    assert aval.resultado is ResultadoAutenticacao.BLOQUEADA
    assert aval.acesso_concedido is False


def test_bloqueio_expira_apos_300_segundos() -> None:
    relogio = RelogioFake()
    ctrl = _controlador(relogio)
    for _ in range(c.MAX_TENTATIVAS_AUTENTICACAO):
        ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=False)
    assert ctrl.esta_bloqueada(ORIGEM) is True

    # Ainda bloqueada um instante antes do fim.
    relogio.avancar(c.BLOQUEIO_AUTENTICACAO_SEGUNDOS - 1)
    assert ctrl.esta_bloqueada(ORIGEM) is True

    # Após 300 s, o bloqueio expira e novas tentativas são avaliadas.
    relogio.avancar(1)
    assert ctrl.esta_bloqueada(ORIGEM) is False
    aval = ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=True)
    assert aval.resultado is ResultadoAutenticacao.SUCESSO


def test_segundos_restantes_decrescem_com_o_tempo() -> None:
    relogio = RelogioFake()
    ctrl = _controlador(relogio)
    for _ in range(c.MAX_TENTATIVAS_AUTENTICACAO):
        ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=False)
    assert ctrl.segundos_restantes_bloqueio(ORIGEM) == 300
    relogio.avancar(100)
    assert ctrl.segundos_restantes_bloqueio(ORIGEM) == 200
    relogio.avancar(200)
    assert ctrl.segundos_restantes_bloqueio(ORIGEM) == 0


# ---------------------------------------------------------------------------
# Um sucesso antes do limite reinicia a contagem (Req. 12.6)
# ---------------------------------------------------------------------------

def test_sucesso_antes_do_limite_reinicia_contagem() -> None:
    relogio = RelogioFake()
    ctrl = _controlador(relogio)
    # 4 falhas (uma a menos que o limite).
    for _ in range(c.MAX_TENTATIVAS_AUTENTICACAO - 1):
        ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=False)
    # Um sucesso zera a contagem.
    aval = ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=True)
    assert aval.falhas_consecutivas == 0
    # Uma nova falha recomeça do 1 e não bloqueia.
    aval = ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=False)
    assert aval.falhas_consecutivas == 1
    assert aval.bloqueada is False
    assert ctrl.esta_bloqueada(ORIGEM) is False


def test_falhas_nao_consecutivas_nao_bloqueiam() -> None:
    relogio = RelogioFake()
    ctrl = _controlador(relogio)
    # Alterna falha/sucesso diversas vezes: nunca acumula 5 falhas seguidas.
    for _ in range(10):
        ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=False)
        aval = ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=True)
        assert aval.resultado is ResultadoAutenticacao.SUCESSO
    assert ctrl.esta_bloqueada(ORIGEM) is False


def test_reiniciar_limpa_estado_da_origem() -> None:
    relogio = RelogioFake()
    ctrl = _controlador(relogio)
    for _ in range(c.MAX_TENTATIVAS_AUTENTICACAO):
        ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=False)
    assert ctrl.esta_bloqueada(ORIGEM) is True
    ctrl.reiniciar(ORIGEM)
    assert ctrl.esta_bloqueada(ORIGEM) is False
    assert ctrl.segundos_restantes_bloqueio(ORIGEM) == 0


# ---------------------------------------------------------------------------
# Isolamento entre origens
# ---------------------------------------------------------------------------

def test_bloqueio_e_por_origem_independente() -> None:
    relogio = RelogioFake()
    outra = "203.0.113.20"
    ctrl = ControladorAutenticacao([ORIGEM, outra], relogio=relogio)
    for _ in range(c.MAX_TENTATIVAS_AUTENTICACAO):
        ctrl.avaliar_tentativa(ORIGEM, credenciais_validas=False)
    assert ctrl.esta_bloqueada(ORIGEM) is True
    # A outra origem permanece livre.
    assert ctrl.esta_bloqueada(outra) is False
    aval = ctrl.avaliar_tentativa(outra, credenciais_validas=True)
    assert aval.resultado is ResultadoAutenticacao.SUCESSO


# ---------------------------------------------------------------------------
# Validação de parâmetros do construtor
# ---------------------------------------------------------------------------

def test_max_tentativas_invalido_levanta_erro() -> None:
    with pytest.raises(ValueError):
        ControladorAutenticacao([ORIGEM], max_tentativas=0)


def test_bloqueio_segundos_negativo_levanta_erro() -> None:
    with pytest.raises(ValueError):
        ControladorAutenticacao([ORIGEM], bloqueio_segundos=-1)


# ---------------------------------------------------------------------------
# Tratamento de falha de autorização via IAM_Role_EC2 (Req. 12.9)
# ---------------------------------------------------------------------------

def test_tratar_falha_autorizacao_constroi_evento_sem_dados_parciais() -> None:
    evento = tratar_falha_autorizacao(
        operacao="SendMessage",
        recurso=c.FILA_STANDARD,
        erro_original="AccessDeniedException: not authorized",
    )
    assert isinstance(evento, EventoFalhaAutorizacao)
    assert evento.operacao == "SendMessage"
    assert evento.recurso == c.FILA_STANDARD
    # O erro original é preservado.
    assert "AccessDeniedException" in evento.erro_original
    # A mensagem em pt-BR deixa claro que nada parcial foi persistido.
    assert "nenhum dado parcial foi persistido" in evento.mensagem


def test_tratar_falha_autorizacao_registra_evento_quando_registro_informado() -> None:
    registro = RegistroFalhasAutorizacao()
    assert registro.quantidade() == 0
    tratar_falha_autorizacao(
        operacao="PutItem",
        recurso=c.TABELA_HISTORICO,
        erro_original="AccessDeniedException",
        registro=registro,
    )
    assert registro.quantidade() == 1
    assert registro.eventos[0].operacao == "PutItem"
    assert registro.eventos[0].recurso == c.TABELA_HISTORICO


def test_falha_autorizacao_iam_preserva_erro_original() -> None:
    erro = FalhaAutorizacaoIAM(
        operacao="ReceiveMessage",
        recurso=c.FILA_FIFO,
        erro_original="AccessDeniedException: denied",
    )
    assert erro.operacao == "ReceiveMessage"
    assert erro.recurso == c.FILA_FIFO
    assert erro.erro_original == "AccessDeniedException: denied"
    assert "IAM_Role_EC2" in str(erro)
