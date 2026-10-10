"""Serviço de autenticação do Dashboard da API_Backend (Tarefa 15.6).

Encapsula a lógica do endpoint ``POST /api/autenticacao``, mantendo o roteador
FastAPI fino. Reutiliza o
:class:`app.seguranca.autenticacao.ControladorAutenticacao`, que implementa o
bloqueio por origem após 5 falhas consecutivas por 300 segundos (Req. 12.6) e a
restrição às origens autorizadas (Req. 12.7).

A verificação das credenciais (usuário/senha) é delegada a um **validador
injetável** (:class:`CredenciaisDashboard`), de modo que os testes forneçam
credenciais determinísticas sem segredos no código. A comparação usa
``hmac.compare_digest`` para evitar vazamento por tempo.

Referências de requisitos: 12.5, 12.6, 12.7.
"""

from __future__ import annotations

import hmac
from dataclasses import dataclass

from app.api.esquemas import RequisicaoAutenticacao, RespostaAutenticacao
from app.seguranca.autenticacao import ControladorAutenticacao


@dataclass(frozen=True)
class CredenciaisDashboard:
    """Credenciais esperadas do Dashboard (usuário/senha) para o login (Req. 12.5).

    Em produção, os valores vêm de uma fonte segura (ex.: parâmetro protegido),
    jamais de Access Keys da AWS. Nos testes, injeta-se um par determinístico. A
    verificação é feita em tempo constante com ``hmac.compare_digest``.
    """

    usuario: str
    senha: str

    def conferem(self, usuario: str, senha: str) -> bool:
        """Retorna ``True`` se usuário e senha informados conferem (tempo constante)."""
        usuario_ok = hmac.compare_digest(self.usuario, usuario)
        senha_ok = hmac.compare_digest(self.senha, senha)
        return usuario_ok and senha_ok


def autenticar(
    requisicao: RequisicaoAutenticacao,
    *,
    controlador: ControladorAutenticacao,
    credenciais: CredenciaisDashboard,
    origem_requisicao: str,
) -> RespostaAutenticacao:
    """Avalia uma tentativa de login do Dashboard (Req. 12.5, 12.6, 12.7).

    A ``origem`` efetiva é a informada no payload ou, na ausência, o host do cliente
    (``origem_requisicao``). As credenciais são verificadas pelo validador injetável
    e o desfecho (incluindo bloqueio por origem) é determinado pelo
    :class:`ControladorAutenticacao`.

    Args:
        requisicao: Payload do login (usuário, senha e origem opcional).
        controlador: Controlador de autenticação com bloqueio por origem.
        credenciais: Validador de usuário/senha injetável.
        origem_requisicao: Host do cliente da requisição (fallback de origem).

    Returns:
        :class:`RespostaAutenticacao` com o desfecho e o estado de bloqueio.
    """
    origem = requisicao.origem or origem_requisicao
    credenciais_validas = credenciais.conferem(requisicao.usuario, requisicao.senha)

    avaliacao = controlador.avaliar_tentativa(origem, credenciais_validas)

    return RespostaAutenticacao(
        acesso_concedido=avaliacao.acesso_concedido,
        resultado=avaliacao.resultado.value,
        origem=avaliacao.origem,
        bloqueada=avaliacao.bloqueada,
        falhas_consecutivas=avaliacao.falhas_consecutivas,
        segundos_restantes_bloqueio=avaliacao.segundos_restantes_bloqueio,
        mensagem=avaliacao.mensagem,
    )
