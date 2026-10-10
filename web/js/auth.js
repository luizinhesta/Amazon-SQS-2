/**
 * Controle da tela de login do Dashboard (Tarefa 17.1 — Req. 12.5, 12.6).
 *
 * Responsável por:
 *  - Enviar o login via POST /api/autenticacao (através do cliente `api`).
 *  - Exibir mensagens de erro em pt-BR e as tentativas restantes.
 *  - Exibir a indicação de bloqueio por origem, com contagem regressiva dos
 *    segundos restantes quando a resposta indica `bloqueada = true` (Req. 12.6).
 *
 * A verificação de credenciais e a regra de bloqueio ficam na API_Backend; esta
 * camada apenas reflete o desfecho retornado (acesso_concedido, bloqueada,
 * falhas_consecutivas, segundos_restantes_bloqueio, mensagem).
 */

import { api, ApiError } from "./api.js";

/**
 * Inicializa a tela de login.
 *
 * @param {object} options
 * @param {() => void} options.onAuthenticated Callback chamado ao autenticar com sucesso.
 * @returns {{ reset: () => void }} API mínima para reiniciar o formulário (ex.: logout).
 */
export function setupLogin({ onAuthenticated }) {
  const form = document.getElementById("login-form");
  const usuarioInput = document.getElementById("login-username");
  const senhaInput = document.getElementById("login-password");
  const submitButton = document.getElementById("login-submit");
  const feedback = document.getElementById("login-feedback");
  const lockout = document.getElementById("login-lockout");
  const lockoutText = document.getElementById("login-lockout-text");

  let lockoutTimer = null;

  function limparFeedback() {
    feedback.hidden = true;
    feedback.textContent = "";
  }

  function mostrarFeedback(mensagem) {
    feedback.textContent = mensagem;
    feedback.hidden = false;
  }

  function pararContagem() {
    if (lockoutTimer !== null) {
      clearInterval(lockoutTimer);
      lockoutTimer = null;
    }
  }

  function esconderBloqueio() {
    pararContagem();
    lockout.hidden = true;
    lockoutText.textContent = "";
    submitButton.disabled = false;
  }

  /**
   * Exibe a indicação de bloqueio e inicia a contagem regressiva, desabilitando o
   * botão de login enquanto a origem estiver bloqueada (Req. 12.6).
   *
   * @param {number} segundos Segundos restantes de bloqueio informados pela API.
   */
  function mostrarBloqueio(segundos) {
    pararContagem();
    submitButton.disabled = true;
    lockout.hidden = false;

    let restantes = Math.max(0, Math.floor(segundos));

    const atualizarTexto = () => {
      lockoutText.textContent =
        `Acesso bloqueado para esta origem após tentativas inválidas. ` +
        `Tente novamente em ${restantes} segundo(s).`;
    };

    atualizarTexto();

    lockoutTimer = setInterval(() => {
      restantes -= 1;
      if (restantes <= 0) {
        esconderBloqueio();
        limparFeedback();
      } else {
        atualizarTexto();
      }
    }, 1000);
  }

  /**
   * Aplica o desfecho retornado por POST /api/autenticacao à interface.
   *
   * @param {object} resposta RespostaAutenticacao da API_Backend.
   */
  function tratarResposta(resposta) {
    if (resposta.acesso_concedido) {
      esconderBloqueio();
      limparFeedback();
      onAuthenticated();
      return;
    }

    // Falha ou bloqueio: mostra a mensagem em pt-BR vinda da API.
    mostrarFeedback(resposta.mensagem || "Não foi possível autenticar.");

    if (resposta.bloqueada) {
      mostrarBloqueio(resposta.segundos_restantes_bloqueio || 0);
    } else {
      esconderBloqueio();
    }
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    limparFeedback();

    const usuario = usuarioInput.value.trim();
    const senha = senhaInput.value;

    if (!usuario || !senha) {
      mostrarFeedback("Informe o usuário e a senha.");
      return;
    }

    submitButton.disabled = true;
    try {
      const resposta = await api.autenticar(usuario, senha);
      tratarResposta(resposta);
    } catch (erro) {
      const detalhe =
        erro instanceof ApiError && erro.detail
          ? ` (${typeof erro.detail === "string" ? erro.detail : "detalhe indisponível"})`
          : "";
      mostrarFeedback(
        `Não foi possível concluir o login no momento.${detalhe}`
      );
    } finally {
      // Só reabilita se não houver bloqueio ativo.
      if (lockout.hidden) {
        submitButton.disabled = false;
      }
    }
  });

  return {
    /** Reinicia o formulário de login (usado no logout). */
    reset() {
      esconderBloqueio();
      limparFeedback();
      form.reset();
    },
  };
}
