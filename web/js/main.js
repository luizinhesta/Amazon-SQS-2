/**
 * Orquestrador do Dashboard do Laboratório Amazon SQS (Tarefas 17.1 e 18.1).
 *
 * Monta a estrutura/esqueleto: controla a transição login -> painel autenticado,
 * inicializa a navegação entre as cinco áreas e injeta o contexto compartilhado
 * (cliente da API e container de cada área) nos módulos de área. O conteúdo
 * detalhado de cada área é preenchido pelas Tarefas 17.2 a 17.5 dentro dos
 * respectivos módulos em `js/areas/`.
 *
 * O wiring final (Tarefa 18.1) conecta aqui as duas pontas deixadas pelas tarefas
 * 17.x:
 *  - a atualização periódica (`createRefreshController`, Tarefa 17.5), iniciada ao
 *    entrar no painel e parada no logout;
 *  - o evento desacoplado `abrir-historico` (disparado pelo monitoramento, Tarefa
 *    17.3), ligado à área de histórico detalhado (Tarefa 17.4): navega para a área
 *    de histórico e dispara a consulta pelo `order_id` recebido.
 */

import { api } from "./api.js";
import { setupLogin } from "./auth.js";
import { setupNavigation } from "./navigation.js";
import { createRefreshController } from "./areas/refresh.js";

import * as overview from "./areas/overview.js";
import * as send from "./areas/send.js";
import * as monitoring from "./areas/monitoring.js";
import * as history from "./areas/history.js";
import * as lab from "./areas/lab.js";

/** Mapa de área -> módulo e id do container, para inicialização e extensão. */
const AREA_MODULES = {
  overview: { module: overview, containerId: "overview-area" },
  send: { module: send, containerId: "send-area" },
  monitoring: { module: monitoring, containerId: "monitoring-area" },
  history: { module: history, containerId: "history-area" },
  lab: { module: lab, containerId: "lab-area" },
};

function mostrarDashboard() {
  document.getElementById("login-screen").hidden = true;
  document.getElementById("dashboard").hidden = false;
}

function mostrarLogin() {
  document.getElementById("dashboard").hidden = true;
  document.getElementById("login-screen").hidden = false;
}

/**
 * Inicializa cada módulo de área uma única vez, passando o contexto compartilhado.
 * As áreas usam este contexto como ponto de extensão (Tarefas 17.2–17.5).
 */
function inicializarAreas() {
  Object.values(AREA_MODULES).forEach(({ module, containerId }) => {
    const container = document.getElementById(containerId);
    if (module && typeof module.init === "function") {
      module.init({ api, container });
    }
  });
}

/**
 * Liga o evento desacoplado `abrir-historico` (Tarefa 17.3) à área de histórico
 * detalhado (Tarefa 17.4), sem acoplar os módulos entre si.
 *
 * Ao receber o `CustomEvent` com `detail.orderId`, navega para a área "history" e
 * dispara a consulta: preenche o campo `#history-order-id` montado por `history.js`
 * e submete o formulário `.history-form` (que executa a consulta à API). Usar o DOM
 * público da área mantém a integração mínima e coesa, sem alterar os módulos.
 *
 * @param {{ activate: (area: string) => void }} nav Controlador de navegação.
 * @returns {() => void} Função que remove o listener (usada no logout).
 */
function conectarAbrirHistorico(nav) {
  const handler = (evento) => {
    const orderId = evento && evento.detail ? evento.detail.orderId : null;
    if (!orderId) {
      return;
    }

    // 1) Navega para a área de histórico detalhado.
    nav.activate("history");

    // 2) Preenche o campo de order_id e dispara a consulta da área de histórico.
    const input = document.getElementById("history-order-id");
    if (!input) {
      return;
    }
    input.value = orderId;

    const form = input.closest(".history-form");
    if (!form) {
      return;
    }
    // requestSubmit dispara o handler de submit da área (consulta à API), sem
    // recarregar a página. Fallback para dispatch de evento quando indisponível.
    if (typeof form.requestSubmit === "function") {
      form.requestSubmit();
    } else {
      form.dispatchEvent(new Event("submit", { cancelable: true, bubbles: true }));
    }
  };

  document.addEventListener("abrir-historico", handler);
  return () => document.removeEventListener("abrir-historico", handler);
}

function iniciarPainel() {
  mostrarDashboard();
  const nav = setupNavigation();
  inicializarAreas();

  // Atualização periódica (Tarefa 17.5): a cada tick, busca estatísticas e
  // monitoramento. As áreas renderizam os dados; aqui apenas disparamos a busca.
  const refresh = createRefreshController({
    onTick: async () => {
      await Promise.all([api.obterEstatisticas(), api.obterMonitoramento()]);
    },
  });
  refresh.start();

  // Liga o evento `abrir-historico` à área de histórico (Tarefas 17.3 e 17.4).
  const desconectarHistorico = conectarAbrirHistorico(nav);

  return {
    /** Encerra os recursos vivos do painel (chamado no logout). */
    encerrar() {
      refresh.stop();
      desconectarHistorico();
    },
  };
}

function main() {
  let painel = null;

  const login = setupLogin({
    onAuthenticated: () => {
      painel = iniciarPainel();
    },
  });

  document.getElementById("logout-button").addEventListener("click", () => {
    if (painel) {
      painel.encerrar();
      painel = null;
    }
    login.reset();
    mostrarLogin();
  });
}

document.addEventListener("DOMContentLoaded", main);
