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
import {
  createRefreshController,
  createObservabilidade,
  MENSAGEM_TIMEOUT_CALCULO,
} from "./areas/refresh.js";

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

/**
 * Indica se há algum consumidor em falha segundo o endpoint de saúde
 * (`GET /api/consumidores/saude`). Um consumidor "morto" (thread encerrada) ou
 * "degradado" caracteriza falha a sinalizar na faixa amarela (Req. frente D).
 *
 * @param {{algum_morto?: boolean, algum_degradado?: boolean}} saude Resposta de saúde.
 * @returns {boolean}
 */
function temConsumidorEmFalha(saude) {
  return Boolean(saude && (saude.algum_morto || saude.algum_degradado));
}

/**
 * Descreve a falha do primeiro slot problemático (morto ou degradado) para a
 * mensagem da faixa amarela, identificando o tipo e o consumer_id.
 *
 * @param {{consumidores?: Array<{tipo?: string, consumer_id?: string, vivo?: boolean, degradado?: boolean}>}} saude
 * @returns {string} Texto em pt-BR.
 */
function descreverFalhaConsumidor(saude) {
  const lista = saude && Array.isArray(saude.consumidores) ? saude.consumidores : [];
  const slot = lista.find((c) => c && (c.vivo === false || c.degradado));
  if (!slot) {
    return "Um consumidor está em falha. Verifique os logs do serviço.";
  }
  const tipo = slot.tipo || "desconhecido";
  const id = slot.consumer_id || "sem id";
  const situacao = slot.vivo === false ? "encerrado" : "degradado";
  return `Consumidor ${tipo} (${id}) ${situacao}. Verifique os logs do serviço.`;
}

function iniciarPainel() {
  mostrarDashboard();
  const nav = setupNavigation();
  inicializarAreas();

  // Atualização periódica (Tarefa 17.5 + frente D): a cada tick, busca
  // estatísticas, monitoramento e saúde dos consumidores. As áreas renderizam os
  // dados; aqui controlamos somente a faixa global de observabilidade.
  //
  // A faixa #global-status tem quatro desfechos, aplicados por PRECEDÊNCIA (do
  // mais crítico para o menos crítico), e o onTick nunca lança (o catch do
  // refresh.js fica apenas como salvaguarda):
  //  (1) API indisponível (vermelha): estatística E monitoramento rejeitaram por
  //      erro real de rede/HTTP (ApiError) -> obs.marcarIndisponivel().
  //  (2) Consumidor em falha (amarela): a saúde resolveu e indica morto/degradado
  //      -> obs.marcarAtencao(...). A falha da PRÓPRIA chamada de saúde (rejeição)
  //      NÃO aciona a amarela.
  //  (3) Timeout de cálculo (laranja): alguma resposta resolveu com `timeout:true`
  //      -> obs.marcarFalha(MENSAGEM_TIMEOUT_CALCULO).
  //  (4) Sucesso: obs.marcarSucesso().
  const obs = createObservabilidade();
  const refresh = createRefreshController({
    observabilidade: obs,
    gerenciaObservabilidade: false,
    onTick: async () => {
      const [rEstat, rMonit, rSaude] = await Promise.allSettled([
        api.obterEstatisticas(),
        api.obterMonitoramento(),
        api.obterSaudeConsumidores(),
      ]);

      // (1) API indisponível: AMBAS estatística e monitoramento rejeitaram.
      if (rEstat.status === "rejected" && rMonit.status === "rejected") {
        obs.marcarIndisponivel();
        return;
      }

      // (2) Consumidor em falha: só quando a chamada de saúde RESOLVEU.
      if (rSaude.status === "fulfilled" && temConsumidorEmFalha(rSaude.value)) {
        obs.marcarAtencao(descreverFalhaConsumidor(rSaude.value));
        return;
      }

      // (3) Timeout de cálculo: alguma resposta HTTP ok trouxe `timeout: true`.
      const algumTimeout = [rEstat, rMonit].some(
        (r) => r.status === "fulfilled" && r.value && r.value.timeout
      );
      if (algumTimeout) {
        obs.marcarFalha(MENSAGEM_TIMEOUT_CALCULO);
        return;
      }

      // (4) Sucesso.
      obs.marcarSucesso();
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
