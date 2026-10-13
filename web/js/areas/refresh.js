/**
 * Utilitário de atualização periódica e observabilidade (Tarefa 17.5).
 *
 * Implementa um "polling" reutilizável para o Dashboard: executa um callback por
 * tick ao final de cada intervalo configurável, SEM recarregar a página, e
 * mantém a observabilidade exigida pelos requisitos:
 *  - exibe o horário da última atualização bem-sucedida (data, hora e fuso);
 *  - em caso de falha, preserva os últimos dados válidos (não limpa a tela) e
 *    exibe uma indicação visível de que os dados podem estar desatualizados;
 *  - mantém inalterado o horário da última atualização bem-sucedida após falha.
 *
 * Requisitos cobertos: 3.9, 3.10, 3.11, 13.1, 13.2, 13.3, 13.4, 13.5.
 *
 * ISOLAMENTO: este módulo NÃO toca nos módulos de área (overview/send/monitoring/
 * history/lab), em api.js, index.html ou navigation.js. A integração final é feita
 * pela Tarefa 18.1 em main.js.
 *
 * ---------------------------------------------------------------------------
 * COMO INTEGRAR EM main.js (Tarefa 18.1) — uma linha de import + uma chamada:
 *
 *   import { createRefreshController } from "./areas/refresh.js";
 *
 *   // Dentro de iniciarPainel(), após inicializarAreas():
 *   const refresh = createRefreshController({
 *     onTick: async () => {
 *       // Atualiza estatísticas e monitoramento usando o cliente da API.
 *       await Promise.all([api.obterEstatisticas(), api.obterMonitoramento()]);
 *       // (As áreas 17.2/17.3 renderizam os dados; aqui só disparamos a busca.)
 *     },
 *   });
 *   refresh.start();          // inicia a atualização periódica (padrão 10 s)
 *   // refresh.setIntervalSeconds(5);  // ex.: visão geral usa 5 s (faixa 1–60 s)
 *   // refresh.stop();         // ex.: ao fazer logout, em login.reset()
 *
 * O controlador atualiza automaticamente os elementos `#last-update` e
 * `#global-status` do index.html; nenhum HTML adicional é necessário.
 * ---------------------------------------------------------------------------
 */

/**
 * Faixas de intervalo (em segundos) para a atualização periódica.
 *
 * - DEFAULT: observabilidade geral do Dashboard (Req. 13.2) — [5, 300], padrão 10.
 * - OVERVIEW: visão geral (Req. 3.9) — [1, 60], padrão 5.
 */
export const INTERVAL_RANGES = Object.freeze({
  default: Object.freeze({ min: 5, max: 300, padrao: 10 }),
  overview: Object.freeze({ min: 1, max: 60, padrao: 5 }),
});

/** Mensagem em pt-BR exibida quando uma atualização periódica falha (Req. 13.5, 3.11). */
export const MENSAGEM_DADOS_DESATUALIZADOS =
  "Não foi possível atualizar os dados. Os últimos dados válidos foram " +
  "preservados e podem estar desatualizados.";

/** Mensagem em pt-BR exibida quando a atualização excede o tempo limite de cálculo (Req. 14.*). */
export const MENSAGEM_TIMEOUT_CALCULO =
  "A atualização excedeu o tempo limite de cálculo. Os últimos dados " +
  "válidos foram preservados.";

/** Mensagem em pt-BR exibida quando a API_Backend está indisponível (sem conexão/HTTP). */
export const MENSAGEM_API_INDISPONIVEL =
  "API indisponível. Verifique a conexão ou se o serviço está no ar.";

/** Prefixo da indicação de horário da última atualização bem-sucedida (Req. 3.10, 13.4). */
const PREFIXO_ULTIMA_ATUALIZACAO = "Última atualização:";

/**
 * Valida um intervalo contra uma faixa [min, max] (Req. 13.3).
 *
 * Aceita o valor se e somente se for um inteiro dentro da faixa. Ao rejeitar,
 * preserva o valor anterior e produz uma mensagem em pt-BR com a faixa válida.
 *
 * @param {number} valor Intervalo informado, em segundos.
 * @param {number} anterior Intervalo em vigor, preservado quando o valor é rejeitado.
 * @param {{min: number, max: number}} faixa Limites inclusivos aceitos.
 * @returns {{aceito: boolean, valor: number, mensagem: string}}
 */
export function validarIntervalo(valor, anterior, faixa) {
  const inteiro = Number.isInteger(valor);
  if (inteiro && valor >= faixa.min && valor <= faixa.max) {
    return { aceito: true, valor, mensagem: "" };
  }
  return {
    aceito: false,
    valor: anterior,
    mensagem:
      `Intervalo inválido: informe um número inteiro entre ${faixa.min} e ` +
      `${faixa.max} segundos. O intervalo anterior (${anterior} s) foi mantido.`,
  };
}

/**
 * Formata um instante como data/hora local com identificação de fuso (Req. 3.10, 13.4).
 *
 * Ex.: "Última atualização: 05/02/2025 14:03:12 (GMT-3)".
 *
 * @param {Date} data Instante da atualização bem-sucedida.
 * @returns {string} Texto em pt-BR com data, hora (h/min/s) e fuso.
 */
export function formatarHorarioAtualizacao(data) {
  const dataHora = new Intl.DateTimeFormat("pt-BR", {
    dateStyle: "short",
    timeStyle: "medium",
    timeZoneName: "short",
  }).format(data);
  return `${PREFIXO_ULTIMA_ATUALIZACAO} ${dataHora}`;
}

/**
 * Cria um indicador de observabilidade ligado aos elementos do Dashboard.
 *
 * Encapsula a leitura/escrita dos elementos `#last-update` (horário da última
 * atualização bem-sucedida) e `#global-status` (faixa de dados desatualizados),
 * tolerando a ausência de qualquer um deles (fora do DOM em testes isolados).
 *
 * @param {object} [options]
 * @param {Document|HTMLElement} [options.root] Raiz de busca dos elementos (padrão: document).
 * @param {string} [options.lastUpdateId] Id do elemento de horário (padrão "last-update").
 * @param {string} [options.statusId] Id da faixa de status (padrão "global-status").
 * A faixa `#global-status` tem três estados mutuamente exclusivos, aplicados por
 * métodos distintos conforme a precedência do onTick (ver main.js):
 *  - `marcarIndisponivel()` — API_Backend fora do ar (classe `global-status--error`, vermelha);
 *  - `marcarAtencao(texto)` — consumidor em falha (classe `global-status--warn`, amarela);
 *  - `marcarFalha(mensagem)` — dados desatualizados/timeout de cálculo (classe `global-status--stale`, laranja).
 * Cada método remove as outras duas classes de estado antes de aplicar a sua, e
 * `marcarSucesso` limpa as três (atualização bem-sucedida).
 *
 * @returns {{
 *   marcarSucesso: (data?: Date) => void,
 *   marcarFalha: (mensagem?: string) => void,
 *   marcarIndisponivel: () => void,
 *   marcarAtencao: (texto: string) => void,
 * }}
 */
export function createObservabilidade({
  root = typeof document !== "undefined" ? document : null,
  lastUpdateId = "last-update",
  statusId = "global-status",
} = {}) {
  const buscar = (id) => (root && root.getElementById ? root.getElementById(id) : null);

  // Classes mutuamente exclusivas da faixa de status global.
  const CLASSES_ESTADO = [
    "global-status--stale",
    "global-status--error",
    "global-status--warn",
  ];

  /** Exibe a faixa com uma mensagem e aplica somente a classe informada. */
  function aplicarEstado(classe, mensagem) {
    const status = buscar(statusId);
    if (!status) {
      return;
    }
    status.textContent = mensagem;
    status.hidden = false;
    status.classList.remove(...CLASSES_ESTADO);
    status.classList.add(classe);
  }

  function marcarSucesso(data = new Date()) {
    const lastUpdate = buscar(lastUpdateId);
    if (lastUpdate) {
      lastUpdate.textContent = formatarHorarioAtualizacao(data);
    }
    // Esconde a faixa: a atualização foi bem-sucedida (limpa os três estados).
    const status = buscar(statusId);
    if (status) {
      status.hidden = true;
      status.textContent = "";
      status.classList.remove(...CLASSES_ESTADO);
    }
  }

  function marcarFalha(mensagem = MENSAGEM_DADOS_DESATUALIZADOS) {
    // Preserva o horário da última atualização bem-sucedida (NÃO altera #last-update).
    aplicarEstado("global-status--stale", mensagem);
  }

  function marcarIndisponivel() {
    // API_Backend fora do ar: estado de erro (vermelha), precedência máxima.
    aplicarEstado("global-status--error", MENSAGEM_API_INDISPONIVEL);
  }

  function marcarAtencao(texto) {
    // Consumidor em falha: estado de atenção (amarela); texto injetado pelo chamador.
    aplicarEstado("global-status--warn", texto);
  }

  return { marcarSucesso, marcarFalha, marcarIndisponivel, marcarAtencao };
}

/**
 * Cria um controlador de atualização periódica configurável.
 *
 * A cada intervalo, executa `onTick` (pode ser assíncrono). Em sucesso, registra o
 * horário da última atualização bem-sucedida; em falha (exceção lançada ou rejeição
 * da promise), preserva os últimos dados válidos e exibe a indicação de dados
 * desatualizados, sem interromper o ciclo. Ticks não se sobrepõem: um novo tick só
 * dispara após o anterior concluir.
 *
 * @param {object} options
 * @param {() => (void|Promise<void>)} options.onTick Callback executado por tick.
 * @param {number} [options.intervalSeconds] Intervalo inicial em segundos (padrão: faixa.padrao).
 * @param {{min: number, max: number, padrao: number}} [options.range] Faixa aceita (padrão: default).
 * @param {boolean} [options.runImmediately=true] Executa um tick imediato ao iniciar.
 * @param {(mensagem: string) => void} [options.onIntervalRejected] Notificado quando um intervalo é rejeitado (Req. 13.3).
 * @param {(erro: unknown) => void} [options.onError] Notificado quando um tick falha (além da indicação visual).
 * @param {ReturnType<typeof createObservabilidade>} [options.observabilidade] Indicador de observabilidade (padrão: elementos do DOM).
 * @param {boolean} [options.gerenciaObservabilidade=true] Quando true (padrão), o controlador marca sucesso/falha automaticamente a cada tick. Quando false, o `onTick` assume a observabilidade (chama obs.* por conta própria) e o controlador só aciona `marcarFalha` no catch, como salvaguarda.
 * @param {{setInterval: Function, clearInterval: Function}} [options.timers] Temporizadores injetáveis (para testes).
 * @returns {{
 *   start: () => void,
 *   stop: () => void,
 *   isRunning: () => boolean,
 *   setIntervalSeconds: (segundos: number) => {aceito: boolean, valor: number, mensagem: string},
 *   getIntervalSeconds: () => number,
 *   tickAgora: () => Promise<void>,
 * }}
 */
export function createRefreshController({
  onTick,
  intervalSeconds,
  range = INTERVAL_RANGES.default,
  runImmediately = true,
  onIntervalRejected,
  onError,
  observabilidade,
  gerenciaObservabilidade = true,
  timers = {
    setInterval:
      typeof setInterval !== "undefined" ? setInterval.bind(globalThis) : null,
    clearInterval:
      typeof clearInterval !== "undefined" ? clearInterval.bind(globalThis) : null,
  },
} = {}) {
  if (typeof onTick !== "function") {
    throw new TypeError("createRefreshController requer um callback onTick.");
  }

  const obs = observabilidade || createObservabilidade();
  let intervaloSegundos =
    Number.isInteger(intervalSeconds) &&
    intervalSeconds >= range.min &&
    intervalSeconds <= range.max
      ? intervalSeconds
      : range.padrao;

  let handle = null;
  let executando = false;
  let tickEmAndamento = false;

  /** Executa um tick, tratando sucesso/falha para a observabilidade. */
  async function tick() {
    if (tickEmAndamento) {
      return; // Evita sobreposição de ticks (um por vez).
    }
    tickEmAndamento = true;
    try {
      await onTick();
      // Quando o onTick assume a observabilidade (gerenciaObservabilidade=false),
      // ele próprio chama obs.* conforme a precedência; não sobrescrevemos aqui.
      if (gerenciaObservabilidade) {
        obs.marcarSucesso(new Date());
      }
    } catch (erro) {
      // Falha: preserva os últimos dados válidos e indica desatualização (Req. 3.11, 13.5).
      // Salvaguarda: mesmo com onTick gerenciando a observabilidade, uma exceção
      // inesperada cai aqui e sinaliza desatualização.
      obs.marcarFalha();
      if (typeof onError === "function") {
        onError(erro);
      }
    } finally {
      tickEmAndamento = false;
    }
  }

  /** (Re)agenda o temporizador com o intervalo atual. */
  function reagendar() {
    if (handle !== null && timers.clearInterval) {
      timers.clearInterval(handle);
      handle = null;
    }
    if (executando && timers.setInterval) {
      handle = timers.setInterval(() => {
        void tick();
      }, intervaloSegundos * 1000);
    }
  }

  function start() {
    if (executando) {
      return;
    }
    executando = true;
    reagendar();
    if (runImmediately) {
      void tick();
    }
  }

  function stop() {
    executando = false;
    if (handle !== null && timers.clearInterval) {
      timers.clearInterval(handle);
      handle = null;
    }
  }

  /**
   * Ajusta o intervalo de atualização, validando a faixa (Req. 13.3, 3.9).
   * Rejeita valores fora da faixa, preservando o intervalo anterior.
   */
  function setIntervalSeconds(segundos) {
    const resultado = validarIntervalo(segundos, intervaloSegundos, range);
    if (resultado.aceito) {
      intervaloSegundos = resultado.valor;
      reagendar(); // Aplica o novo intervalo imediatamente, se em execução.
    } else if (typeof onIntervalRejected === "function") {
      onIntervalRejected(resultado.mensagem);
    }
    return resultado;
  }

  return {
    start,
    stop,
    isRunning: () => executando,
    setIntervalSeconds,
    getIntervalSeconds: () => intervaloSegundos,
    tickAgora: tick,
  };
}
