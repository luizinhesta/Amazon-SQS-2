/**
 * Área "Visão geral" do Dashboard (Tarefa 17.2 — Req. 2.1-2.13, 3.1-3.12).
 *
 * Responsabilidades desta área:
 *  - Exibir as estatísticas agregadas por fluxo (Standard e FIFO): enviadas,
 *    disponíveis, em processamento, processadas, com falha, encaminhadas para DLQ
 *    e recebidas por cada consumidor (Req. 3.1-3.8, 3.12).
 *  - Exibir o horário da última atualização bem-sucedida das estatísticas, com
 *    data, hora (horas/minutos/segundos) e identificação do fuso (Req. 3.10).
 *  - Exibir os parâmetros de configuração com rótulo distinto Config_Aplicada e
 *    Config_Simulacao (Req. 2.1-2.10, 2.13).
 *  - Permitir ajustar os parâmetros de Config_Simulacao (PUT /api/config/simulacao)
 *    com validação de faixa e mensagens em pt-BR (Req. 2.11, 2.12).
 *  - Preservar os últimos valores exibidos e indicar falha de atualização quando a
 *    consulta falhar, sem apagar os dados anteriores nem alterar o horário da
 *    última atualização bem-sucedida (Req. 3.11).
 *
 * A lógica de agregação, as faixas e os rótulos vêm da API_Backend; esta camada
 * apenas renderiza o que os endpoints `GET /api/estatisticas`, `GET /api/config` e
 * `PUT /api/config/simulacao` retornam (ver `app/api/esquemas.py`).
 *
 * O código, nomes de variáveis e funções ficam em inglês; a interface, rótulos e
 * mensagens para o usuário ficam em pt-BR.
 */

/**
 * Rótulos em pt-BR das estatísticas por fluxo, na ordem de exibição.
 * As chaves correspondem aos campos de `EstatisticasFluxoSaida` da API.
 */
const STAT_LABELS = [
  { key: "enviadas", label: "Enviadas" },
  { key: "disponiveis", label: "Disponíveis" },
  { key: "em_processamento", label: "Em processamento" },
  { key: "processadas", label: "Processadas" },
  { key: "com_falha", label: "Com falha" },
  { key: "encaminhadas_dlq", label: "Encaminhadas para DLQ" },
];

/** Rótulos dos dois fluxos exibidos (Req. 3.8). */
const FLOWS = [
  { key: "standard", label: "Standard" },
  { key: "fifo", label: "FIFO" },
];

/**
 * Mapa de nome de parâmetro de Config_Simulacao -> chave do payload de ajuste
 * aceito por `PUT /api/config/simulacao` (ver `AjusteConfigSimulacao`). Os nomes
 * dos parâmetros vêm da API em `GET /api/config` (`ParametroConfig.nome`).
 */
const SIMULACAO_PARAM_FIELD = {
  "Tempo de processamento simulado": "tempo_processamento_simulado_segundos",
  "Quantidade de mensagens por envio": "quantidade_mensagens",
  "Intervalo de atualização": "intervalo_atualizacao_segundos",
};

/**
 * Cria um elemento com classe e, opcionalmente, texto e atributos.
 *
 * @param {string} tag Nome da tag.
 * @param {string} [className] Classe(s) CSS.
 * @param {string} [text] Conteúdo textual (inserido como texto, nunca HTML).
 * @param {Record<string, string>} [attrs] Atributos adicionais.
 * @returns {HTMLElement}
 */
function el(tag, className, text, attrs) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = text;
  if (attrs) {
    Object.entries(attrs).forEach(([name, value]) => node.setAttribute(name, value));
  }
  return node;
}

/**
 * Formata um instante como data/hora com segundos e identificação do fuso (Req. 3.10).
 *
 * @param {Date} date Instante a formatar.
 * @returns {string} Ex.: "08/02/2025 14:03:21 (Horário Padrão de Brasília)".
 */
function formatTimestamp(date) {
  // Data e hora completas (com segundos) no fuso local do navegador.
  const dataHora = date.toLocaleString("pt-BR", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  // Identificação do fuso horário (nome longo quando disponível; recua ao offset).
  let fuso = "";
  try {
    const partes = new Intl.DateTimeFormat("pt-BR", {
      timeZoneName: "long",
    }).formatToParts(date);
    const parteFuso = partes.find((p) => p.type === "timeZoneName");
    fuso = parteFuso ? parteFuso.value : "";
  } catch {
    fuso = "";
  }
  if (!fuso) {
    const offsetMin = -date.getTimezoneOffset();
    const sinal = offsetMin >= 0 ? "+" : "-";
    const abs = Math.abs(offsetMin);
    const hh = String(Math.floor(abs / 60)).padStart(2, "0");
    const mm = String(abs % 60).padStart(2, "0");
    fuso = `UTC${sinal}${hh}:${mm}`;
  }

  return `${dataHora} (${fuso})`;
}

/**
 * Inicializa a área de visão geral.
 *
 * @param {object} ctx Contexto compartilhado do Dashboard.
 * @param {import("../api.js").api} ctx.api Cliente da API_Backend.
 * @param {HTMLElement} ctx.container Seção `#overview-area` desta área.
 */
export function init(ctx) {
  const { api, container } = ctx;
  if (!container) return;

  const overview = new OverviewArea(api, container);
  overview.render();
  overview.refreshAll();
}

/**
 * Encapsula o estado e a renderização da visão geral. Mantém os últimos valores
 * exibidos com sucesso para preservá-los quando uma atualização falha (Req. 3.11).
 */
class OverviewArea {
  /**
   * @param {import("../api.js").api} api Cliente da API_Backend.
   * @param {HTMLElement} container Seção desta área.
   */
  constructor(api, container) {
    this.api = api;
    this.container = container;

    // Último estado válido exibido (preservado em falha — Req. 3.11).
    this.lastStats = null; // RespostaEstatisticas bem-sucedida.
    this.lastSuccessAt = null; // Date da última atualização bem-sucedida.
    this.lastConfig = null; // RespostaConfig bem-sucedida.

    // Referências de nós preenchidas em render().
    this.nodes = {};
  }

  /** Monta a estrutura estática da área (uma única vez), substituindo o placeholder. */
  render() {
    this.container.textContent = "";

    this.container.appendChild(el("h2", "area-title", "Visão geral"));

    // Barra superior da área: horário da última atualização + indicação de falha.
    const statusBar = el("div", "ov-statusbar");
    const lastUpdate = el("p", "ov-last-update");
    lastUpdate.appendChild(el("span", "ov-last-update-label", "Última atualização bem-sucedida: "));
    const lastUpdateValue = el("span", "ov-last-update-value", "—");
    lastUpdate.appendChild(lastUpdateValue);

    const failureBanner = el(
      "p",
      "ov-failure",
      "",
      { role: "alert", "aria-live": "assertive", hidden: "" }
    );

    statusBar.appendChild(lastUpdate);
    statusBar.appendChild(failureBanner);
    this.container.appendChild(statusBar);

    // Seção de estatísticas por fluxo.
    const statsSection = el("section", "ov-section", undefined, {
      "aria-label": "Estatísticas por fluxo",
    });
    statsSection.appendChild(el("h3", "ov-section-title", "Estatísticas por fluxo"));
    const statsGrid = el("div", "ov-flows");
    statsSection.appendChild(statsGrid);
    this.container.appendChild(statsSection);

    // Seção de configuração (Config_Aplicada e Config_Simulacao).
    const configSection = el("section", "ov-section", undefined, {
      "aria-label": "Parâmetros de configuração",
    });
    configSection.appendChild(el("h3", "ov-section-title", "Configuração"));
    const configOrientation = el("p", "ov-config-orientation");
    const appliedBlock = el("div", "ov-config-block");
    const simulationBlock = el("div", "ov-config-block");
    configSection.appendChild(configOrientation);
    configSection.appendChild(appliedBlock);
    configSection.appendChild(simulationBlock);
    this.container.appendChild(configSection);

    this.nodes = {
      lastUpdateValue,
      failureBanner,
      statsGrid,
      configOrientation,
      appliedBlock,
      simulationBlock,
    };
  }

  /** Dispara a atualização de estatísticas e de configuração em paralelo. */
  refreshAll() {
    this.refreshStats();
    this.refreshConfig();
  }

  /**
   * Atualiza as estatísticas por fluxo. Em sucesso, preserva o novo estado e o
   * horário; em falha (erro de rede/HTTP ou timeout da API), mantém os últimos
   * valores e exibe a indicação de falha (Req. 3.11).
   */
  async refreshStats() {
    try {
      const stats = await this.api.obterEstatisticas();
      // A API pode indicar timeout sem erro HTTP: tratamos como falha de
      // atualização, preservando os últimos dados válidos (Req. 14.3 / 3.11).
      if (stats && stats.timeout) {
        this.showFailure(
          stats.mensagem ||
            "A atualização das estatísticas excedeu o tempo limite. Dados anteriores preservados."
        );
        return;
      }

      this.lastStats = stats;
      this.lastSuccessAt = new Date();
      this.clearFailure();
      this.renderStats();
      this.renderLastUpdate();
    } catch (erro) {
      this.showFailure(
        "Não foi possível atualizar as estatísticas. Os últimos valores foram preservados."
      );
    }
  }

  /**
   * Atualiza os parâmetros de configuração. Em falha, preserva o último estado de
   * configuração exibido e sinaliza a falha de atualização.
   */
  async refreshConfig() {
    try {
      const config = await this.api.obterConfig();
      this.lastConfig = config;
      this.renderConfig();
    } catch (erro) {
      this.showFailure(
        "Não foi possível atualizar a configuração. Os últimos valores foram preservados."
      );
    }
  }

  /** Renderiza o horário da última atualização bem-sucedida (Req. 3.10). */
  renderLastUpdate() {
    if (!this.lastSuccessAt) {
      this.nodes.lastUpdateValue.textContent = "—";
      return;
    }
    this.nodes.lastUpdateValue.textContent = formatTimestamp(this.lastSuccessAt);
  }

  /** Exibe a indicação visível de falha de atualização sem apagar dados (Req. 3.11). */
  showFailure(mensagem) {
    const banner = this.nodes.failureBanner;
    banner.textContent = mensagem;
    banner.hidden = false;
  }

  /** Oculta a indicação de falha após uma atualização bem-sucedida. */
  clearFailure() {
    const banner = this.nodes.failureBanner;
    banner.hidden = true;
    banner.textContent = "";
  }

  /** Renderiza as estatísticas por fluxo a partir do último estado válido. */
  renderStats() {
    const grid = this.nodes.statsGrid;
    grid.textContent = "";

    if (!this.lastStats) {
      grid.appendChild(
        el("p", "ov-empty", "Nenhuma estatística disponível ainda.")
      );
      return;
    }

    FLOWS.forEach(({ key, label }) => {
      const fluxo = this.lastStats[key] || null;
      grid.appendChild(this.renderFlowCard(label, fluxo));
    });
  }

  /**
   * Monta o cartão de um fluxo (Standard ou FIFO) com seus contadores e os
   * recebimentos por consumidor.
   *
   * @param {string} label Rótulo do fluxo em pt-BR.
   * @param {object|null} fluxo EstatisticasFluxoSaida da API, ou null.
   * @returns {HTMLElement}
   */
  renderFlowCard(label, fluxo) {
    const card = el("article", "ov-flow-card");
    card.appendChild(el("h4", "ov-flow-title", label));

    const list = el("dl", "ov-stat-list");
    STAT_LABELS.forEach(({ key, label: statLabel }) => {
      // Req. 3.12: sem dados, exibe 0.
      const valor = fluxo && Number.isFinite(fluxo[key]) ? fluxo[key] : 0;
      const row = el("div", "ov-stat-row");
      row.appendChild(el("dt", "ov-stat-label", statLabel));
      row.appendChild(el("dd", "ov-stat-value", String(valor)));
      list.appendChild(row);
    });
    card.appendChild(list);

    // Recebidas por consumidor, identificando cada consumidor (Req. 3.7).
    const consumers = (fluxo && Array.isArray(fluxo.recebidas_por_consumidor))
      ? fluxo.recebidas_por_consumidor
      : [];
    const consumersWrap = el("div", "ov-consumers");
    consumersWrap.appendChild(
      el("p", "ov-consumers-title", "Recebidas por consumidor")
    );
    if (consumers.length === 0) {
      consumersWrap.appendChild(
        el("p", "ov-empty", "Nenhum consumidor registrou recebimentos.")
      );
    } else {
      const clist = el("dl", "ov-stat-list");
      consumers.forEach((item) => {
        const nome = item && item.consumidor ? String(item.consumidor) : "—";
        const qtd = item && Number.isFinite(item.recebidas) ? item.recebidas : 0;
        const row = el("div", "ov-stat-row");
        row.appendChild(el("dt", "ov-stat-label", nome));
        row.appendChild(el("dd", "ov-stat-value", String(qtd)));
        clist.appendChild(row);
      });
      consumersWrap.appendChild(clist);
    }
    card.appendChild(consumersWrap);

    return card;
  }

  /** Renderiza os parâmetros de Config_Aplicada e Config_Simulacao. */
  renderConfig() {
    const config = this.lastConfig;
    if (!config) return;

    // Orientação em pt-BR de que Config_Aplicada só muda no Console AWS (Req. 2.13).
    this.nodes.configOrientation.textContent =
      config.orientacao_config_aplicada || "";

    this.renderAppliedConfig(config.config_aplicada || []);
    this.renderSimulationConfig(config.config_simulacao || []);
  }

  /**
   * Renderiza os parâmetros de Config_Aplicada (somente leitura), cada um com o
   * rótulo distinto Config_Aplicada e a orientação de alteração no Console AWS.
   *
   * @param {Array<object>} parametros Lista de ParametroConfig da API.
   */
  renderAppliedConfig(parametros) {
    const block = this.nodes.appliedBlock;
    block.textContent = "";
    block.appendChild(el("h4", "ov-config-title", "Config_Aplicada"));
    block.appendChild(
      el(
        "p",
        "ov-config-hint",
        "Parâmetros efetivamente configurados nos recursos SQS. A alteração deve ser feita no Console AWS."
      )
    );

    const list = el("div", "ov-param-list");
    parametros.forEach((p) => {
      const row = el("div", "ov-param-row");
      const header = el("div", "ov-param-header");
      header.appendChild(el("span", "ov-param-name", p.nome));
      header.appendChild(el("span", "ov-badge ov-badge-applied", "Config_Aplicada"));
      row.appendChild(header);
      row.appendChild(el("span", "ov-param-value", this.formatParamValue(p)));
      list.appendChild(row);
    });
    block.appendChild(list);
  }

  /**
   * Renderiza os parâmetros ajustáveis de Config_Simulacao, cada um com um campo
   * numérico limitado à faixa [faixa_minima, faixa_maxima] e um botão de ajuste.
   *
   * @param {Array<object>} parametros Lista de ParametroConfig ajustáveis da API.
   */
  renderSimulationConfig(parametros) {
    const block = this.nodes.simulationBlock;
    block.textContent = "";
    block.appendChild(el("h4", "ov-config-title", "Config_Simulacao"));
    block.appendChild(
      el(
        "p",
        "ov-config-hint",
        "Parâmetros usados apenas pela lógica de simulação. Ajuste dentro da faixa válida."
      )
    );

    const list = el("div", "ov-param-list");
    parametros.forEach((p) => {
      list.appendChild(this.renderSimulationParam(p));
    });
    block.appendChild(list);
  }

  /**
   * Monta a linha de um parâmetro de Config_Simulacao com entrada e ajuste.
   *
   * @param {object} p ParametroConfig ajustável da API.
   * @returns {HTMLElement}
   */
  renderSimulationParam(p) {
    const field = SIMULACAO_PARAM_FIELD[p.nome];

    const row = el("div", "ov-param-row ov-param-adjustable");

    const header = el("div", "ov-param-header");
    header.appendChild(el("span", "ov-param-name", p.nome));
    header.appendChild(el("span", "ov-badge ov-badge-sim", "Config_Simulacao"));
    row.appendChild(header);

    const faixaTexto =
      Number.isFinite(p.faixa_minima) && Number.isFinite(p.faixa_maxima)
        ? `Faixa válida: ${p.faixa_minima} a ${p.faixa_maxima}` +
          (p.unidade ? ` ${p.unidade}` : "")
        : "";
    if (faixaTexto) {
      row.appendChild(el("span", "ov-param-range", faixaTexto));
    }

    // Sem mapeamento de campo conhecido: exibe apenas leitura (fallback seguro).
    if (!field) {
      row.appendChild(el("span", "ov-param-value", this.formatParamValue(p)));
      return row;
    }

    const inputId = `ov-sim-${field}`;
    const controls = el("div", "ov-param-controls");

    const label = el("label", "ov-param-input-label", "Novo valor", {
      for: inputId,
    });
    const input = el("input", "ov-param-input", undefined, {
      id: inputId,
      type: "number",
      value: String(p.valor),
      inputmode: "numeric",
    });
    if (Number.isFinite(p.faixa_minima)) input.setAttribute("min", String(p.faixa_minima));
    if (Number.isFinite(p.faixa_maxima)) input.setAttribute("max", String(p.faixa_maxima));
    input.setAttribute("step", "1");

    const button = el("button", "btn btn-primary ov-param-apply", "Ajustar", {
      type: "button",
    });

    const feedback = el("span", "ov-param-feedback", "", {
      role: "status",
      "aria-live": "polite",
    });

    button.addEventListener("click", () =>
      this.applySimulation(p, field, input, feedback, button)
    );

    controls.appendChild(label);
    controls.appendChild(input);
    controls.appendChild(button);
    row.appendChild(controls);
    row.appendChild(feedback);

    return row;
  }

  /**
   * Valida localmente a faixa (mensagem em pt-BR) e, se válida, envia o ajuste a
   * `PUT /api/config/simulacao`. Rejeições (locais ou da API) preservam o valor
   * anterior no campo e exibem a faixa válida (Req. 2.12).
   *
   * @param {object} p ParametroConfig original.
   * @param {string} field Campo do payload de ajuste.
   * @param {HTMLInputElement} input Entrada numérica do parâmetro.
   * @param {HTMLElement} feedback Nó de mensagem de feedback.
   * @param {HTMLButtonElement} button Botão de ajuste.
   */
  async applySimulation(p, field, input, feedback, button) {
    const bruto = input.value.trim();
    const valor = Number(bruto);
    const faixa =
      Number.isFinite(p.faixa_minima) && Number.isFinite(p.faixa_maxima)
        ? `${p.faixa_minima} a ${p.faixa_maxima}` + (p.unidade ? ` ${p.unidade}` : "")
        : "válida";

    // Validação local de faixa, com mensagem em pt-BR (Req. 2.12).
    const foraDaFaixa =
      bruto === "" ||
      !Number.isInteger(valor) ||
      (Number.isFinite(p.faixa_minima) && valor < p.faixa_minima) ||
      (Number.isFinite(p.faixa_maxima) && valor > p.faixa_maxima);

    if (foraDaFaixa) {
      input.value = String(p.valor); // Preserva o valor anterior (Req. 2.12).
      this.setFeedback(
        feedback,
        `Valor inválido. Informe um número inteiro dentro da faixa ${faixa}.`,
        false
      );
      return;
    }

    button.disabled = true;
    this.setFeedback(feedback, "Ajustando...", null);

    try {
      const resposta = await this.api.ajustarConfigSimulacao({ [field]: valor });
      const ajuste =
        resposta && Array.isArray(resposta.ajustes)
          ? resposta.ajustes.find((a) => a.parametro === p.nome)
          : null;

      if (ajuste && ajuste.aceito) {
        p.valor = ajuste.valor_efetivo;
        input.value = String(ajuste.valor_efetivo);
        this.setFeedback(
          feedback,
          ajuste.mensagem || `Valor ajustado para ${ajuste.valor_efetivo}.`,
          true
        );
      } else {
        // Rejeitado pela API: preserva o valor efetivo anterior e exibe a faixa.
        const efetivo = ajuste ? ajuste.valor_efetivo : p.valor;
        p.valor = efetivo;
        input.value = String(efetivo);
        this.setFeedback(
          feedback,
          (ajuste && ajuste.mensagem) ||
            `Valor rejeitado. A faixa válida é ${faixa}.`,
          false
        );
      }

      // Reflete o estado atualizado de Config_Simulacao, se a API o devolveu.
      if (resposta && Array.isArray(resposta.config_simulacao) && this.lastConfig) {
        this.lastConfig.config_simulacao = resposta.config_simulacao;
      }
    } catch (erro) {
      input.value = String(p.valor); // Preserva o valor anterior em falha.
      this.setFeedback(
        feedback,
        "Não foi possível ajustar o parâmetro agora. O valor anterior foi preservado.",
        false
      );
    } finally {
      button.disabled = false;
    }
  }

  /**
   * Aplica uma mensagem de feedback ao nó, com classe de sucesso/erro/neutra.
   *
   * @param {HTMLElement} node Nó de feedback.
   * @param {string} mensagem Texto em pt-BR.
   * @param {boolean|null} sucesso true (ok), false (erro) ou null (neutro).
   */
  setFeedback(node, mensagem, sucesso) {
    node.textContent = mensagem;
    node.classList.remove("is-ok", "is-error");
    if (sucesso === true) node.classList.add("is-ok");
    else if (sucesso === false) node.classList.add("is-error");
  }

  /**
   * Formata o valor de um parâmetro de configuração, acrescentando a unidade.
   *
   * @param {object} p ParametroConfig.
   * @returns {string}
   */
  formatParamValue(p) {
    const valor = p.valor === null || p.valor === undefined ? "—" : String(p.valor);
    return p.unidade ? `${valor} ${p.unidade}` : valor;
  }
}
