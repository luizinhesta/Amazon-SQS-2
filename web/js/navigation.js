/**
 * Navegação entre as cinco áreas do Dashboard (Tarefa 17.1).
 *
 * As áreas são: overview (visão geral), send (envio), monitoring (monitoramento),
 * history (histórico detalhado) e lab (laboratório de falhas). Cada botão de
 * navegação tem `data-area` e cada seção de conteúdo tem o id `<area>-area` e o
 * mesmo `data-area`, o que mantém a navegação desacoplada do conteúdo que as
 * Tarefas 17.2 a 17.5 preencherão.
 */

/** Identificadores das cinco áreas, na ordem exibida na navegação. */
export const AREAS = ["overview", "send", "monitoring", "history", "lab"];

/**
 * Inicializa a navegação por abas entre as áreas.
 *
 * @param {object} [options]
 * @param {(area: string) => void} [options.onChange] Callback ao trocar de área;
 *   recebe o identificador da área ativada. As áreas usam isso para carregar/
 *   atualizar seus dados sob demanda (Tarefas 17.2–17.5).
 * @returns {{ activate: (area: string) => void, current: () => string }}
 */
export function setupNavigation({ onChange } = {}) {
  const navItems = Array.from(document.querySelectorAll(".nav-item"));
  const sections = Array.from(document.querySelectorAll(".area"));
  let areaAtual = AREAS[0];

  /**
   * Ativa uma área: destaca o botão, exibe a seção correspondente e oculta as
   * demais. Áreas desconhecidas são ignoradas.
   *
   * @param {string} area Identificador da área a ativar.
   */
  function activate(area) {
    if (!AREAS.includes(area)) {
      return;
    }
    areaAtual = area;

    navItems.forEach((item) => {
      item.classList.toggle("is-active", item.dataset.area === area);
    });

    sections.forEach((section) => {
      const ativa = section.dataset.area === area;
      section.classList.toggle("is-active", ativa);
      section.hidden = !ativa;
    });

    if (typeof onChange === "function") {
      onChange(area);
    }
  }

  navItems.forEach((item) => {
    item.addEventListener("click", () => activate(item.dataset.area));
  });

  return {
    activate,
    current: () => areaAtual,
  };
}
