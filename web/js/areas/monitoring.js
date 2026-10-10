/**
 * Área "Monitoramento" (Tarefa 17.3 — Req. 5.1 a 5.8).
 *
 * Monta a tabela de monitoramento das mensagens a partir do endpoint real
 * `GET /api/monitoramento` (ver `api.obterMonitoramento` e
 * `RespostaMonitoramento` em `app/api/esquemas.py`). Colunas exibidas (Req. 5.1):
 * ID, Fila, Consumidor, Tentativas, Processamento, Estado, Último evento e Ações.
 *
 * A coluna "Ações" é responsabilidade do Dashboard (não faz parte do contrato de
 * dados): cada linha oferece a ação de visualizar o histórico detalhado da
 * mensagem (Req. 5.6). Como a área de histórico é implementada em outra tarefa
 * (17.4), a ação dispara um `CustomEvent` desacoplado (`abrir-historico`) com o
 * `order_id`, sem acoplar os módulos.
 *
 * Estados tratados:
 *  - vazio: nenhuma mensagem registrada na Tabela_Historico (Req. 5.8);
 *  - erro/indisponível: falha ao consultar a API, preservando a última tabela
 *    válida já exibida (Req. 5.7, análogo à preservação de dados);
 *  - timeout: a API sinaliza `timeout=True`; preserva os últimos dados válidos
 *    e indica a situação (Req. 14.4).
 *
 * Convenção: comentários e textos de interface em pt-BR; identificadores de
 * código em inglês. Nomes técnicos (order_id) são mantidos por serem contratos.
 */

// Rótulos das colunas exibidas na tabela (Req. 5.1). A API também devolve as
// colunas de dados em `colunas`; "Ações" é acrescentada pelo Dashboard.
const COLUNAS = [
  "ID",
  "Fila",
  "Consumidor",
  "Tentativas",
  "Processamento",
  "Estado",
  "Último evento",
  "Ações",
];

// Mapeamento do Estado derivado para uma classe CSS de destaque. Os valores de
// Estado seguem o Req. 5.4 (derivados pelo backend).
const CLASSE_ESTADO = {
  Aguardando: "estado-aguardando",
  Recebida: "estado-recebida",
  "Em processamento": "estado-processando",
  Processada: "estado-processada",
  Falha: "estado-falha",
  "Aguardando nova tentativa": "estado-retentativa",
  "Encaminhada para DLQ": "estado-dlq",
};

/**
 * Monta a casca da área (barra de ações, área de status e tabela) uma única vez.
 *
 * @param {HTMLElement} container Seção `#monitoring-area`.
 * @returns {object} Referências aos elementos manipulados pela área.
 */
function montarEstrutura(container) {
  const placeholder = container.querySelector(".area-placeholder");
  if (placeholder) {
    placeholder.remove();
  }

  const bloco = document.createElement("div");
  bloco.className = "monitoring-block";
  bloco.innerHTML = `
    <div class="monitoring-toolbar">
      <button id="monitoring-refresh" type="button" class="btn btn-ghost">
        Atualizar
      </button>
      <span id="monitoring-status" class="monitoring-status" role="status" aria-live="polite"></span>
    </div>

    <div
      id="monitoring-error"
      class="monitoring-error"
      role="alert"
      aria-live="assertive"
      hidden
    ></div>

    <div class="monitoring-table-wrap">
      <table class="monitoring-table">
        <thead>
          <tr>
            ${COLUNAS.map((coluna) => `<th scope="col">${coluna}</th>`).join("")}
          </tr>
        </thead>
        <tbody id="monitoring-tbody">
          <tr class="monitoring-empty-row">
            <td colspan="${COLUNAS.length}">
              Carregando a tabela de monitoramento...
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  `;

  container.appendChild(bloco);

  return {
    refresh: bloco.querySelector("#monitoring-refresh"),
    status: bloco.querySelector("#monitoring-status"),
    error: bloco.querySelector("#monitoring-error"),
    tbody: bloco.querySelector("#monitoring-tbody"),
  };
}

/** Escapa texto para inserção segura como conteúdo HTML. */
function escapar(valor) {
  const texto = String(valor ?? "");
  return texto
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

/**
 * Formata o horário do último evento (ISO-8601 UTC) para dia/mês/ano hora:min:seg
 * com identificação do fuso local (Req. 5.5). Mantém o valor bruto se não parsear.
 */
function formatarUltimoEvento(iso) {
  if (!iso) {
    return "—";
  }
  const data = new Date(iso);
  if (Number.isNaN(data.getTime())) {
    return escapar(iso);
  }
  try {
    return escapar(
      new Intl.DateTimeFormat("pt-BR", {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        timeZoneName: "short",
      }).format(data)
    );
  } catch {
    return escapar(data.toISOString());
  }
}

/** Monta a célula de Estado com destaque visual conforme o valor (Req. 5.4). */
function celulaEstado(estado) {
  const classe = CLASSE_ESTADO[estado] || "estado-neutro";
  return `<td><span class="estado-badge ${classe}">${escapar(estado)}</span></td>`;
}

/** Renderiza as linhas da tabela a partir das linhas de monitoramento. */
function renderizarLinhas(tbody, linhas) {
  tbody.innerHTML = linhas
    .map((linha) => {
      const consumidor = linha.consumidor ? escapar(linha.consumidor) : "—";
      const processamento =
        linha.processamento_segundos === null ||
        linha.processamento_segundos === undefined
          ? "—"
          : `${escapar(linha.processamento_segundos)} s`;
      return `
        <tr>
          <td class="col-id">${escapar(linha.id)}</td>
          <td>${escapar(linha.fila)}</td>
          <td>${consumidor}</td>
          <td class="col-num">${escapar(linha.tentativas)}</td>
          <td class="col-num">${processamento}</td>
          ${celulaEstado(linha.estado)}
          <td>${formatarUltimoEvento(linha.ultimo_evento)}</td>
          <td class="col-acoes">
            <button
              type="button"
              class="btn btn-ghost btn-sm monitoring-history-btn"
              data-order-id="${escapar(linha.id)}"
            >
              Ver histórico
            </button>
          </td>
        </tr>
      `;
    })
    .join("");
}

/** Exibe uma linha única ocupando todas as colunas (estado vazio/carregando). */
function renderizarLinhaUnica(tbody, texto, classe = "monitoring-empty-row") {
  tbody.innerHTML = `
    <tr class="${classe}">
      <td colspan="${COLUNAS.length}">${escapar(texto)}</td>
    </tr>
  `;
}

/**
 * Carrega e renderiza a tabela de monitoramento.
 *
 * Preserva a última tabela válida quando ocorre erro ou timeout, atualizando
 * apenas as faixas de status/erro (Req. 5.7, 14.4).
 *
 * @param {object} els Referências dos elementos.
 * @param {import("../api.js").api} api Cliente da API_Backend.
 * @param {{temDados: boolean}} estado Estado mutável de controle de renderização.
 */
async function carregar(els, api, estado) {
  els.error.hidden = true;
  els.status.textContent = "Atualizando...";

  try {
    const resposta = await api.obterMonitoramento();

    // Timeout sinalizado pela API: preserva os dados válidos exibidos (Req. 14.4).
    if (resposta.timeout) {
      els.error.hidden = false;
      els.error.textContent =
        resposta.mensagem ||
        "Tempo limite excedido ao montar a tabela. Dados anteriores preservados.";
      if (!estado.temDados) {
        renderizarLinhaUnica(
          els.tbody,
          "Sem dados válidos ainda: a última tentativa excedeu o tempo limite."
        );
      }
      els.status.textContent = "";
      return;
    }

    // Estado vazio: nenhuma mensagem registrada (Req. 5.8).
    if (resposta.vazio || !resposta.linhas || resposta.linhas.length === 0) {
      renderizarLinhaUnica(
        els.tbody,
        resposta.mensagem_vazia ||
          "Nenhuma mensagem monitorada no momento."
      );
      estado.temDados = false;
      els.status.textContent = `Atualizado às ${new Date().toLocaleTimeString(
        "pt-BR"
      )}`;
      return;
    }

    renderizarLinhas(els.tbody, resposta.linhas);
    estado.temDados = true;
    els.status.textContent = `Atualizado às ${new Date().toLocaleTimeString(
      "pt-BR"
    )} — ${resposta.linhas.length} mensagem(ns)`;
  } catch (erro) {
    // Falha de rede/HTTP: preserva a tabela anterior e indica indisponibilidade.
    els.error.hidden = false;
    els.error.textContent =
      (erro && erro.message) ||
      "Monitoramento indisponível: não foi possível consultar a API.";
    if (!estado.temDados) {
      renderizarLinhaUnica(
        els.tbody,
        "Monitoramento indisponível no momento. Tente atualizar novamente."
      );
    }
    els.status.textContent = "";
  }
}

/**
 * Inicializa a área de monitoramento.
 *
 * @param {object} ctx Contexto compartilhado do Dashboard.
 * @param {import("../api.js").api} ctx.api Cliente da API_Backend.
 * @param {HTMLElement} ctx.container Seção `#monitoring-area` desta área.
 */
export function init(ctx) {
  if (!ctx || !ctx.container || !ctx.api) {
    return;
  }
  const { api, container } = ctx;

  const els = montarEstrutura(container);
  const estado = { temDados: false };

  // Botão de atualização manual.
  els.refresh.addEventListener("click", () => carregar(els, api, estado));

  // Ação "Ver histórico": dispara um evento desacoplado para a área de histórico
  // (Tarefa 17.4), passando o order_id. Delegação no tbody (Req. 5.6).
  els.tbody.addEventListener("click", (evento) => {
    const botao = evento.target.closest(".monitoring-history-btn");
    if (!botao) {
      return;
    }
    const orderId = botao.getAttribute("data-order-id");
    document.dispatchEvent(
      new CustomEvent("abrir-historico", { detail: { orderId } })
    );
  });

  // Carga inicial da tabela.
  carregar(els, api, estado);
}
