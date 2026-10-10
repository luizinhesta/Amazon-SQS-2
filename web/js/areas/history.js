/**
 * Área "Histórico detalhado" (Tarefa 17.4).
 *
 * Exibe a linha do tempo CRONOLÓGICA de eventos de uma mensagem
 * (GET /api/mensagens/{order_id}/historico), com indicador Observado/Inferido
 * por evento (apenas exibição, sem interação) e horário com identificação de
 * fuso. Trata os estados vazio (sem eventos) e indisponível (falha de consulta)
 * expostos pela API (Req. 6.1–6.8, 10.7).
 *
 * O cliente da API (`api.obterHistorico(orderId)`) retorna o contrato
 * `RespostaHistorico` de `app/api/esquemas.py`:
 *  { order_id, indisponivel, vazio, mensagem, eventos: [EventoHistorico, ...] }
 * Cada EventoHistorico traz `indicador_origem` ("Observado"/"Inferido"),
 * `observado`, `app_time` (ISO-8601 UTC com fuso) e metadados didáticos.
 */

/** Rótulos textuais das colunas de metadados exibidas por evento. */
const META_CAMPOS = [
  { chave: "fila", rotulo: "Fila" },
  { chave: "consumidor", rotulo: "Consumidor" },
  { chave: "tentativas", rotulo: "Tentativas" },
  { chave: "processamento_segundos", rotulo: "Processamento (s)" },
  { chave: "message_group_id", rotulo: "MessageGroupId" },
  { chave: "detalhe_erro", rotulo: "Detalhe do erro" },
];

/**
 * Formata um horário ISO-8601 UTC para exibição local com identificação de fuso.
 * Mantém o valor original (com sufixo Z) acessível no atributo `title`/`datetime`.
 *
 * @param {string} appTime Horário ISO-8601 UTC (ex.: "2024-06-25T12:00:00.123Z").
 * @returns {string} Texto legível com data, hora e fuso do navegador.
 */
function formatarHorario(appTime) {
  if (!appTime) return "—";
  const data = new Date(appTime);
  if (Number.isNaN(data.getTime())) return appTime;
  try {
    return new Intl.DateTimeFormat("pt-BR", {
      dateStyle: "short",
      timeStyle: "medium",
      timeZoneName: "short",
    }).format(data);
  } catch {
    return data.toISOString();
  }
}

/** Cria um elemento com classe e, opcionalmente, texto. */
function criarElemento(tag, className, texto) {
  const el = document.createElement(tag);
  if (className) el.className = className;
  if (texto !== undefined && texto !== null) el.textContent = String(texto);
  return el;
}

/**
 * Monta a linha do tempo cronológica a partir dos eventos retornados pela API.
 *
 * @param {Array<object>} eventos Eventos em ordem cronológica crescente.
 * @returns {HTMLElement} Lista ordenada representando a linha do tempo.
 */
function montarLinhaDoTempo(eventos) {
  const lista = criarElemento("ol", "timeline");

  eventos.forEach((evento) => {
    const item = criarElemento("li", "timeline-item");
    item.classList.add(evento.observado ? "is-observed" : "is-inferred");

    const marcador = criarElemento("span", "timeline-marker");
    marcador.setAttribute("aria-hidden", "true");
    item.appendChild(marcador);

    const corpo = criarElemento("div", "timeline-body");

    const cabecalho = criarElemento("div", "timeline-head");
    cabecalho.appendChild(
      criarElemento("span", "timeline-type", evento.event_type)
    );

    // Indicador Observado/Inferido — apenas exibição, sem interação (Req. 6.4).
    const origemClasse = evento.observado
      ? "badge badge-observed"
      : "badge badge-inferred";
    const origem = criarElemento(
      "span",
      origemClasse,
      evento.indicador_origem || (evento.observado ? "Observado" : "Inferido")
    );
    cabecalho.appendChild(origem);
    corpo.appendChild(cabecalho);

    // Horário com identificação de fuso (Req. 6.3). O valor ISO fica no title.
    const tempo = criarElemento("time", "timeline-time", formatarHorario(evento.app_time));
    if (evento.app_time) {
      tempo.setAttribute("datetime", evento.app_time);
      tempo.setAttribute("title", evento.app_time);
    }
    corpo.appendChild(tempo);

    // Metadados didáticos relevantes, exibidos apenas quando presentes.
    const metas = META_CAMPOS.filter(({ chave }) => {
      const valor = evento[chave];
      return valor !== null && valor !== undefined && valor !== "";
    });
    if (metas.length > 0) {
      const grade = criarElemento("dl", "timeline-meta");
      metas.forEach(({ chave, rotulo }) => {
        grade.appendChild(criarElemento("dt", "timeline-meta-label", rotulo));
        grade.appendChild(
          criarElemento("dd", "timeline-meta-value", evento[chave])
        );
      });
      corpo.appendChild(grade);
    }

    item.appendChild(corpo);
    lista.appendChild(item);
  });

  return lista;
}

/**
 * Inicializa a área de histórico detalhado.
 *
 * @param {object} ctx Contexto compartilhado do Dashboard.
 * @param {import("../api.js").api} ctx.api Cliente da API_Backend.
 * @param {HTMLElement} ctx.container Seção `#history-area` desta área.
 */
export function init(ctx) {
  const { api, container } = ctx;
  if (!container) return;

  // Substitui o placeholder da estrutura (Tarefa 17.1) pelo conteúdo real,
  // preservando o título da área.
  const placeholder = container.querySelector(".area-placeholder");
  if (placeholder) placeholder.remove();

  const painel = criarElemento("div", "history-panel");

  // Formulário de consulta por order_id. Sem recarregar a página.
  const form = document.createElement("form");
  form.className = "history-form";
  form.noValidate = true;

  const campo = criarElemento("div", "field");
  const rotulo = criarElemento("label", null, "Identificador do pedido (order_id)");
  rotulo.setAttribute("for", "history-order-id");
  const input = document.createElement("input");
  input.id = "history-order-id";
  input.name = "order_id";
  input.type = "text";
  input.placeholder = "ex.: pedido-0001";
  input.required = true;
  campo.appendChild(rotulo);
  campo.appendChild(input);

  const botao = criarElemento("button", "btn btn-primary", "Consultar histórico");
  botao.type = "submit";

  form.appendChild(campo);
  form.appendChild(botao);
  painel.appendChild(form);

  // Região de resultado (linha do tempo / estados vazio e indisponível).
  const resultado = criarElemento("div", "history-result");
  resultado.setAttribute("aria-live", "polite");
  painel.appendChild(resultado);

  container.appendChild(painel);

  /** Exibe uma mensagem de estado (vazio, indisponível, erro ou carregando). */
  function mostrarMensagem(texto, variante) {
    resultado.replaceChildren();
    const classe = variante ? `history-message history-message-${variante}` : "history-message";
    resultado.appendChild(criarElemento("p", classe, texto));
  }

  async function consultar(orderId) {
    mostrarMensagem(`Consultando o histórico de "${orderId}"…`, "loading");
    botao.disabled = true;
    try {
      const resposta = await api.obterHistorico(orderId);

      // Estado indisponível: a consulta à Tabela_Historico falhou (Req. 6.7).
      if (resposta.indisponivel) {
        mostrarMensagem(
          resposta.mensagem ||
            "Histórico indisponível: não foi possível consultar os eventos desta mensagem.",
          "unavailable"
        );
        return;
      }

      // Estado vazio: a mensagem não possui eventos registrados (Req. 6.8).
      if (resposta.vazio || !resposta.eventos || resposta.eventos.length === 0) {
        mostrarMensagem(
          resposta.mensagem ||
            "Não há eventos registrados para esta mensagem na Tabela_Historico.",
          "empty"
        );
        return;
      }

      // Linha do tempo cronológica (a API já entrega em ordem crescente).
      resultado.replaceChildren();
      const titulo = criarElemento(
        "h3",
        "history-result-title",
        `Linha do tempo — ${resposta.order_id || orderId}`
      );
      resultado.appendChild(titulo);
      resultado.appendChild(montarLinhaDoTempo(resposta.eventos));
    } catch (erro) {
      // Falha de rede ou erro HTTP: preserva a tela e orienta a nova tentativa.
      const detalhe =
        erro && erro.detail ? ` (${String(erro.detail)})` : "";
      mostrarMensagem(
        `Não foi possível consultar o histórico agora${detalhe}. Tente novamente.`,
        "unavailable"
      );
    } finally {
      botao.disabled = false;
    }
  }

  form.addEventListener("submit", (evento) => {
    evento.preventDefault();
    const orderId = input.value.trim();
    if (!orderId) {
      mostrarMensagem("Informe um order_id para consultar o histórico.", "empty");
      input.focus();
      return;
    }
    void consultar(orderId);
  });
}
