/**
 * Área "Laboratório de falhas" (Tarefa 17.4).
 *
 * Oferece:
 *  - Painel de cenários de falha (POST /api/laboratorio/cenario), permitindo
 *    disparar os Cenario_Falha do laboratório numa fila Standard ou FIFO
 *    (Req. 7.1–7.6, 7.8, 7.11).
 *  - Reprocessamento de DLQ (POST /api/laboratorio/reprocessar-dlq), movendo
 *    mensagens da DLQ de volta à fila principal (Req. 7.7).
 *  - Comparação Standard × FIFO e por MessageGroupId: o campo MessageGroupId
 *    só é exigido/habilitado para o fluxo FIFO, evidenciando a diferença de
 *    ordenação por grupo (Req. 10.7).
 *
 * Usa o cliente da API: `api.executarCenario(requisicao)` e
 * `api.reprocessarDlq(requisicao)`. Os contratos seguem `RequisicaoCenario`,
 * `RespostaCenario`, `RequisicaoReprocessarDlq` e `RespostaReprocessarDlq` de
 * `app/api/esquemas.py`.
 */

/** Tipos de fila aceitos pela API (nomes técnicos do recurso SQS). */
const TIPOS_FILA = [
  { valor: "standard", rotulo: "Standard" },
  { valor: "fifo", rotulo: "FIFO" },
];

/**
 * Cenários de falha disponíveis, alinhados ao enum CenarioFalha de
 * `app/consumidores/processamento.py`. O rótulo em pt-BR descreve a didática.
 */
const CENARIOS = [
  { valor: "sucesso", rotulo: "Sucesso (processa e exclui)" },
  { valor: "excecao_proposital", rotulo: "Exceção proposital (falha)" },
  { valor: "duracao_superior_visibility", rotulo: "Duração superior ao Visibility_Timeout" },
  { valor: "interrupcao", rotulo: "Interrupção do consumidor" },
  { valor: "recebimento_repetido", rotulo: "Recebimento repetido" },
  { valor: "encaminhar_dlq", rotulo: "Encaminhar para a DLQ" },
];

/** Cria um elemento com classe e, opcionalmente, texto. */
function criarElemento(tag, className, texto) {
  const el = document.createElement(tag);
  if (className) el.className = className;
  if (texto !== undefined && texto !== null) el.textContent = String(texto);
  return el;
}

/** Monta um campo de formulário (label + controle) com os atributos dados. */
function criarCampo(id, rotuloTexto, controle) {
  const campo = criarElemento("div", "field");
  const rotulo = criarElemento("label", null, rotuloTexto);
  rotulo.setAttribute("for", id);
  controle.id = id;
  campo.appendChild(rotulo);
  campo.appendChild(controle);
  return campo;
}

/** Monta um <select> a partir de uma lista de { valor, rotulo }. */
function criarSelect(opcoes) {
  const select = document.createElement("select");
  opcoes.forEach(({ valor, rotulo }) => {
    const opcao = document.createElement("option");
    opcao.value = valor;
    opcao.textContent = rotulo;
    select.appendChild(opcao);
  });
  return select;
}

/**
 * Inicializa a área do laboratório de falhas.
 *
 * @param {object} ctx Contexto compartilhado do Dashboard.
 * @param {import("../api.js").api} ctx.api Cliente da API_Backend.
 * @param {HTMLElement} ctx.container Seção `#lab-area` desta área.
 */
export function init(ctx) {
  const { api, container } = ctx;
  if (!container) return;

  const placeholder = container.querySelector(".area-placeholder");
  if (placeholder) placeholder.remove();

  const painel = criarElemento("div", "lab-panel");

  // ----------------------------------------------------------------------
  // Painel 1: cenários de falha (POST /api/laboratorio/cenario)
  // ----------------------------------------------------------------------
  const cartaoCenario = criarElemento("section", "lab-card");
  cartaoCenario.appendChild(criarElemento("h3", "lab-card-title", "Cenários de falha"));
  cartaoCenario.appendChild(
    criarElemento(
      "p",
      "lab-card-desc",
      "Dispare um Cenario_Falha controlado numa fila Standard ou FIFO. Compare o " +
        "comportamento entre os fluxos; para FIFO, informe o MessageGroupId para " +
        "observar a ordenação por grupo."
    )
  );

  const formCenario = document.createElement("form");
  formCenario.className = "lab-form";
  formCenario.noValidate = true;

  const grade = criarElemento("div", "lab-form-grid");

  const selFila = criarSelect(TIPOS_FILA);
  const selCenario = criarSelect(CENARIOS);

  const inpQuantidade = document.createElement("input");
  inpQuantidade.type = "number";
  inpQuantidade.min = "1";
  inpQuantidade.max = "10";
  inpQuantidade.value = "1";

  const inpOrderBase = document.createElement("input");
  inpOrderBase.type = "text";
  inpOrderBase.value = "cenario";
  inpOrderBase.placeholder = "ex.: cenario";

  const inpGrupo = document.createElement("input");
  inpGrupo.type = "text";
  inpGrupo.placeholder = "ex.: grupo-A";

  const campoFila = criarCampo("lab-tipo-fila", "Fila", selFila);
  const campoCenario = criarCampo("lab-cenario", "Cenário", selCenario);
  const campoQuantidade = criarCampo("lab-quantidade", "Quantidade (1 a 10)", inpQuantidade);
  const campoOrderBase = criarCampo("lab-order-base", "Prefixo do order_id", inpOrderBase);
  const campoGrupo = criarCampo("lab-group-id", "MessageGroupId (apenas FIFO)", inpGrupo);

  grade.appendChild(campoFila);
  grade.appendChild(campoCenario);
  grade.appendChild(campoQuantidade);
  grade.appendChild(campoOrderBase);
  grade.appendChild(campoGrupo);
  formCenario.appendChild(grade);

  const botaoCenario = criarElemento("button", "btn btn-primary", "Executar cenário");
  botaoCenario.type = "submit";
  formCenario.appendChild(botaoCenario);

  const saidaCenario = criarElemento("div", "lab-output");
  saidaCenario.setAttribute("aria-live", "polite");

  cartaoCenario.appendChild(formCenario);
  cartaoCenario.appendChild(saidaCenario);
  painel.appendChild(cartaoCenario);

  // O MessageGroupId só faz sentido para FIFO: habilita/desabilita conforme a
  // fila selecionada, evidenciando a diferença entre Standard e FIFO (Req. 10.7).
  function sincronizarGrupo() {
    const ehFifo = selFila.value === "fifo";
    inpGrupo.disabled = !ehFifo;
    campoGrupo.classList.toggle("is-disabled", !ehFifo);
    if (!ehFifo) inpGrupo.value = "";
  }
  selFila.addEventListener("change", sincronizarGrupo);
  sincronizarGrupo();

  /** Exibe uma mensagem simples numa área de saída. */
  function mostrarSaida(area, texto, variante) {
    area.replaceChildren();
    const classe = variante ? `lab-message lab-message-${variante}` : "lab-message";
    area.appendChild(criarElemento("p", classe, texto));
  }

  /** Renderiza o resultado de um cenário (RespostaCenario). */
  function renderizarResultadoCenario(resposta) {
    saidaCenario.replaceChildren();

    if (resposta.interrompido) {
      saidaCenario.appendChild(
        criarElemento(
          "p",
          "lab-message lab-message-error",
          resposta.causa || resposta.mensagem || "Cenário interrompido."
        )
      );
      return;
    }

    const variante = resposta.executado ? "success" : "error";
    saidaCenario.appendChild(
      criarElemento("p", `lab-message lab-message-${variante}`, resposta.mensagem)
    );

    // Detalhamento do envio das mensagens do cenário, quando houver.
    const envio = resposta.envio;
    if (envio) {
      const resumo = criarElemento(
        "p",
        "lab-output-summary",
        `Fila: ${envio.nome_fila || resposta.tipo_fila} · ` +
          `Solicitadas: ${envio.total_solicitadas} · ` +
          `Enviadas: ${envio.total_enviadas}` +
          (envio.houve_falha ? " · houve falha" : "")
      );
      saidaCenario.appendChild(resumo);

      if (Array.isArray(envio.resultados) && envio.resultados.length > 0) {
        const lista = criarElemento("ul", "lab-result-list");
        envio.resultados.forEach((item) => {
          const li = criarElemento(
            "li",
            item.enviada ? "is-ok" : "is-fail",
            `${item.order_id}: ${item.enviada ? "enviada" : "falhou"}` +
              (item.detalhe_erro ? ` — ${item.detalhe_erro}` : "")
          );
          lista.appendChild(li);
        });
        saidaCenario.appendChild(lista);
      }
    }
  }

  formCenario.addEventListener("submit", async (evento) => {
    evento.preventDefault();

    const quantidade = Number.parseInt(inpQuantidade.value, 10);
    if (!Number.isInteger(quantidade) || quantidade < 1 || quantidade > 10) {
      mostrarSaida(saidaCenario, "A quantidade deve ser um inteiro de 1 a 10.", "error");
      inpQuantidade.focus();
      return;
    }

    const ehFifo = selFila.value === "fifo";
    const grupo = inpGrupo.value.trim();
    if (ehFifo && !grupo) {
      mostrarSaida(
        saidaCenario,
        "Para a fila FIFO, informe o MessageGroupId do cenário.",
        "error"
      );
      inpGrupo.focus();
      return;
    }

    const requisicao = {
      tipo_fila: selFila.value,
      cenario: selCenario.value,
      quantidade,
      order_id_base: inpOrderBase.value.trim() || "cenario",
    };
    if (ehFifo && grupo) requisicao.message_group_id = grupo;

    mostrarSaida(saidaCenario, "Executando o cenário…", "loading");
    botaoCenario.disabled = true;
    try {
      const resposta = await api.executarCenario(requisicao);
      renderizarResultadoCenario(resposta);
    } catch (erro) {
      const detalhe = erro && erro.detail ? ` (${String(erro.detail)})` : "";
      mostrarSaida(
        saidaCenario,
        `Não foi possível executar o cenário agora${detalhe}. Tente novamente.`,
        "error"
      );
    } finally {
      botaoCenario.disabled = false;
    }
  });

  // ----------------------------------------------------------------------
  // Painel 2: reprocessamento de DLQ (POST /api/laboratorio/reprocessar-dlq)
  // ----------------------------------------------------------------------
  const cartaoDlq = criarElemento("section", "lab-card");
  cartaoDlq.appendChild(criarElemento("h3", "lab-card-title", "Reprocessamento de DLQ"));
  cartaoDlq.appendChild(
    criarElemento(
      "p",
      "lab-card-desc",
      "Move as mensagens da DLQ do fluxo selecionado de volta à fila principal " +
        "para que sejam reprocessadas."
    )
  );

  const formDlq = document.createElement("form");
  formDlq.className = "lab-form";
  formDlq.noValidate = true;

  const gradeDlq = criarElemento("div", "lab-form-grid");
  const selFilaDlq = criarSelect(TIPOS_FILA);
  gradeDlq.appendChild(criarCampo("lab-dlq-fila", "Fluxo", selFilaDlq));
  formDlq.appendChild(gradeDlq);

  const botaoDlq = criarElemento("button", "btn btn-primary", "Reprocessar DLQ");
  botaoDlq.type = "submit";
  formDlq.appendChild(botaoDlq);

  const saidaDlq = criarElemento("div", "lab-output");
  saidaDlq.setAttribute("aria-live", "polite");

  cartaoDlq.appendChild(formDlq);
  cartaoDlq.appendChild(saidaDlq);
  painel.appendChild(cartaoDlq);

  /** Renderiza o resultado do reprocessamento (RespostaReprocessarDlq). */
  function renderizarResultadoDlq(resposta) {
    saidaDlq.replaceChildren();

    if (resposta.interrompido) {
      saidaDlq.appendChild(
        criarElemento(
          "p",
          "lab-message lab-message-error",
          resposta.causa || resposta.mensagem || "Reprocessamento interrompido."
        )
      );
      return;
    }

    const variante = resposta.reprocessado ? "success" : "empty";
    saidaDlq.appendChild(
      criarElemento("p", `lab-message lab-message-${variante}`, resposta.mensagem)
    );
    saidaDlq.appendChild(
      criarElemento(
        "p",
        "lab-output-summary",
        `DLQ: ${resposta.dlq || "—"} → Fila principal: ${resposta.fila_principal || "—"} · ` +
          `Movidas: ${resposta.total_movidas}`
      )
    );

    if (Array.isArray(resposta.mensagens_movidas) && resposta.mensagens_movidas.length > 0) {
      const lista = criarElemento("ul", "lab-result-list");
      resposta.mensagens_movidas.forEach((messageId) => {
        lista.appendChild(criarElemento("li", "is-ok", messageId));
      });
      saidaDlq.appendChild(lista);
    }
  }

  formDlq.addEventListener("submit", async (evento) => {
    evento.preventDefault();
    const requisicao = { tipo_fila: selFilaDlq.value };

    mostrarSaida(saidaDlq, "Reprocessando a DLQ…", "loading");
    botaoDlq.disabled = true;
    try {
      const resposta = await api.reprocessarDlq(requisicao);
      renderizarResultadoDlq(resposta);
    } catch (erro) {
      const detalhe = erro && erro.detail ? ` (${String(erro.detail)})` : "";
      mostrarSaida(
        saidaDlq,
        `Não foi possível reprocessar a DLQ agora${detalhe}. Tente novamente.`,
        "error"
      );
    } finally {
      botaoDlq.disabled = false;
    }
  });

  container.appendChild(painel);
}
