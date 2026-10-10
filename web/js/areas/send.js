/**
 * Área "Envio de mensagens" (Tarefa 17.3 — Req. 4.1 a 4.9).
 *
 * Monta o formulário de envio e trata a submissão contra o endpoint real
 * `POST /api/mensagens` (ver `api.enviarMensagens` e `RequisicaoEnvioMensagens`
 * em `app/api/esquemas.py`). Campos do formulário:
 *  - fila de destino (Standard ou FIFO) — Req. 4.1;
 *  - quantidade de mensagens, de 1 a 10 — Req. 4.2;
 *  - identificador do pedido (order_id), de 1 a 80 caracteres — Req. 4.3;
 *  - Tempo_Processamento_Simulado, de 0 a 300 segundos — Req. 4.4;
 *  - Cenario_Falha — Req. 4.5;
 *  - MessageGroupId (1 a 128), exibido apenas quando a fila é FIFO — Req. 4.6;
 *  - Deduplicacao_Por_Conteudo e MessageDeduplicationId (1 a 128), exibidos
 *    apenas para FIFO; o MessageDeduplicationId é exigido quando a deduplicação
 *    por conteúdo está desabilitada — Req. 4.7.
 *
 * A validação acontece em pt-BR ANTES do envio: quando algum campo é inválido, o
 * envio é rejeitado, os valores informados são preservados e a mensagem indica o
 * campo inválido (Req. 4.9). Os valores informados nunca são apagados pela
 * validação nem pela resposta de erro da API.
 *
 * Convenção: comentários e textos de interface em pt-BR; identificadores de
 * código em inglês. Os nomes técnicos do SQS (MessageGroupId, order_id, cenários)
 * são mantidos por serem contratos da API/AWS.
 */

// Faixas espelhadas das constantes do backend (app/dominio/constantes.py). São
// repetidas aqui apenas para a validação de interface; a validação autoritativa
// permanece no backend (app/dominio/validacao_envio.py).
const LIMITES = {
  quantidadeMin: 1,
  quantidadeMax: 10,
  orderIdMin: 1,
  orderIdMax: 80,
  tempoMin: 0,
  tempoMax: 300,
  grupoMin: 1,
  grupoMax: 128,
  dedupMin: 1,
  dedupMax: 128,
};

// Cenários de falha aceitos pelo backend (enum CenarioFalha em
// app/consumidores/processamento.py). Rótulo em pt-BR para a interface.
const CENARIOS = [
  { valor: "sucesso", rotulo: "Processamento com sucesso" },
  { valor: "excecao_proposital", rotulo: "Exceção proposital no consumidor" },
  { valor: "interrupcao", rotulo: "Interrupção durante o processamento" },
  {
    valor: "duracao_superior_visibility",
    rotulo: "Duração maior que o Visibility_Timeout",
  },
  { valor: "recebimento_repetido", rotulo: "Recebimento repetido" },
  { valor: "encaminhar_dlq", rotulo: "Encaminhamento para a DLQ" },
];

/**
 * Monta a marcação do formulário de envio dentro do container da área.
 *
 * @param {HTMLElement} container Seção `#send-area`.
 * @returns {object} Referências aos elementos manipulados pela área.
 */
function montarFormulario(container) {
  // Remove o placeholder da estrutura (Tarefa 17.1), preservando o título.
  const placeholder = container.querySelector(".area-placeholder");
  if (placeholder) {
    placeholder.remove();
  }

  const opcoesCenario = CENARIOS.map(
    (cenario) => `<option value="${cenario.valor}">${cenario.rotulo}</option>`
  ).join("");

  const form = document.createElement("form");
  form.id = "send-form";
  form.className = "send-form";
  form.setAttribute("novalidate", "");
  form.innerHTML = `
    <div class="send-grid">
      <div class="field">
        <label for="send-queue">Fila de destino</label>
        <select id="send-queue" name="queue">
          <option value="standard">Standard (lab-sqs-standard)</option>
          <option value="fifo">FIFO (lab-sqs-fifo.fifo)</option>
        </select>
      </div>

      <div class="field">
        <label for="send-quantity">Quantidade de mensagens (1 a 10)</label>
        <input
          id="send-quantity"
          name="quantity"
          type="number"
          min="${LIMITES.quantidadeMin}"
          max="${LIMITES.quantidadeMax}"
          step="1"
          value="5"
        />
      </div>

      <div class="field">
        <label for="send-order-id">Identificador do pedido (1 a 80 caracteres)</label>
        <input
          id="send-order-id"
          name="orderId"
          type="text"
          maxlength="${LIMITES.orderIdMax}"
          placeholder="ex.: pedido-0001"
        />
        <small class="field-hint">
          Para mais de uma mensagem, o número sequencial é anexado ao final
          (ex.: pedido-0001-1, pedido-0001-2).
        </small>
      </div>

      <div class="field">
        <label for="send-processing">Tempo de processamento simulado (0 a 300 s)</label>
        <input
          id="send-processing"
          name="processing"
          type="number"
          min="${LIMITES.tempoMin}"
          max="${LIMITES.tempoMax}"
          step="1"
          value="45"
        />
      </div>

      <div class="field">
        <label for="send-scenario">Cenário de falha</label>
        <select id="send-scenario" name="scenario">
          ${opcoesCenario}
        </select>
      </div>
    </div>

    <fieldset id="send-fifo-fields" class="send-fifo" hidden>
      <legend>Campos FIFO</legend>

      <div class="field">
        <label for="send-group-id">MessageGroupId (1 a 128 caracteres)</label>
        <input
          id="send-group-id"
          name="groupId"
          type="text"
          maxlength="${LIMITES.grupoMax}"
          placeholder="ex.: grupo-A"
        />
      </div>

      <div class="field field-inline">
        <input
          id="send-content-dedup"
          name="contentDedup"
          type="checkbox"
          checked
        />
        <label for="send-content-dedup">
          Deduplicação por conteúdo habilitada
        </label>
      </div>

      <div class="field" id="send-dedup-field" hidden>
        <label for="send-dedup-id">MessageDeduplicationId (1 a 128 caracteres)</label>
        <input
          id="send-dedup-id"
          name="dedupId"
          type="text"
          maxlength="${LIMITES.dedupMax}"
          placeholder="ex.: dedup-0001"
        />
        <small class="field-hint">
          Exigido quando a deduplicação por conteúdo está desabilitada.
        </small>
      </div>
    </fieldset>

    <div class="send-actions">
      <button id="send-submit" type="submit" class="btn btn-primary">
        Enviar mensagens
      </button>
    </div>

    <div
      id="send-feedback"
      class="send-feedback"
      role="status"
      aria-live="polite"
      hidden
    ></div>
  `;

  container.appendChild(form);

  return {
    form,
    queue: form.querySelector("#send-queue"),
    quantity: form.querySelector("#send-quantity"),
    orderId: form.querySelector("#send-order-id"),
    processing: form.querySelector("#send-processing"),
    scenario: form.querySelector("#send-scenario"),
    fifoFields: form.querySelector("#send-fifo-fields"),
    groupId: form.querySelector("#send-group-id"),
    contentDedup: form.querySelector("#send-content-dedup"),
    dedupField: form.querySelector("#send-dedup-field"),
    dedupId: form.querySelector("#send-dedup-id"),
    submit: form.querySelector("#send-submit"),
    feedback: form.querySelector("#send-feedback"),
  };
}

/** Converte um texto para inteiro; retorna null quando não é inteiro válido. */
function paraInteiro(texto) {
  const bruto = String(texto ?? "").trim();
  if (bruto === "" || !/^-?\d+$/.test(bruto)) {
    return null;
  }
  return Number.parseInt(bruto, 10);
}

/**
 * Valida os campos do formulário em pt-BR antes do envio (Req. 4.2 a 4.9).
 *
 * @param {object} els Referências dos elementos do formulário.
 * @returns {{erros: string[]}} Lista de mensagens de erro (vazia quando válido).
 */
function validarCampos(els) {
  const erros = [];
  const ehFifo = els.queue.value === "fifo";

  const quantidade = paraInteiro(els.quantity.value);
  if (
    quantidade === null ||
    quantidade < LIMITES.quantidadeMin ||
    quantidade > LIMITES.quantidadeMax
  ) {
    erros.push(
      `A quantidade de mensagens deve ser um número inteiro entre ` +
        `${LIMITES.quantidadeMin} e ${LIMITES.quantidadeMax}.`
    );
  }

  const orderId = els.orderId.value.trim();
  if (orderId.length < LIMITES.orderIdMin || orderId.length > LIMITES.orderIdMax) {
    erros.push(
      `O identificador do pedido deve ter de ${LIMITES.orderIdMin} a ` +
        `${LIMITES.orderIdMax} caracteres.`
    );
  }

  const tempo = paraInteiro(els.processing.value);
  if (tempo === null || tempo < LIMITES.tempoMin || tempo > LIMITES.tempoMax) {
    erros.push(
      `O tempo de processamento simulado deve ser um número inteiro de ` +
        `${LIMITES.tempoMin} a ${LIMITES.tempoMax} segundos.`
    );
  }

  if (ehFifo) {
    const grupo = els.groupId.value.trim();
    if (grupo.length < LIMITES.grupoMin || grupo.length > LIMITES.grupoMax) {
      erros.push(
        `Para filas FIFO, o MessageGroupId deve ter de ${LIMITES.grupoMin} a ` +
          `${LIMITES.grupoMax} caracteres.`
      );
    }

    if (!els.contentDedup.checked) {
      const dedup = els.dedupId.value.trim();
      if (dedup.length < LIMITES.dedupMin || dedup.length > LIMITES.dedupMax) {
        erros.push(
          `Com a deduplicação por conteúdo desabilitada, o ` +
            `MessageDeduplicationId deve ter de ${LIMITES.dedupMin} a ` +
            `${LIMITES.dedupMax} caracteres.`
        );
      }
    }
  }

  return { erros };
}

/**
 * Monta o payload de `POST /api/mensagens` a partir do formulário já validado.
 *
 * A quantidade informada gera N itens; quando há mais de uma mensagem, o número
 * sequencial é anexado ao order_id base para mantê-los distintos.
 *
 * @param {object} els Referências dos elementos do formulário.
 * @returns {import("../api.js").api} Payload conforme `RequisicaoEnvioMensagens`.
 */
function montarPayload(els) {
  const ehFifo = els.queue.value === "fifo";
  const quantidade = paraInteiro(els.quantity.value) ?? 0;
  const orderIdBase = els.orderId.value.trim();
  const tempo = paraInteiro(els.processing.value);
  const cenario = els.scenario.value;
  const dedupPorConteudo = ehFifo ? els.contentDedup.checked : true;

  const mensagens = [];
  for (let i = 0; i < quantidade; i += 1) {
    const orderId = quantidade > 1 ? `${orderIdBase}-${i + 1}` : orderIdBase;
    const item = {
      order_id: orderId,
      tempo_processamento_simulado_segundos: tempo,
      cenario_falha: cenario,
    };
    if (ehFifo) {
      item.message_group_id = els.groupId.value.trim();
      if (!dedupPorConteudo) {
        // Sufixo sequencial para evitar colisão de deduplicação entre itens.
        const dedupBase = els.dedupId.value.trim();
        item.message_deduplication_id =
          quantidade > 1 ? `${dedupBase}-${i + 1}` : dedupBase;
      }
    }
    mensagens.push(item);
  }

  return {
    tipo_fila: els.queue.value,
    mensagens,
    deduplicacao_por_conteudo: dedupPorConteudo,
  };
}

/** Exibe uma mensagem de feedback (sucesso ou erro) preservando o formulário. */
function mostrarFeedback(feedback, { tipo, titulo, itens }) {
  feedback.hidden = false;
  feedback.classList.toggle("is-error", tipo === "erro");
  feedback.classList.toggle("is-success", tipo === "sucesso");

  const lista = (itens ?? [])
    .map((texto) => `<li>${texto}</li>`)
    .join("");
  feedback.innerHTML = `
    <p class="send-feedback-title">${titulo}</p>
    ${lista ? `<ul class="send-feedback-list">${lista}</ul>` : ""}
  `;
}

/** Alterna a visibilidade dos campos FIFO conforme a fila selecionada (Req. 4.6). */
function alternarCamposFifo(els) {
  const ehFifo = els.queue.value === "fifo";
  els.fifoFields.hidden = !ehFifo;
  atualizarCampoDedup(els);
}

/** Mostra o MessageDeduplicationId apenas quando a deduplicação está desligada. */
function atualizarCampoDedup(els) {
  const ehFifo = els.queue.value === "fifo";
  els.dedupField.hidden = !ehFifo || els.contentDedup.checked;
}

/**
 * Descreve, em pt-BR, o resultado de um envio bem-sucedido ou com falha parcial.
 *
 * @param {object} resposta Corpo de `RespostaEnvioMensagens`.
 * @returns {{tipo: string, titulo: string, itens: string[]}}
 */
function descreverResposta(resposta) {
  // Payload rejeitado pela validação do backend (Req. 4.9).
  if (resposta.aceito === false) {
    const itens = (resposta.campos_invalidos ?? []).map((campo) => {
      const indice =
        campo.indice_mensagem !== null && campo.indice_mensagem !== undefined
          ? ` (mensagem ${campo.indice_mensagem + 1})`
          : "";
      return `${campo.campo}${indice}: ${campo.mensagem}`;
    });
    return {
      tipo: "erro",
      titulo:
        resposta.mensagem ||
        "O envio foi rejeitado: há campos inválidos no formulário.",
      itens: itens.length ? itens : ["Revise os campos e tente novamente."],
    };
  }

  // Falha parcial: alguns itens falharam ao enviar (Req. 4.12).
  if (resposta.houve_falha) {
    const falhas = (resposta.resultados ?? [])
      .filter((item) => !item.enviada)
      .map(
        (item) =>
          `${item.order_id}: ${item.detalhe_erro || "falha ao enviar."}`
      );
    return {
      tipo: "erro",
      titulo:
        resposta.mensagem ||
        `Envio parcial: ${resposta.total_enviadas} de ` +
          `${resposta.total_solicitadas} mensagens enviadas.`,
      itens: falhas,
    };
  }

  // Sucesso total.
  return {
    tipo: "sucesso",
    titulo:
      resposta.mensagem ||
      `${resposta.total_enviadas} mensagem(ns) enviada(s) para ` +
        `${resposta.nome_fila || resposta.tipo_fila}.`,
    itens: [],
  };
}

/**
 * Inicializa a área de envio.
 *
 * @param {object} ctx Contexto compartilhado do Dashboard.
 * @param {import("../api.js").api} ctx.api Cliente da API_Backend.
 * @param {HTMLElement} ctx.container Seção `#send-area` desta área.
 */
export function init(ctx) {
  if (!ctx || !ctx.container || !ctx.api) {
    return;
  }
  const { api, container } = ctx;

  const els = montarFormulario(container);

  // Visibilidade inicial e reações aos controles que afetam os campos FIFO.
  alternarCamposFifo(els);
  els.queue.addEventListener("change", () => alternarCamposFifo(els));
  els.contentDedup.addEventListener("change", () => atualizarCampoDedup(els));

  els.form.addEventListener("submit", async (evento) => {
    evento.preventDefault();

    // 1) Validação em pt-BR antes de enviar (Req. 4.9). Preserva os valores.
    const { erros } = validarCampos(els);
    if (erros.length > 0) {
      mostrarFeedback(els.feedback, {
        tipo: "erro",
        titulo: "Corrija os campos abaixo antes de enviar:",
        itens: erros,
      });
      return;
    }

    // 2) Envio ao backend. Desabilita o botão durante a chamada.
    const payload = montarPayload(els);
    els.submit.disabled = true;
    mostrarFeedback(els.feedback, {
      tipo: "sucesso",
      titulo: "Enviando mensagens...",
      itens: [],
    });

    try {
      const resposta = await api.enviarMensagens(payload);
      mostrarFeedback(els.feedback, descreverResposta(resposta));
    } catch (erro) {
      // Erro de rede ou HTTP (ApiError). Preserva os valores informados.
      const detalhe =
        erro && erro.detail
          ? typeof erro.detail === "string"
            ? erro.detail
            : JSON.stringify(erro.detail)
          : "";
      mostrarFeedback(els.feedback, {
        tipo: "erro",
        titulo:
          (erro && erro.message) ||
          "Não foi possível enviar as mensagens. Tente novamente.",
        itens: detalhe ? [detalhe] : [],
      });
    } finally {
      els.submit.disabled = false;
    }
  });
}
