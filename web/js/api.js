/**
 * Cliente HTTP da API_Backend (FastAPI) do Laboratório Amazon SQS.
 *
 * Centraliza o acesso aos endpoints reais expostos em `app/api` para que as áreas
 * do Dashboard (Tarefas 17.2 a 17.5) não repitam detalhes de fetch, cabeçalhos e
 * tratamento de erro. Todos os caminhos têm o prefixo `/api`, conforme os
 * roteadores da API_Backend.
 *
 * Contratos alinhados com `app/api/esquemas.py`:
 *  - POST /api/autenticacao        -> { usuario, senha, origem? }
 *  - GET  /api/saude
 *  - GET  /api/config
 *  - PUT  /api/config/simulacao
 *  - POST /api/mensagens
 *  - GET  /api/estatisticas
 *  - GET  /api/monitoramento
 *  - GET  /api/mensagens/{order_id}/historico
 *  - POST /api/laboratorio/cenario
 *  - POST /api/laboratorio/reprocessar-dlq
 *  - PUT  /api/consumidores/{tipo}/instancias
 */

const BASE_URL = "/api";

/**
 * Erro de chamada à API, preservando o status HTTP e o detalhe retornado pela
 * API_Backend (campo `detail` do FastAPI ou corpo bruto).
 */
export class ApiError extends Error {
  constructor(message, status, detail) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
  }
}

/**
 * Executa uma requisição JSON e normaliza o tratamento de erro.
 *
 * @param {string} path Caminho relativo ao BASE_URL (ex.: "/saude").
 * @param {RequestInit} [options] Opções do fetch (method, body já serializado...).
 * @returns {Promise<any>} Corpo JSON da resposta.
 */
async function request(path, options = {}) {
  let response;
  try {
    response = await fetch(`${BASE_URL}${path}`, {
      headers: { "Content-Type": "application/json", ...(options.headers || {}) },
      ...options,
    });
  } catch (networkError) {
    throw new ApiError(
      "Não foi possível conectar à API_Backend. Verifique sua conexão.",
      0,
      String(networkError)
    );
  }

  const text = await response.text();
  let body = null;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = text;
    }
  }

  if (!response.ok) {
    const detail = body && typeof body === "object" ? body.detail : body;
    throw new ApiError(
      "A API_Backend retornou um erro ao processar a solicitação.",
      response.status,
      detail
    );
  }

  return body;
}

const get = (path) => request(path, { method: "GET" });
const post = (path, payload) =>
  request(path, { method: "POST", body: JSON.stringify(payload ?? {}) });
const put = (path, payload) =>
  request(path, { method: "PUT", body: JSON.stringify(payload ?? {}) });

/**
 * Cliente da API_Backend. Cada método reflete um endpoint real.
 * As áreas (17.2–17.5) importam este objeto e chamam somente estes métodos.
 */
export const api = {
  /** POST /api/autenticacao — login com bloqueio por origem (Req. 12.5, 12.6). */
  autenticar(usuario, senha, origem) {
    return post("/autenticacao", { usuario, senha, origem });
  },

  /** GET /api/saude — verificação de inicialização dos recursos AWS. */
  obterSaude() {
    return get("/saude");
  },

  /** GET /api/config — Config_Aplicada e Config_Simulacao. */
  obterConfig() {
    return get("/config");
  },

  /** PUT /api/config/simulacao — ajuste de parâmetros de Config_Simulacao. */
  ajustarConfigSimulacao(ajuste) {
    return put("/config/simulacao", ajuste);
  },

  /** POST /api/mensagens — envio de 1 a 10 mensagens. */
  enviarMensagens(requisicao) {
    return post("/mensagens", requisicao);
  },

  /** GET /api/estatisticas — estatísticas agregadas por fluxo. */
  obterEstatisticas() {
    return get("/estatisticas");
  },

  /** GET /api/monitoramento — tabela de monitoramento. */
  obterMonitoramento() {
    return get("/monitoramento");
  },

  /** GET /api/mensagens/{order_id}/historico — linha do tempo da mensagem. */
  obterHistorico(orderId) {
    return get(`/mensagens/${encodeURIComponent(orderId)}/historico`);
  },

  /** POST /api/laboratorio/cenario — executa um cenário de falha. */
  executarCenario(requisicao) {
    return post("/laboratorio/cenario", requisicao);
  },

  /** POST /api/laboratorio/reprocessar-dlq — move mensagens da DLQ. */
  reprocessarDlq(requisicao) {
    return post("/laboratorio/reprocessar-dlq", requisicao);
  },

  /** PUT /api/consumidores/{tipo}/instancias — ajusta instâncias (1 a 10). */
  ajustarInstancias(tipo, instancias) {
    return put(`/consumidores/${encodeURIComponent(tipo)}/instancias`, {
      instancias,
    });
  },
};
