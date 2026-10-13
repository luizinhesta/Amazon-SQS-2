# Revisão de design — Resiliência FIFO e tratamento de `DeleteMessage` falho (iteração de incorporação)

Escopo desta revisão: verificar se as **6 decisões** fechadas na iteração anterior
(CHANGES_REQUESTED: 4 MEDIUM + 2 NIT) foram corretamente incorporadas ao `design.md`
e se não restam ambiguidades/gaps/HIGH/MEDIUM novos que bloqueiem a implementação.
A arquitetura das frentes A/B/C/D **não** é reaberta — já estava aprovada na prática.
Só bloqueio por problema concreto novo.

Método: li o `design.md` inteiro e verifiquei por leitura direta do código do worktree
(`c:\github\amazon-sqs2\.worktrees\fix-fifo-resiliencia\app\...` e `tests\...`) os
pressupostos que embasam cada decisão.

## Resumo do veredito

As 6 decisões estão **presentes e corretas** quanto ao comportamento que especificam.
Nenhum finding HIGH ou MEDIUM foi encontrado. Há **um** finding NIT: a *justificativa*
da Decisão 1 contém uma afirmação factualmente incorreta sobre o código (o seletor de
`order_id` do Standard), embora a decisão em si (usar corpo JSON com `order_id`
explícito) seja válida e não quebre nada.

Veredito mecânico: 0 HIGH + 0 MEDIUM ⇒ **APPROVED**.

## Verificação das 6 decisões

### Decisão 1 — corpo JSON dos fakes com `order_id` explícito
PRESENTE. A subseção "Extensão obrigatória dos dublês" e a resposta ao Finding 1 fixam
o corpo `{"order_id":"pedido-1","cenario":"sucesso","processing_seconds":5}` para os
testes de consumidor. O exemplo e a decisão são corretos e suficientes.
Ressalva factual registrada no Finding NIT-1 abaixo (rationale incorreto, não a decisão).

### Decisão 2 — modelo de reentrega do `SqsFake`
PRESENTE e CORRETA. A subseção "Modelo de reentrega do `SqsFake` (opção (a))" especifica
que mensagem não excluída permanece na fila do fake e retorna no próximo
`receive_messages` com `approximate_receive_count` incrementado; só sai da fila quando o
`delete_message` confirma. O parâmetro novo `efeitos_delete: list[Exception|None]|None`
é retrocompatível e `erro_ao_excluir` permanece. Verifiquei que o `SqsFake` atual
(`tests/test_processamento.py`) tem exatamente o `erro_ao_excluir` que "lança sempre"
descrito, confirmando que a extensão é aditiva e não quebra os testes existentes.

### Decisão 3 — `EXCLUSAO_NAO_CONFIRMADA` na precedência do StateDeriver
PRESENTE e CORRETA. A seção "StateDeriver" posiciona o novo estado **abaixo** de DLQ e
PROCESSADA (ambos terminais, resolvidos por `tipos_presentes`/`_dlq_confirmada` antes do
laço `reversed`), **dentro** do laço `reversed` junto aos demais desfechos, e declara a
invariante "nenhum `FALHA` coexiste com `exclusao_nao_confirmada` na mesma tentativa".
Confirmei a estrutura real de `state_deriver.py`: DLQ e PROCESSADA são decididos antes do
`for ... in reversed(lista)`, e os estados intermediários dentro dele — exatamente como o
design descreve. `ESTADOS_TERMINAIS = frozenset({PROCESSADA, ENCAMINHADA_DLQ})` em
`tipos.py` confirma que o novo estado NÃO é terminal (o design declara isso
explicitamente). O caso de reentrega ativa (laço `reversed` encontra o
`processamento_iniciado` da 2ª tentativa antes ⇒ `EM_PROCESSAMENTO`) é coerente com o
código lido.

### Decisão 4 — contador dedicado `exclusoes_nao_confirmadas`
PRESENTE e CORRETA. A subseção "Contabilização do resultado nos consumidores (Opção A)"
adiciona `EstatisticasConsumo.exclusoes_nao_confirmadas` com ramo `elif
resultado.exclusao_nao_confirmada` **antes** do ramo de falhas, garantindo que nunca cai
em `falhas_processamento`. Confirmei que `EstatisticasConsumo` é `@dataclass` mutável e
que `_tratar_mensagem` hoje tem exatamente o `if resultado.sucesso: ... else:
falhas_processamento += 1` que o design substitui pelo `elif` dedicado. No lado de
estatísticas agregadas, o campo `EstatisticasFluxo.exclusao_nao_confirmada` (default 0) e
o schema `EstatisticasFluxoSaida.exclusao_nao_confirmada = Field(0, ge=0)` são
retrocompatíveis — confirmei que os campos atuais de `EstatisticasFluxoSaida` usam
`Field(..., ge=0)` (obrigatórios), então o default 0 é a escolha certa para não quebrar
serialização. O `_fluxo_para_saida` em `servico_monitoramento.py` passa o novo contador,
e o `overview.js` ganha linha distinta "Exclusão não confirmada" separada de "Com falha".

### Decisão 5 — renumeração da Frente D Fase 1
PRESENTE. Os itens da Fase 1 estão numerados em sequência contínua 1..6 (estender
`createObservabilidade`; reescrever `onTick`; `api.obterSaudeConsumidores`; endpoint
`GET /api/consumidores/saude`; mensagens disponíveis vs em processamento; falha de
exclusão visível). Sem numeração duplicada.

### Decisão 6 — faixa laranja usa `MENSAGEM_TIMEOUT_CALCULO` do frontend
PRESENTE e EXPLÍCITA. No item 2 da Fase 1 e na resposta ao Finding 6, o design declara que
no caminho de timeout de cálculo o texto é **sempre** a constante do frontend
`MENSAGEM_TIMEOUT_CALCULO` (texto fixo) e que o campo `mensagem` do backend é **ignorado**
nesse caminho. O snippet de `onTick` usa `obs.marcarFalha(MENSAGEM_TIMEOUT_CALCULO)`.
Confirmei que `refresh.js` hoje exporta `MENSAGEM_DADOS_DESATUALIZADOS`, tem
`createObservabilidade` com `marcarSucesso`/`marcarFalha` e aplica `global-status--stale`
no `catch` do `tick()` — exatamente o ponto que o design estende, então a decisão é
implementável como escrita.

## Findings

1. **NIT — rationale factualmente incorreto na Decisão 1 (seletor de `order_id` do Standard).**
   Onde: seção "Testabilidade", subseção "Corpo das mensagens fake nos testes de consumidor",
   e resposta ao Finding 1 ("Respostas às constatações da revisão").
   Problema: o design afirma que o `ConsumidorStandard` deriva o `order_id` de
   `payload.order_id` **"sem fallback para o corpo cru (ao contrário de
   `consumidor_fifo._order_id_da_mensagem`)"** e que, sem JSON com `order_id`, a
   idempotência do Standard decidiria `REJEITAR_VALIDACAO`. Isso está incorreto: verifiquei
   `app/consumidores/payload_mensagem.py::extrair_payload` — ele aplica um fallback
   documentado que trata o **corpo cru como `order_id`** quando o corpo não é JSON de
   contrato (`fallback = PayloadMensagem(order_id=body or None)`). O `ConsumidorStandard`
   usa esse `extrair_payload` em `_tratar_mensagem`, logo um corpo como `"pedido-1"` vira
   `order_id="pedido-1"` e a idempotência decidiria `PROCESSAR` (não `REJEITAR_VALIDACAO`).
   Na prática, **tanto** o Standard **quanto** o FIFO têm fallback de corpo cru (o FIFO
   via `_order_id_da_mensagem`, que inclusive delega a `extrair_payload`).
   Por que é NIT e não bloqueia: a **decisão** em si (usar corpo JSON de contrato com
   `order_id` explícito nos testes) continua válida, correta e desejável — corpo JSON é o
   contrato real do produtor e faz os testes 4/5/10 fecharem como escritos. O coder que
   seguir a decisão escreve o corpo JSON correto independentemente do rationale. O erro é
   só na justificativa de *necessidade* ("senão quebra"), não na instrução.
   Fix concreto: corrigir a frase para refletir o código real, por exemplo:
   "Os testes de consumidor usam corpo JSON de contrato com `order_id` explícito por ser o
   formato real do produtor e por carregar também `cenario`/`processing_seconds` que os
   testes 4/5/10 precisam. (Nota: um corpo cru também seria aceito como `order_id` pelo
   fallback de `extrair_payload`, tanto no Standard quanto no FIFO; preferimos o JSON de
   contrato para fidelidade e para transportar os demais campos.)" Remover a afirmação de
   que o Standard não tem fallback e de que cairia em `REJEITAR_VALIDACAO`.

## Verified Assumptions

- `ESTADOS_TERMINAIS = frozenset({Estado.PROCESSADA, Estado.ENCAMINHADA_DLQ})` em
  `app/dominio/tipos.py` — confirmado. O novo `EXCLUSAO_NAO_CONFIRMADA` fica fora, como o
  design declara (não terminal).
- `classificador_origem.py` tem `assert set(ORIGEM_POR_TIPO) == set(EventType)` em tempo de
  import e um laço que valida `ORIGEM_POR_TIPO[_tipo] is _origem_fixa` só sobre
  `ORIGEM_OBRIGATORIA_POR_TIPO` — confirmado. A obrigação de adicionar
  `EXCLUSAO_NAO_CONFIRMADA: OBSERVADO` a `ORIGEM_POR_TIPO` (senão o import quebra) é real e
  corretamente destacada.
- `ORIGEM_OBRIGATORIA_POR_TIPO` fixa `PROVAVEL_EXPIRACAO_VISIBILITY` e `ENCAMINHADA_DLQ`;
  o novo tipo não precisa entrar ali — confirmado.
- `state_deriver.derivar_estado`: DLQ (`_dlq_confirmada`) e PROCESSADA (`tipos_presentes`)
  são resolvidos **antes** do laço `for ... in reversed(lista)`; os demais estados dentro
  dele — confirmado, sustenta a precedência da Decisão 3 e o comportamento de reentrega.
- `EstatisticasConsumo` é `@dataclass` mutável e `_tratar_mensagem` tem hoje
  `if resultado.sucesso: processadas_com_sucesso += 1 else: falhas_processamento += 1` —
  confirmado; o ramo `elif` dedicado da Decisão 4 encaixa exatamente aí.
- `EstatisticasFluxoSaida` (em `app/api/esquemas.py`) usa `Field(..., ge=0)` nos contadores
  atuais (`com_falha` etc.) — confirmado; o novo campo com `Field(0, ge=0)` é a escolha
  retrocompatível correta. `_fluxo_para_saida` em `servico_monitoramento.py` é o mapeador a
  estender — confirmado.
- `SqsFake` atual (`tests/test_processamento.py`) usa `erro_ao_excluir` que "lança sempre"
  em `delete_message` — confirmado; a extensão `efeitos_delete` da Decisão 2 é aditiva.
- Frontend: `web/js/areas/monitoring.js` tem `CLASSE_ESTADO` fechado com fallback
  `|| "estado-neutro"` — confirmado; a entrada do novo estado é necessária como o design diz.
- `web/js/areas/refresh.js` exporta `MENSAGEM_DADOS_DESATUALIZADOS`, tem
  `createObservabilidade` (`marcarSucesso`/`marcarFalha`) e aplica `global-status--stale` no
  `catch` do `tick()` — confirmado; a Decisão 6 e a reescrita do `onTick` são implementáveis.

## Unverified / Wrong Assumptions

- **ERRADA (Finding NIT-1):** afirmação do design de que o `ConsumidorStandard` deriva
  `order_id` **sem fallback de corpo cru** e cairia em `REJEITAR_VALIDACAO` sem JSON. O
  código (`extrair_payload`) tem fallback de corpo cru para ambos os consumidores. Não
  bloqueia (ver NIT-1).
- **Não verificado nesta revisão (fora do escopo de bloqueio):** o conteúdo exato de
  `app/main.py` (`GerenciadorConsumidores`), `consumidor_fifo.py` (`ResultadoIteracaoFifo`
  frozen) e `servico_saude_consumidores.py` (ainda não existe). Essas peças pertencem à
  arquitetura das frentes B/D já aprovada; o design as especifica em detalhe e as decisões
  sob revisão não dependem delas. Nenhum sinal de contradição com o que foi lido.

## Conclusão

Todas as 6 decisões foram incorporadas corretamente. O único finding é NIT (rationale a
corrigir, não a decisão). 0 HIGH + 0 MEDIUM ⇒ **APPROVED**.
