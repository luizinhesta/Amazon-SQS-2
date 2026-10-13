# Roteiro de Testes — Laboratório Amazon SQS (`aws-sqs-lab`)

Este documento descreve o **roteiro dos testes de laboratório** que validam, de ponta a ponta, o comportamento do laboratório didático de Amazon SQS sobre serviços reais da AWS na região `us-east-1`. Os **testes 1 a 6** cobrem os comportamentos originais do laboratório; os **testes 7 a 10** cobrem a correção de resiliência dos consumidores (estado "Exclusão não confirmada", endpoint de saúde, faixa de status diferenciada, extensão opt-in de Visibility_Timeout e recuperação de thread encerrada). Cada teste é executado a partir do **Dashboard** (pt-BR) servido pela instância EC2 e observado pela área de **Monitoramento**, pelo **Histórico detalhado** de cada mensagem e pela **Visão geral** (estatísticas por fluxo).

Os testes exercitam a aplicação real: envio via `POST /api/mensagens`, cenários controlados via `POST /api/laboratorio/cenario`, reprocessamento via `POST /api/laboratorio/reprocessar-dlq`, estatísticas via `GET /api/monitoramento` e `GET /api/estatisticas`, e a linha do tempo de cada mensagem via `GET /api/mensagens/{order_id}/historico`.

> **Importante sobre os resultados reais:** os campos **"7. Resultado real"** de cada teste estão **em branco, destinados ao preenchimento pelo executor**, pois dependem da execução efetiva contra os recursos reais da AWS. Conforme a política do projeto, nenhum resultado deve ser estimado, inventado ou presumido — registre apenas o que foi efetivamente observado durante a execução.

## Convenções e termos

- **Fluxos:** `standard` (fila `lab-sqs-standard` + DLQ `lab-sqs-standard-dlq`) e `fifo` (fila `lab-sqs-fifo.fifo` + DLQ `lab-sqs-fifo-dlq.fifo`).
- **Config_Aplicada** (configurada manualmente no Console AWS, imutável pelo Dashboard): `Visibility_Timeout` = 30 s; `maxReceiveCount` da Redrive_Policy = 2; retenção 4 dias (filas) / 14 dias (DLQs); `Long_Polling` (`WaitTimeSeconds`) = 20 s; `SSE_SQS` habilitado; `Deduplicacao_Por_Conteudo` habilitada nas filas FIFO.
- **Config_Simulacao** (ajustável no Dashboard): `Tempo_Processamento_Simulado` [0, 300] s (padrão 45 s); quantidade de mensagens [1, 10]; número de instâncias por consumidor [1, 10]; intervalo de atualização [5, 300] s (padrão 10 s; visão geral 1–60 s, padrão 5 s). A **extensão opt-in de Visibility_Timeout** (via `ChangeMessageVisibility`) vem **desligada por padrão**; quando ligada, aceita um valor estendido em `[20, 43200]` s e **não** altera os 30 s da fila no Console AWS.
- **Cenario_Falha** (valores reais do enum `CenarioFalha` em `app/consumidores/processamento.py`): `sucesso`, `excecao_proposital`, `interrupcao`, `duracao_superior_visibility`, `recebimento_repetido`, `encaminhar_dlq`.
- **Evento_Observado × Evento_Inferido:** eventos confirmados pela AWS (ex.: `enviada`, `recebida`, `excluida`, `encaminhada_dlq`, `exclusao_nao_confirmada`) aparecem como **Observado**; deduções da aplicação (ex.: `provavel_expiracao_visibility`) aparecem como **Inferido**. O Dashboard exibe o indicador por evento sem exigir interação.
- **Estados derivados:** `Aguardando`, `Recebida`, `Em processamento`, `Processada` (terminal), `Exclusão não confirmada` (não terminal — negócio concluído mas `DeleteMessage` falhou), `Falha`, `Aguardando nova tentativa` e `Encaminhada para DLQ` (terminal).
- **Faixa de status global do Dashboard (precedência):** API indisponível (vermelha) > consumidor em falha (amarela) > timeout de cálculo (laranja) > sucesso.
- **Pré-requisitos comuns a todos os testes** (não repetidos em cada roteiro, salvo quando específicos):
  - As 4 filas SQS, a Tabela_Historico, a IAM_Role_EC2 e a instância EC2 criadas manualmente conforme `IMPLANTACAO.md`.
  - A aplicação em execução na EC2 (API_Backend + consumidores + Dashboard) com a verificação de inicialização (`GET /api/saude`) bem-sucedida.
  - Login no Dashboard efetuado (`POST /api/autenticacao`) a partir de origem autorizada.
  - Área de **Visão geral** aberta com atualização periódica ativa para acompanhar as estatísticas.

Cada teste a seguir contém os **8 campos obrigatórios**: (1) Objetivo, (2) Pré-requisitos, (3) Configuração, (4) Passo a passo numerado, (5) Resultado esperado, (6) Evidências, (7) Resultado real (a preencher) e (8) Critério de aprovação.

---

## Teste 1 — Fluxo Standard (envio → recebimento → exclusão)

**1. Objetivo**
Validar o caminho feliz do fluxo Standard: uma mensagem enviada à `lab-sqs-standard` é recebida pelo `Consumidor_Standard`, processada com sucesso e excluída da fila, com os eventos de ciclo de vida registrados na Tabela_Historico.

**2. Pré-requisitos**
- Pré-requisitos comuns satisfeitos (filas, tabela, aplicação em execução, login efetuado).
- Fila `lab-sqs-standard` disponível e sem backlog de mensagens anteriores que possa confundir a observação.
- Ao menos 1 instância ativa de `Consumidor_Standard`.

**3. Configuração**
- Fluxo: `standard`.
- Quantidade: 1 mensagem (pode-se usar o envio de conjunto de 5 pedidos, mas 1 basta para aprovação).
- `order_id`: `teste1-standard-0001` (1–80 caracteres).
- `Cenario_Falha`: `sucesso`.
- `Tempo_Processamento_Simulado`: 10 s (valor baixo para observar a conclusão rapidamente; dentro de [0, 300]).

**4. Passo a passo numerado**
1. No Dashboard, abra a área **Envio de mensagens**.
2. Selecione a fila de destino **Standard** (`lab-sqs-standard`).
3. Informe `order_id` = `teste1-standard-0001`, `Tempo_Processamento_Simulado` = 10 s e `Cenario_Falha` = `sucesso`.
4. Confirme o envio (equivalente a `POST /api/mensagens` com `tipo_fila: "standard"`).
5. Abra a área de **Monitoramento** e localize a linha com ID `teste1-standard-0001`.
6. Acompanhe a coluna **Estado** evoluir: `Aguardando` → `Recebida` → `Em processamento` → `Processada`.
7. Acione a ação **Visualizar histórico** da mensagem (`GET /api/mensagens/teste1-standard-0001/historico`).
8. Verifique a linha do tempo com os eventos `enviada`, `recebida`, `processamento_iniciado`, `processada` e `excluida`.

**5. Resultado esperado**
- A mensagem é enviada sem erro e registrada com evento `enviada` (Observado).
- O `Consumidor_Standard` recebe a mensagem (`recebida`, Observado) com `receive_count` = 1 e identificação do consumidor responsável.
- Após o Tempo_Processamento_Simulado, o estado chega a **Processada** e a mensagem é excluída (`excluida`, Observado), não voltando a aparecer como disponível.
- A linha do tempo exibe os eventos em ordem cronológica crescente, cada um com indicador **Observado**.

**6. Evidências (o que capturar)**
- Tela do **Dashboard** — Envio confirmando o envio da mensagem.
- Tela de **Monitoramento** mostrando a linha no estado **Processada** (ID, Fila, Consumidor, Tentativas = 1, Último evento).
- Tela de **Histórico detalhado** com a linha do tempo completa (`enviada` → `excluida`).
- **Visão geral**: incremento em "enviadas" e "processadas" do fluxo Standard e horário da última atualização bem-sucedida.

**7. Resultado real** _(a preencher pelo executor após a execução na AWS)_

```
Data/hora da execução:
Estado final observado no Monitoramento:
Eventos observados na linha do tempo:
Consumidor responsável (consumer_id):
Observações:
```

**8. Critério de aprovação**
Aprovado quando ao menos 1 mensagem for enviada e sua recepção e exclusão pelo consumidor ocorrerem sem erro reportado (estado final **Processada** com evento `excluida` Observado registrado).

---

## Teste 2 — Expiração de Visibility_Timeout (reentrega)

**1. Objetivo**
Demonstrar que, quando o processamento não confirma a exclusão dentro do `Visibility_Timeout`, a mesma mensagem volta a ficar visível na fila e é reentregue, com a contagem de recebimentos incrementada.

**2. Pré-requisitos**
- Pré-requisitos comuns satisfeitos.
- `Visibility_Timeout` da fila `lab-sqs-standard` confirmado em **30 segundos** (Config_Aplicada, visível na Visão geral).
- 1 instância ativa de `Consumidor_Standard` (facilita observar a reentrega na mesma instância).

**3. Configuração**
- Fluxo: `standard`.
- `Cenario_Falha`: `duracao_superior_visibility` (processamento dura mais que o Visibility_Timeout; o consumidor abstém-se de excluir).
- `Tempo_Processamento_Simulado`: 45 s (maior que o Visibility_Timeout de 30 s).
- Quantidade: 1 mensagem, `order_id` = `teste2-visibility-0001`.

**4. Passo a passo numerado**
1. Na **Visão geral**, confirme que o `Visibility_Timeout` exibido é **30 s** (rótulo Config_Aplicada).
2. Abra o **Laboratório de falhas** e selecione o cenário **duração superior ao Visibility_Timeout** para o fluxo **Standard** (equivalente a `POST /api/laboratorio/cenario` com `cenario: "duracao_superior_visibility"`), com `Tempo_Processamento_Simulado` = 45 s e `order_id` base `teste2-visibility`.
3. Dispare o cenário.
4. No **Monitoramento**, localize a mensagem e observe o estado passar a **Em processamento**.
5. Aguarde o decurso do `Visibility_Timeout` (30 s) sem exclusão.
6. Acompanhe o estado transitar para **Aguardando nova tentativa** e, na sequência, nova **Recebida** com **Tentativas** incrementada para 2.
7. Abra o **Histórico detalhado** da mensagem e verifique o evento `provavel_expiracao_visibility` (indicador **Inferido**) seguido de nova `recebida` (indicador **Observado**) com `receive_count` = 2.

**5. Resultado esperado**
- A mensagem não é excluída durante o primeiro processamento.
- Após ~30 s, a aplicação registra `provavel_expiracao_visibility` como **Evento_Inferido** e a mensagem é reentregue.
- A nova entrega gera um evento `recebida` **Observado** com `receive_count` incrementado (de 1 para 2) e a coluna **Tentativas** reflete o incremento.

**6. Evidências (o que capturar)**
- Tela da **Visão geral** destacando o `Visibility_Timeout` = 30 s como Config_Aplicada.
- Tela de **Monitoramento** mostrando a coluna **Tentativas** passando de 1 para 2.
- Tela de **Histórico detalhado** com o par de eventos: `provavel_expiracao_visibility` (Inferido) → `recebida` (Observado, receive_count = 2).

**7. Resultado real** _(a preencher pelo executor após a execução na AWS)_

```
Data/hora da execução:
Visibility_Timeout aplicado (s): 30
Tentativas antes/depois da reentrega:
Evento de expiração registrado (tipo/origem):
Instante da reentrega:
Observações:
```

**8. Critério de aprovação**
Aprovado quando, decorrido o `Visibility_Timeout` de **30 segundos** sem a exclusão da mensagem, a mesma mensagem for reentregue à fila com a contagem de recebimentos incrementada.

---

## Teste 3 — Encaminhamento para DLQ

**1. Objetivo**
Validar que uma mensagem que falha repetidamente é movida pela Redrive_Policy para a DLQ após a contagem de recebimentos atingir o `maxReceiveCount`, e que o encaminhamento é registrado como **Evento_Observado** (nunca inferido apenas por expiração de Visibility_Timeout).

**2. Pré-requisitos**
- Pré-requisitos comuns satisfeitos.
- Redrive_Policy da `lab-sqs-standard` → `lab-sqs-standard-dlq` com `maxReceiveCount` = **2** confirmado (Config_Aplicada).
- 1 instância ativa de `Consumidor_Standard`.

**3. Configuração**
- Fluxo: `standard`.
- `Cenario_Falha`: `encaminhar_dlq` (a mensagem falha a cada recebimento, sem exclusão).
- `Tempo_Processamento_Simulado`: 5 s (curto, para encadear as falhas mais rápido).
- Quantidade: 1 mensagem, `order_id` = `teste3-dlq-0001`.

**4. Passo a passo numerado**
1. Na **Visão geral**, confirme `maxReceiveCount` = **2** (rótulo Config_Aplicada).
2. No **Laboratório de falhas**, selecione o cenário **encaminhamento para DLQ** para o fluxo **Standard** (`POST /api/laboratorio/cenario` com `cenario: "encaminhar_dlq"`), `order_id` base `teste3-dlq`.
3. Dispare o cenário.
4. No **Monitoramento**, observe a mensagem ser recebida e falhar (estado **Falha** / **Aguardando nova tentativa**), com **Tentativas** = 1.
5. Aguarde a reentrega após o Visibility_Timeout; a mensagem é recebida novamente (**Tentativas** = 2) e falha de novo.
6. Após o segundo recebimento atingir o `maxReceiveCount`, o SQS move a mensagem para a `lab-sqs-standard-dlq`.
7. No **Histórico detalhado** da mensagem, verifique o evento `encaminhada_dlq` com indicador **Observado** e `receive_count` ≥ 2.
8. Confirme na **Visão geral** o incremento do contador "encaminhadas para DLQ" do fluxo Standard.

**5. Resultado esperado**
- A mensagem falha nos recebimentos e **não** é excluída.
- Ao atingir `receive_count` = 2 (= `maxReceiveCount`), o SQS encaminha a mensagem para a DLQ.
- A aplicação registra `encaminhada_dlq` como **Evento_Observado** (presença confirmada na DLQ) e o estado derivado passa a **Encaminhada para DLQ** (estado terminal).
- O encaminhamento **não** é afirmado apenas por expiração do Visibility_Timeout.

**6. Evidências (o que capturar)**
- Tela da **Visão geral** com `maxReceiveCount` = 2 e o contador "encaminhadas para DLQ" incrementado.
- Tela de **Monitoramento** com a mensagem no estado **Encaminhada para DLQ** e **Tentativas** = 2.
- Tela de **Histórico detalhado** com o evento `encaminhada_dlq` (Observado) e os eventos `recebida`/`falha` anteriores.

**7. Resultado real** _(a preencher pelo executor após a execução na AWS)_

```
Data/hora da execução:
maxReceiveCount aplicado: 2
Tentativas até o encaminhamento:
Evento encaminhada_dlq (origem Observado? sim/não):
Fila de destino observada (lab-sqs-standard-dlq):
Observações:
```

**8. Critério de aprovação**
Aprovado quando a mensagem estiver presente na DLQ (`lab-sqs-standard-dlq`) após o número de recebimentos atingir o `maxReceiveCount` = **2**, com o evento `encaminhada_dlq` registrado como Observado.

---

## Teste 4 — Ordenação FIFO

**1. Objetivo**
Validar que mensagens enviadas a um mesmo `MessageGroupId` na fila `lab-sqs-fifo.fifo` são recebidas e processadas na mesma ordem em que foram enviadas.

**2. Pré-requisitos**
- Pré-requisitos comuns satisfeitos.
- Fila `lab-sqs-fifo.fifo` disponível com `Deduplicacao_Por_Conteudo` habilitada (Config_Aplicada).
- 1 instância ativa de `Consumidor_FIFO` (processa um único `MessageGroupId` por vez, preservando a ordem).

**3. Configuração**
- Fluxo: `fifo`.
- Quantidade: 3 mensagens no mesmo `MessageGroupId` = `grupo-teste4`.
- `order_id`: `teste4-fifo-0001`, `teste4-fifo-0002`, `teste4-fifo-0003` (enviados nesta ordem).
- `Cenario_Falha`: `sucesso` para todas.
- `Tempo_Processamento_Simulado`: 10 s.
- Deduplicação por conteúdo habilitada (não é necessário informar `MessageDeduplicationId`).

**4. Passo a passo numerado**
1. No Dashboard, abra a área **Envio de mensagens** e selecione a fila **FIFO** (`lab-sqs-fifo.fifo`).
2. Informe o `MessageGroupId` = `grupo-teste4`.
3. Envie, nesta ordem, as mensagens `teste4-fifo-0001`, `teste4-fifo-0002` e `teste4-fifo-0003`, todas com `Cenario_Falha` = `sucesso` (`POST /api/mensagens` com `tipo_fila: "fifo"`).
4. Abra o **Monitoramento** e localize as três linhas do grupo `grupo-teste4`.
5. Acompanhe o processamento das três mensagens pelo `Consumidor_FIFO`.
6. Use a **comparação por MessageGroupId** no Laboratório/Monitoramento para visualizar a sequência de processamento e o consumidor responsável por cada mensagem.
7. Abra o **Histórico detalhado** de cada mensagem e compare os horários (`app_time`) dos eventos `recebida`/`processada`.

**5. Resultado esperado**
- As três mensagens do grupo `grupo-teste4` são recebidas e processadas na ordem `0001` → `0002` → `0003`.
- Nenhuma mensagem posterior do grupo é processada antes da anterior ser concluída (um `MessageGroupId` por instância por vez).
- Todas terminam no estado **Processada** com `excluida` registrado.

**6. Evidências (o que capturar)**
- Tela de **Envio** confirmando os 3 envios com o mesmo `MessageGroupId`.
- Tela de **comparação por MessageGroupId** mostrando a sequência de processamento `0001 → 0002 → 0003` e o consumidor responsável.
- Telas de **Histórico detalhado** das 3 mensagens, evidenciando a ordem cronológica dos eventos `recebida`/`processada`.

**7. Resultado real** _(a preencher pelo executor após a execução na AWS)_

```
Data/hora da execução:
MessageGroupId: grupo-teste4
Ordem de envio: 0001, 0002, 0003
Ordem de processamento observada:
Consumidor(es) responsável(is):
Observações:
```

**8. Critério de aprovação**
Aprovado quando ao menos **3 mensagens** do mesmo `MessageGroupId` forem recebidas/processadas na **mesma ordem** em que foram enviadas.

---

## Teste 5 — Concorrência com ≥ 2 consumidores

**1. Objetivo**
Validar que, com múltiplos consumidores ativos simultaneamente, cada mensagem é processada por **exatamente um** consumidor, sem duplicidade de processamento dentro do `Visibility_Timeout`, e que o consumidor responsável por cada mensagem é identificado.

**2. Pré-requisitos**
- Pré-requisitos comuns satisfeitos.
- Capacidade de ajustar o número de instâncias de `Consumidor_Standard` para **≥ 2** (via `PUT /api/consumidores/standard/instancias`).
- Idempotência por `order_id` ativa (garante um único processamento por pedido).

**3. Configuração**
- Fluxo: `standard`.
- Número de instâncias de `Consumidor_Standard`: **3** (dentro de [1, 10]).
- Quantidade: 6 mensagens, `order_id` = `teste5-conc-0001` a `teste5-conc-0006`.
- `Cenario_Falha`: `sucesso`.
- `Tempo_Processamento_Simulado`: 15 s (tempo suficiente para haver sobreposição entre consumidores dentro do Visibility_Timeout de 30 s).

**4. Passo a passo numerado**
1. No Dashboard, ajuste o número de instâncias de `Consumidor_Standard` para **3** (`PUT /api/consumidores/standard/instancias` com `instancias: 3`) e confirme a aplicação (até 5 s).
2. Abra a área **Envio de mensagens**, selecione **Standard** e envie 6 mensagens (`teste5-conc-0001` a `0006`), todas com `Cenario_Falha` = `sucesso` e `Tempo_Processamento_Simulado` = 15 s.
3. Abra o **Monitoramento** e acompanhe as 6 linhas sendo processadas pelas instâncias.
4. Observe a coluna **Consumidor** de cada linha para identificar qual instância processou cada mensagem.
5. Para cada `order_id`, abra o **Histórico detalhado** e confirme que há **exatamente um** processamento concluído (`processada` + `excluida`), sem `processada` duplicada por consumidores diferentes dentro do Visibility_Timeout.
6. Confira na **Visão geral** as "recebidas por consumidor", que devem identificar cada consumidor individualmente.

**5. Resultado esperado**
- As 6 mensagens são distribuídas entre as 3 instâncias (o `consumer_id` identifica cada uma).
- Cada `order_id` é processado por exatamente um consumidor, com um único evento `processada`/`excluida`; recebimentos repetidos, se houver, geram `recebimento_repetido` sem reexecutar efeitos (idempotência).
- Não há duplicidade de processamento dentro do `Visibility_Timeout`.

**6. Evidências (o que capturar)**
- Tela de ajuste de **instâncias** confirmando 3 consumidores aplicados.
- Tela de **Monitoramento** com a coluna **Consumidor** variando entre as instâncias.
- Tela de **Visão geral** com "recebidas por consumidor" discriminando cada `consumer_id`.
- Telas de **Histórico detalhado** evidenciando um único `processada`/`excluida` por `order_id`.

**7. Resultado real** _(a preencher pelo executor após a execução na AWS)_

```
Data/hora da execução:
Nº de consumidores Standard ativos: 3
Distribuição observada (order_id → consumer_id):
Algum order_id processado mais de uma vez? (sim/não):
Observações:
```

**8. Critério de aprovação**
Aprovado quando, com **≥ 2 consumidores** simultâneos, cada mensagem for processada por **exatamente um** consumidor, sem duplicidade dentro do `Visibility_Timeout`.

---

## Teste 6 — Recuperação (reprocessamento de DLQ / retomada)

**1. Objetivo**
Validar a recuperação de mensagens: (a) mensagens que reaparecem na fila quando o consumidor encerra sem excluí-las e (b) o reprocessamento de mensagens da DLQ de volta à fila principal, com novo processamento bem-sucedido.

**2. Pré-requisitos**
- Pré-requisitos comuns satisfeitos.
- Recomendado executar o **Teste 3** antes, de modo a haver ao menos 1 mensagem na `lab-sqs-standard-dlq` para reprocessar (ou gerar uma no início deste teste).
- 1 instância ativa de `Consumidor_Standard`.

**3. Configuração**
- Fluxo: `standard`.
- **Parte A (retomada):** `Cenario_Falha` = `interrupcao` (o consumidor é interrompido durante o processamento, sem exclusão), `order_id` = `teste6-retomada-0001`, `Tempo_Processamento_Simulado` = 20 s.
- **Parte B (reprocessamento de DLQ):** reprocessar a DLQ do fluxo Standard (`POST /api/laboratorio/reprocessar-dlq` com `tipo_fila: "standard"`); garantir ao menos 1 mensagem na DLQ (reaproveitar a do Teste 3 ou disparar o cenário `encaminhar_dlq` com `order_id` = `teste6-dlq-0001`).

**4. Passo a passo numerado**
1. **Parte A — retomada:** no **Laboratório de falhas**, dispare o cenário **interrupção do consumidor** para o fluxo Standard (`cenario: "interrupcao"`), `order_id` base `teste6-retomada`.
2. No **Monitoramento**, observe a mensagem entrar em processamento e **não** ser excluída (consumidor interrompido).
3. Aguarde o `Visibility_Timeout` (30 s) e confirme que a mensagem **reaparece** disponível e é recebida novamente (**Tentativas** incrementada).
4. **Parte B — reprocessamento de DLQ:** assegure ao menos 1 mensagem na `lab-sqs-standard-dlq` (reaproveite a do Teste 3 ou dispare `encaminhar_dlq` com `order_id` `teste6-dlq-0001` e aguarde o encaminhamento).
5. No **Laboratório de falhas**, acione **Reprocessar DLQ** para o fluxo Standard (`POST /api/laboratorio/reprocessar-dlq`).
6. Confirme na resposta/Monitoramento as mensagens movidas da DLQ de volta à `lab-sqs-standard`.
7. Para a nova rodada, envie/garanta `Cenario_Falha` = `sucesso` de modo que a mensagem reprocessada seja finalmente **Processada** e excluída.
8. Abra o **Histórico detalhado** das mensagens envolvidas e verifique a sequência completa (interrupção/reentrega e, para a DLQ, o retorno à fila principal e o novo processamento).

**5. Resultado esperado**
- **Parte A:** a mensagem interrompida não é excluída e **reaparece** na fila após o Visibility_Timeout, sendo recebida novamente (recuperação por reentrega).
- **Parte B:** as mensagens da DLQ são movidas de volta à fila principal (`mensagens_movidas`/`total_movidas` > 0) e passam por novo processamento; com `sucesso`, terminam em **Processada** com `excluida`.

**6. Evidências (o que capturar)**
- Tela de **Monitoramento** mostrando a mensagem interrompida reaparecendo (Tentativas incrementada).
- Tela/resposta do **Reprocessar DLQ** indicando as mensagens movidas (nome da DLQ, fila principal e `total_movidas`).
- Tela de **Histórico detalhado** evidenciando o retorno da mensagem da DLQ e seu novo processamento bem-sucedido.
- **Visão geral** com os contadores atualizados após a recuperação.

**7. Resultado real** _(a preencher pelo executor após a execução na AWS)_

```
Data/hora da execução:
Parte A — mensagem reapareceu após o Visibility_Timeout? (sim/não):
Parte A — Tentativas antes/depois:
Parte B — total de mensagens movidas da DLQ:
Parte B — order_id reprocessado(s):
Estado final após reprocessamento:
Observações:
```

**8. Critério de aprovação**
Aprovado quando ao menos **1 mensagem reaparecer** na fila após o consumidor encerrar sem excluí-la (Parte A) e as mensagens da DLQ forem efetivamente movidas de volta à fila principal e reprocessadas (Parte B).

---

## Teste 7 — `DeleteMessage` falho vira "Exclusão não confirmada" (consumidor segue vivo)

**1. Objetivo**
Demonstrar que, quando o `DeleteMessage` falha por `ReceiptHandle` expirado (processamento mais longo que o Visibility_Timeout, no fluxo FIFO, com a extensão opt-in **desligada**), o consumidor **não** encerra: a mensagem recebe o estado **Exclusão não confirmada**, o histórico registra o evento `exclusao_nao_confirmada` com o erro original da AWS, e **nenhum** evento `processada`/`excluida` é gravado para aquela tentativa.

**2. Pré-requisitos**
- Pré-requisitos comuns satisfeitos.
- Correção de resiliência implantada (Parte 6.1 do `IMPLANTACAO.md`).
- Extensão opt-in de Visibility_Timeout **desligada** (padrão) na Config_Simulacao.
- `Visibility_Timeout` da fila `lab-sqs-fifo.fifo` confirmado em **30 segundos** (Config_Aplicada).
- 1 instância ativa de `Consumidor_FIFO`.

**3. Configuração**
- Fluxo: `fifo`, `MessageGroupId` = `grupo-teste7`.
- Quantidade: 3 mensagens, `order_id` = `teste7-fifo-0001` a `teste7-fifo-0003`.
- `Cenario_Falha`: `sucesso` (o negócio conclui; a falha é apenas na exclusão por expiração do prazo).
- `Tempo_Processamento_Simulado`: **45 s** (maior que o Visibility_Timeout de 30 s).

**4. Passo a passo numerado**
1. Confirme, na **Visão geral**, que o `Visibility_Timeout` exibido é **30 s** (Config_Aplicada) e que a extensão de Visibility_Timeout está **desligada**.
2. Na área **Envio de mensagens**, selecione a fila **FIFO**, informe `MessageGroupId` = `grupo-teste7` e envie as 3 mensagens com `Cenario_Falha` = `sucesso` e `Tempo_Processamento_Simulado` = 45 s.
3. No **Monitoramento**, acompanhe a primeira mensagem do grupo entrar em **Em processamento**.
4. Após ~45 s (quando o consumidor tenta o `DeleteMessage` com o `ReceiptHandle` já expirado), observe o estado passar a **Exclusão não confirmada** (badge próprio, distinto de "Processada" e de "Falha").
5. Abra o **Histórico detalhado** da mensagem e verifique o evento `exclusao_nao_confirmada` (Observado), com o erro original da AWS no detalhe, e a **ausência** de `processada`/`excluida` naquela tentativa.
6. Confirme no endpoint `GET /api/consumidores/saude` (ou pela faixa de status) que o `Consumidor_FIFO` continua **vivo** (`"vivo": true`, `"degradado": false`).
7. Na **Visão geral**, confirme o incremento do contador **Exclusão não confirmada** do fluxo FIFO, separado do contador "Com falha".

**5. Resultado esperado**
- A mensagem fica no estado **Exclusão não confirmada** (não terminal), com o evento `exclusao_nao_confirmada` registrado e o erro original preservado.
- **Nenhum** evento `processada` ou `excluida` é gravado para a tentativa cujo `DeleteMessage` falhou.
- O `Consumidor_FIFO` permanece vivo e continua consumindo (a thread não morre).
- O contador "Exclusão não confirmada" do fluxo FIFO incrementa, sem contar como "Com falha".

**6. Evidências (o que capturar)**
- Tela de **Monitoramento** com a mensagem no estado **Exclusão não confirmada**.
- Tela de **Histórico detalhado** com o evento `exclusao_nao_confirmada` (Observado) e o erro original.
- Resposta de `GET /api/consumidores/saude` mostrando o `Consumidor_FIFO` vivo.
- Tela da **Visão geral** com o contador "Exclusão não confirmada" do fluxo FIFO incrementado.

**7. Resultado real** _(a preencher pelo executor após a execução na AWS)_

```
Data/hora da execução:
Estado final observado no Monitoramento:
Evento exclusao_nao_confirmada registrado (origem/erro):
Houve processada/excluida nessa tentativa? (sim/não):
Consumidor_FIFO vivo após a falha? (sim/não):
Contador "Exclusão não confirmada" (antes/depois):
Observações:
```

**8. Critério de aprovação**
Aprovado quando a mensagem atingir o estado **Exclusão não confirmada** com o evento `exclusao_nao_confirmada` Observado (sem `processada`/`excluida`), o `Consumidor_FIFO` permanecer vivo e o contador dedicado incrementar sem ser contado como falha.

---

## Teste 8 — Reentrega sem duplicar operação de negócio

**1. Objetivo**
Validar que, após uma "Exclusão não confirmada", uma reentrega com `ReceiptHandle` novo e válido reprocessa a mensagem e **confirma** a exclusão, sem duplicar a operação de negócio — ou seja, ao final há **exatamente um** `processada`/`excluida` por `order_id`.

**2. Pré-requisitos**
- Pré-requisitos comuns satisfeitos; correção de resiliência implantada.
- Extensão opt-in de Visibility_Timeout **desligada**.
- Para observar a convergência em uma única reentrega, use um `Tempo_Processamento_Simulado` **menor** que o Visibility_Timeout na reentrega (ver Configuração).

**3. Configuração**
- Fluxo: `fifo`, `MessageGroupId` = `grupo-teste8`, `order_id` = `teste8-fifo-0001`.
- 1ª tentativa: `Cenario_Falha` = `sucesso`, `Tempo_Processamento_Simulado` = **45 s** (força a expiração e a "Exclusão não confirmada").
- Reentrega: processamento com `Tempo_Processamento_Simulado` **menor que 30 s** (ex.: 10 s), de modo que o `DeleteMessage` ocorra dentro do prazo e seja confirmado.

**4. Passo a passo numerado**
1. Envie `teste8-fifo-0001` ao grupo `grupo-teste8` com `sucesso` e 45 s, reproduzindo o Teste 7 (a mensagem vai para **Exclusão não confirmada**).
2. Ajuste a Config_Simulacao para `Tempo_Processamento_Simulado` = 10 s (menor que o Visibility_Timeout), de modo que a próxima reentrega conclua a exclusão dentro do prazo.
3. Aguarde a reentrega (após a expiração do Visibility_Timeout) com o `ApproximateReceiveCount` incrementado.
4. No **Monitoramento**, observe o estado transitar de **Exclusão não confirmada** para **Em processamento** e, ao final, **Processada**.
5. Abra o **Histórico detalhado** e verifique a linha do tempo: `exclusao_nao_confirmada` (1ª tentativa) → `recebida`/`processamento_iniciado` (reentrega) → `processada` → `excluida`.
6. Confirme que há **exatamente um** `processada` e **um** `excluida` no total para o `order_id`.

**5. Resultado esperado**
- A reentrega não é bloqueada pela idempotência (não há `processada` prévio), então a mensagem é reprocessada.
- A exclusão é confirmada na reentrega (handle válido), gravando `processada` + `excluida`.
- Ao final, o estado derivado é **Processada** e existe **exatamente um** par `processada`/`excluida` — a operação de negócio não é duplicada em sentido observável.

**6. Evidências (o que capturar)**
- Tela de **Histórico detalhado** com a sequência completa (`exclusao_nao_confirmada` → reentrega → `processada` → `excluida`).
- Tela de **Monitoramento** com o estado final **Processada** e `Tentativas` ≥ 2.
- Contagem evidenciando um único `processada`/`excluida`.

**7. Resultado real** _(a preencher pelo executor após a execução na AWS)_

```
Data/hora da execução:
Tentativas até a confirmação da exclusão:
Sequência de eventos observada:
Nº de processada / nº de excluida no total:
Estado final observado:
Observações:
```

**8. Critério de aprovação**
Aprovado quando, após uma "Exclusão não confirmada", a reentrega concluir com estado final **Processada** e houver **exatamente um** `processada` e **um** `excluida` para o `order_id`.

---

## Teste 9 — Extensão opt-in de Visibility_Timeout (desligada × ligada)

**1. Objetivo**
Demonstrar os dois comportamentos didáticos do cenário de 45 s com Visibility_Timeout de 30 s: com a extensão **desligada** (padrão), a expiração acontece e gera "Exclusão não confirmada"/reentrega; com a extensão **ligada**, o prazo é renovado via `ChangeMessageVisibility` e a mensagem é excluída na primeira tentativa — **sem** alterar os 30 s configurados no Console AWS.

**2. Pré-requisitos**
- Pré-requisitos comuns satisfeitos; correção de resiliência implantada.
- Permissão `sqs:ChangeMessageVisibility` adicionada à IAM Role **apenas** para a parte com a extensão ligada (ver `IMPLANTACAO.md`, Etapa 14.5).
- `Visibility_Timeout` da fila mantido em **30 s** (não alterar no Console).

**3. Configuração**
- Fluxo: `fifo`, `MessageGroupId` = `grupo-teste9`.
- `Cenario_Falha`: `sucesso`, `Tempo_Processamento_Simulado` = **45 s** em ambas as partes.
- Parte A: extensão **desligada** (padrão), `order_id` = `teste9-off-0001`.
- Parte B: extensão **ligada** com valor estendido dentro de `[20, 43200]` s (ex.: 60 s), `order_id` = `teste9-on-0001`.

**4. Passo a passo numerado**
1. **Parte A (desligada):** confirme que a extensão está desligada; envie `teste9-off-0001` com 45 s. Observe no **Monitoramento** o estado chegar a **Exclusão não confirmada** (como no Teste 7) e a mensagem ser reentregue.
2. Confirme no Console AWS (SQS → fila FIFO) que o `Visibility_Timeout` da fila **permanece 30 s**.
3. **Parte B (ligada):** ligue a extensão na Config_Simulacao (ou via `PUT /api/config/simulacao`) com um valor estendido válido; envie `teste9-on-0001` com 45 s.
4. No **Monitoramento**, observe a mensagem permanecer **Em processamento** além dos 30 s (o prazo é renovado em segundo plano) e terminar em **Processada** na **primeira** tentativa, com `excluida` registrado.
5. Abra o **Histórico detalhado** de ambas: a Parte A tem `exclusao_nao_confirmada` seguido de reentrega; a Parte B tem `recebida` → `processamento_iniciado` → `processada` → `excluida` sem expiração.
6. Confirme novamente no Console AWS que o `Visibility_Timeout` da fila continua **30 s** após a Parte B.
7. **Importante:** ao concluir, **desligue** a extensão para preservar o comportamento didático padrão nos demais testes.

**5. Resultado esperado**
- **Parte A:** o cenário de 45 s gera **Exclusão não confirmada** e reentrega (expiração proposital preservada).
- **Parte B:** com a extensão ligada, a mensagem é processada e excluída na primeira tentativa (`Processada`), sem reentrega.
- Em ambas as partes, o `Visibility_Timeout` da fila no Console permanece **30 s** — a extensão age apenas sobre a mensagem recebida.

**6. Evidências (o que capturar)**
- Parte A: **Histórico** com `exclusao_nao_confirmada` + reentrega.
- Parte B: **Histórico** com `processada`/`excluida` na primeira tentativa, sem expiração.
- Capturas do Console AWS mostrando o `Visibility_Timeout` da fila em **30 s** antes e depois.

**7. Resultado real** _(a preencher pelo executor após a execução na AWS)_

```
Data/hora da execução:
Parte A — estado final / houve reentrega? (sim/não):
Parte B — estado final / excluída na 1ª tentativa? (sim/não):
Visibility_Timeout da fila no Console (antes/depois): 30 s / 30 s
Extensão desligada ao final? (sim/não):
Observações:
```

**8. Critério de aprovação**
Aprovado quando a Parte A demonstrar a expiração/reentrega com a extensão desligada e a Parte B demonstrar a exclusão na primeira tentativa com a extensão ligada, **sem** que o `Visibility_Timeout` da fila no Console AWS seja alterado em nenhum momento.

---

## Teste 10 — Recuperação de consumidor encerrado por erro (supervisão por slots)

**1. Objetivo**
Validar que, se a thread de um consumidor encerrar inesperadamente, a supervisão por slots recria **uma** reposição com um novo `consumer_id` (sem duplicar consumidores), e que o Dashboard sinaliza a falha com a faixa amarela enquanto o endpoint de saúde reflete o estado real.

**2. Pré-requisitos**
- Pré-requisitos comuns satisfeitos; correção de resiliência implantada.
- Acesso ao endpoint `GET /api/consumidores/saude` e à faixa de status do Dashboard.

> **Observação:** no uso normal, a guarda por iteração impede que falhas pontuais encerrem a thread; este teste observa o mecanismo de supervisão quando uma thread encerra de fato. A recuperação automatizada é validada pela suíte (`test_main_composicao.py`); este roteiro valida a observabilidade pelo Dashboard/endpoint.

**3. Configuração**
- Fluxo: ambos (standard e fifo).
- Nenhum parâmetro de fila é alterado; nenhum recurso AWS é tocado.

**4. Passo a passo numerado**
1. Com a aplicação em execução, consulte `GET /api/consumidores/saude` e registre o estado inicial (todos `"vivo": true`, `"reinicios": 0`).
2. Observe a faixa de status global do Dashboard em condição normal (sucesso, sem cor de alerta).
3. Acompanhe a aplicação durante a operação com cenários de falha (Testes 2, 3 e 7). Se, por qualquer motivo, uma thread de consumidor encerrar, a thread supervisora recria o slot em até ~`INTERVALO_SUPERVISAO_SEGUNDOS` (5 s) com um novo `consumer_id`.
4. Consulte novamente `GET /api/consumidores/saude`: enquanto o slot está morto, `"algum_morto": true`; após a recriação, o consumidor volta a `"vivo": true` com `"reinicios"` incrementado e um `consumer_id` diferente do anterior.
5. Observe a **faixa amarela** no Dashboard enquanto houver `algum_morto`/`algum_degradado`, com o texto identificando o consumidor afetado.
6. Confirme que **não** há consumidores duplicados: o número de slots por tipo permanece o configurado (não cresce a cada recriação).
7. (Opcional) Se um slot exceder o teto de reinícios na janela, confirme que ele é marcado `"degradado": true` e para de reiniciar, com o log `consumidor_em_falha_persistente` (ERROR) no `journalctl`.

**5. Resultado esperado**
- Uma thread encerrada é reposta por **exatamente uma** nova thread no mesmo slot, com `consumer_id` novo e `reinicios` incrementado — sem duplicar consumidores.
- O endpoint de saúde reflete `vivo`/`degradado`/`reinicios` reais; a faixa amarela aparece enquanto houver consumidor morto/degradado e desaparece quando a saúde normaliza.
- Nenhum recurso AWS é alterado e o encerramento da aplicação (quando ocorrer) é limpo (a supervisora encerra antes dos slots).

**6. Evidências (o que capturar)**
- Respostas de `GET /api/consumidores/saude` antes, durante (slot morto) e depois (reposto) — evidenciando o novo `consumer_id` e `reinicios` incrementado.
- Captura da **faixa amarela** com o texto do consumidor afetado.
- Trecho do `journalctl` com `consumidor_reiniciado` (WARNING) e, se aplicável, `consumidor_em_falha_persistente` (ERROR).

**7. Resultado real** _(a preencher pelo executor após a execução na AWS)_

```
Data/hora da execução:
consumer_id antes / depois da recriação:
reinicios antes / depois:
Faixa amarela observada no Dashboard? (sim/não):
Houve duplicação de consumidores? (sim/não):
Algum slot marcado como degradado? (sim/não):
Observações:
```

**8. Critério de aprovação**
Aprovado quando uma thread encerrada for reposta por **exatamente uma** nova thread (novo `consumer_id`, `reinicios` incrementado), sem duplicar consumidores, com o endpoint de saúde e a faixa amarela refletindo o estado real, e sem alterar nenhum recurso AWS.

---

## Resumo de rastreabilidade

| Teste | Comportamento validado | Endpoints/áreas do Dashboard | Config_Aplicada relevante |
|-------|------------------------|------------------------------|---------------------------|
| 1. Fluxo Standard | Envio → recebimento → exclusão | Envio, Monitoramento, Histórico (`POST /api/mensagens`, `GET /api/monitoramento`, `GET /api/mensagens/{order_id}/historico`) | — |
| 2. Expiração de Visibility_Timeout | Reentrega sem exclusão | Laboratório (`cenario: duracao_superior_visibility`), Monitoramento, Histórico | `Visibility_Timeout` = 30 s |
| 3. Encaminhamento para DLQ | Movimentação para DLQ | Laboratório (`cenario: encaminhar_dlq`), Monitoramento, Histórico | `maxReceiveCount` = 2 |
| 4. Ordenação FIFO | Ordem preservada no grupo | Envio FIFO, comparação por `MessageGroupId`, Histórico | `Deduplicacao_Por_Conteudo` |
| 5. Concorrência (≥ 2) | Um processamento por mensagem | Instâncias (`PUT /api/consumidores/standard/instancias`), Monitoramento, Visão geral | `Visibility_Timeout` = 30 s |
| 6. Recuperação | Reentrega + reprocessamento de DLQ | Laboratório (`cenario: interrupcao`, `POST /api/laboratorio/reprocessar-dlq`), Monitoramento, Histórico | `Visibility_Timeout` = 30 s; `maxReceiveCount` = 2 |
| 7. Exclusão não confirmada | `DeleteMessage` falho vira estado não terminal; consumidor segue vivo | Envio FIFO (45 s), Monitoramento, Histórico, `GET /api/consumidores/saude`, Visão geral | `Visibility_Timeout` = 30 s; extensão opt-in desligada |
| 8. Reentrega sem duplicar | Reentrega confirma exclusão; um único `processada`/`excluida` | Monitoramento, Histórico | `Visibility_Timeout` = 30 s; idempotência por `order_id` |
| 9. Extensão opt-in de Visibility_Timeout | Expiração (desligada) × mitigação (ligada), sem alterar os 30 s da fila | Config_Simulacao / `PUT /api/config/simulacao`, Monitoramento, Histórico, Console AWS | `Visibility_Timeout` = 30 s (inalterado); extensão `[20, 43200]` s |
| 10. Recuperação de consumidor | Supervisão recria slot morto sem duplicar; faixa amarela | `GET /api/consumidores/saude`, faixa de status, `journalctl` | `INTERVALO_SUPERVISAO_SEGUNDOS` = 5; `MAX_REINICIOS_JANELA` = 5 |

> **Lembrete de completude:** este roteiro só é considerado completo se os **dez testes** estiverem presentes e **cada um** contiver os **8 campos obrigatórios** (objetivo, pré-requisitos, configuração, passo a passo numerado, resultado esperado, evidências, resultado real e critério de aprovação). O campo **"Resultado real"** deve ser preenchido pelo executor com os dados efetivamente obtidos na execução contra a AWS — sem valores estimados, inventados ou presumidos.
