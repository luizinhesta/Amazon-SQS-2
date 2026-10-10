# Roteiro de Testes — Laboratório Amazon SQS (`aws-sqs-lab`)

Este documento descreve o **roteiro dos seis testes de laboratório** que validam, de ponta a ponta, o comportamento do laboratório didático de Amazon SQS sobre serviços reais da AWS na região `us-east-1`. Cada teste é executado a partir do **Dashboard** (pt-BR) servido pela instância EC2 e observado pela área de **Monitoramento**, pelo **Histórico detalhado** de cada mensagem e pela **Visão geral** (estatísticas por fluxo).

Os testes exercitam a aplicação real: envio via `POST /api/mensagens`, cenários controlados via `POST /api/laboratorio/cenario`, reprocessamento via `POST /api/laboratorio/reprocessar-dlq`, estatísticas via `GET /api/monitoramento` e `GET /api/estatisticas`, e a linha do tempo de cada mensagem via `GET /api/mensagens/{order_id}/historico`.

> **Importante sobre os resultados reais:** os campos **"7. Resultado real"** de cada teste estão **em branco, destinados ao preenchimento pelo executor**, pois dependem da execução efetiva contra os recursos reais da AWS. Conforme a política do projeto, nenhum resultado deve ser estimado, inventado ou presumido — registre apenas o que foi efetivamente observado durante a execução.

## Convenções e termos

- **Fluxos:** `standard` (fila `lab-sqs-standard` + DLQ `lab-sqs-standard-dlq`) e `fifo` (fila `lab-sqs-fifo.fifo` + DLQ `lab-sqs-fifo-dlq.fifo`).
- **Config_Aplicada** (configurada manualmente no Console AWS, imutável pelo Dashboard): `Visibility_Timeout` = 30 s; `maxReceiveCount` da Redrive_Policy = 2; retenção 4 dias (filas) / 14 dias (DLQs); `Long_Polling` (`WaitTimeSeconds`) = 20 s; `SSE_SQS` habilitado; `Deduplicacao_Por_Conteudo` habilitada nas filas FIFO.
- **Config_Simulacao** (ajustável no Dashboard): `Tempo_Processamento_Simulado` [0, 300] s (padrão 45 s); quantidade de mensagens [1, 10]; número de instâncias por consumidor [1, 10]; intervalo de atualização [5, 300] s (padrão 10 s; visão geral 1–60 s, padrão 5 s).
- **Cenario_Falha** (valores reais do enum `CenarioFalha` em `app/consumidores/processamento.py`): `sucesso`, `excecao_proposital`, `interrupcao`, `duracao_superior_visibility`, `recebimento_repetido`, `encaminhar_dlq`.
- **Evento_Observado × Evento_Inferido:** eventos confirmados pela AWS (ex.: `enviada`, `recebida`, `excluida`, `encaminhada_dlq`) aparecem como **Observado**; deduções da aplicação (ex.: `provavel_expiracao_visibility`) aparecem como **Inferido**. O Dashboard exibe o indicador por evento sem exigir interação.
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

## Resumo de rastreabilidade

| Teste | Comportamento validado | Endpoints/áreas do Dashboard | Config_Aplicada relevante |
|-------|------------------------|------------------------------|---------------------------|
| 1. Fluxo Standard | Envio → recebimento → exclusão | Envio, Monitoramento, Histórico (`POST /api/mensagens`, `GET /api/monitoramento`, `GET /api/mensagens/{order_id}/historico`) | — |
| 2. Expiração de Visibility_Timeout | Reentrega sem exclusão | Laboratório (`cenario: duracao_superior_visibility`), Monitoramento, Histórico | `Visibility_Timeout` = 30 s |
| 3. Encaminhamento para DLQ | Movimentação para DLQ | Laboratório (`cenario: encaminhar_dlq`), Monitoramento, Histórico | `maxReceiveCount` = 2 |
| 4. Ordenação FIFO | Ordem preservada no grupo | Envio FIFO, comparação por `MessageGroupId`, Histórico | `Deduplicacao_Por_Conteudo` |
| 5. Concorrência (≥ 2) | Um processamento por mensagem | Instâncias (`PUT /api/consumidores/standard/instancias`), Monitoramento, Visão geral | `Visibility_Timeout` = 30 s |
| 6. Recuperação | Reentrega + reprocessamento de DLQ | Laboratório (`cenario: interrupcao`, `POST /api/laboratorio/reprocessar-dlq`), Monitoramento, Histórico | `Visibility_Timeout` = 30 s; `maxReceiveCount` = 2 |

> **Lembrete de completude:** este roteiro só é considerado completo se os **seis testes** estiverem presentes e **cada um** contiver os **8 campos obrigatórios** (objetivo, pré-requisitos, configuração, passo a passo numerado, resultado esperado, evidências, resultado real e critério de aprovação). O campo **"Resultado real"** deve ser preenchido pelo executor com os dados efetivamente obtidos na execução contra a AWS — sem valores estimados, inventados ou presumidos.
