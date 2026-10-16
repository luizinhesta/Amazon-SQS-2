# Amazon SQS na prática — Desacoplamento e Processamento Assíncrono

> Laboratório web didático que mostra, de ponta a ponta e sem "mágica", o ciclo de vida das mensagens no **Amazon SQS** — filas **Standard** e **FIFO**, falhas, reentrega, DLQ e reprocessamento. Projeto de estudo para a certificação **AWS Solutions Architect Associate (SAA)**.

![Amazon SQS — Desacoplamento e Processamento Assíncrono](<imagens/imagem (1).png>)

A aplicação roda em uma instância **Amazon EC2** em `us-east-1`, autentica na AWS **só** pela IAM Role da instância (sem Access Keys) e registra cada passo do ciclo de vida das mensagens no **Amazon DynamoDB**. Um Dashboard em português mostra tudo acontecendo em tempo real.

Documentos complementares: [`ARQUITETURA.md`](ARQUITETURA.md) · [`IMPLANTACAO.md`](IMPLANTACAO.md) · [`TESTES.md`](TESTES.md)

---

## O problema que este projeto resolve

O SQS entrega mensagens muito bem, mas tem uma limitação que surpreende quem está começando: **ele não guarda o estado individual de cada mensagem**. Não existe uma API do tipo "me diga onde está a mensagem X e por quais etapas ela passou". Você consegue enviar, receber e excluir — mas a história de cada mensagem se perde.

Esse laboratório preenche essa lacuna: cada ação (envio, recebimento, processamento, falha, exclusão, ida para a DLQ) vira um **evento registrado no DynamoDB**. O estado de cada mensagem é **derivado** desses eventos, nunca inventado. O resultado é uma linha do tempo honesta, que separa o que foi **observado** (um `ReceiveMessage` real) do que foi apenas **inferido** (a provável expiração de um Visibility Timeout, que o SQS não confirma).

## Como funciona, em 4 passos

1. **Envio** — pelo Dashboard, você dispara de 1 a 10 mensagens escolhendo a fila, o `order_id`, o tempo de processamento simulado, um cenário de falha e (no FIFO) o `MessageGroupId`.
2. **Consumo** — os consumidores Standard e FIFO recebem as mensagens com Long Polling, checam idempotência por `order_id`, simulam o processamento e, só em caso de sucesso, excluem a mensagem.
3. **Registro** — cada passo grava um evento no DynamoDB (`enviada`, `recebida`, `processada`, `excluida`, `encaminhada_dlq`, etc.).
4. **Observação** — o Dashboard lê esses eventos, deriva o estado de cada mensagem e mostra estatísticas, linha do tempo e saúde dos consumidores.

## Arquitetura

Tudo roda em uma única EC2: a API (FastAPI), o Dashboard estático e os dois consumidores em threads de segundo plano. A AWS é acessada por `boto3` com credenciais temporárias da IAM Role — nenhuma chave fixa em lugar nenhum. Os recursos AWS são criados **manualmente** no Console (sem IaC); a aplicação só verifica se existem e os usa, nunca cria nem apaga nada.

![Visão geral da arquitetura do laboratório](<imagens/Visão Geral do Laboratório Amazon SQS.png>)

| Componente | Papel |
|---|---|
| **Amazon EC2** (`lab-sqs-ec2`) | Hospeda API, Dashboard e os consumidores Standard/FIFO |
| **Amazon SQS** | 4 filas: `lab-sqs-standard`, `lab-sqs-fifo.fifo` e suas DLQs, com Redrive Policy |
| **Amazon DynamoDB** (`lab-sqs-historico`) | Log de eventos append-only — a fonte de verdade do estado observável |
| **AWS IAM** | Role de privilégio mínimo anexada à EC2 (sem Access Keys) |
| **AWS Systems Manager** | Administração via Session Manager, com a porta 22 fechada |

O estado de cada mensagem (`Aguardando`, `Recebida`, `Em processamento`, `Processada`, `Exclusão não confirmada`, `Falha`, `Aguardando nova tentativa`, `Encaminhada para DLQ`) nunca é um campo gravado: é sempre **derivado** dos eventos por um componente puro (`StateDeriver`). A chave do DynamoDB usa `PK = PEDIDO#<order_id>` e `SK = EVENTO#<tempo>#<seq>` para a linha do tempo, mais um índice `GSI1` por fila para as estatísticas.

<!-- Sugestão de screenshot: tela "Visão geral" do Dashboard (estatísticas Standard x FIFO lado a lado). -->

## Conceitos de SQS que o laboratório demonstra

Em vez de explicar cada conceito em páginas de texto, o lab deixa cada um **visível na prática**. Resumo de como aparecem:

| Conceito | O que é | Como aparece no lab |
|---|---|---|
| **Standard vs FIFO** | Standard = alta vazão, sem ordem, *at-least-once*; FIFO = ordem por grupo, *exactly-once* na janela de dedupe | Duas filas reais, consumidores distintos, estatísticas lado a lado |
| **Visibility Timeout** | Tempo que a mensagem fica invisível após ser recebida | 30 s; o cenário "processamento longo" mostra a reentrega com `ApproximateReceiveCount` subindo |
| **Retenção** | Tempo que o SQS guarda a mensagem não consumida | 4 dias nas filas principais, 14 dias nas DLQs (mais tempo para investigar falhas) |
| **Long Polling** | `ReceiveMessage` espera por mensagens em vez de voltar vazio | 20 s; retorno vazio é tratado como normal, sem erro |
| **MessageGroupId** | Escopo da ordenação no FIFO | Um grupo por consumidor por vez; a ordem dentro do grupo é preservada |
| **Deduplicação** | Evita reprocessar a mesma mensagem (por conteúdo ou por ID) | Dedupe por conteúdo ligada no FIFO; não confundir com a idempotência de negócio por `order_id` |
| **DLQ + Redrive Policy** | Fila para mensagens que falharam N vezes | `maxReceiveCount = 2`; DLQ só é afirmada com presença **confirmada**, nunca por suposição |

Um ponto de rigor do projeto: a **idempotência de negócio** (por `order_id`, guardada no DynamoDB) é diferente da deduplicação de transporte do FIFO (janela de 5 min, dentro do SQS). A idempotência vale inclusive para o Standard, que não tem dedupe nativo.

## Um bug real que corrigimos (e o que ele ensina)

Durante os testes do FIFO apareceu uma falha de verdade: com processamento de **45 s** e Visibility Timeout de **30 s**, o `ReceiptHandle` expirava antes do consumidor chamar `DeleteMessage`. O SQS respondia `InvalidParameterValue` ("receipt handle has expired"), a exceção subia pela thread e **matava o consumidor** — a fila voltava a ter mensagens e nenhum consumidor ativo.

A correção ataca o problema em quatro frentes, sem tocar na AWS e preservando os cenários didáticos:

- **Ordem invertida + novo estado.** O `DeleteMessage` passa a ser tentado **antes** de registrar `processada`. Se o delete falha por erro de infraestrutura, grava-se o estado **Exclusão não confirmada** (não terminal) com o erro original — e **nunca** `excluida` sem confirmação. Na reentrega, a idempotência libera o reprocessamento e o delete é concluído com um handle válido.
- **Supervisão por slots.** Cada consumidor ganhou uma guarda por iteração: uma exceção é logada e o laço continua, em vez de morrer. Uma thread supervisora recria apenas threads mortas (sem duplicar), com teto anti-tempestade.
- **Endpoint de saúde.** `GET /api/consumidores/saude` lê só o estado em memória e informa se cada consumidor está vivo, degradado e quantas vezes reiniciou.
- **Faixas diferenciadas no Dashboard.** A faixa de status distingue, por precedência: API indisponível (vermelha) → consumidor em falha (amarela) → timeout de cálculo (laranja) → sucesso.

A lição vale ouro: **nunca tratar uma chamada que falhou como se tivesse dado certo**, e projetar consumidores que sobrevivem a falhas pontuais de rede ou da AWS.

## O que você aprende com este projeto

- **O SQS não guarda estado por mensagem** — qualquer visão de "onde está cada mensagem" exige um repositório de observabilidade próprio (aqui, o DynamoDB).
- **Observado ≠ inferido** — reentrega é fato (novo `ReceiveMessage`); expiração de Visibility Timeout é dedução. Tratar os dois como iguais produz uma linha do tempo enganosa.
- **Idempotência não é opcional** — a entrega *at-least-once* do Standard torna a idempotência de negócio uma necessidade, não um detalhe.
- **DLQ se confirma, não se adivinha** — o estado "Encaminhada para DLQ" só aparece com presença real na DLQ (`receive_count >= maxReceiveCount`).
- **Resiliência de consumidor é design** — tratar erro de `DeleteMessage`, supervisionar threads e diferenciar tipos de falha são decisões de arquitetura, não remendos.
- **Segurança por padrão** — IAM Role de privilégio mínimo, sem Access Keys, porta 22 fechada (Session Manager), SSE-SQS e login com bloqueio por origem.

<!-- Sugestão de screenshot: tela "Histórico detalhado" do Dashboard (linha do tempo de uma mensagem, observado vs inferido). -->
<!-- Sugestão de screenshot: tela "Laboratório de falhas" do Dashboard (cenários controlados). -->

## Evidências reais

O projeto roda em recursos AWS de verdade. Algumas capturas do Console:

**Instância EC2 em execução** (`lab-sqs-ec2`, t3.micro, com IAM Role anexada):

![EC2 em execução](<imagens/imagem (27).png>)

**Tabela DynamoDB `lab-sqs-historico`** — o log de eventos (`event_source = OBSERVADO`, chave `PEDIDO#...` / `EVENTO#...`) que é a fonte de verdade do estado:

![Itens de evento no DynamoDB](<imagens/imagem (33).png>)

**Monitoramento das DLQs no SQS** (métricas CloudWatch da DLQ Standard e da DLQ FIFO):

![Monitoramento da DLQ Standard](<imagens/imagem (36).png>)

## Tecnologias

**Backend:** Python 3.10+, FastAPI, Uvicorn, boto3. **Frontend:** HTML/CSS/JS puro, organizado por área (`overview`, `send`, `monitoring`, `history`, `lab`). **AWS:** SQS, DynamoDB, EC2, IAM, Systems Manager. **Testes:** pytest, Hypothesis (baseado em propriedade) e httpx. Versões em [`requirements.txt`](requirements.txt) e [`pyproject.toml`](pyproject.toml).

## Como rodar e testar

A implantação completa (criação manual dos recursos no Console AWS + deploy na EC2) está passo a passo em [`IMPLANTACAO.md`](IMPLANTACAO.md). Em resumo, na instância:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

A suíte de testes roda **sem tocar na AWS** (tudo com fakes/mocks):

```bash
python -m pytest
```

Estado atual: **390 testes passando**, incluindo os testes da correção de resiliência FIFO. Detalhes e roteiro de validação em [`TESTES.md`](TESTES.md).

---

Para aprofundar na arquitetura e nas decisões de design, veja [`ARQUITETURA.md`](ARQUITETURA.md). Para o passo a passo de infraestrutura e deploy, [`IMPLANTACAO.md`](IMPLANTACAO.md). Para a estratégia de testes, [`TESTES.md`](TESTES.md).
