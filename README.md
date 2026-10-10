# Laboratório didático de Amazon SQS

> Artigo técnico de portfólio sobre o estudo prático de **Amazon SQS** para a certificação **AWS Solutions Architect Associate (SAA)**.

Este projeto é um laboratório web que demonstra, de forma visual e honesta, o ciclo de vida completo das mensagens em filas **Amazon SQS**: envio, recebimento, processamento, falhas, reprocessamento e encaminhamento para **Dead-Letter Queue (DLQ)** — tanto no fluxo **Standard** quanto no fluxo **FIFO**. A aplicação roda em uma instância **Amazon EC2** Linux na região `us-east-1`, autentica-se na AWS exclusivamente pela IAM Role da instância (sem Access Keys fixas) e registra cada evento no **Amazon DynamoDB**, porque o SQS não oferece uma API para consultar o estado individual de todas as mensagens.

Para os detalhes complementares, consulte os documentos que acompanham este repositório: [`ARQUITETURA.md`](ARQUITETURA.md) (visão aprofundada da arquitetura e dos componentes), [`IMPLANTACAO.md`](IMPLANTACAO.md) (passo a passo de criação manual dos recursos no Console AWS e implantação na EC2) e [`TESTES.md`](TESTES.md) (estratégia e execução da suíte de testes).

---

## 1. Amazon SQS (o que é)

O **Amazon Simple Queue Service (SQS)** é um serviço gerenciado de filas de mensagens que desacopla produtores e consumidores de um sistema distribuído. Em vez de um componente chamar o outro diretamente, o produtor envia uma mensagem para a fila e o consumidor a recebe quando estiver pronto, o que absorve picos de carga, aumenta a resiliência e permite escalar cada lado de forma independente. Por ser totalmente gerenciado, o SQS cuida de disponibilidade, durabilidade e escala, cobrando por requisição e sem exigir gestão de servidores.

Neste laboratório, o SQS é o coração do fluxo: o backend envia mensagens via `boto3` (`SendMessage`), os consumidores as recebem (`ReceiveMessage`), simulam o processamento e, em caso de sucesso, as removem (`DeleteMessage`). O ponto didático central é que o SQS entrega mensagens, mas **não** mantém um estado consultável por mensagem — por isso a aplicação complementa o SQS com um log de eventos próprio no DynamoDB.

## 2. Standard vs FIFO

As filas **Standard** priorizam vazão praticamente ilimitada e entrega *at-least-once*. Em troca, não garantem ordenação e podem entregar a mesma mensagem mais de uma vez, o que torna a **idempotência** no consumidor uma necessidade real, e não um detalhe opcional. É o modelo adequado quando a ordem não importa e o sistema tolera (ou neutraliza) duplicatas.

As filas **FIFO** (First-In-First-Out) garantem ordenação dentro de cada `MessageGroupId` e entrega *exactly-once* dentro da janela de deduplicação, ao custo de uma vazão menor que a Standard. O laboratório materializa essa diferença com duas filas reais — `lab-sqs-standard` e `lab-sqs-fifo.fifo` — cada uma com sua própria DLQ, e com consumidores distintos (`Consumidor_Standard` e `Consumidor_FIFO`). O Dashboard apresenta as estatísticas dos dois fluxos lado a lado, com rótulos separados, para que a comparação entre entrega ordenada e entrega de alta vazão fique evidente.

## 3. Visibility_Timeout

O **Visibility_Timeout** é o período durante o qual uma mensagem recebida fica invisível para os demais consumidores. Enquanto o consumidor processa, a mensagem some da fila para evitar processamento concorrente; se ele concluir e chamar `DeleteMessage` dentro desse prazo, a mensagem desaparece de vez. Se o prazo expirar sem exclusão — porque o processamento demorou demais, falhou ou o consumidor foi interrompido — a mensagem volta a ficar visível e será reentregue, com o `ApproximateReceiveCount` incrementado.

No laboratório o valor inicial é de **30 segundos**, configurado manualmente na fila (Config_Aplicada). O cenário de "processamento mais longo que o Visibility_Timeout" demonstra exatamente a reentrega: a mensagem reaparece e a contagem de recebimentos sobe. Vale destacar uma decisão de design importante: a expiração do Visibility_Timeout em si é tratada como **Evento_Inferido** (`provavel_expiracao_visibility`), pois o SQS não emite confirmação direta dela; já a reentrega subsequente é um **Evento_Observado**, porque corresponde a um novo `ReceiveMessage` concreto.

## 4. Retenção de mensagens

O **período de retenção** define por quanto tempo o SQS guarda uma mensagem não excluída antes de descartá-la automaticamente. É uma salvaguarda contra mensagens que nunca são consumidas e um parâmetro de design relevante para DLQs, onde se quer tempo hábil para investigar e reprocessar falhas. O valor é configurado por fila e, como parâmetro de infraestrutura, só muda no Console AWS.

Neste laboratório as filas principais usam retenção inicial de **4 dias** e as DLQs, de **14 dias** (ambos Config_Aplicada). A retenção mais longa nas DLQs reflete uma boa prática: mensagens que falharam precisam de uma janela maior de inspeção do que mensagens em trânsito no caminho feliz. O Dashboard exibe esses valores com rótulo de Config_Aplicada, deixando claro que são configurações reais dos recursos, não parâmetros de simulação da aplicação.

## 5. Long_Polling

O **Long_Polling** instrui o `ReceiveMessage` a aguardar até `WaitTimeSeconds` por mensagens antes de responder, em vez de retornar imediatamente vazio (short polling). Isso reduz drasticamente a quantidade de chamadas "em branco" ao SQS, diminui custo e latência percebida e evita o desperdício de requisições em filas pouco movimentadas. Quando uma mensagem chega durante a espera, ela é entregue na hora.

Os consumidores do laboratório usam Long_Polling com `WaitTimeSeconds` de **20 segundos** (Config_Aplicada), e tratam o retorno vazio como condição normal: simplesmente tentam de novo, sem registrar erro. Essa escolha é coerente com uma aplicação de laboratório que fica ociosa boa parte do tempo e só recebe tráfego quando o estudante dispara envios ou cenários.

## 6. MessageGroupId

O **MessageGroupId** é um identificador obrigatório em filas FIFO que define o escopo da ordenação. Mensagens com o mesmo `MessageGroupId` são entregues e processadas estritamente na ordem de envio; mensagens de grupos diferentes podem ser processadas em paralelo. Isso permite ordenar o que precisa ser ordenado (por exemplo, todos os eventos de um mesmo pedido) sem serializar o sistema inteiro.

No `Consumidor_FIFO`, a regra adotada é processar **um único MessageGroupId por instância por vez**, preservando a ordem de recebimento dentro do grupo. O SQS FIFO garante que mensagens de um grupo não são entregues a outro consumidor enquanto a anterior não for excluída (ou seu Visibility_Timeout expirar); combinando essa garantia com o processamento single-group por instância, a ordem é mantida ponta a ponta. O Dashboard tem uma visão de comparação por grupos que mostra a sequência de processamento por `MessageGroupId` e qual consumidor atendeu cada mensagem.

## 7. Deduplicação (dedupe por ID e por conteúdo)

Filas FIFO oferecem **deduplicação** dentro de uma janela de 5 minutos, evitando que a mesma mensagem seja processada duas vezes quando reenviada nesse intervalo. Existem duas estratégias: deduplicação **por conteúdo** (*content-based deduplication*), em que o SQS calcula um hash do corpo da mensagem e o usa como chave de dedupe; e deduplicação **por ID**, em que o produtor fornece explicitamente um `MessageDeduplicationId`.

O laboratório usa, por padrão, a **Deduplicacao_Por_Conteudo** nas filas FIFO (Config_Aplicada). Quando ela está desabilitada, a aplicação permite informar manualmente o `MessageDeduplicationId` (de 1 a 128 caracteres) no envio. É importante não confundir essa deduplicação de **transporte** (janela curta, no SQS) com a **idempotência de negócio** por `order_id` (ver seção 11), que a aplicação garante de forma independente e duradoura via DynamoDB — inclusive para o fluxo Standard, que não tem dedupe nativo.

## 8. DLQ (Dead-Letter Queue)

Uma **Dead-Letter Queue** é uma fila secundária para onde o SQS move automaticamente mensagens que falharam em ser processadas um número definido de vezes. A associação é feita pela **Redrive_Policy**, que aponta a fila principal para sua DLQ e define o `maxReceiveCount`: quando o `ApproximateReceiveCount` de uma mensagem atinge esse limite, a mensagem é transferida para a DLQ em vez de ser reentregue indefinidamente. Isso isola o "veneno" (mensagens problemáticas) do fluxo normal e dá um lugar seguro para análise.

Neste laboratório cada fila principal tem sua DLQ (`lab-sqs-standard-dlq` e `lab-sqs-fifo-dlq.fifo`) com `maxReceiveCount` inicial de **2**. Um ponto de rigor que o projeto defende: a aplicação **nunca** afirma que uma mensagem foi para a DLQ só porque o Visibility_Timeout expirou. O estado "Encaminhada para DLQ" só é derivado quando a presença na DLQ é confirmada (Evento_Observado), com `receive_count >= maxReceiveCount`. O cenário de reprocessamento de DLQ move as mensagens de volta à fila principal e exibe o novo processamento, demonstrando o ciclo completo de recuperação.

## 9. Arquitetura da solução

A solução roda inteiramente em uma instância **EC2** Linux em `us-east-1`. O backend em Python expõe a `API_Backend` (FastAPI) e hospeda os processos `Consumidor_Standard` e `Consumidor_FIFO`, todos acessando a AWS por `boto3` com credenciais temporárias resolvidas automaticamente pela **IAM Role** da EC2 (sem Access Keys em código, config ou variáveis de ambiente). O `Dashboard` (HTML/CSS/JS) consome a API e apresenta cinco áreas em pt-BR: visão geral, envio, monitoramento, histórico detalhado e laboratório de falhas.

O estado observável de cada mensagem **não** vem do SQS, e sim de um **log de eventos append-only** na `Tabela_Historico` (DynamoDB). Cada ação do ciclo de vida gera um item de evento; o **Estado** (`Aguardando`, `Recebida`, `Em processamento`, `Processada`, `Falha`, `Aguardando nova tentativa`, `Encaminhada para DLQ`) é **derivado** desses eventos por um componente puro (`StateDeriver`), nunca armazenado como campo mutável. A modelagem usa `PK = PEDIDO#<order_id>` e `SK = EVENTO#<app_time_ms>#<seq>` para a linha do tempo cronológica, e um índice global `GSI1` (`FILA#<queue_name>`) para as consultas por fila que alimentam estatísticas e monitoramento. Todos os recursos AWS são criados **manualmente** pelo usuário no Console (sem IaC); a aplicação apenas verifica a existência e a acessibilidade deles na inicialização e os utiliza — nunca cria, modifica ou exclui recursos. O detalhamento completo, com diagramas, está em [`ARQUITETURA.md`](ARQUITETURA.md).

## 10. Tecnologias utilizadas

O backend é escrito em **Python** (requer 3.10+) e usa **FastAPI** para a API HTTP, **Uvicorn** como servidor ASGI e **boto3** como SDK oficial da AWS para SQS e DynamoDB. O frontend é um Dashboard em **HTML, CSS e JavaScript** puro, organizado por área funcional (`overview`, `send`, `monitoring`, `history`, `lab`), sem framework pesado, mantendo a aplicação simples de servir diretamente da EC2.

Os serviços AWS envolvidos são **Amazon SQS** (filas Standard e FIFO, com DLQs e Redrive_Policy), **Amazon DynamoDB** (log de eventos com GSI), **Amazon EC2** (hospedagem), **AWS IAM** (role de privilégio mínimo anexada à instância) e **AWS Systems Manager Session Manager** (administração sem abrir a porta 22). A suíte de testes usa **pytest**, **Hypothesis** (testes baseados em propriedade) e **httpx** (cliente HTTP para testar a API). As versões exatas estão em [`requirements.txt`](requirements.txt) e em [`pyproject.toml`](pyproject.toml).

## 11. Funcionamento da aplicação

Na inicialização, a aplicação executa uma verificação de recursos: confirma que as quatro filas, a `Tabela_Historico`, a IAM Role e a EC2 existem e estão acessíveis antes de liberar o Dashboard. Se algum recurso estiver ausente ou inacessível, a inicialização é interrompida sem alterar nada na AWS, e o Dashboard mostra em pt-BR qual recurso falhou, por nome e tipo. A partir daí, o usuário envia de 1 a 10 mensagens por uma única ação, escolhendo fila, `order_id`, Tempo_Processamento_Simulado, cenário de falha e — para FIFO — o `MessageGroupId`. Toda entrada é validada antes de qualquer chamada à AWS; cada envio bem-sucedido registra exatamente um Evento_Observado `enviada`.

Os consumidores recebem as mensagens com Long_Polling, registram o recebimento (com `consumer_id` e contagem de tentativas), **verificam a idempotência por `order_id`** consultando o DynamoDB, aguardam o Tempo_Processamento_Simulado, aplicam o cenário de falha e, só em caso de sucesso, excluem a mensagem. Se o `order_id` já tiver sido processado, o consumidor não repete os efeitos colaterais e registra `recebimento_repetido`; se o `order_id` for inválido ou ausente, rejeita e registra `erro_validacao_order_id`, preservando a mensagem. O Dashboard atualiza estatísticas e monitoramento periodicamente, exibe o horário da última atualização bem-sucedida e, se uma atualização falhar, preserva os últimos dados válidos com indicação visível. Exposto à internet, o Dashboard exige login e bloqueia a origem por 300 segundos após 5 tentativas inválidas consecutivas.

## 12. Experimentos realizados

O laboratório de falhas permite executar cenários controlados que cobrem os comportamentos didáticos essenciais do SQS: processamento com sucesso (mensagem `Processada` e excluída); processamento mais longo que o Visibility_Timeout (mensagem volta a ficar visível, contagem de recebimentos incrementada); exceção proposital no consumidor (estado `Falha` com erro registrado); interrupção do consumidor durante o processamento (mensagem preservada e reentregue após o timeout); recebimento repetido (mesma mensagem recebida ao menos duas vezes); encaminhamento para DLQ (após atingir o `maxReceiveCount`); reprocessamento de DLQ (mensagens movidas de volta à fila principal); e comparação lado a lado entre Standard e FIFO.

Cada cenário é observável na prática: o histórico detalhado de uma mensagem mostra sua linha do tempo em ordem cronológica, distinguindo visualmente **Evento_Observado** de **Evento_Inferido**, com horário e fuso. Isso transforma conceitos abstratos (reentrega, redrive, ordenação FIFO, idempotência) em sequências concretas de eventos que o estudante pode inspecionar passo a passo. Os parâmetros de simulação (tempo de processamento, quantidade de mensagens, intervalo de atualização, número de instâncias de consumidor) são ajustáveis pelo Dashboard dentro de faixas validadas, permitindo variar as condições sem tocar no código.

## 13. Resultados

Os resultados aqui descritos são os **efetivamente verificáveis no repositório**, sem presumir métricas de uma execução real na AWS. A principal evidência objetiva é a suíte de testes automatizados: ela cobre a lógica de domínio (derivação de Estado, idempotência, ordenação FIFO, validação de envio e de faixa, classificação de origem de eventos), as camadas de acesso ao SQS e ao DynamoDB, os serviços da API e a composição da aplicação. No ambiente de desenvolvimento, a suíte completa executa com **307 testes passando** (via `python -m pytest`), combinando testes unitários de exemplos e de borda com testes baseados em propriedade (Hypothesis) que exercitam as propriedades de correção descritas no design.

Em termos de estrutura entregue, o backend está organizado em módulos coesos sob `app/` (domínio, acesso, API, consumidores e segurança) e o Dashboard em `web/`, com as cinco áreas funcionais implementadas. O comportamento verificável inclui: estado derivado sempre único e dentro do conjunto permitido; estatísticas sempre inteiras e não-negativas, particionadas corretamente entre Standard e FIFO; DLQ nunca inferida apenas por expiração de Visibility_Timeout; eventos sempre listados em ordem cronológica estável; e processamento idempotente por `order_id`. Os passos para reproduzir a execução dos testes estão em [`TESTES.md`](TESTES.md).

## 14. Boas práticas

Do ponto de vista de segurança, o projeto adota **IAM Role na EC2** com privilégio mínimo e **nenhuma Access Key fixa**, administração via **Session Manager** com a **porta 22 fechada**, acesso ao DynamoDB restrito à aplicação (sem endpoint público), **SSE-SQS** para criptografia em repouso das mensagens e autenticação do Dashboard com bloqueio por origem após tentativas inválidas. Esse conjunto reflete recomendações centrais do SAA para proteger credenciais, reduzir a superfície de ataque e criptografar dados.

Do ponto de vista de arquitetura e resiliência, o laboratório demonstra o uso de **DLQ com Redrive_Policy**, **idempotência no consumidor** para lidar com a entrega *at-least-once* das filas Standard, **Long_Polling** para reduzir chamadas vazias, **retenção** diferenciada entre filas principais e DLQs, e um modelo **event-sourced** que separa fatos observados de inferências. Decisões transversais reforçam a honestidade didática: estado sempre derivado (nunca mutável), distinção explícita entre Config_Aplicada e Config_Simulacao, e a regra de nunca marcar DLQ sem confirmação real.

## 15. Lições aprendidas

A lição mais marcante é que **o SQS não guarda o estado por mensagem**. Qualquer dashboard que pretenda mostrar "onde cada mensagem está" precisa de um repositório de observabilidade próprio — aqui, um log de eventos no DynamoDB com o Estado derivado. Tentar inferir estado só a partir das APIs do SQS leva a conclusões erradas, especialmente em torno da DLQ e da expiração do Visibility_Timeout.

A segunda lição é a importância de separar **o que foi observado do que foi inferido**. Reentrega é observável (novo `ReceiveMessage`); expiração de Visibility_Timeout, não (é deduzida). Tratar as duas como iguais produziria uma linha do tempo enganosa. A terceira é que a entrega *at-least-once* das filas Standard torna a **idempotência de negócio** inegociável — e que ela é um conceito distinto da deduplicação de transporte do FIFO. Por fim, modelar bem a chave do DynamoDB (partição por pedido, ordenação por tempo, GSI por fila) desde o começo foi o que viabilizou, com consultas simples, tanto a linha do tempo individual quanto as estatísticas agregadas por fluxo.

## 16. Conclusão

Este laboratório cumpre seu objetivo de ensinar Amazon SQS na prática para o SAA, cobrindo Standard e FIFO, Visibility_Timeout, retenção, Long_Polling, `MessageGroupId`, deduplicação, DLQ e reprocessamento — tudo observável em uma linha do tempo honesta que separa fatos de inferências. A combinação de SQS, DynamoDB como fonte de verdade do estado observável, IAM Role de privilégio mínimo e um Dashboard em pt-BR entrega uma ferramenta de estudo que torna tangíveis conceitos normalmente abstratos.

Como artefato de portfólio, o projeto evidencia domínio tanto dos conceitos de mensageria quanto de boas práticas de arquitetura e segurança na AWS, sustentado por uma suíte de testes automatizados que valida a lógica central. Para aprofundar, consulte [`ARQUITETURA.md`](ARQUITETURA.md), [`IMPLANTACAO.md`](IMPLANTACAO.md) e [`TESTES.md`](TESTES.md).
