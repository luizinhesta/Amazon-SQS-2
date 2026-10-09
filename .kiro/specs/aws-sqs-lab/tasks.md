# Implementation Plan: Laboratório Amazon SQS (aws-sqs-lab)

## Overview

Este plano converte o design do laboratório didático de Amazon SQS em tarefas de codificação incrementais em **Python** (API_Backend com FastAPI + boto3, consumidores Standard e FIFO) e **HTML/CSS/JS** (Dashboard pt-BR), com histórico no **DynamoDB** (Tabela_Historico).

A ordem segue a lógica de dependências: primeiro a estrutura do projeto e os modelos de evento, depois a lógica pura (StateDeriver, validações, agregadores, idempotência), em seguida as camadas de acesso (SqsGateway, HistoricoRepository), os consumidores, a API_Backend, o Dashboard, a segurança e a observabilidade, e por fim os artefatos de documentação. Cada propriedade de correção do design (1 a 17) vira um teste baseado em propriedades (PBT) com **Hypothesis**, posicionado perto da implementação que valida.

Convenções:
- Testes de propriedade usam no mínimo 100 iterações (`@settings(max_examples=100)`) e são anotados no formato `# Feature: aws-sqs-lab, Property {n}: {texto}`.
- Dependências externas (SQS, DynamoDB) são substituídas por mocks/fakes em memória nos testes de propriedade e unidade.
- Tarefas marcadas com `*` são opcionais (testes) e não precisam ser executadas para um MVP.
- A região é `us-east-1`; a aplicação **não** cria, modifica nem exclui recursos AWS.

## Tasks

- [x] 1. Estruturar o projeto e os contratos base
  - Criar a estrutura de diretórios do backend (`app/`, `app/dominio/`, `app/acesso/`, `app/consumidores/`, `app/api/`, `app/seguranca/`, `tests/`) e do frontend (`web/`)
  - Definir `pyproject.toml`/`requirements.txt` com `fastapi`, `uvicorn`, `boto3`, `hypothesis`, `pytest`, `httpx` e configurar `pytest`
  - Definir constantes do laboratório (nomes das 4 filas, nome da Tabela_Historico, região `us-east-1`, Visibility_Timeout=30, maxReceiveCount=2, Long_Polling=20, retenção 4/14 dias, Tempo_Processamento_Simulado=45)
  - Definir os tipos/enum de `event_type`, `event_source` (OBSERVADO/INFERIDO) e o conjunto de Estados (Aguardando, Recebida, Em processamento, Processada, Falha, Aguardando nova tentativa, Encaminhada para DLQ)
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 2.1, 2.2, 2.3, 2.4, 2.5, 5.4_

- [x] 2. Implementar o modelo de evento e a classificação de origem
  - [x] 2.1 Implementar o modelo de evento da Tabela_Historico
    - Definir a estrutura do item de evento (PK `PEDIDO#<order_id>`, SK `EVENTO#<app_time_ms>#<seq>`, `order_id`, `queue_name`, `event_type`, `event_source`, `app_time`, `app_time_ms`, `consumer_id`, `receive_count`, `processing_seconds`, `message_group_id`, `sqs_message_id`, `error_detail`, `GSI1PK`, `GSI1SK`)
    - Implementar a construção das chaves (PK/SK/GSI1PK/GSI1SK) e a serialização/desserialização do item
    - _Requirements: 11.1, 11.3, 11.4, 11.6, 6.1_
  - [x] 2.2 Implementar o classificador de origem do evento (Evento_Observado vs Evento_Inferido)
    - Mapear cada `event_type` à sua origem obrigatória (ex.: `provavel_expiracao_visibility` → INFERIDO; `encaminhada_dlq` → OBSERVADO)
    - _Requirements: 6.4, 6.5, 11.9_
  - [ ]* 2.3 Escrever teste de propriedade para a classificação de origem
    - **Property 7: Classificação de origem de cada evento é válida e consistente**
    - **Validates: Requirements 6.4, 6.5, 11.9**

- [x] 3. Implementar validações de entrada (faixas e envio)
  - [x] 3.1 Implementar o validador de faixa genérico para Config_Simulacao, instâncias e intervalo de atualização
    - Aceitar valor se e somente se `min <= valor <= max`; ao rejeitar, preservar o valor anterior e produzir mensagem em pt-BR com a faixa válida
    - Aplicar às faixas: Tempo_Processamento_Simulado [0,300], quantidade [1,10], instâncias [1,10], intervalo de atualização [5,300]
    - _Requirements: 2.8, 2.9, 2.11, 2.12, 10.3, 13.2, 13.3_
  - [ ]* 3.2 Escrever teste de propriedade para a validação de faixa
    - **Property 1: Validação de faixa aceita se e só se dentro dos limites**
    - **Validates: Requirements 2.12, 10.3, 13.3**
  - [x] 3.3 Implementar o validador de payload de envio de mensagens
    - Validar quantidade [1,10], `order_id` [1,80] caracteres, Tempo_Processamento_Simulado [0,300] e, para FIFO, `MessageGroupId` [1,128]; ao rejeitar, identificar ao menos um campo inválido e preservar os valores informados (mensagem em pt-BR)
    - _Requirements: 4.2, 4.3, 4.4, 4.6, 4.7, 4.9_
  - [ ]* 3.4 Escrever teste de propriedade para a validação de envio
    - **Property 8: Validação de envio aceita se e só se todos os campos são válidos**
    - **Validates: Requirements 4.9**

- [x] 4. Implementar o derivador de Estado (StateDeriver)
  - [x] 4.1 Implementar o StateDeriver como função pura
    - Receber a lista de eventos de uma mensagem e retornar exatamente um Estado do conjunto permitido, seguindo a máquina de estados do design (incluindo precedência dos estados terminais Processada e Encaminhada para DLQ)
    - Garantir que `Encaminhada para DLQ` só seja derivado com `encaminhada_dlq` confirmado (OBSERVADO) e `receive_count >= maxReceiveCount`, nunca por expiração de Visibility_Timeout
    - _Requirements: 5.4, 5.9, 5.10, 6.6, 7.9, 7.10_
  - [ ]* 4.2 Escrever teste de propriedade para a unicidade do Estado derivado
    - **Property 4: Estado derivado é único e pertence ao conjunto permitido**
    - **Validates: Requirements 5.4, 5.9**
  - [ ]* 4.3 Escrever teste de propriedade para a regra de não inferência de DLQ
    - **Property 5: DLQ nunca é inferida apenas por expiração do Visibility_Timeout**
    - **Validates: Requirements 7.9, 7.10, 6.6**
  - [ ]* 4.4 Escrever testes unitários do StateDeriver
    - Casos concretos para cada estado intermediário e terminal
    - _Requirements: 5.4_

- [x] 5. Implementar a listagem cronológica e o agregador de estatísticas
  - [x] 5.1 Implementar a ordenação cronológica da listagem de eventos
    - Ordenar por `app_time_ms` de forma não-decrescente, usando `seq` (ordem de registro) como desempate estável
    - _Requirements: 6.1, 11.6_
  - [ ]* 5.2 Escrever teste de propriedade para a ordenação cronológica
    - **Property 6: Eventos são listados em ordem cronológica crescente com desempate estável**
    - **Validates: Requirements 6.1, 11.6**
  - [x] 5.3 Implementar o agregador de estatísticas por fluxo (Standard/FIFO)
    - Calcular, a partir do log de eventos (via StateDeriver), os contadores de enviadas, disponíveis, em processamento, processadas, com falha, encaminhadas para DLQ e recebidas por consumidor, além da contagem de tentativas por mensagem; todos inteiros ≥ 0 e separados por fluxo
    - Garantir que o log vazio produza 0 em todos os contadores
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.12, 5.2, 11.7_
  - [ ]* 5.4 Escrever teste de propriedade para não-negatividade das estatísticas
    - **Property 2: Estatísticas derivadas são inteiros não-negativos**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.12, 5.2, 11.7**
  - [ ]* 5.5 Escrever teste de propriedade para o particionamento por fluxo
    - **Property 3: Estatísticas particionam corretamente por fluxo**
    - **Validates: Requirements 3.8**

- [x] 6. Checkpoint — lógica de domínio pura validada
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Implementar a camada de acesso ao DynamoDB (HistoricoRepository)
  - [x] 7.1 Implementar `registrar_evento` com retry de até 3 tentativas
    - Executar PutItem com no máximo 3 tentativas em falhas transitórias; ao esgotar, preservar a mensagem original e sinalizar o erro sem exceder 3 tentativas; registrar `app_time` com precisão de milissegundos
    - _Requirements: 8.3, 11.1, 11.2_
  - [ ]* 7.2 Escrever teste de propriedade para o retry de escrita
    - **Property 16: Escrita de evento tenta no máximo 3 vezes e preserva a mensagem no esgotamento**
    - **Validates: Requirements 11.2**
  - [x] 7.3 Implementar as consultas do repositório (por pedido, por fila, cronológica, contagem de tentativas)
    - Implementar `consultar_por_pedido`, `consultar_por_fila` (via GSI1), `listar_eventos_cronologico` e `contar_tentativas`; retornar resultado vazio sem erro quando não houver registros
    - _Requirements: 11.3, 11.4, 11.5, 11.6, 11.7, 11.8_
  - [ ]* 7.4 Escrever teste de propriedade para o round-trip de registro e consulta
    - **Property 15: Round-trip de registro e consulta preserva e filtra os eventos**
    - **Validates: Requirements 11.3, 11.4, 11.5**
  - [ ]* 7.5 Escrever testes unitários do repositório com fake DynamoDB em memória
    - Consultas sem correspondência retornando vazio e precisão de milissegundos no `app_time`
    - _Requirements: 11.1, 11.5_

- [x] 8. Implementar a camada de acesso ao SQS (SqsGateway)
  - [x] 8.1 Implementar o SqsGateway sobre boto3
    - Expor `send_message`, `receive_messages` (Long_Polling WaitTimeSeconds configurável 1–20), `delete_message`, `get_queue_attributes` e `redrive_from_dlq`; ler `ApproximateReceiveCount`; repassar sempre o erro original da AWS
    - Resolver credenciais exclusivamente via IAM_Role_EC2 (sem Access Keys) usando o provedor padrão do boto3
    - _Requirements: 4.10, 7.7, 8.1, 8.2, 8.10, 8.11, 12.1, 12.2, 14.5, 14.6_
  - [ ]* 8.2 Escrever testes unitários do SqsGateway com mock de boto3
    - Verificar passagem de parâmetros (WaitTimeSeconds, MessageGroupId) e preservação do erro original
    - _Requirements: 4.10, 8.1, 8.10_

- [x] 9. Implementar a verificação de inicialização dos recursos AWS
  - [x] 9.1 Implementar a verificação de existência e acessibilidade dos recursos
    - Verificar as 4 filas, a Tabela_Historico e a acessibilidade via IAM Role antes de disponibilizar o Dashboard; ao faltar/erro, registrar o erro original, interromper sem alterar recursos e expor o recurso indisponível por nome e tipo (pt-BR); nunca criar/modificar/excluir recursos
    - _Requirements: 1.5, 1.6, 1.7, 1.8, 1.9_
  - [ ]* 9.2 Escrever teste de integração da verificação de inicialização
    - Recursos presentes e ausentes; confirmação de SSE_SQS via `get_queue_attributes`
    - _Requirements: 1.6, 1.8, 12.8_

- [x] 10. Implementar a lógica de idempotência e processamento do consumidor
  - [x] 10.1 Implementar a verificação de idempotência por `order_id`
    - Consultar a Tabela_Historico por `order_id`; se já houver `processada`, não reexecutar efeitos e registrar `recebimento_repetido` (OBSERVADO) com `order_id` e instante; garantir exatamente um processamento por `order_id`
    - Se `order_id` ausente/inválido, rejeitar, registrar `erro_validacao_order_id` e preservar a mensagem; se a Tabela_Historico falhar, não executar efeitos, preservar a mensagem e registrar `erro_acesso_historico`
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6_
  - [ ]* 10.2 Escrever teste de propriedade para a idempotência
    - **Property 11: Processamento é idempotente por identificador de pedido**
    - **Validates: Requirements 9.1, 9.4, 9.6**
  - [ ]* 10.3 Escrever teste de propriedade para rejeição de `order_id` inválido
    - **Property 12: Identificador de pedido inválido rejeita o processamento**
    - **Validates: Requirements 9.3**
  - [x] 10.4 Implementar a lógica de processamento e exclusão sob Cenario_Falha
    - Registrar `recebida` (consumidor + tentativas), aguardar Tempo_Processamento_Simulado, aplicar o Cenario_Falha e executar DeleteMessage se e somente se o processamento concluir com sucesso; em qualquer outro desfecho preservar a mensagem e registrar o erro original
    - _Requirements: 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9_
  - [ ]* 10.5 Escrever teste de propriedade para exclusão condicionada ao sucesso
    - **Property 13: Mensagem é excluída se e só se o processamento concluir com sucesso**
    - **Validates: Requirements 8.6, 8.7, 8.9**

- [x] 11. Checkpoint — camadas de acesso e processamento validadas
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 12. Implementar o Consumidor_Standard
  - [-] 12.1 Implementar o loop do Consumidor_Standard
    - Receber da Fila_Principal_Standard em lotes de 1–10 com Long_Polling (WaitTimeSeconds=20), aplicar idempotência e processamento, atribuir `consumer_id` único, registrar recebimento e repetir sem erro quando o recebimento vier vazio
    - _Requirements: 8.1, 8.11, 10.4, 14.5_
  - [ ]* 12.2 Escrever testes unitários do Consumidor_Standard com SQS/DynamoDB simulados
    - Fluxo feliz, falha e recebimento vazio
    - _Requirements: 8.1, 8.11_

- [ ] 13. Implementar o Consumidor_FIFO
  - [-] 13.1 Implementar o loop do Consumidor_FIFO com um único MessageGroupId por instância por vez
    - Receber da Fila_Principal_FIFO em lotes de 1–10 com Long_Polling (WaitTimeSeconds=20), processar um único `MessageGroupId` por vez preservando a ordem de recebimento dentro do grupo e aplicar idempotência/processamento
    - Suportar envio com Deduplicacao_Por_Conteudo e, quando desabilitada, `MessageDeduplicationId` informado
    - _Requirements: 8.2, 10.5, 10.6, 14.6, 4.6, 4.7_
  - [ ]* 13.2 Escrever teste de propriedade para a preservação da ordem FIFO
    - **Property 14: Ordenação FIFO é preservada dentro de cada MessageGroupId**
    - **Validates: Requirements 10.5, 10.6**

- [x] 14. Implementar a segurança (autenticação do Dashboard e acesso)
  - [x] 14.1 Implementar o controle de autenticação com bloqueio por origem
    - Exigir usuário/senha quando exposto à internet; bloquear a origem após 5 falhas consecutivas por 300 segundos; um sucesso antes do limite reinicia a contagem; restringir origens às autorizadas
    - Tratar falha de autorização via IAM_Role_EC2 interrompendo a operação, registrando o evento e expondo erro sem persistir dados parciais
    - _Requirements: 12.5, 12.6, 12.7, 12.9_
  - [ ]* 14.2 Escrever teste de propriedade para o bloqueio de autenticação
    - **Property 17: Bloqueio de autenticação ativa após 5 falhas consecutivas**
    - **Validates: Requirements 12.6**

- [ ] 15. Implementar os endpoints da API_Backend (FastAPI)
  - [~] 15.1 Implementar os endpoints de configuração e saúde
    - `GET /api/saude` (verificação de inicialização), `GET /api/config` (Config_Aplicada e Config_Simulacao com rótulos distintos), `PUT /api/config/simulacao` (ajuste dentro das faixas); orientar em pt-BR que Config_Aplicada só muda no Console AWS
    - _Requirements: 1.6, 1.8, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.10, 2.11, 2.12, 2.13_
  - [~] 15.2 Implementar o endpoint de envio de mensagens
    - `POST /api/mensagens`: validar o payload, enviar 1–10 mensagens via SqsGateway e registrar exatamente um Evento_Observado `enviada` por mensagem; em falha parcial, preservar os envios anteriores e o erro original (pt-BR)
    - _Requirements: 4.1, 4.5, 4.8, 4.10, 4.11, 4.12_
  - [ ]* 15.3 Escrever teste de propriedade para o registro de envio bem-sucedido
    - **Property 9: Envio bem-sucedido registra exatamente um Evento_Observado "enviada" por mensagem**
    - **Validates: Requirements 4.11**
  - [ ]* 15.4 Escrever teste de propriedade para falha parcial de envio
    - **Property 10: Falha parcial de envio preserva envios anteriores e o erro original**
    - **Validates: Requirements 4.12**
  - [~] 15.5 Implementar os endpoints de estatísticas e monitoramento com timeout
    - `GET /api/estatisticas` e `GET /api/monitoramento` usando agregador e StateDeriver; aplicar timeout de 3 s (p95 sob 10 requisições simultâneas) retornando indicação de timeout ao exceder
    - _Requirements: 3.1-3.8, 3.12, 5.1, 5.2, 5.3, 5.4, 5.5, 5.9, 5.10, 14.1, 14.2, 14.3, 14.4_
  - [~] 15.6 Implementar os endpoints de histórico, laboratório e consumidores
    - `GET /api/mensagens/{order_id}/historico` (linha do tempo), `POST /api/laboratorio/cenario`, `POST /api/laboratorio/reprocessar-dlq`, `PUT /api/consumidores/{tipo}/instancias` (1–10) e `POST /api/autenticacao`; interromper cenário com causa em pt-BR quando fila indisponível/autenticação falhar
    - _Requirements: 6.1, 6.2, 6.3, 6.7, 6.8, 7.1-7.11, 10.1, 10.2, 10.3, 10.7, 12.5, 12.6_
  - [ ]* 15.7 Escrever testes de integração dos endpoints da API
    - Fluxo ponta a ponta Standard (envio→recebimento→exclusão), reentrega por Visibility_Timeout, encaminhamento para DLQ, ordenação FIFO, concorrência (≥2 consumidores) e recuperação
    - _Requirements: 18.1, 18.2, 18.3, 18.4, 18.5, 18.6_

- [~] 16. Checkpoint — API e consumidores integrados
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 17. Implementar o Dashboard (HTML/CSS/JS, pt-BR)
  - [~] 17.1 Implementar a estrutura do Dashboard e a tela de login
    - Montar as cinco áreas (visão geral, envio, monitoramento, histórico detalhado, laboratório de falhas) em pt-BR e a tela de login com indicação de bloqueio
    - _Requirements: 12.5, 12.6_
  - [~] 17.2 Implementar a visão geral e a configuração
    - Exibir estatísticas por fluxo (Standard/FIFO), horário da última atualização bem-sucedida (data/hora/fuso) e os parâmetros com rótulo Config_Aplicada/Config_Simulacao; preservar últimos valores e indicar falha de atualização
    - _Requirements: 2.1-2.13, 3.1-3.12_
  - [~] 17.3 Implementar o envio de mensagens e o monitoramento
    - Formulário de envio (fila, quantidade, order_id, tempo, cenário, MessageGroupId/MessageDeduplicationId para FIFO) com validação em pt-BR; tabela de monitoramento (ID, Fila, Consumidor, Tentativas, Processamento, Estado, Último evento, Ações) e estados vazios/erro
    - _Requirements: 4.1-4.9, 5.1-5.8_
  - [~] 17.4 Implementar o histórico detalhado e o laboratório de falhas
    - Linha do tempo cronológica com indicador Observado/Inferido por evento (sem interação), horário com fuso; painel de cenários de falha e comparação Standard×FIFO e por MessageGroupId
    - _Requirements: 6.1-6.8, 7.1-7.8, 10.7_
  - [~] 17.5 Implementar a atualização periódica e a observabilidade
    - Atualizar estatísticas e monitoramento ao final de cada intervalo configurável (5–300 s, padrão 10 s; visão geral 1–60 s, padrão 5 s) sem recarregar; exibir horário da atualização e indicar dados possivelmente desatualizados em falha
    - _Requirements: 3.9, 3.10, 3.11, 13.1, 13.2, 13.3, 13.4, 13.5_

- [ ] 18. Integração e wiring final da aplicação
  - [~] 18.1 Compor a aplicação na EC2 (API + consumidores + Dashboard)
    - Inicializar a API_Backend servindo o Dashboard, iniciar os processos Consumidor_Standard e Consumidor_FIFO e condicionar a disponibilização do Dashboard à verificação de inicialização bem-sucedida
    - _Requirements: 1.6, 8.1, 8.2, 10.1, 10.2_
  - [ ]* 18.2 Escrever teste de smoke de segurança (ausência de Access Keys)
    - Verificar ausência de `aws_access_key_id`/`aws_secret_access_key` em código, configuração e variáveis de ambiente e autenticação via IAM Role contra SQS/DynamoDB
    - _Requirements: 12.1, 12.2, 12.3_

- [~] 19. Checkpoint — aplicação completa validada
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 20. Produzir os artefatos de documentação do projeto
  - [~] 20.1 Escrever `ARQUITETURA.md`
    - Incluir visão geral, serviços AWS e suas funções, fluxos Standard/FIFO/falhas/DLQ, integração DynamoDB e Dashboard, lista de permissões IAM, ao menos um diagrama Mermaid de todos os fluxos e ao menos um diagrama com ícones oficiais AWS
    - _Requirements: 15.1, 15.2, 17.1, 17.6_
  - [~] 20.2 Escrever `IMPLANTACAO.md`
    - Passo a passo numerado em pt-BR para criar manualmente as 4 filas, a Tabela_Historico, a IAM_Role_EC2, a EC2 e os Security Groups, configurar o Session_Manager (porta 22 fechada), instalar/executar a aplicação (explicando cada comando), verificar pós-implantação, excluir recursos ao final e tratar falhas de etapa; incluir custo mensal estimado em USD e lista de exclusão
    - _Requirements: 15.3, 15.4, 15.5, 15.7, 16.1, 16.2, 16.3, 16.4, 16.5, 16.6, 16.7, 16.8, 16.9, 16.10, 17.2, 17.6_
  - [~] 20.3 Escrever `README.md` (artigo técnico de portfólio)
    - Incluir, cada seção com ao menos um parágrafo: SQS, Standard vs FIFO, Visibility_Timeout, retenção, Long_Polling, MessageGroupId, deduplicação, DLQ, arquitetura, tecnologias, funcionamento, experimentos, resultados, boas práticas, lições aprendidas e conclusão; apenas resultados efetivamente obtidos
    - _Requirements: 17.3, 17.5, 17.6_
  - [~] 20.4 Escrever `TESTES.md`
    - Roteiro dos seis testes (Standard; expiração de Visibility_Timeout; encaminhamento para DLQ; ordenação FIFO; concorrência ≥2; recuperação), cada um com os 8 campos obrigatórios (objetivo, pré-requisitos, configuração, passo a passo numerado, resultado esperado, evidências, resultado real e critério de aprovação)
    - _Requirements: 17.4, 17.5, 17.6, 18.1, 18.2, 18.3, 18.4, 18.5, 18.6, 18.7, 18.8_

- [~] 21. Checkpoint final — artefatos e testes completos
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tarefas marcadas com `*` são opcionais (testes) e podem ser puladas para um MVP mais rápido.
- Cada tarefa referencia os critérios de aceitação específicos para rastreabilidade.
- As 17 propriedades de correção do design viram testes PBT com Hypothesis, posicionados perto da implementação que validam.
- Os checkpoints garantem validação incremental ao longo do desenvolvimento.
- A aplicação nunca cria, modifica ou exclui recursos AWS; a infraestrutura é criada manualmente conforme `IMPLANTACAO.md`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "2.2", "3.1", "3.3", "4.1", "14.1"] },
    { "id": 2, "tasks": ["2.3", "3.2", "3.4", "4.2", "4.3", "4.4", "5.1", "5.3", "7.1", "7.3", "8.1", "14.2"] },
    { "id": 3, "tasks": ["5.2", "5.4", "5.5", "7.2", "7.4", "7.5", "8.2", "9.1", "10.1", "10.4"] },
    { "id": 4, "tasks": ["9.2", "10.2", "10.3", "10.5", "12.1", "13.1"] },
    { "id": 5, "tasks": ["12.2", "13.2", "15.1", "15.2", "15.5", "15.6"] },
    { "id": 6, "tasks": ["15.3", "15.4", "15.7", "17.1"] },
    { "id": 7, "tasks": ["17.2", "17.3", "17.4", "17.5"] },
    { "id": 8, "tasks": ["18.1"] },
    { "id": 9, "tasks": ["18.2", "20.1", "20.2", "20.3", "20.4"] }
  ]
}
```
