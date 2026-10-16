# Implementation Plan: AWS SNS Delivery Lab (aws-sns-delivery-lab)

## Overview

Este plano converte o design do laboratório didático de Amazon SNS em tarefas de codificação incrementais em **Python** (API_Backend com FastAPI + boto3, Verificador_Entrega, gateways de acesso) e **HTML/CSS/JS** (Dashboard pt-BR), reaproveitando a organização de camadas da aplicação existente em `app/` (`acesso`, `api`, `dominio`, `observabilidade`, `seguranca`).

A ordem segue a lógica de dependências: primeiro a estrutura e os contratos base (constantes, tipos, envelope de mensagem, modelo de estados), depois a lógica pura (derivação de estado de entrega, correlação por ID_Pedido, validações, agregador de estatísticas, apresentação de ordem FIFO, round-trip de Access Policy, bloqueio de autenticação), em seguida as camadas de acesso (SnsGateway, SqsLeituraGateway), os componentes de orquestração (Verificador_Entrega, Gerenciador_Falha_Controlada, Verificacao_Inicializacao), a API_Backend, o Dashboard, o wiring final e, por fim, os artefatos de documentação. Cada propriedade de correção do design (1 a 11) vira um teste baseado em propriedades (PBT) com **Hypothesis**, posicionado perto da implementação que valida.

Convenções:
- Testes de propriedade usam no mínimo 100 iterações (`@settings(max_examples=100)`) e são anotados no formato `# Feature: aws-sns-delivery-lab, Property {n}: {texto}`.
- Dependências externas (SNS, SQS) são substituídas por mocks/fakes em memória nos testes de propriedade e de unidade.
- Tarefas marcadas com `*` são opcionais (testes) e não precisam ser executadas para um MVP.
- A região é `us-east-1`; a aplicação **não** cria, modifica nem exclui recursos AWS, exceto o ajuste temporário e reversível de Access_Policy_Fila do cenário de Falha_Entrega_Controlada.
- As tarefas de infraestrutura são de **documentação** do passo a passo manual (`IMPLANTACAO.md`) e de **verificação de inicialização** por código, nunca de provisionamento automático.

## Tasks

- [x] 1. Estruturar o projeto e os contratos base
  - Confirmar/estender a estrutura de diretórios do backend (`app/dominio/`, `app/acesso/`, `app/api/`, `app/seguranca/`, `app/observabilidade/`, `tests/`) e do frontend (`web/`)
  - Garantir dependências em `requirements.txt`/`pyproject.toml`: `fastapi`, `uvicorn`, `boto3`, `hypothesis`, `pytest`, `httpx` e configurar `pytest`
  - Definir as constantes do laboratório (região `us-east-1`; nomes dos dois tópicos; nomes das seis filas de destino; `delivery-sns-dlq-standard` e `delivery-sns-dlq.fifo`; Deduplicacao_Por_Conteudo do Topico_FIFO habilitada; Timeout_Verificacao_Entrega padrão 30s faixa 5–120s; Timeout_Verificacao_DLQ padrão 120s faixa 30–300s; intervalo de atualização padrão 10s faixa 5–300s; Long Polling 1–20s; os três destinos e o texto visual "Seu pedido foi aprovado e está sendo preparado")
  - Definir os tipos/enum de cenário (`standard`/`fifo`), de estado de entrega (`Entrega_Publicada`, `Entrega_Confirmada`, `Não confirmado`) e de origem de evento (`Evento_Observado`/`Evento_Inferido`)
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 2.3, 2.5_

- [x] 2. Implementar o envelope de mensagem e o modelo de estados da entrega
  - [x] 2.1 Implementar o envelope de mensagem e a extração de ID_Pedido
    - Definir a estrutura do payload de aplicação (`id_pedido`, `cenario`, `destino`, `titulo_destino`, `conteudo`, `app_time`, `sequencia`) e dos `MessageAttributes` (`id_pedido`, `cenario`)
    - Implementar a extração do ID_Pedido a partir do envelope SNS recebido na fila (campo `Message` e `MessageAttributes`)
    - _Requirements: 6.1, 6.3, 10.1_
  - [x] 2.2 Implementar o derivador de estado de entrega como função pura
    - Receber as observações de uma mensagem e derivar `Entrega_Confirmada` se e somente se houver leitura na fila de destino correlacionada pelo ID_Pedido; nunca derivar `Entrega_Confirmada` apenas da aceitação da publicação pelo SNS
    - Derivar `Não confirmado` quando o Timeout_Verificacao for atingido sem leitura correlacionada, nunca "entregue"
    - _Requirements: 6.1, 6.2, 6.6, 4.3_
  - [ ]* 2.3 Escrever teste de propriedade para a exigência de leitura real
    - **Property 1: Entrega_Confirmada exige leitura real correlacionada**
    - **Validates: Requirements 6.1, 6.2, 4.3**
  - [ ]* 2.4 Escrever teste de propriedade para o timeout "Não confirmado"
    - **Property 4: Timeout produz "Não confirmado", nunca "entregue"**
    - **Validates: Requirements 6.6, 7.9, 8.7**

- [x] 3. Implementar as validações de entrada (disparo e faixa)
  - [x] 3.1 Implementar o validador de payload de disparo (Standard e FIFO)
    - Aceitar se e somente se o ID_Pedido tiver 1–80 caracteres e, no FIFO, o MessageGroupId tiver 1–128 caracteres; rejeitar sempre publicação FIFO sem MessageGroupId; ao rejeitar, identificar ao menos um campo inválido e preservar os valores informados (mensagem em pt-BR)
    - _Requirements: 4.1, 4.7, 5.1, 5.2, 5.9, 11.5_
  - [ ]* 3.2 Escrever teste de propriedade para a validação de disparo
    - **Property 6: Validação de disparo aceita se e só se os campos são válidos**
    - **Validates: Requirements 4.1, 4.7, 5.1, 5.2, 5.9, 11.5**
  - [x] 3.3 Implementar o validador de faixa genérico para Config_Simulacao e intervalo de atualização
    - Aceitar valor se e somente se `min <= valor <= max`; ao rejeitar, preservar o valor anterior e produzir mensagem em pt-BR com a faixa válida; aplicar às faixas Timeout_Verificacao_Entrega [5,120], Timeout_Verificacao_DLQ [30,300] e intervalo de atualização [5,300]
    - _Requirements: 2.5, 2.8, 13.3_
  - [ ]* 3.4 Escrever teste de propriedade para a validação de faixa
    - **Property 7: Validação de faixa aceita se e só se dentro dos limites**
    - **Validates: Requirements 2.5, 2.8, 13.3**

- [x] 4. Implementar o agregador de estatísticas e a apresentação de ordem FIFO
  - [x] 4.1 Implementar o agregador de estatísticas por cenário (Standard/FIFO)
    - Calcular, a partir das observações, os contadores de publicadas por tópico, Entrega_Confirmada por fila de destino e presentes em cada DLQ; todos inteiros ≥ 0; ausência de dados produz 0; particionar por cenário sem contabilizar um cenário no outro; manter o horário da última atualização bem-sucedida (data, hora com segundos e fuso)
    - _Requirements: 3.4, 3.5, 3.6, 3.7, 3.8, 3.10_
  - [ ]* 4.2 Escrever teste de propriedade para não-negatividade das estatísticas
    - **Property 8: Estatísticas derivadas são inteiros não-negativos**
    - **Validates: Requirements 3.4, 3.5, 3.6, 3.10**
  - [ ]* 4.3 Escrever teste de propriedade para o particionamento por cenário
    - **Property 9: Estatísticas particionam corretamente por cenário**
    - **Validates: Requirements 3.7**
  - [x] 4.4 Implementar a apresentação da ordem de recebimento FIFO
    - Apresentar a ordem de recebimento exatamente igual à ordem em que as mensagens foram lidas, sem reconstruir nem reordenar, agrupando por MessageGroupId
    - _Requirements: 11.1, 11.2_
  - [ ]* 4.5 Escrever teste de propriedade para a apresentação de ordem FIFO
    - **Property 10: Ordem FIFO é apresentada sem reordenar**
    - **Validates: Requirements 11.1, 11.2**

- [x] 5. Checkpoint — lógica de domínio pura validada
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Implementar a camada de acesso ao SNS (SnsGateway)
  - [x] 6.1 Implementar o SnsGateway sobre boto3
    - Expor `publicar_standard`, `publicar_fifo` (sempre com MessageGroupId; MessageDeduplicationId apenas quando a Deduplicacao_Por_Conteudo estiver desabilitada) e `obter_assinaturas`; rejeitar publicação FIFO sem MessageGroupId antes da chamada; retornar o MessageId e repassar sempre o erro original da AWS sem mascarar
    - Resolver credenciais exclusivamente via IAM_Role_EC2 (sem Access Keys) usando o provedor padrão do boto3
    - _Requirements: 4.2, 4.8, 5.3, 5.4, 5.5, 5.6, 5.10, 11.3, 11.5, 12.1, 12.2_
  - [ ]* 6.2 Escrever testes unitários do SnsGateway com mock de boto3
    - Verificar passagem de parâmetros (MessageGroupId, MessageDeduplicationId condicionado à dedup, MessageAttributes), captura do MessageId e preservação do erro original
    - _Requirements: 4.8, 5.3, 5.4, 5.10_

- [x] 7. Implementar a camada de leitura não destrutiva (SqsLeituraGateway) e o Verificador_Entrega
  - [x] 7.1 Implementar o SqsLeituraGateway (somente leitura)
    - Expor `receber_para_visualizacao` com `ReceiveMessage` + Long Polling (WaitTimeSeconds 1–20) e VisibilityTimeout curto; **nunca** chamar `DeleteMessage`; repassar o erro original da AWS
    - _Requirements: 6.4, 10.4, 10.5, 14.4_
  - [ ]* 7.2 Escrever teste de propriedade para a leitura não destrutiva
    - **Property 2: Verificação não exclui mensagens das filas de destino**
    - **Validates: Requirements 6.4, 10.4**
  - [x] 7.3 Implementar o Verificador_Entrega (correlação por ID_Pedido e timeouts)
    - Implementar `verificar_entrega` e `verificar_dlq`: correlacionar cada mensagem lida pelo ID_Pedido, ignorando e preservando as não correlacionadas; repetir leituras apenas até o Timeout_Verificacao (sem registrar erro em retorno vazio); ao atingir o timeout retornar "Não confirmado"; registrar Evento_Observado de Entrega_Confirmada com fila/ID_Pedido/MessageId
    - Aplicar Timeout_Verificacao_Entrega nas filas de destino e Timeout_Verificacao_DLQ nas DLQs
    - _Requirements: 6.1, 6.3, 6.5, 6.6, 10.1, 10.2, 10.6, 8.5, 8.7_
  - [ ]* 7.4 Escrever teste de propriedade para a seletividade por ID_Pedido
    - **Property 3: Verificação é seletiva por ID_Pedido**
    - **Validates: Requirements 6.1, 6.3, 10.1, 10.2**
  - [ ]* 7.5 Escrever testes unitários do Verificador_Entrega com relógio simulado
    - Confirmação dentro do timeout, retorno vazio sem erro e "Não confirmado" ao atingir o timeout
    - _Requirements: 6.6, 10.6_

- [x] 8. Implementar o Gerenciador_Falha_Controlada (round-trip de Access Policy)
  - [x] 8.1 Implementar salvar/aplicar Deny/restaurar da Access_Policy_Fila
    - `salvar_policy_original`, `aplicar_deny_temporario` (statement Deny para `sns.amazonaws.com` com condição por `SourceArn` do tópico, sem excluir filas nem assinaturas) e `restaurar_policy` que restaura integralmente a policy original; se a restauração falhar, sinalizar a fila afetada e orientar restauração manual pelo Console AWS (pt-BR)
    - _Requirements: 8.2, 8.8, 8.9_
  - [ ]* 8.2 Escrever teste de propriedade para o round-trip da Access Policy
    - **Property 5: Round-trip da Access_Policy_Fila preserva a policy original**
    - **Validates: Requirements 8.2, 8.8**
  - [ ]* 8.3 Escrever teste unitário da montagem do statement Deny
    - Verificar principal `sns.amazonaws.com` e condição por `SourceArn` do tópico correspondente
    - _Requirements: 8.2_

- [x] 9. Implementar a verificação de inicialização dos recursos AWS
  - [x] 9.1 Implementar a verificação de existência e acessibilidade dos recursos
    - Verificar os dois tópicos, as seis assinaturas, as seis filas de destino, as duas DLQs, a IAM Role e a instância EC2 (timeout de 10s por recurso) antes de disponibilizar o Dashboard; ao faltar/erro, registrar o erro original, interromper sem alterar recursos e expor o recurso indisponível por nome e tipo (pt-BR); nunca criar/modificar/excluir recursos
    - _Requirements: 1.9, 1.11, 1.10_
  - [ ]* 9.2 Escrever teste de integração da verificação de inicialização
    - Recursos presentes e ausentes; confirmação de interrupção sem alteração de recursos
    - _Requirements: 1.9, 1.11_

- [x] 10. Implementar a segurança (autenticação do Dashboard e acesso)
  - [x] 10.1 Implementar o controle de autenticação com bloqueio por origem
    - Exigir usuário/senha quando exposto à internet; bloquear a origem após 5 falhas consecutivas por 300 segundos; um sucesso antes do limite reinicia a contagem; restringir origens às autorizadas; não expor credenciais da AWS no navegador
    - Tratar falha de autorização via IAM_Role_EC2 interrompendo a operação, registrando o evento e expondo erro sem persistir dados parciais
    - _Requirements: 12.4, 12.5, 12.6, 12.7, 12.9_
  - [ ]* 10.2 Escrever teste de propriedade para o bloqueio de autenticação
    - **Property 11: Bloqueio de autenticação ativa após 5 falhas consecutivas**
    - **Validates: Requirements 12.6**

- [x] 11. Checkpoint — camadas de acesso, verificação e segurança validadas
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Implementar os endpoints de configuração, saúde e estatísticas da API_Backend (FastAPI)
  - [x] 12.1 Implementar os endpoints de saúde e configuração
    - `GET /api/saude` (verificação de inicialização), `GET /api/config` (Config_Aplicada e Config_Simulacao com rótulos distintos), `PUT /api/config/simulacao` (ajuste de Timeout_Verificacao_Entrega e Timeout_Verificacao_DLQ dentro das faixas); orientar em pt-BR que Config_Aplicada só muda no Console AWS
    - _Requirements: 1.9, 1.11, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9_
  - [x] 12.2 Implementar o endpoint de estatísticas e dados de arquitetura com timeout
    - `GET /api/estatisticas` (agregador por cenário, publicadas/confirmadas/DLQ, horário da última atualização) e `GET /api/arquitetura`; aplicar timeout de 3s (p95 sob 10 requisições simultâneas) retornando indicação de timeout ao exceder e preservando os últimos dados válidos
    - _Requirements: 3.1, 3.2, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10, 13.5, 14.1, 14.3_

- [x] 13. Implementar os endpoints de disparo, comparação e DLQ da API_Backend
  - [x] 13.1 Implementar o disparo Standard e FIFO
    - `POST /api/disparar/standard` e `POST /api/disparar/fifo`: validar o payload, publicar via SnsGateway, registrar Evento_Observado de Entrega_Publicada com o MessageId e disparar a verificação de entrega nas três filas de destino; em falha de publicação, registrar o erro original e indicar a falha (pt-BR); abster-se de enviar SMS/e-mail reais; desabilitar o botão enquanto o teste estiver ativo
    - _Requirements: 4.2, 4.3, 4.4, 4.5, 4.6, 4.8, 5.5, 5.6, 5.7, 5.8, 5.10, 9.1, 9.3, 9.4, 11.5_
  - [x] 13.2 Implementar o endpoint de comparação Standard x FIFO
    - `POST /api/comparar`: executar dois testes independentes publicando as três mensagens "Pedido criado", "Compra aprovada" e "Pedido em preparação" (FIFO com um único MessageGroupId), retornar ordem de publicação, ordem de recebimento e MessageId lado a lado, com texto em pt-BR de que o Standard não garante ordenação; sem simular mensagens fora de ordem; "Não confirmado" quando não confirmar no Timeout_Verificacao_Entrega; interromper com causa em pt-BR se o tópico estiver indisponível ou a autenticação falhar
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9_
  - [x] 13.3 Implementar o endpoint de teste de DLQ com Falha_Entrega_Controlada
    - `POST /api/dlq/simular-falha`: exigir acionamento manual e seleção de uma Assinatura_SNS; salvar e aplicar o Deny temporário via Gerenciador_Falha_Controlada, publicar a mensagem de teste, verificar as demais assinaturas e a DLQ compatível (até Timeout_Verificacao_DLQ), restaurar a Access_Policy_Fila ao final e, em falha de restauração, identificar a fila afetada e orientar restauração manual; exibir texto explicativo em pt-BR sobre a DLQ do SNS
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9, 8.10, 8.11, 9.1_
  - [x] 13.4 Implementar o endpoint de autenticação
    - `POST /api/autenticacao`: login com usuário/senha e bloqueio após 5 falhas consecutivas por 300s, reutilizando a camada de segurança
    - _Requirements: 12.5, 12.6_
  - [ ]* 13.5 Escrever testes de integração dos endpoints da API
    - Fan-out Standard (publicar 1 e confirmar nas três filas), fan-out FIFO, ordenação FIFO (≥3 no mesmo MessageGroupId), comparação lado a lado, DLQ do SNS com restauração da policy e distinção Entrega_Publicada×Entrega_Confirmada; execução única e manual, sem loops
    - _Requirements: 18.1, 18.2, 18.3, 18.4, 18.5, 18.6_

- [x] 14. Checkpoint — API_Backend integrada
  - Ensure all tests pass, ask the user if questions arise.

- [x] 15. Implementar o Dashboard (HTML/CSS/JS, pt-BR)
  - [x] 15.1 Implementar a estrutura do Dashboard, a tela de login e a arquitetura visual
    - Montar o layout com título "AWS SNS Delivery Lab", a tela de login com indicação de bloqueio e a representação visual da arquitetura (dois tópicos com tipo, seis assinaturas, seis filas de destino e duas DLQs) e as quatro ações principais rotuladas em pt-BR
    - _Requirements: 3.1, 3.2, 3.3, 12.5, 12.6_
  - [x] 15.2 Implementar a visão geral, as estatísticas e a configuração
    - Exibir estatísticas separadas por cenário (publicadas por tópico, Entrega_Confirmada por fila, presentes por DLQ, todas inteiros ≥ 0 com 0 quando ausente), o horário da última atualização bem-sucedida (data/hora/fuso) e os parâmetros com rótulo Config_Aplicada/Config_Simulacao; permitir ajustar Config_Simulacao dentro das faixas (rejeitando fora da faixa com a faixa válida) e orientar que Config_Aplicada muda no Console AWS; preservar últimos valores e indicar falha de atualização
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10, 13.6_
  - [x] 15.3 Implementar os disparos Standard e FIFO
    - Formulário de disparo Standard (ID_Pedido 1–80) e FIFO (ID_Pedido 1–80, MessageGroupId 1–128; não solicitar MessageDeduplicationId com dedup por conteúdo habilitada) com validação em pt-BR; exibir após a publicação tópico, tipo, ID_Pedido, (MessageGroupId no FIFO), MessageId e os três destinos, com a notificação visual "Seu pedido foi aprovado e está sendo preparado"; desabilitar o botão durante o teste e reabilitar ao concluir
    - _Requirements: 4.1, 4.4, 4.5, 4.7, 5.1, 5.2, 5.3, 5.4, 5.7, 5.9, 9.3, 9.4, 11.4, 13.6_
  - [x] 15.4 Implementar a comparação Standard x FIFO e o teste de DLQ
    - Painel de comparação lado a lado (ordem de publicação, ordem de recebimento, MessageId, tipo) com texto explicativo em pt-BR; painel de DLQ com seleção de Assinatura_SNS, botão "SIMULAR FALHA DE ENTREGA" manual, exibição do resultado na DLQ como Evento_Observado, estado "Não confirmado" quando aplicável, confirmação de policy restaurada e orientação de restauração manual em falha; texto explicativo em pt-BR sobre a DLQ do SNS
    - _Requirements: 7.4, 7.5, 7.6, 7.9, 8.1, 8.6, 8.7, 8.9, 8.11, 9.3, 9.4_
  - [x] 15.5 Implementar a atualização periódica e os indicadores de estado
    - Atualizar estatísticas ao final de cada intervalo configurável (5–300s, padrão 10s) sem recarregar a página, rejeitando intervalo fora da faixa; exibir o horário da atualização e indicar dados possivelmente desatualizados em falha; exibir, por mensagem, indicador textual distinto de Entrega_Publicada×Entrega_Confirmada
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5, 13.6_

- [ ] 16. Integração e wiring final da aplicação
  - [x] 16.1 Compor a aplicação na EC2 (API_Backend + Verificador_Entrega + Dashboard)
    - Inicializar a API_Backend servindo o Dashboard e hospedando o Verificador_Entrega, condicionando a disponibilização do Dashboard à verificação de inicialização bem-sucedida; garantir que cada teste rode apenas por acionamento manual, sem loops, carga ou agendamento automático
    - _Requirements: 1.9, 9.1, 9.2, 9.5, 9.6, 9.7_
  - [ ]* 16.2 Escrever teste de smoke de segurança (ausência de Access Keys)
    - Verificar ausência de `aws_access_key_id`/`aws_secret_access_key` em código, configuração e variáveis de ambiente e autenticação via IAM Role contra SNS/SQS
    - _Requirements: 12.1, 12.2, 12.3_

- [x] 17. Checkpoint — aplicação completa validada
  - Ensure all tests pass, ask the user if questions arise.

- [x] 18. Produzir os artefatos de documentação do projeto
  - [x] 18.1 Escrever `ARQUITETURA.md`
    - Incluir visão geral, serviços AWS e suas funções, fluxos fan-out Standard e FIFO, fluxo de falha de entrega e DLQ do SNS, integração com as filas de destino e o Dashboard, lista de permissões IAM, ao menos um diagrama Mermaid de todos os fluxos e ao menos um diagrama com ícones oficiais AWS
    - _Requirements: 15.1, 15.2, 17.1, 17.6_
  - [x] 18.2 Escrever `IMPLANTACAO.md`
    - Passo a passo numerado em pt-BR para criar manualmente os dois tópicos (Deduplicacao_Por_Conteudo no FIFO), as seis filas de destino, as duas DLQs, as Access_Policy_Fila, as seis assinaturas com Redrive_Policy_SNS, a IAM_Role_EC2 (privilégios mínimos, sem Access Keys), a EC2 e os Security Groups (porta 22 fechada, Session_Manager), instalar/executar a aplicação (explicando cada comando quanto ao que faz, por que e qual resultado esperar), verificar pós-implantação, excluir recursos ao final e tratar falhas de etapa; incluir custo mensal estimado em USD e lista de exclusão com ordem de dependência
    - _Requirements: 15.3, 15.4, 15.5, 15.7, 16.1, 16.2, 16.3, 16.4, 16.5, 16.6, 16.7, 16.8, 16.9, 16.10, 16.11, 16.12, 17.2, 17.6_
  - [x] 18.3 Escrever `README.md` (artigo técnico de portfólio)
    - Incluir, cada seção com ao menos um parágrafo: SNS, Standard vs FIFO, tópicos, assinaturas, fan-out, ordenação de mensagens, DLQ do SNS, arquitetura, tecnologias, funcionamento, experimentos, resultados, boas práticas, custos estimados, como executar, como remover os recursos AWS, lições aprendidas e conclusão; apenas resultados efetivamente obtidos
    - _Requirements: 15.4, 15.5, 17.3, 17.5, 17.6_
  - [x] 18.4 Escrever `TESTES.md`
    - Roteiro dos seis testes (fan-out Standard; fan-out FIFO; ordenação FIFO ≥3 no mesmo MessageGroupId; comparação Standard x FIFO lado a lado; DLQ do SNS com Falha_Entrega_Controlada e restauração da policy; confirmação de entrega real distinguindo Entrega_Publicada×Entrega_Confirmada), cada um com os 8 campos obrigatórios (objetivo, pré-requisitos, configuração, passo a passo numerado, resultado esperado, evidências, campo para resultado real e critério de aprovação)
    - _Requirements: 17.4, 17.5, 17.6, 18.1, 18.2, 18.3, 18.4, 18.5, 18.6, 18.7, 18.8_

- [x] 19. Checkpoint final — artefatos e testes completos
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tarefas marcadas com `*` são opcionais (testes) e podem ser puladas para um MVP mais rápido.
- Cada tarefa referencia os critérios de aceitação específicos para rastreabilidade.
- As 11 propriedades de correção do design viram testes PBT com Hypothesis (mínimo 100 iterações), posicionados perto da implementação que validam, com dependências AWS substituídas por mocks/fakes em memória.
- Os checkpoints garantem validação incremental ao longo do desenvolvimento.
- A aplicação nunca cria, modifica ou exclui recursos AWS, exceto o ajuste temporário e reversível de Access_Policy_Fila da Falha_Entrega_Controlada; a infraestrutura é criada manualmente conforme `IMPLANTACAO.md`.
- A confirmação de entrega é sempre por leitura real não destrutiva correlacionada pelo ID_Pedido; nunca se afirma entrega apenas pela aceitação da publicação pelo SNS.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1"] },
    { "id": 1, "tasks": ["2.1", "2.2", "3.1", "3.3", "4.1", "4.4", "10.1"] },
    { "id": 2, "tasks": ["2.3", "2.4", "3.2", "3.4", "4.2", "4.3", "4.5", "6.1", "7.1", "8.1", "10.2"] },
    { "id": 3, "tasks": ["6.2", "7.2", "7.3", "8.2", "8.3", "9.1"] },
    { "id": 4, "tasks": ["7.4", "7.5", "9.2", "12.1", "12.2"] },
    { "id": 5, "tasks": ["13.1", "13.2", "13.3", "13.4"] },
    { "id": 6, "tasks": ["13.5", "15.1", "15.2"] },
    { "id": 7, "tasks": ["15.3", "15.4", "15.5"] },
    { "id": 8, "tasks": ["16.1"] },
    { "id": 9, "tasks": ["16.2", "18.1", "18.2", "18.3", "18.4"] }
  ]
}
```
