# Requirements Document

## Introduction

Esta funcionalidade descreve um laboratório prático e didático de **Amazon SQS** construído sobre serviços reais da AWS na região `us-east-1` (Norte da Virgínia). O objetivo é apoiar o estudo para a certificação **AWS Solutions Architect Associate (SAA)**, demonstrando de forma visual todo o ciclo de vida das mensagens: envio, recebimento, processamento, falhas, reprocessamento e encaminhamento para **Dead-Letter Queue (DLQ)**.

A solução é composta por uma aplicação web em Português do Brasil (pt-BR) hospedada em uma instância **Amazon EC2** Linux. O backend é escrito em Python com `boto3` e `FastAPI` (expondo a API e executando os consumidores), e o frontend usa HTML, CSS e JavaScript. O histórico, as tentativas e os eventos de cada mensagem são registrados no **Amazon DynamoDB**, pois o SQS não oferece uma API para consultar o estado individual de todas as mensagens.

Todos os recursos AWS (filas SQS, DLQs, tabelas DynamoDB, IAM Role, instância EC2, Security Groups) são criados **manualmente pelo próprio usuário no Console AWS em pt-BR**. A aplicação **não cria recursos AWS automaticamente** e não utiliza Infrastructure as Code (Terraform, CloudFormation, CDK). A autenticação com a AWS ocorre exclusivamente por meio de uma **IAM Role** associada à instância EC2, sem Access Keys fixas no código.

Este documento define os requisitos usando os padrões EARS e as regras de qualidade INCOSE. O detalhamento de tabelas, chaves, índices, exemplos JSON e padrões de acesso do DynamoDB será elaborado no documento de design (`design.md`).

## Glossary

- **Dashboard**: Aplicação web em pt-BR, servida pela EC2, que apresenta as cinco áreas funcionais (visão geral, envio, monitoramento, histórico e laboratório de falhas).
- **API_Backend**: Serviço FastAPI em Python que expõe os endpoints HTTP consumidos pelo Dashboard.
- **Consumidor_Standard**: Processo que recebe e processa mensagens da fila `lab-sqs-standard` via `boto3`.
- **Consumidor_FIFO**: Processo que recebe e processa mensagens da fila `lab-sqs-fifo.fifo` via `boto3`, respeitando o agrupamento por `MessageGroupId`.
- **Sistema**: Conjunto formado por API_Backend, Consumidor_Standard, Consumidor_FIFO e Dashboard executando na EC2.
- **Fila_Principal_Standard**: Fila SQS do tipo Standard de nome `lab-sqs-standard`.
- **DLQ_Standard**: Fila SQS do tipo Standard de nome `lab-sqs-standard-dlq`, usada como Dead-Letter Queue da Fila_Principal_Standard.
- **Fila_Principal_FIFO**: Fila SQS do tipo FIFO de nome `lab-sqs-fifo.fifo`.
- **DLQ_FIFO**: Fila SQS do tipo FIFO de nome `lab-sqs-fifo-dlq.fifo`, usada como Dead-Letter Queue da Fila_Principal_FIFO.
- **Tabela_Historico**: Tabela DynamoDB que armazena o histórico, as tentativas e os eventos de cada mensagem.
- **Visibility_Timeout**: Período, em segundos, durante o qual uma mensagem recebida fica invisível para outros consumidores. Valor inicial do laboratório: 30 segundos.
- **maxReceiveCount**: Número máximo de recebimentos de uma mensagem na redrive policy antes de ser movida para a DLQ. Valor inicial do laboratório: 2.
- **Redrive_Policy**: Política do SQS que associa uma fila principal à sua DLQ e define o `maxReceiveCount`.
- **Long_Polling**: Técnica de recebimento de mensagens com `WaitTimeSeconds` configurado. Valor inicial do laboratório: 20 segundos.
- **SSE_SQS**: Criptografia em repouso de mensagens SQS usando chaves gerenciadas pelo SQS (Server-Side Encryption).
- **MessageGroupId**: Identificador de grupo exigido por filas FIFO que garante ordenação dentro do grupo.
- **MessageDeduplicationId**: Identificador de deduplicação de mensagens em filas FIFO.
- **Deduplicacao_Por_Conteudo**: Recurso de fila FIFO que gera o `MessageDeduplicationId` a partir do conteúdo da mensagem (content-based deduplication).
- **Tempo_Processamento_Simulado**: Duração, em segundos, que o consumidor simula para processar uma mensagem. Valor inicial do laboratório: 45 segundos.
- **Cenario_Falha**: Opção de simulação que determina o comportamento do consumidor ao processar uma mensagem (sucesso, exceção, interrupção etc.).
- **Idempotencia**: Propriedade pela qual o processamento repetido da mesma mensagem não gera efeitos colaterais duplicados.
- **Evento_Observado**: Evento registrado pela aplicação a partir de uma ação concreta que ela executou ou constatou (ex.: recebimento via `boto3`, exclusão confirmada).
- **Evento_Inferido**: Evento deduzido pela aplicação a partir de condições indiretas (ex.: provável expiração do Visibility_Timeout), sem confirmação direta do SQS.
- **IAM_Role_EC2**: IAM Role com privilégios mínimos associada à instância EC2 para acesso ao SQS e ao DynamoDB.
- **Session_Manager**: Recurso do AWS Systems Manager usado para administrar a EC2 sem abrir porta SSH.
- **Config_Aplicada**: Parâmetro efetivamente configurado em um recurso SQS (ex.: Visibility_Timeout da fila).
- **Config_Simulacao**: Parâmetro usado apenas pela lógica de simulação da aplicação (ex.: Tempo_Processamento_Simulado).

## Requirements

### Requisito 1: Infraestrutura manual de filas SQS

**User Story:** Como estudante de SAA, quero que o laboratório utilize filas SQS reais criadas manualmente, para que eu pratique a configuração de recursos pelo Console AWS.

#### Critérios de Aceitação

1. THE Sistema SHALL utilizar a Fila_Principal_Standard de nome `lab-sqs-standard` do tipo Standard na região `us-east-1`.
2. THE Sistema SHALL utilizar a DLQ_Standard de nome `lab-sqs-standard-dlq` do tipo Standard na região `us-east-1`.
3. THE Sistema SHALL utilizar a Fila_Principal_FIFO de nome `lab-sqs-fifo.fifo` do tipo FIFO na região `us-east-1`.
4. THE Sistema SHALL utilizar a DLQ_FIFO de nome `lab-sqs-fifo-dlq.fifo` do tipo FIFO na região `us-east-1`.
5. THE Sistema SHALL operar exclusivamente sobre as filas Standard e FIFO, as DLQs, as tabelas DynamoDB, a IAM Role e a instância EC2 criadas manualmente pelo usuário no Console AWS, sem criar recursos próprios.
6. WHEN a inicialização do Sistema é iniciada, THE Sistema SHALL verificar a existência e a acessibilidade de cada recurso AWS esperado (filas, DLQs, tabelas DynamoDB, IAM Role e instância EC2) antes de disponibilizar o Dashboard.
7. THE Sistema SHALL abster-se de criar, modificar ou excluir recursos AWS de forma automática.
8. IF um recurso AWS esperado estiver ausente ou inacessível durante a verificação de inicialização, THEN THE Sistema SHALL registrar o erro original retornado pela AWS, interromper a inicialização sem alterar qualquer recurso AWS e exibir no Dashboard uma mensagem em pt-BR que identifique o recurso indisponível por nome e tipo.
9. THE Sistema SHALL abster-se de utilizar Infrastructure as Code (Terraform, CloudFormation, CDK) para provisionar recursos.

### Requisito 2: Configuração inicial e distinção entre parâmetros aplicados e de simulação

**User Story:** Como estudante de SAA, quero distinguir os parâmetros aplicados aos recursos SQS dos parâmetros usados apenas na simulação, para que eu entenda o que é comportamento real da AWS e o que é lógica da aplicação.

#### Critérios de Aceitação

1. WHEN o Dashboard é carregado, THE Dashboard SHALL apresentar o Visibility_Timeout inicial de 30 segundos como Config_Aplicada das filas principais.
2. WHEN o Dashboard é carregado, THE Dashboard SHALL apresentar o maxReceiveCount inicial de 2 como Config_Aplicada da Redrive_Policy.
3. WHEN o Dashboard é carregado, THE Dashboard SHALL apresentar o período de retenção inicial de 4 dias como Config_Aplicada das filas principais.
4. WHEN o Dashboard é carregado, THE Dashboard SHALL apresentar o período de retenção inicial de 14 dias como Config_Aplicada das DLQs.
5. WHEN o Dashboard é carregado, THE Dashboard SHALL apresentar o Long_Polling inicial de 20 segundos como Config_Aplicada das filas principais.
6. WHEN o Dashboard é carregado, THE Dashboard SHALL apresentar o SSE_SQS como Config_Aplicada de todas as filas.
7. WHEN o Dashboard é carregado, THE Dashboard SHALL apresentar a Deduplicacao_Por_Conteudo como Config_Aplicada das filas FIFO.
8. WHEN o Dashboard é carregado, THE Dashboard SHALL apresentar o Tempo_Processamento_Simulado inicial de 45 segundos como Config_Simulacao, ajustável entre 0 e 300 segundos.
9. WHEN o Dashboard é carregado, THE Dashboard SHALL apresentar a quantidade inicial de 5 mensagens por fila como Config_Simulacao, ajustável entre 1 e 10 mensagens.
10. THE Dashboard SHALL exibir, para cada parâmetro, um rótulo textual distinto que identifique se é Config_Aplicada ou Config_Simulacao.
11. WHERE um parâmetro é de Config_Simulacao, THE Dashboard SHALL permitir que o usuário ajuste o valor, dentro da faixa definida, para novos testes sem alterar o código-fonte.
12. IF o usuário informar, para um parâmetro de Config_Simulacao, um valor fora da faixa definida, THEN THE Dashboard SHALL rejeitar o valor, preservar o valor anterior e exibir mensagem em pt-BR indicando a faixa válida.
13. IF o usuário solicitar alterar um parâmetro que é Config_Aplicada em um recurso SQS, THEN THE Dashboard SHALL exibir orientação em pt-BR de que a alteração deve ser feita manualmente no Console AWS.

### Requisito 3: Dashboard — Visão geral

**User Story:** Como estudante de SAA, quero uma visão geral com estatísticas das filas, para que eu acompanhe o estado agregado do laboratório em tempo quase real.

#### Critérios de Aceitação

1. THE Dashboard SHALL exibir o total de mensagens enviadas como um valor numérico inteiro maior ou igual a 0.
2. THE Dashboard SHALL exibir a quantidade de mensagens disponíveis como um valor numérico inteiro maior ou igual a 0.
3. THE Dashboard SHALL exibir a quantidade de mensagens em processamento como um valor numérico inteiro maior ou igual a 0.
4. THE Dashboard SHALL exibir a quantidade de mensagens processadas com sucesso como um valor numérico inteiro maior ou igual a 0.
5. THE Dashboard SHALL exibir a quantidade de mensagens com falha como um valor numérico inteiro maior ou igual a 0.
6. THE Dashboard SHALL exibir a quantidade de mensagens encaminhadas para DLQ como um valor numérico inteiro maior ou igual a 0.
7. THE Dashboard SHALL exibir a quantidade de mensagens recebida por cada consumidor como um valor numérico inteiro maior ou igual a 0, identificando cada consumidor individualmente.
8. THE Dashboard SHALL exibir as estatísticas dos critérios 1 a 7 separadas em dois conjuntos distintos e rotulados: um para o fluxo Standard e um para o fluxo FIFO.
9. WHILE a área de visão geral estiver aberta, THE Dashboard SHALL atualizar as estatísticas automaticamente a cada intervalo configurável entre 1 e 60 segundos, com valor padrão de 5 segundos.
10. THE Dashboard SHALL exibir o horário da última atualização bem-sucedida das estatísticas contendo data, hora (horas, minutos e segundos) e identificação do fuso horário.
11. IF uma atualização de estatísticas falhar, THEN THE Dashboard SHALL preservar os últimos valores exibidos com sucesso, manter inalterado o horário da última atualização bem-sucedida e exibir uma indicação visível de que a atualização falhou.
12. IF não houver dados registrados para uma estatística, THEN THE Dashboard SHALL exibir o valor 0 para essa estatística.

### Requisito 4: Dashboard — Envio de mensagens

**User Story:** Como estudante de SAA, quero enviar mensagens configuráveis para as filas, para que eu provoque e observe diferentes comportamentos do SQS.

#### Critérios de Aceitação

1. THE Dashboard SHALL permitir selecionar a fila de destino entre Fila_Principal_Standard e Fila_Principal_FIFO.
2. THE Dashboard SHALL permitir informar a quantidade de mensagens a enviar, de 1 a 10.
3. THE Dashboard SHALL permitir informar o identificador do pedido de cada mensagem, com 1 a 80 caracteres.
4. THE Dashboard SHALL permitir informar o Tempo_Processamento_Simulado de cada mensagem, de 0 a 300 segundos.
5. THE Dashboard SHALL permitir selecionar o Cenario_Falha de cada mensagem.
6. WHERE a fila selecionada é FIFO, THE Dashboard SHALL permitir informar o MessageGroupId com 1 a 128 caracteres.
7. WHERE a fila selecionada é FIFO e a Deduplicacao_Por_Conteudo está desabilitada, THE Dashboard SHALL permitir informar o MessageDeduplicationId com 1 a 128 caracteres.
8. THE Dashboard SHALL permitir enviar um conjunto de 5 pedidos para a fila selecionada em uma única ação.
9. IF o usuário confirmar o envio com algum campo obrigatório ausente ou fora dos limites definidos, THEN THE Dashboard SHALL rejeitar o envio, preservar os valores informados e exibir mensagem em pt-BR indicando o campo inválido.
10. WHEN o usuário confirma o envio com todos os campos válidos, THE API_Backend SHALL enviar as mensagens à fila SQS selecionada via `boto3`.
11. WHEN uma mensagem é enviada com sucesso, THE API_Backend SHALL registrar o Evento_Observado de envio na Tabela_Historico com o horário real da aplicação.
12. IF o envio de uma mensagem falhar, THEN THE API_Backend SHALL preservar as mensagens já enviadas com sucesso, registrar o erro original e exibir no Dashboard uma mensagem em pt-BR indicando a falha de envio.

### Requisito 5: Dashboard — Monitoramento

**User Story:** Como estudante de SAA, quero uma tabela de monitoramento das mensagens, para que eu acompanhe o estado de cada mensagem ao longo do processamento.

#### Critérios de Aceitação

1. THE Dashboard SHALL exibir uma tabela de monitoramento com as colunas ID, Fila, Consumidor, Tentativas, Processamento, Estado, Último evento e Ações.
2. THE Dashboard SHALL exibir na coluna Tentativas a contagem de recebimentos de cada mensagem registrada na Tabela_Historico, como um número inteiro maior ou igual a 0.
3. THE Dashboard SHALL exibir na coluna Processamento o Tempo_Processamento_Simulado configurado para a mensagem, expresso em segundos.
4. THE Dashboard SHALL exibir na coluna Estado exatamente um dos seguintes valores: Aguardando, Recebida, Em processamento, Processada, Falha, Aguardando nova tentativa ou Encaminhada para DLQ.
5. THE Dashboard SHALL exibir na coluna Último evento a data e a hora do evento mais recente da mensagem, usando o horário real da aplicação registrado na Tabela_Historico, no formato dia, mês, ano, hora, minuto e segundo.
6. WHEN o usuário aciona a ação de visualizar histórico de uma mensagem que possui eventos registrados na Tabela_Historico, THE Dashboard SHALL abrir o histórico detalhado correspondente a essa mensagem.
7. IF o usuário aciona a ação de visualizar histórico de uma mensagem sem eventos registrados ou cuja consulta à Tabela_Historico falhar, THEN THE Dashboard SHALL preservar a tabela de monitoramento e exibir uma mensagem em pt-BR indicando que o histórico está indisponível.
8. IF não existir nenhuma mensagem registrada na Tabela_Historico, THEN THE Dashboard SHALL exibir a tabela de monitoramento sem linhas de dados e uma mensagem em pt-BR indicando a ausência de mensagens monitoradas.
9. THE Sistema SHALL derivar o Estado de cada mensagem a partir dos eventos armazenados na Tabela_Historico.
10. THE Sistema SHALL abster-se de assumir que o SQS fornece uma API para consultar o estado individual de todas as mensagens.

### Requisito 6: Dashboard — Histórico detalhado

**User Story:** Como estudante de SAA, quero a linha do tempo detalhada de uma mensagem, para que eu entenda cada etapa do seu ciclo de vida.

#### Critérios de Aceitação

1. WHEN o usuário abre o histórico de uma mensagem, THE Dashboard SHALL exibir, em até 3 segundos, a linha do tempo dos eventos em ordem cronológica crescente, usando a ordem de registro como desempate entre eventos de mesmo horário.
2. WHEN um evento de ciclo de vida da mensagem tiver sido registrado, THE Dashboard SHALL exibi-lo no histórico, abrangendo envio, recebimento, consumidor responsável, início de processamento, Visibility_Timeout configurado, tentativa de recebimento posterior, falha, confirmação de sucesso, exclusão e encaminhamento para DLQ.
3. THE Dashboard SHALL exibir o horário real da aplicação para cada evento com precisão de segundos e identificação do fuso horário.
4. THE Dashboard SHALL exibir, para cada evento, um indicador textual distinto que identifique se é Evento_Observado ou Evento_Inferido, sem exigir interação do usuário.
5. WHERE houver possível expiração do Visibility_Timeout, THE Dashboard SHALL registrar o evento correspondente como Evento_Inferido.
6. WHEN uma mensagem é confirmada como encaminhada para DLQ, THE Dashboard SHALL exibir o evento de encaminhamento para DLQ como Evento_Observado.
7. IF a consulta do histórico de uma mensagem falhar, THEN THE Dashboard SHALL preservar a tela anterior e exibir mensagem em pt-BR indicando que o histórico está indisponível.
8. IF a mensagem não possuir eventos registrados, THEN THE Dashboard SHALL exibir uma indicação em pt-BR de que não há eventos para a mensagem.

### Requisito 7: Dashboard — Laboratório de falhas

**User Story:** Como estudante de SAA, quero simular cenários de falha controlados, para que eu observe o comportamento do SQS em situações adversas.

#### Critérios de Aceitação

1. WHEN o usuário executa o cenário de processamento com sucesso, THE Dashboard SHALL exibir a mensagem como Processada e registrar a exclusão da mensagem da fila.
2. WHEN o usuário executa o cenário de processamento com duração superior ao Visibility_Timeout, THE Dashboard SHALL exibir que a mensagem voltou a ficar visível na fila e que a contagem de recebimentos foi incrementada.
3. WHEN o usuário executa o cenário de exceção proposital no consumidor, THE Dashboard SHALL exibir a mensagem no estado Falha e registrar o erro.
4. WHEN o usuário executa o cenário de interrupção do consumidor durante o processamento, THE Dashboard SHALL exibir que a mensagem não foi excluída e voltou a ficar disponível após o Visibility_Timeout.
5. WHEN o usuário executa o cenário de recebimento repetido, THE Dashboard SHALL exibir a mesma mensagem recebida ao menos 2 vezes, com a contagem de recebimentos correspondente.
6. WHEN o usuário executa o cenário de encaminhamento para DLQ, THE Dashboard SHALL exibir a mensagem na DLQ após a contagem de recebimentos atingir o maxReceiveCount da Redrive_Policy.
7. WHEN o usuário executa o cenário de reprocessamento de mensagens da DLQ, THE Dashboard SHALL exibir as mensagens movidas da DLQ de volta à fila principal e seu novo processamento.
8. WHEN o usuário executa o cenário de comparação entre Standard e FIFO, THE Dashboard SHALL exibir lado a lado o resultado de processamento dos dois fluxos.
9. THE Sistema SHALL abster-se de afirmar que uma mensagem foi encaminhada para a DLQ apenas porque o Visibility_Timeout expirou.
10. THE Sistema SHALL considerar uma mensagem como encaminhada para a DLQ somente quando a Redrive_Policy e a contagem de recebimentos confirmarem a transferência.
11. IF uma fila estiver indisponível ou a autenticação falhar durante a execução de um cenário, THEN THE Dashboard SHALL interromper o cenário e exibir mensagem em pt-BR indicando a causa.

### Requisito 8: Consumo de mensagens pelos consumidores

**User Story:** Como estudante de SAA, quero consumidores independentes para Standard e FIFO, para que eu observe o processamento real das mensagens.

#### Critérios de Aceitação

1. WHILE o Consumidor_Standard estiver em execução, THE Consumidor_Standard SHALL receber mensagens da Fila_Principal_Standard via `boto3`, em lotes de 1 a 10 mensagens.
2. WHILE o Consumidor_FIFO estiver em execução, THE Consumidor_FIFO SHALL receber mensagens da Fila_Principal_FIFO via `boto3`, em lotes de 1 a 10 mensagens.
3. WHEN um consumidor recebe uma mensagem, THE consumidor SHALL registrar o Evento_Observado de recebimento na Tabela_Historico com o horário real da aplicação em UTC.
4. WHEN um consumidor recebe uma mensagem, THE consumidor SHALL registrar o identificador do consumidor responsável e a contagem de recebimentos.
5. WHEN um consumidor processa uma mensagem, THE consumidor SHALL aguardar um período equivalente ao Tempo_Processamento_Simulado da mensagem antes de concluir.
6. WHEN o processamento de uma mensagem é concluído com sucesso, THE consumidor SHALL excluir a mensagem da fila SQS.
7. THE consumidor SHALL abster-se de excluir uma mensagem antes de o processamento concluir com sucesso.
8. IF ocorrer um erro durante o processamento, THEN THE consumidor SHALL preservar a mensagem na fila para reprocessamento e registrar o erro original na Tabela_Historico.
9. WHERE o Cenario_Falha indica ausência de confirmação, THE consumidor SHALL abster-se de excluir a mensagem para que o Visibility_Timeout expire.
10. WHILE um consumidor estiver aguardando mensagens, THE consumidor SHALL usar Long_Polling com WaitTimeSeconds de 1 a 20 segundos.
11. WHEN uma solicitação de recebimento retornar sem mensagens, THE consumidor SHALL repetir a tentativa sem registrar erro.

### Requisito 9: Idempotência no processamento

**User Story:** Como estudante de SAA, quero controle de idempotência nos consumidores, para que recebimentos repetidos não gerem efeitos duplicados.

#### Critérios de Aceitação

1. WHEN um consumidor recebe uma mensagem cujo identificador de pedido já consta na Tabela_Historico com estado de processamento concluído com sucesso, THE consumidor SHALL abster-se de executar os efeitos colaterais do processamento associados a essa mensagem.
2. THE consumidor SHALL identificar mensagens já processadas comparando o identificador do pedido da mensagem recebida com os identificadores de pedido registrados na Tabela_Historico.
3. IF uma mensagem recebida não contém identificador de pedido ou contém um identificador de pedido inválido, THEN THE consumidor SHALL rejeitar o processamento da mensagem e registrar na Tabela_Historico um Evento_Observado indicando a falha de validação do identificador, preservando a mensagem para reprocessamento.
4. WHEN um consumidor detecta um recebimento repetido, THE consumidor SHALL registrar na Tabela_Historico um Evento_Observado de recebimento repetido contendo o identificador do pedido e o instante da detecção.
5. IF a leitura ou a escrita na Tabela_Historico falhar durante a verificação de idempotência, THEN THE consumidor SHALL abster-se de executar os efeitos colaterais do processamento, preservar a mensagem para reprocessamento e sinalizar a falha por meio de um Evento_Observado de erro de acesso ao histórico.
6. THE Idempotencia SHALL preservar exatamente um resultado de processamento por identificador de pedido, independentemente do número de recebimentos da mensagem correspondente.

### Requisito 10: Consumidores concorrentes

**User Story:** Como estudante de SAA, quero ampliar a quantidade de consumidores, para que eu teste concorrência nas filas Standard e FIFO.

#### Critérios de Aceitação

1. WHEN o usuário ajusta a quantidade de instâncias de Consumidor_Standard, THE Dashboard SHALL aplicar o novo número de instâncias (de 1 a 10) em até 5 segundos.
2. WHEN o usuário ajusta a quantidade de instâncias de Consumidor_FIFO, THE Dashboard SHALL aplicar o novo número de instâncias (de 1 a 10) em até 5 segundos.
3. IF o usuário informar uma quantidade de instâncias fora do intervalo de 1 a 10, THEN THE Dashboard SHALL rejeitar o ajuste, preservar o número anterior de instâncias e exibir mensagem em pt-BR indicando o intervalo válido.
4. WHILE múltiplos consumidores estiverem ativos, THE Sistema SHALL registrar na Tabela_Historico o identificador único do consumidor responsável e o instante de cada recebimento.
5. THE Consumidor_FIFO SHALL processar um único MessageGroupId por instância por vez.
6. WHILE houver mensagens de um mesmo MessageGroupId em processamento, THE Consumidor_FIFO SHALL preservar a ordem de processamento conforme a ordem de recebimento dentro do grupo.
7. WHEN o usuário solicita a comparação entre grupos, THE Dashboard SHALL exibir a sequência de processamento por MessageGroupId e o consumidor responsável por cada mensagem.

### Requisito 11: Registro de histórico no DynamoDB

**User Story:** Como estudante de SAA, quero que o histórico das mensagens fique no DynamoDB, para que eu consulte eventos, tentativas e falhas de forma estruturada.

#### Critérios de Aceitação

1. WHEN um evento de ciclo de vida de uma mensagem ocorre, THE Sistema SHALL registrá-lo na Tabela_Historico em até 5 segundos, com o horário real da aplicação com precisão de milissegundos.
2. IF a escrita de um evento na Tabela_Historico falhar, THEN THE Sistema SHALL repetir a tentativa até 3 vezes e, persistindo a falha, preservar a mensagem original e sinalizar o erro.
3. WHEN uma consulta por identificador de pedido é solicitada, THE Tabela_Historico SHALL retornar os eventos associados a esse identificador.
4. WHEN uma consulta por fila é solicitada, THE Tabela_Historico SHALL retornar as mensagens associadas a essa fila.
5. IF uma consulta não encontrar registros, THEN THE Tabela_Historico SHALL retornar um resultado vazio sem erro.
6. WHEN os eventos de uma mensagem são listados, THE Tabela_Historico SHALL retorná-los em ordem cronológica crescente pelo horário real da aplicação.
7. THE Tabela_Historico SHALL permitir consultar a contagem de tentativas de uma mensagem como um inteiro maior ou igual a 0.
8. THE Tabela_Historico SHALL permitir identificar as mensagens cujos eventos indiquem estado de falha.
9. THE Sistema SHALL marcar cada evento registrado como Evento_Observado, quando resultante de resposta direta de um serviço, ou como Evento_Inferido, quando deduzido pela aplicação.

### Requisito 12: Segurança — autenticação e acesso

**User Story:** Como estudante de SAA, quero que a aplicação acesse a AWS com segurança, para que eu siga boas práticas de IAM e proteção de dados.

#### Critérios de Aceitação

1. THE Sistema SHALL autenticar-se nos serviços AWS exclusivamente por meio das credenciais temporárias fornecidas pela IAM_Role_EC2 associada à instância EC2.
2. THE Sistema SHALL operar sem qualquer Access Key fixa (atributos aws_access_key_id e aws_secret_access_key) presente no código-fonte, em arquivos de configuração ou em variáveis de ambiente.
3. THE IAM_Role_EC2 SHALL conceder somente as ações necessárias para enviar, receber e excluir mensagens nas filas SQS e para ler e gravar registros na Tabela_Historico, sem conceder privilégios administrativos ou de acesso a outros recursos AWS.
4. THE Sistema SHALL manter a Tabela_Historico acessível apenas pela aplicação em execução na instância EC2, sem endpoint público nem regra de rede que permita acesso originado da internet.
5. WHERE o Dashboard estiver exposto à internet, THE Dashboard SHALL exigir usuário e senha válidos antes de conceder acesso a qualquer conteúdo.
6. WHERE o Dashboard estiver exposto à internet, IF forem registradas 5 tentativas de autenticação com credenciais inválidas de forma consecutiva, THEN THE Dashboard SHALL bloquear novas tentativas pela origem por 300 segundos e exibir indicação de bloqueio.
7. THE Sistema SHALL restringir o acesso de rede ao Dashboard apenas às origens explicitamente autorizadas, rejeitando conexões de origens não autorizadas.
8. THE Sistema SHALL utilizar SSE_SQS para criptografia em repouso de todas as mensagens armazenadas nas filas SQS.
9. IF a autenticação do Sistema via IAM_Role_EC2 falhar ao acessar as filas SQS ou a Tabela_Historico, THEN THE Sistema SHALL interromper a operação afetada, registrar o evento de falha e expor indicação de erro de autorização sem persistir dados parciais.

### Requisito 13: Observabilidade e atualização periódica

**User Story:** Como estudante de SAA, quero que o Dashboard atualize os dados periodicamente, para que eu acompanhe a evolução do processamento sem recarregar a página manualmente.

#### Critérios de Aceitação

1. WHILE o Dashboard estiver aberto, THE Dashboard SHALL atualizar automaticamente as estatísticas e a tabela de monitoramento ao final de cada intervalo configurado, sem recarregar a página.
2. THE Dashboard SHALL permitir configurar o intervalo de atualização periódica entre 5 e 300 segundos, com valor padrão de 10 segundos.
3. IF o usuário informar um intervalo de atualização fora da faixa de 5 a 300 segundos, THEN THE Dashboard SHALL rejeitar o valor, preservar o intervalo anterior e exibir mensagem em pt-BR indicando a faixa válida.
4. WHEN uma atualização periódica é concluída com sucesso, THE Dashboard SHALL exibir o horário dessa atualização com data e hora da aplicação.
5. IF uma atualização periódica falhar, THEN THE Dashboard SHALL preservar os últimos dados válidos exibidos e exibir uma mensagem em pt-BR indicando que os dados podem estar desatualizados.

### Requisito 14: Desempenho da aplicação

**User Story:** Como estudante de SAA, quero que o Dashboard responda em tempo adequado, para que a experiência de uso seja fluida durante os testes.

#### Critérios de Aceitação

1. WHEN o usuário solicita as estatísticas da visão geral sob carga de até 10 requisições simultâneas, THE API_Backend SHALL responder em até 3 segundos no percentil 95.
2. WHEN o usuário solicita a tabela de monitoramento sob carga de até 10 requisições simultâneas, THE API_Backend SHALL responder em até 3 segundos no percentil 95.
3. IF a solicitação de estatísticas da visão geral exceder 3 segundos, THEN THE API_Backend SHALL retornar uma indicação de timeout e o Dashboard SHALL preservar os últimos dados válidos.
4. IF a solicitação da tabela de monitoramento exceder 3 segundos, THEN THE API_Backend SHALL retornar uma indicação de timeout e o Dashboard SHALL preservar os últimos dados válidos.
5. WHILE o Consumidor_Standard estiver aguardando mensagens, THE Consumidor_Standard SHALL usar Long_Polling com WaitTimeSeconds de 20 segundos para reduzir chamadas vazias ao SQS.
6. WHILE o Consumidor_FIFO estiver aguardando mensagens, THE Consumidor_FIFO SHALL usar Long_Polling com WaitTimeSeconds de 20 segundos para reduzir chamadas vazias ao SQS.

### Requisito 15: Custos e encerramento do laboratório

**User Story:** Como estudante de SAA, quero controlar os custos do laboratório, para que eu evite cobranças desnecessárias após os testes.

#### Critérios de Aceitação

1. THE Sistema SHALL provisionar a arquitetura sem recurso NAT Gateway, exceto quando houver justificativa técnica registrada na documentação do projeto.
2. THE Sistema SHALL provisionar a arquitetura sem recurso Elastic Load Balancer, exceto quando houver justificativa técnica registrada na documentação do projeto.
3. THE Sistema SHALL habilitar o acesso administrativo à instância EC2 exclusivamente via AWS Systems Manager Session Manager, mantendo a porta 22 (SSH) sem regra de entrada liberada no Security Group.
4. WHEN a documentação do projeto for publicada, THE documentação do projeto SHALL registrar, para cada recurso AWS utilizado, o custo mensal estimado em dólares americanos (USD) e a premissa de uso (região, tipo de instância, horas de execução ou volume de requisições) adotada no cálculo.
5. WHEN a documentação do projeto for publicada, THE documentação do projeto SHALL registrar a lista nominal de todos os recursos AWS a excluir ao final do laboratório, com o nome ou identificador de cada recurso e a ordem de exclusão quando houver dependência entre eles.
6. WHEN o laboratório for encerrado, THE Sistema SHALL ter todos os recursos AWS da lista de exclusão removidos.
7. IF algum recurso AWS da lista de exclusão permanecer ativo após a execução do procedimento de encerramento, THEN THE Sistema SHALL sinalizar a ocorrência por meio de uma indicação de recursos remanescentes, informando o nome ou identificador de cada recurso não removido.

### Requisito 16: Implantação manual sem IaC

**User Story:** Como estudante de SAA, quero implantar os recursos manualmente pelo Console AWS, para que eu pratique a criação e configuração de cada serviço.

#### Critérios de Aceitação

1. THE documentação de implantação SHALL descrever, em pt-BR, o passo a passo para criar manualmente no Console AWS a Fila_Principal_Standard (`lab-sqs-standard`), a DLQ_Standard (`lab-sqs-standard-dlq`), a Fila_Principal_FIFO (`lab-sqs-fifo.fifo`) e a DLQ_FIFO (`lab-sqs-fifo-dlq.fifo`) na região `us-east-1`, incluindo os valores de Config_Aplicada: Visibility_Timeout de 30 segundos, Long_Polling de 20 segundos, SSE_SQS habilitado, período de retenção de 4 dias nas filas principais e de 14 dias nas DLQs, Redrive_Policy com maxReceiveCount igual a 2 e Deduplicacao_Por_Conteudo habilitada nas filas FIFO.
2. THE documentação de implantação SHALL descrever, em pt-BR, o passo a passo para criar manualmente a Tabela_Historico no Console AWS, incluindo a definição da chave e dos atributos necessários às consultas por identificador de pedido, por fila e à listagem cronológica de eventos.
3. THE documentação de implantação SHALL descrever, em pt-BR, o passo a passo para criar manualmente a IAM_Role_EC2 no Console AWS com os privilégios mínimos de acesso às quatro filas SQS e à Tabela_Historico, sem Access Keys fixas.
4. THE documentação de implantação SHALL descrever, em pt-BR, o passo a passo para criar manualmente a instância EC2 Linux e os Security Groups no Console AWS, especificando as portas de entrada e saída liberadas.
5. THE documentação de implantação SHALL descrever, em pt-BR, o passo a passo para configurar o Session_Manager do AWS Systems Manager no Console AWS, de modo a administrar a EC2 sem abrir porta SSH.
6. THE documentação de implantação SHALL descrever, em pt-BR, o passo a passo para instalar e executar na EC2 a API_Backend, o Consumidor_Standard, o Consumidor_FIFO e o Dashboard, explicando cada comando Linux utilizado (o que faz, por que é executado e qual resultado esperar).
7. THE documentação de implantação SHALL descrever, em pt-BR, o passo a passo para excluir ao final do laboratório as quatro filas SQS, a Tabela_Historico, a IAM_Role_EC2, a instância EC2 e os Security Groups, apresentando-os pelos nomes definidos no Glossário.
8. THE documentação de implantação SHALL apresentar todos os recursos pelos nomes definidos no Glossário e pelos nomes reais exibidos no Console AWS, mantendo em inglês apenas termos técnicos, nomes de serviços AWS, nomes de parâmetros, nomes de variáveis e comandos.
9. THE documentação de implantação SHALL descrever, em pt-BR, o passo a passo de verificação pós-implantação que confirme que as quatro filas SQS, a Tabela_Historico, a IAM_Role_EC2 e a instância EC2 foram criadas com os nomes e os valores de Config_Aplicada previstos.
10. IF um passo da implantação não puder ser concluído por recurso ausente, permissão insuficiente ou valor de configuração divergente, THEN THE documentação de implantação SHALL indicar, em pt-BR, como identificar a causa e como corrigir antes de prosseguir para o passo seguinte.

### Requisito 17: Artefatos de documentação do projeto

**User Story:** Como estudante de SAA, quero documentação completa do projeto, para que eu use o laboratório como material de estudo e portfólio.

#### Critérios de Aceitação

1. THE projeto SHALL conter o arquivo `ARQUITETURA.md` contendo todas as seguintes seções: visão geral, lista dos serviços AWS utilizados com a descrição da função de cada um, descrição dos fluxos Standard, FIFO, de falhas e DLQ, descrição da integração com DynamoDB e Dashboard, lista das permissões IAM, pelo menos um diagrama Mermaid que represente todos os fluxos citados e pelo menos um diagrama visual utilizando ícones oficiais AWS.
2. THE projeto SHALL conter o arquivo `IMPLANTACAO.md` contendo o passo a passo da implantação manual descrito no Requisito 16, com cada etapa numerada em ordem sequencial, o comando ou ação correspondente e o resultado esperado de cada etapa.
3. THE projeto SHALL conter o arquivo `README.md` no formato de artigo técnico de portfólio contendo todas as seguintes seções, cada uma com no mínimo um parágrafo: SQS, Standard vs FIFO, Visibility_Timeout, retenção, Long_Polling, MessageGroupId, deduplicação, DLQ, arquitetura, tecnologias, funcionamento, experimentos, resultados, boas práticas, lições aprendidas e conclusão.
4. THE projeto SHALL conter o arquivo `TESTES.md` contendo o roteiro dos seis testes definidos no Requisito 18, cada teste descrito com objetivo, pré-condições, passos de execução e resultado esperado.
5. IF a documentação do projeto apresentar qualquer resultado de experimento, THEN THE documentação SHALL incluir somente resultados efetivamente obtidos durante a execução dos testes, sem valores estimados, inventados ou presumidos.
6. THE documentação do projeto SHALL ser escrita em pt-BR, mantendo em inglês apenas termos técnicos, nomes de serviços AWS, nomes de parâmetros, nomes de variáveis e comandos.
7. IF qualquer um dos arquivos `ARQUITETURA.md`, `IMPLANTACAO.md`, `README.md` ou `TESTES.md` estiver ausente ou não contiver todas as seções obrigatórias definidas neste requisito, THEN THE documentação do projeto SHALL ser considerada incompleta.

### Requisito 18: Roteiro de testes

**User Story:** Como estudante de SAA, quero um roteiro de testes estruturado, para que eu valide cada comportamento do laboratório de forma reproduzível.

#### Critérios de Aceitação

1. THE arquivo `TESTES.md` SHALL conter um teste de fila Standard cujo critério de aprovação seja o envio de ao menos 1 mensagem e sua recepção e exclusão pelo consumidor sem erro reportado.
2. THE arquivo `TESTES.md` SHALL conter um teste de processamento com expiração do Visibility_Timeout que especifique o valor do Visibility_Timeout em segundos e cujo critério de aprovação seja a reentrega da mesma mensagem à fila após decorrido esse intervalo sem a exclusão da mensagem.
3. THE arquivo `TESTES.md` SHALL conter um teste de encaminhamento para DLQ que especifique o valor de maxReceiveCount e cujo critério de aprovação seja a presença da mensagem na DLQ após o número de recebimentos atingir esse limite.
4. THE arquivo `TESTES.md` SHALL conter um teste de ordenação em fila FIFO cujo critério de aprovação seja a recepção de ao menos 3 mensagens, dentro do mesmo MessageGroupId, na mesma ordem em que foram enviadas.
5. THE arquivo `TESTES.md` SHALL conter um teste de concorrência que especifique o número de consumidores simultâneos (mínimo de 2) e cujo critério de aprovação seja o processamento de cada mensagem por exatamente um consumidor, sem duplicidade dentro do Visibility_Timeout.
6. THE arquivo `TESTES.md` SHALL conter um teste de recuperação de mensagens cujo critério de aprovação seja a reaparição de ao menos 1 mensagem na fila após o consumidor encerrar sem excluí-la.
7. THE arquivo `TESTES.md` SHALL descrever, para cada teste, os seguintes campos obrigatórios: objetivo, pré-requisitos, configuração, passo a passo numerado, resultado esperado, evidências, campo para resultado real e critério de aprovação.
8. IF um teste descrito no arquivo `TESTES.md` não contiver todos os 8 campos obrigatórios definidos no critério 7, THEN THE roteiro de testes SHALL ser considerado reprovado na verificação de completude.
