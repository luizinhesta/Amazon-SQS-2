# Guia de Implantação Manual — Laboratório Amazon SQS

Este documento descreve, passo a passo e em ordem sequencial, como criar **manualmente no Console AWS** toda a infraestrutura do laboratório didático de Amazon SQS e como instalar e operar a aplicação na instância EC2. Todos os recursos são criados pelo próprio usuário no Console AWS; a aplicação **nunca cria, modifica ou exclui recursos AWS** e **não utiliza Infrastructure as Code** (Terraform, CloudFormation ou CDK).

- **Região de todos os recursos:** `us-east-1` (Norte da Virgínia).
- **Autenticação com a AWS:** exclusivamente pela IAM_Role_EC2 anexada à instância EC2 (credenciais temporárias, sem Access Keys fixas).
- **Administração da EC2:** apenas via AWS Systems Manager Session Manager, com a porta `22` (SSH) **fechada**.
- **Idioma:** o texto é em português do Brasil; nomes de serviços, parâmetros, recursos e comandos permanecem em inglês.

## Nomes reais dos recursos

Os nomes abaixo são os valores reais usados pela aplicação (definidos em `app/dominio/constantes.py`). Crie os recursos exatamente com estes nomes, caso contrário a verificação de inicialização falhará.

| Recurso (Glossário) | Nome real no Console AWS | Tipo |
|---------------------|--------------------------|------|
| Fila_Principal_Standard | `lab-sqs-standard` | SQS Standard |
| DLQ_Standard | `lab-sqs-standard-dlq` | SQS Standard |
| Fila_Principal_FIFO | `lab-sqs-fifo.fifo` | SQS FIFO |
| DLQ_FIFO | `lab-sqs-fifo-dlq.fifo` | SQS FIFO |
| Tabela_Historico | `lab-sqs-historico` | DynamoDB |
| IAM_Role_EC2 | `IAM_Role_EC2` (sugerido) | IAM Role |
| Instância EC2 | `lab-sqs-ec2` (sugerido) | EC2 |

Valores de Config_Aplicada (iguais aos de `app/dominio/constantes.py`):

- Visibility_Timeout: `30` segundos (filas principais).
- maxReceiveCount da Redrive_Policy: `2`.
- Long_Polling (ReceiveMessageWaitTimeSeconds): `20` segundos.
- Retenção das filas principais: `4` dias.
- Retenção das DLQs: `14` dias.
- SSE_SQS (criptografia em repouso): habilitada em todas as filas.
- Deduplicação por conteúdo: habilitada nas filas FIFO.

> **Antes de começar:** confirme no canto superior direito do Console AWS que a região selecionada é **US East (N. Virginia) us-east-1**. Todos os passos abaixo pressupõem essa região.

---

## Parte 1 — Criar as filas SQS

A ordem recomendada é criar primeiro as DLQs e depois as filas principais, porque a Redrive_Policy da fila principal precisa referenciar o ARN de uma DLQ já existente.

### Etapa 1 — Criar a DLQ_Standard (`lab-sqs-standard-dlq`)

1. Acesse o serviço **Amazon SQS** → **Create queue**.
2. Em **Type**, selecione **Standard**.
3. Em **Name**, informe `lab-sqs-standard-dlq`.
4. Em **Configuration**, defina **Message retention period** = `14 days`.
5. Em **Encryption**, selecione **Amazon SQS key (SSE-SQS)**.
6. Deixe o restante com os valores padrão e clique em **Create queue**.
- **Resultado esperado:** a fila `lab-sqs-standard-dlq` aparece na lista com SSE-SQS habilitado e retenção de 14 dias. Anote o **ARN** exibido nos detalhes da fila.

### Etapa 2 — Criar a Fila_Principal_Standard (`lab-sqs-standard`)

1. **Create queue** → **Type** = **Standard**.
2. **Name** = `lab-sqs-standard`.
3. Em **Configuration**:
   - **Visibility timeout** = `30 seconds`.
   - **Message retention period** = `4 days`.
   - **Receive message wait time** = `20 seconds` (Long_Polling).
4. Em **Encryption**, selecione **Amazon SQS key (SSE-SQS)**.
5. Em **Dead-letter queue**, selecione **Enabled**, escolha `lab-sqs-standard-dlq` e defina **Maximum receives** = `2`.
6. Clique em **Create queue**.
- **Resultado esperado:** a fila `lab-sqs-standard` aparece na lista com Visibility timeout 30 s, Long_Polling 20 s, SSE-SQS e Redrive_Policy apontando para `lab-sqs-standard-dlq` com maxReceiveCount 2.

### Etapa 3 — Criar a DLQ_FIFO (`lab-sqs-fifo-dlq.fifo`)

1. **Create queue** → **Type** = **FIFO**.
2. **Name** = `lab-sqs-fifo-dlq.fifo` (o sufixo `.fifo` é obrigatório).
3. **Message retention period** = `14 days`.
4. Em **Encryption**, selecione **Amazon SQS key (SSE-SQS)**.
5. Em **FIFO queue settings**, marque **Content-based deduplication** = **Enabled**.
6. Clique em **Create queue**.
- **Resultado esperado:** a fila `lab-sqs-fifo-dlq.fifo` aparece na lista como FIFO com SSE-SQS e retenção de 14 dias. Anote o **ARN**.

### Etapa 4 — Criar a Fila_Principal_FIFO (`lab-sqs-fifo.fifo`)

1. **Create queue** → **Type** = **FIFO**.
2. **Name** = `lab-sqs-fifo.fifo`.
3. Em **Configuration**:
   - **Visibility timeout** = `30 seconds`.
   - **Message retention period** = `4 days`.
   - **Receive message wait time** = `20 seconds`.
4. Em **Encryption**, selecione **Amazon SQS key (SSE-SQS)**.
5. Em **FIFO queue settings**, marque **Content-based deduplication** = **Enabled**.
6. Em **Dead-letter queue**, selecione **Enabled**, escolha `lab-sqs-fifo-dlq.fifo` e defina **Maximum receives** = `2`.
7. Clique em **Create queue**.
- **Resultado esperado:** a fila `lab-sqs-fifo.fifo` aparece como FIFO com Visibility timeout 30 s, Long_Polling 20 s, SSE-SQS, deduplicação por conteúdo e Redrive_Policy para `lab-sqs-fifo-dlq.fifo` com maxReceiveCount 2.

> **Tratamento de falha (Parte 1):** se não for possível selecionar a DLQ ao criar a fila principal, confirme que a DLQ foi criada **no mesmo tipo** (Standard com Standard, FIFO com FIFO) e **na mesma região**. SQS exige que a DLQ seja do mesmo tipo da fila principal. Corrija a DLQ antes de prosseguir.

---

## Parte 2 — Criar a Tabela_Historico no DynamoDB

A Tabela_Historico é um **log de eventos append-only**. O modelo de evento da aplicação (`app/dominio/evento.py`) usa a chave composta `PK`/`SK` e um índice secundário global `GSI1` para consulta por fila. As três consultas exigidas são: por identificador de pedido (`PK`), por fila (`GSI1`) e listagem cronológica (ordenação pela `SK`).

### Etapa 5 — Criar a tabela `lab-sqs-historico`

1. Acesse o serviço **DynamoDB** → **Tables** → **Create table**.
2. **Table name** = `lab-sqs-historico`.
3. **Partition key** = `PK` (tipo **String**).
4. **Sort key** = `SK` (tipo **String**).
5. Em **Table settings**, escolha **Customize settings**.
6. Em **Table class**, mantenha **DynamoDB Standard**.
7. Em **Capacity mode**, selecione **On-demand** (PAY_PER_REQUEST), adequado à carga variável de laboratório.
8. Em **Secondary indexes**, clique em **Create global index** e configure:
   - **Partition key** = `GSI1PK` (tipo **String**).
   - **Sort key** = `GSI1SK` (tipo **String**).
   - **Index name** = `GSI1`.
   - **Attribute projections** = **All**.
9. Em **Encryption at rest**, mantenha a chave gerenciada (**Owned by Amazon DynamoDB** ou **AWS managed key**).
10. Clique em **Create table**.
- **Resultado esperado:** a tabela `lab-sqs-historico` fica com status **Active**, chave `PK`/`SK` e o índice `GSI1` com chave `GSI1PK`/`GSI1SK`.

> **Observação sobre atributos:** no DynamoDB só se declaram os atributos usados em chaves (`PK`, `SK`, `GSI1PK`, `GSI1SK`). Os demais atributos do item de evento (`order_id`, `queue_name`, `event_type`, `event_source`, `app_time`, `app_time_ms`, `consumer_id`, `receive_count`, `processing_seconds`, `message_group_id`, `sqs_message_id`, `error_detail`) são gravados dinamicamente pela aplicação e **não** precisam ser declarados na criação da tabela.

> **Tratamento de falha (Parte 2):** se a criação do índice falhar após a tabela já existir, aguarde a tabela sair de **Updating** e crie o `GSI1` em **Indexes** → **Create index**. Confirme que os nomes dos atributos-chave são exatamente `GSI1PK` e `GSI1SK`.

---

## Parte 3 — Criar a IAM_Role_EC2 com permissões mínimas

A aplicação se autentica apenas pela IAM Role anexada à EC2 (sem Access Keys). A role precisa de permissões mínimas de SQS (nas quatro filas) e de DynamoDB (na Tabela_Historico e no índice `GSI1`), além da permissão gerenciada que habilita o Session Manager.

### Etapa 6 — Criar a policy de permissões mínimas

1. Acesse **IAM** → **Policies** → **Create policy** → aba **JSON**.
2. Cole a policy abaixo, substituindo `<ACCOUNT_ID>` pelo ID da sua conta AWS:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AcessoFilasSQS",
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueUrl",
        "sqs:GetQueueAttributes"
      ],
      "Resource": [
        "arn:aws:sqs:us-east-1:<ACCOUNT_ID>:lab-sqs-standard",
        "arn:aws:sqs:us-east-1:<ACCOUNT_ID>:lab-sqs-standard-dlq",
        "arn:aws:sqs:us-east-1:<ACCOUNT_ID>:lab-sqs-fifo.fifo",
        "arn:aws:sqs:us-east-1:<ACCOUNT_ID>:lab-sqs-fifo-dlq.fifo"
      ]
    },
    {
      "Sid": "AcessoTabelaHistorico",
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:Query",
        "dynamodb:DescribeTable"
      ],
      "Resource": [
        "arn:aws:dynamodb:us-east-1:<ACCOUNT_ID>:table/lab-sqs-historico",
        "arn:aws:dynamodb:us-east-1:<ACCOUNT_ID>:table/lab-sqs-historico/index/GSI1"
      ]
    }
  ]
}
```

3. Clique em **Next**, dê o nome `lab-sqs-policy-minima` e clique em **Create policy**.
- **Resultado esperado:** a policy `lab-sqs-policy-minima` concede apenas enviar, receber, excluir e consultar atributos nas quatro filas e ler/gravar/consultar a Tabela_Historico e seu índice `GSI1`, sem privilégios administrativos.

### Etapa 7 — Criar a IAM_Role_EC2

1. Acesse **IAM** → **Roles** → **Create role**.
2. Em **Trusted entity type**, selecione **AWS service** e, em **Use case**, escolha **EC2**.
3. Em **Add permissions**, anexe:
   - a policy `lab-sqs-policy-minima` criada na Etapa 6;
   - a policy gerenciada **`AmazonSSMManagedInstanceCore`** (necessária para o Session Manager).
4. Em **Role name**, informe `IAM_Role_EC2` e clique em **Create role**.
- **Resultado esperado:** a role `IAM_Role_EC2` existe com as duas policies anexadas e com a relação de confiança para o serviço `ec2.amazonaws.com`.

> **Tratamento de falha (Parte 3):** se a aplicação reportar `AccessDenied` ao acessar SQS ou DynamoDB, confirme o `<ACCOUNT_ID>` e os nomes exatos dos recursos nos ARNs. Se o Session Manager não conectar, verifique se `AmazonSSMManagedInstanceCore` está anexada à role.

---

## Parte 4 — Criar os Security Groups e a instância EC2

A arquitetura **não usa NAT Gateway nem Elastic Load Balancer** (para reduzir custo). A porta `22` (SSH) permanece **sem regra de entrada**; a administração ocorre pelo Session Manager.

### Etapa 8 — Criar o Security Group

1. Acesse **EC2** → **Security Groups** → **Create security group**.
2. **Security group name** = `lab-sqs-sg`; informe uma descrição em inglês (ex.: `Lab SQS web access`).
3. **VPC**: selecione a VPC padrão da região.
4. Em **Inbound rules**, adicione **apenas**:
   - **Type** = **Custom TCP**, **Port range** = `8000`, **Source** = seu IP (ex.: `Meu IP`/`My IP`) ou a faixa de origens autorizadas. Essa é a porta em que a aplicação é servida pelo `uvicorn`.
   - **Não** adicione regra para a porta `22`.
5. Em **Outbound rules**, mantenha a regra padrão **All traffic** para `0.0.0.0/0` (necessária para a EC2 alcançar SQS, DynamoDB e os endpoints do SSM).
6. Clique em **Create security group**.
- **Resultado esperado:** o Security Group `lab-sqs-sg` permite entrada apenas na porta `8000` a partir das origens autorizadas, sem porta `22` aberta, e permite saída total.

### Etapa 9 — Criar a instância EC2

1. Acesse **EC2** → **Instances** → **Launch instances**.
2. **Name** = `lab-sqs-ec2`.
3. **Application and OS Images (AMI)**: selecione **Amazon Linux 2023** (x86_64).
4. **Instance type**: `t3.micro` (ou `t2.micro` se elegível ao Free Tier).
5. **Key pair**: selecione **Proceed without a key pair** — não haverá acesso por SSH; a administração é via Session Manager.
6. Em **Network settings** → **Edit**:
   - selecione a mesma VPC do Security Group;
   - **Auto-assign public IP** = **Enable** (para que o navegador alcance a porta 8000);
   - em **Firewall (security groups)**, selecione **Select existing security group** e escolha `lab-sqs-sg`.
7. Em **Advanced details** → **IAM instance profile**, selecione `IAM_Role_EC2`.
8. Clique em **Launch instance**.
- **Resultado esperado:** a instância `lab-sqs-ec2` entra em estado **Running**, com IP público, usando o perfil `IAM_Role_EC2` e o Security Group `lab-sqs-sg`.

> **Tratamento de falha (Parte 4):** se a instância não assumir a role, confirme o campo **IAM instance profile**. Caso o IP público não apareça, verifique **Auto-assign public IP** e as rotas da subnet (deve ter rota de saída para a internet via Internet Gateway).

---

## Parte 5 — Configurar o AWS Systems Manager Session Manager

### Etapa 10 — Conectar via Session Manager (porta 22 fechada)

1. Pré-requisitos já atendidos: a role `IAM_Role_EC2` tem `AmazonSSMManagedInstanceCore` (Etapa 7) e a EC2 tem saída para a internet (Etapa 8/9). A AMI Amazon Linux 2023 já traz o **SSM Agent** pré-instalado.
2. Acesse **Systems Manager** → **Fleet Manager** (ou **Session Manager** → **Managed nodes**) e confirme que `lab-sqs-ec2` aparece como **Managed node** (pode levar alguns minutos após o boot).
3. Acesse **EC2** → **Instances** → selecione `lab-sqs-ec2` → **Connect** → aba **Session Manager** → **Connect**.
- **Resultado esperado:** abre um terminal web da instância **sem usar SSH**. A porta `22` permanece fechada no Security Group.

> **Tratamento de falha (Parte 5):** se a instância não aparecer como Managed node, verifique: (a) a role anexada tem `AmazonSSMManagedInstanceCore`; (b) a instância tem rota de saída para a internet (os endpoints do SSM são acessados por HTTPS); (c) aguarde o registro do SSM Agent concluir.

---

## Parte 6 — Instalar e executar a aplicação na EC2

Execute os comandos abaixo no terminal do **Session Manager**. Cada comando é explicado: o que faz, por que é executado e o resultado esperado.

### Etapa 11 — Preparar o ambiente

1. Elevar para o usuário padrão e ir ao diretório home:

```bash
sudo su - ec2-user
```

- **O que faz:** troca para o usuário `ec2-user` (a sessão do Session Manager inicia como `ssm-user`). **Por quê:** manter os arquivos e o serviço sob um usuário consistente. **Resultado esperado:** o prompt passa a indicar `ec2-user`.

2. Atualizar os pacotes do sistema:

```bash
sudo dnf update -y
```

- **O que faz:** atualiza os pacotes do Amazon Linux 2023 (que usa o gerenciador `dnf`). **Por quê:** aplicar correções antes de instalar dependências. **Resultado esperado:** termina com `Complete!`.

3. Instalar Python, `pip`, `venv` e `git`:

```bash
sudo dnf install -y python3 python3-pip git
```

- **O que faz:** instala o interpretador Python 3, o `pip`, o módulo de ambientes virtuais e o `git`. **Por quê:** a aplicação requer Python 3.10+ (ver `pyproject.toml`) e `git` para clonar o repositório. **Resultado esperado:** `Complete!`; `python3 --version` deve mostrar 3.10 ou superior.

### Etapa 12 — Obter o código da aplicação

```bash
git clone <URL_DO_REPOSITORIO> amazon-sqs2
cd amazon-sqs2
```

- **O que faz:** clona o repositório do laboratório e entra no diretório. Substitua `<URL_DO_REPOSITORIO>` pela URL real do seu repositório. **Por quê:** trazer o código (`app/`, `web/`, `requirements.txt`) para a EC2. **Resultado esperado:** o diretório `amazon-sqs2` contém as pastas `app/` e `web/` e o arquivo `requirements.txt`.

### Etapa 13 — Criar o ambiente virtual e instalar as dependências

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

- **O que faz:** cria um ambiente virtual isolado em `.venv`, ativa-o, atualiza o `pip` e instala as dependências de `requirements.txt` (`fastapi`, `uvicorn[standard]`, `boto3` e as de teste). **Por quê:** isolar as dependências do laboratório do Python do sistema. **Resultado esperado:** `pip` conclui a instalação sem erros; `pip list` mostra `fastapi`, `uvicorn` e `boto3`.

### Etapa 14 — Iniciar a aplicação

A aplicação é um único processo que serve a API_Backend, o Dashboard estático e inicia os consumidores Standard e FIFO (ponto de entrada `app/main.py`, objeto `app`).

Para um teste inicial em primeiro plano:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

- **O que faz:** inicia o servidor ASGI `uvicorn` servindo a aplicação na porta `8000`, acessível por qualquer origem autorizada no Security Group (`--host 0.0.0.0`). **Por quê:** é a forma de subir a API, o Dashboard e os consumidores. **Resultado esperado:** linhas de log do `uvicorn` indicando `Application startup complete` e `Uvicorn running on http://0.0.0.0:8000`. A aplicação faz a verificação de inicialização dos recursos AWS; se algum recurso estiver ausente, o Dashboard retorna HTTP 503 com a causa em pt-BR (o endpoint `/api/saude` continua respondendo).

Para manter a aplicação rodando após fechar a sessão, use um serviço `systemd`. Crie o arquivo com o editor da sessão:

```bash
sudo tee /etc/systemd/system/lab-sqs.service > /dev/null <<'EOF'
[Unit]
Description=Laboratorio Amazon SQS
After=network.target

[Service]
User=ec2-user
WorkingDirectory=/home/ec2-user/amazon-sqs2
ExecStart=/home/ec2-user/amazon-sqs2/.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
```

- **O que faz:** cria uma unit `systemd` que executa o `uvicorn` sob o `ec2-user`, no diretório do projeto, reiniciando em falha. **Por quê:** manter a aplicação ativa de forma gerenciada, sobrevivendo a reboots e a desconexões do Session Manager. **Resultado esperado:** o arquivo `/etc/systemd/system/lab-sqs.service` é criado.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now lab-sqs.service
sudo systemctl status lab-sqs.service
```

- **O que faz:** recarrega as units do `systemd`, habilita e inicia o serviço, e mostra seu status. **Por quê:** ativar o serviço e confirmar que subiu. **Resultado esperado:** status **active (running)**.

> **Tratamento de falha (Parte 6):** se o `uvicorn` encerrar com erro de credenciais da AWS, confirme que a EC2 está com a `IAM_Role_EC2` anexada (Etapa 9). Se o `systemctl status` indicar falha, veja os logs com `journalctl -u lab-sqs.service -n 50`. Se faltar algum recurso AWS, o `/api/saude` identifica o recurso ausente por nome e tipo.

---

## Parte 7 — Verificação pós-implantação

### Etapa 15 — Validar que a aplicação e os recursos subiram

1. No navegador (de uma origem autorizada), acesse o endpoint de saúde:

```
http://<IP_PUBLICO_DA_EC2>:8000/api/saude
```

- **O que valida:** a verificação de inicialização confirma a existência e a acessibilidade das quatro filas SQS e da Tabela_Historico via IAM Role. **Resultado esperado:** JSON com `"saudavel": true` e a lista `recursos` com `"disponivel": true` para `lab-sqs-standard`, `lab-sqs-standard-dlq`, `lab-sqs-fifo.fifo`, `lab-sqs-fifo-dlq.fifo` e `lab-sqs-historico`. Se algum estiver indisponível, o campo `mensagem` identifica o recurso por nome e tipo.

2. Acesse o Dashboard:

```
http://<IP_PUBLICO_DA_EC2>:8000/
```

- **Resultado esperado:** o Dashboard em pt-BR é exibido (tela de login e as cinco áreas: visão geral, envio, monitoramento, histórico e laboratório de falhas). Se a verificação tiver falhado, a página retorna HTTP 503 com a causa em pt-BR.

3. Confirme os valores de Config_Aplicada no Console AWS:
   - **SQS** → cada fila: Visibility timeout `30 s` (principais), Long_Polling `20 s`, SSE-SQS habilitado, retenção `4 dias` (principais) / `14 dias` (DLQs), Redrive_Policy com maxReceiveCount `2`, deduplicação por conteúdo nas FIFO.
   - **DynamoDB** → `lab-sqs-historico`: status **Active**, chave `PK`/`SK`, índice `GSI1`.
   - **IAM** → `IAM_Role_EC2`: policies anexadas.
   - **EC2** → `lab-sqs-ec2`: estado **Running**, perfil `IAM_Role_EC2`, Security Group `lab-sqs-sg` sem porta 22.

4. Teste funcional rápido: no Dashboard, envie uma mensagem para a fila Standard e verifique na área de monitoramento que a mensagem é recebida e processada; confirme que apareceram itens na tabela `lab-sqs-historico` do DynamoDB.
- **Resultado esperado:** a mensagem percorre o ciclo (enviada → recebida → processada) e o histórico registra os eventos.

> **Tratamento de falha (Parte 7):** `saudavel: false` indica recurso ausente/inacessível — corrija o recurso indicado no Console AWS e recarregue. Dashboard inacessível pelo navegador costuma ser regra de entrada da porta `8000` no Security Group ou origem (IP) não autorizada.

---

## Parte 8 — Excluir todos os recursos ao final (evitar custo)

Ao encerrar o laboratório, **exclua todos os recursos** abaixo. A ordem respeita as dependências entre eles. Após a exclusão, confirme que nenhum recurso remanescente permanece ativo.

### Etapa 16 — Lista nominal de exclusão (nesta ordem)

1. **Instância EC2 `lab-sqs-ec2`** — EC2 → Instances → selecionar → **Instance state** → **Terminate instance**. (Exclua a EC2 antes do Security Group, pois o SG está em uso pela instância.)
2. **Security Group `lab-sqs-sg`** — EC2 → Security Groups → selecionar → **Actions** → **Delete security group** (só é possível após a EC2 terminar).
3. **Fila_Principal_Standard `lab-sqs-standard`** — SQS → selecionar → **Delete**.
4. **DLQ_Standard `lab-sqs-standard-dlq`** — SQS → selecionar → **Delete**.
5. **Fila_Principal_FIFO `lab-sqs-fifo.fifo`** — SQS → selecionar → **Delete**.
6. **DLQ_FIFO `lab-sqs-fifo-dlq.fifo`** — SQS → selecionar → **Delete**.
7. **Tabela_Historico `lab-sqs-historico`** — DynamoDB → Tables → selecionar → **Delete** (o índice `GSI1` é removido junto com a tabela).
8. **IAM_Role_EC2 `IAM_Role_EC2`** — IAM → Roles → selecionar → **Delete**.
9. **Policy `lab-sqs-policy-minima`** — IAM → Policies → selecionar → **Delete** (após a role que a usa ser excluída).

### Etapa 17 — Confirmar ausência de recursos remanescentes

- Verifique em cada serviço (EC2, SQS, DynamoDB, IAM) que nenhum dos recursos acima aparece mais nas listas da região `us-east-1`.
- **Resultado esperado:** nenhum recurso da lista permanece ativo. Se algum permanecer (ex.: Security Group que não pôde ser excluído porque a EC2 ainda não terminou), repita a exclusão na ordem correta e registre o nome do recurso remanescente até removê-lo.

---

## Custo mensal estimado (USD)

Valores aproximados para a região `us-east-1`, considerando uso leve e descartável de laboratório. São estimativas de referência; os preços oficiais estão na calculadora e nas páginas de preços da AWS.

| Recurso | Premissa de uso | Custo mensal estimado (USD) |
|---------|-----------------|-----------------------------|
| EC2 `t3.micro` (On-Demand, Linux) | ~730 h/mês ligada; `us-east-1` | ~`7.50` (0 no Free Tier com `t2.micro`/`t3.micro` por 750 h) |
| EBS (volume raiz gp3, 8 GB) | 8 GB persistentes | ~`0.64` |
| Amazon SQS (4 filas) | Volume de laboratório, muito abaixo de 1 milhão de requisições/mês | ~`0.00` (1 milhão de requisições/mês no Free Tier) |
| DynamoDB `lab-sqs-historico` (On-Demand) | Poucos milhares de escritas/leituras no mês | ~`0.00`–`0.50` (dentro ou perto do Free Tier) |
| IAM Role / Security Group | Sem cobrança | `0.00` |
| Session Manager (SSM) | Sem cobrança para gerência de nós | `0.00` |
| **Total aproximado** | EC2 ligada o mês inteiro, sem Free Tier | **~`8` a `9` USD/mês** |

Observações de custo:
- A arquitetura **não** inclui NAT Gateway nem Elastic Load Balancer, justamente para evitar os maiores custos fixos de rede.
- O maior componente de custo é a EC2. Para reduzir, **pare** (stop) a instância quando não estiver usando (não há cobrança de computação enquanto parada, apenas do volume EBS) ou **termine** a instância ao final (Parte 8).
- Transferência de dados de saída tende a ficar dentro da franquia gratuita neste volume de laboratório.

---

## Resumo das etapas

1. Criar DLQ_Standard (`lab-sqs-standard-dlq`).
2. Criar Fila_Principal_Standard (`lab-sqs-standard`) com Redrive_Policy.
3. Criar DLQ_FIFO (`lab-sqs-fifo-dlq.fifo`).
4. Criar Fila_Principal_FIFO (`lab-sqs-fifo.fifo`) com Redrive_Policy.
5. Criar Tabela_Historico (`lab-sqs-historico`) com `GSI1`.
6. Criar a policy mínima (`lab-sqs-policy-minima`).
7. Criar a IAM_Role_EC2 (`IAM_Role_EC2`).
8. Criar o Security Group (`lab-sqs-sg`), porta 8000 restrita, porta 22 fechada.
9. Criar a instância EC2 (`lab-sqs-ec2`) com a role anexada.
10. Conectar via Session Manager.
11. Preparar o ambiente (Python, pip, git).
12. Clonar o código.
13. Criar o `venv` e instalar dependências.
14. Iniciar a aplicação (`uvicorn app.main:app`) e opcionalmente como serviço `systemd`.
15. Verificar pós-implantação (`/api/saude`, Dashboard, Console AWS).
16. Excluir todos os recursos na ordem indicada.
17. Confirmar ausência de recursos remanescentes.
