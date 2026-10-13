# Guia de Implantação Manual — Laboratório Amazon SQS

Este documento descreve, passo a passo e em ordem sequencial, como criar **manualmente no Console AWS** toda a infraestrutura do laboratório didático de Amazon SQS e como instalar e operar a aplicação na instância EC2. Todos os recursos são criados pelo próprio usuário no Console AWS; a aplicação **nunca cria, modifica ou exclui recursos AWS** e **não utiliza Infrastructure as Code** (Terraform, CloudFormation ou CDK).

- **Região de todos os recursos:** `us-east-1` (Norte da Virgínia).
- **Autenticação com a AWS:** exclusivamente pela IAM_Role_EC2 anexada à instância EC2 (credenciais temporárias, sem Access Keys fixas).
- **Administração da EC2:** apenas via AWS Systems Manager Session Manager, com a porta `22` (SSH) **fechada**.
- **Idioma:** este guia pressupõe o **Console AWS configurado em Português (Brasil)**. Os rótulos de menus e botões são apresentados em português do Brasil, com o termo original em inglês entre parênteses na primeira ocorrência, pois alguns rótulos podem variar conforme a versão do Console. Nomes de serviços, de recursos, parâmetros técnicos e comandos permanecem em inglês.

> **Dica — Mudar o idioma do Console para Português (Brasil):** role até o rodapé de qualquer página do Console AWS e, no seletor de idioma (canto inferior esquerdo), escolha **Português (Brasil)**.

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

> **Antes de começar:** confirme no canto superior direito do Console AWS que a regiao selecionada e **Leste dos EUA (Norte da Virginia) us-east-1** (*US East (N. Virginia)*). Todos os passos abaixo pressupõem essa região.

---

## Parte 1 — Criar as filas SQS

A ordem recomendada é criar primeiro as DLQs e depois as filas principais, porque a Redrive_Policy da fila principal precisa referenciar o ARN de uma DLQ já existente.

### Etapa 1 — Criar a DLQ_Standard (`lab-sqs-standard-dlq`)

1. Acesse o serviço **Amazon SQS** → **Criar fila** (*Create queue*).
2. Em **Tipo** (*Type*), selecione **Standard**.
3. Em **Nome** (*Name*), informe `lab-sqs-standard-dlq`.
4. Em **Configuração** (*Configuration*), defina **Período de retenção de mensagens** (*Message retention period*) = `14 dias` (*14 days*).
5. Em **Criptografia** (*Encryption*), selecione **Chave do Amazon SQS (SSE-SQS)** (*Amazon SQS key (SSE-SQS)*).
6. Deixe o restante com os valores padrão e clique em **Criar fila** (*Create queue*).
- **Resultado esperado:** a fila `lab-sqs-standard-dlq` aparece na lista com SSE-SQS habilitado e retenção de 14 dias. Anote o **ARN** exibido nos detalhes da fila.

### Etapa 2 — Criar a Fila_Principal_Standard (`lab-sqs-standard`)

1. **Criar fila** (*Create queue*) → **Tipo** (*Type*) = **Standard**.
2. **Nome** (*Name*) = `lab-sqs-standard`.
3. Em **Configuração** (*Configuration*):
   - **Tempo limite de visibilidade** (*Visibility timeout*) = `30 segundos` (*30 seconds*).
   - **Período de retenção de mensagens** (*Message retention period*) = `4 dias` (*4 days*).
   - **Tempo de espera para recebimento de mensagem** (*Receive message wait time*) = `20 segundos` (*20 seconds*) — Long_Polling.
4. Em **Criptografia** (*Encryption*), selecione **Chave do Amazon SQS (SSE-SQS)** (*Amazon SQS key (SSE-SQS)*).
5. Em **Fila de mensagens mortas** (*Dead-letter queue*), selecione **Habilitada** (*Enabled*), escolha `lab-sqs-standard-dlq` e defina **Máximo de recebimentos** (*Maximum receives*) = `2`.
6. Clique em **Criar fila** (*Create queue*).
- **Resultado esperado:** a fila `lab-sqs-standard` aparece na lista com Visibility timeout 30 s, Long_Polling 20 s, SSE-SQS e Redrive_Policy apontando para `lab-sqs-standard-dlq` com maxReceiveCount 2.

### Etapa 3 — Criar a DLQ_FIFO (`lab-sqs-fifo-dlq.fifo`)

1. **Criar fila** (*Create queue*) → **Tipo** (*Type*) = **FIFO**.
2. **Nome** (*Name*) = `lab-sqs-fifo-dlq.fifo` (o sufixo `.fifo` é obrigatório).
3. **Período de retenção de mensagens** (*Message retention period*) = `14 dias` (*14 days*).
4. Em **Criptografia** (*Encryption*), selecione **Chave do Amazon SQS (SSE-SQS)** (*Amazon SQS key (SSE-SQS)*).
5. Em **Configurações de fila FIFO** (*FIFO queue settings*), marque **Desduplicação baseada em conteúdo** (*Content-based deduplication*) = **Habilitada** (*Enabled*).
6. Clique em **Criar fila** (*Create queue*).
- **Resultado esperado:** a fila `lab-sqs-fifo-dlq.fifo` aparece na lista como FIFO com SSE-SQS e retenção de 14 dias. Anote o **ARN**.

### Etapa 4 — Criar a Fila_Principal_FIFO (`lab-sqs-fifo.fifo`)

1. **Criar fila** (*Create queue*) → **Tipo** (*Type*) = **FIFO**.
2. **Nome** (*Name*) = `lab-sqs-fifo.fifo`.
3. Em **Configuração** (*Configuration*):
   - **Tempo limite de visibilidade** (*Visibility timeout*) = `30 segundos` (*30 seconds*).
   - **Período de retenção de mensagens** (*Message retention period*) = `4 dias` (*4 days*).
   - **Tempo de espera para recebimento de mensagem** (*Receive message wait time*) = `20 segundos` (*20 seconds*).
4. Em **Criptografia** (*Encryption*), selecione **Chave do Amazon SQS (SSE-SQS)** (*Amazon SQS key (SSE-SQS)*).
5. Em **Configurações de fila FIFO** (*FIFO queue settings*), marque **Desduplicação baseada em conteúdo** (*Content-based deduplication*) = **Habilitada** (*Enabled*).
6. Em **Fila de mensagens mortas** (*Dead-letter queue*), selecione **Habilitada** (*Enabled*), escolha `lab-sqs-fifo-dlq.fifo` e defina **Máximo de recebimentos** (*Maximum receives*) = `2`.
7. Clique em **Criar fila** (*Create queue*).
- **Resultado esperado:** a fila `lab-sqs-fifo.fifo` aparece como FIFO com Visibility timeout 30 s, Long_Polling 20 s, SSE-SQS, deduplicação por conteúdo e Redrive_Policy para `lab-sqs-fifo-dlq.fifo` com maxReceiveCount 2.

> **Tratamento de falha (Parte 1):** se não for possível selecionar a DLQ ao criar a fila principal, confirme que a DLQ foi criada **no mesmo tipo** (Standard com Standard, FIFO com FIFO) e **na mesma região**. SQS exige que a DLQ seja do mesmo tipo da fila principal. Corrija a DLQ antes de prosseguir.

---

## Parte 2 — Criar a Tabela_Historico no DynamoDB

A Tabela_Historico é um **log de eventos append-only**. O modelo de evento da aplicação (`app/dominio/evento.py`) usa a chave composta `PK`/`SK` e um índice secundário global `GSI1` para consulta por fila. As três consultas exigidas são: por identificador de pedido (`PK`), por fila (`GSI1`) e listagem cronológica (ordenação pela `SK`).

### Etapa 5 — Criar a tabela `lab-sqs-historico`

1. Acesse o serviço **DynamoDB** → **Tabelas** (*Tables*) → **Criar tabela** (*Create table*).
2. **Nome da tabela** (*Table name*) = `lab-sqs-historico`.
3. **Chave de partição** (*Partition key*) = `PK` (tipo **String**).
4. **Chave de classificação** (*Sort key*) = `SK` (tipo **String**).
5. Em **Configurações da tabela** (*Table settings*), escolha **Personalizar configurações** (*Customize settings*).
6. Em **Classe da tabela** (*Table class*), mantenha **DynamoDB Standard**.
7. Em **Modo de capacidade** (*Capacity mode*), selecione **Sob demanda** (*On-demand*) — PAY_PER_REQUEST, adequado à carga variável de laboratório.
8. Em **Índices secundários** (*Secondary indexes*), clique em **Criar índice global** (*Create global index*) e configure:
   - **Chave de partição** (*Partition key*) = `GSI1PK` (tipo **String**).
   - **Chave de classificação** (*Sort key*) = `GSI1SK` (tipo **String**).
   - **Nome do índice** (*Index name*) = `GSI1`.
   - **Projeções de atributos** (*Attribute projections*) = **Tudo** (*All*).
9. Em **Encryption at rest**, mantenha a chave gerenciada (**Owned by Amazon DynamoDB** ou **AWS managed key**).
10. Clique em **Create table**.
- **Resultado esperado:** a tabela `lab-sqs-historico` fica com status **Ativa** (*Active*), chave `PK`/`SK` e o índice `GSI1` com chave `GSI1PK`/`GSI1SK`.

> **Observação sobre atributos:** no DynamoDB só se declaram os atributos usados em chaves (`PK`, `SK`, `GSI1PK`, `GSI1SK`). Os demais atributos do item de evento (`order_id`, `queue_name`, `event_type`, `event_source`, `app_time`, `app_time_ms`, `consumer_id`, `receive_count`, `processing_seconds`, `message_group_id`, `sqs_message_id`, `error_detail`) são gravados dinamicamente pela aplicação e **não** precisam ser declarados na criação da tabela.

> **Tratamento de falha (Parte 2):** se a criação do índice falhar após a tabela já existir, aguarde a tabela sair de **Atualizando** (*Updating*) e crie o `GSI1` em **Índices** (*Indexes*) → **Criar índice** (*Create index*). Confirme que os nomes dos atributos-chave são exatamente `GSI1PK` e `GSI1SK`.

---

## Parte 3 — Criar a IAM_Role_EC2 com permissões mínimas

A aplicação se autentica apenas pela IAM Role anexada à EC2 (sem Access Keys). A role precisa de permissões mínimas de SQS (nas quatro filas) e de DynamoDB (na Tabela_Historico e no índice `GSI1`), além da permissão gerenciada que habilita o Session Manager.

### Etapa 6 — Criar a policy de permissões mínimas

1. Acesse **IAM** → **Políticas** (*Policies*) → **Criar política** (*Create policy*) → aba **JSON**.
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

3. Clique em **Avançar** (*Next*), dê o nome `lab-sqs-policy-minima` e clique em **Criar política** (*Create policy*).
- **Resultado esperado:** a policy `lab-sqs-policy-minima` concede apenas enviar, receber, excluir e consultar atributos nas quatro filas e ler/gravar/consultar a Tabela_Historico e seu índice `GSI1`, sem privilégios administrativos.

### Etapa 7 — Criar a IAM_Role_EC2

1. Acesse **IAM** → **Perfis** (*Roles*) → **Criar perfil** (*Create role*).
2. Em **Tipo de entidade confiável** (*Trusted entity type*), selecione **Serviço da AWS** (*AWS service*) e, em **Caso de uso** (*Use case*), escolha **EC2**.
3. Em **Adicionar permissões** (*Add permissions*), anexe:
   - a policy `lab-sqs-policy-minima` criada na Etapa 6;
   - a política gerenciada **`AmazonSSMManagedInstanceCore`** (necessária para o Session Manager).
4. Em **Nome do perfil** (*Role name*), informe `IAM_Role_EC2` e clique em **Criar perfil** (*Create role*).
- **Resultado esperado:** a role `IAM_Role_EC2` existe com as duas policies anexadas e com a relação de confiança para o serviço `ec2.amazonaws.com`.

> **Tratamento de falha (Parte 3):** se a aplicação reportar `AccessDenied` ao acessar SQS ou DynamoDB, confirme o `<ACCOUNT_ID>` e os nomes exatos dos recursos nos ARNs. Se o Session Manager não conectar, verifique se `AmazonSSMManagedInstanceCore` está anexada à role.

---

## Parte 4 — Criar os Security Groups e a instância EC2

A arquitetura **não usa NAT Gateway nem Elastic Load Balancer** (para reduzir custo). A porta `22` (SSH) permanece **sem regra de entrada**; a administração ocorre pelo Session Manager.

### Etapa 8 — Criar o Security Group

1. Acesse **EC2** → **Grupos de segurança** (*Security Groups*) → **Criar grupo de segurança** (*Create security group*).
2. **Nome do grupo de segurança** (*Security group name*) = `lab-sqs-sg`; informe uma descrição (ex.: `Lab SQS web access`).
3. **VPC**: selecione a VPC padrão da região.
4. Em **Regras de entrada** (*Inbound rules*), adicione **apenas**:
   - **Tipo** (*Type*) = **TCP personalizado** (*Custom TCP*), **Intervalo de portas** (*Port range*) = `8000`, **Origem** (*Source*) = seu IP (ex.: **Meu IP** / *My IP*) ou a faixa de origens autorizadas. Essa é a porta em que a aplicação é servida pelo `uvicorn`.
   - **Não** adicione regra para a porta `22`.
5. Em **Regras de saída** (*Outbound rules*), mantenha a regra padrão **Todo o tráfego** (*All traffic*) para `0.0.0.0/0` (necessária para a EC2 alcançar SQS, DynamoDB e os endpoints do SSM).
6. Clique em **Criar grupo de segurança** (*Create security group*).
- **Resultado esperado:** o Security Group `lab-sqs-sg` permite entrada apenas na porta `8000` a partir das origens autorizadas, sem porta `22` aberta, e permite saída total.

### Etapa 9 — Criar a instância EC2

1. Acesse **EC2** → **Instâncias** (*Instances*) → **Executar instâncias** (*Launch instances*).
2. **Nome** (*Name*) = `lab-sqs-ec2`.
3. **Imagens de aplicações e sistema operacional (AMI)** (*Application and OS Images (AMI)*): selecione **Amazon Linux 2023** (x86_64).
4. **Tipo de instância** (*Instance type*): `t3.micro` (ou `t2.micro` se elegível ao Nível Gratuito / *Free Tier*).
5. **Par de chaves** (*Key pair*): selecione **Continuar sem um par de chaves** (*Proceed without a key pair*) — não haverá acesso por SSH; a administração é via Session Manager.
6. Em **Configurações de rede** (*Network settings*) → **Editar** (*Edit*):
   - selecione a mesma VPC do Security Group;
   - **Atribuir IP público automaticamente** (*Auto-assign public IP*) = **Habilitar** (*Enable*) — para que o navegador alcance a porta 8000;
   - em **Firewall (grupos de segurança)** (*Firewall (security groups)*), selecione **Selecionar grupo de segurança existente** (*Select existing security group*) e escolha `lab-sqs-sg`.
7. Em **Detalhes avançados** (*Advanced details*) → **Perfil de instância do IAM** (*IAM instance profile*), selecione `IAM_Role_EC2`.
8. Clique em **Executar instância** (*Launch instance*).
- **Resultado esperado:** a instância `lab-sqs-ec2` entra em estado **Em execução** (*Running*), com IP público, usando o perfil `IAM_Role_EC2` e o Security Group `lab-sqs-sg`.

> **Tratamento de falha (Parte 4):** se a instância não assumir a role, confirme o campo **Perfil de instância do IAM** (*IAM instance profile*). Caso o IP público não apareça, verifique **Atribuir IP público automaticamente** (*Auto-assign public IP*) e as rotas da subnet (deve ter rota de saída para a internet via Internet Gateway).

---

## Parte 5 — Configurar o AWS Systems Manager Session Manager

### Etapa 10 — Conectar via Session Manager (porta 22 fechada)

1. Pré-requisitos já atendidos: a role `IAM_Role_EC2` tem `AmazonSSMManagedInstanceCore` (Etapa 7) e a EC2 tem saída para a internet (Etapa 8/9). A AMI Amazon Linux 2023 já traz o **SSM Agent** pré-instalado.
2. Acesse **Systems Manager** → **Fleet Manager** (ou **Session Manager** → **Nós gerenciados** / *Managed nodes*) e confirme que `lab-sqs-ec2` aparece como **Nó gerenciado** (*Managed node*) — pode levar alguns minutos após o boot.
3. Acesse **EC2** → **Instâncias** (*Instances*) → selecione `lab-sqs-ec2` → **Conectar** (*Connect*) → aba **Session Manager** → **Conectar** (*Connect*).
- **Resultado esperado:** abre um terminal web da instância **sem usar SSH**. A porta `22` permanece fechada no Security Group.

> **Tratamento de falha (Parte 5):** se a instância não aparecer como nó gerenciado, verifique: (a) a role anexada tem `AmazonSSMManagedInstanceCore`; (b) a instância tem rota de saída para a internet (os endpoints do SSM são acessados por HTTPS); (c) aguarde o registro do SSM Agent concluir.

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
Description=Laboratório Amazon SQS
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

## Parte 6.1 — Aplicar a correção de resiliência FIFO na EC2 (sem reiniciar automaticamente)

Esta parte descreve como implantar a correção de resiliência dos consumidores (reordenação `delete`→`processada`, evento/estado `exclusao_nao_confirmada`, supervisão por slots, endpoint `GET /api/consumidores/saude`, faixas diferenciadas do Dashboard e extensão opt-in de Visibility_Timeout) em uma EC2 **já em produção**, com a aplicação em execução.

> **Princípio desta parte:** **não** reinicie o serviço automaticamente durante a análise. Primeiro faça **backup** e versione a mudança; depois **apresente as alterações e peça confirmação**; só então, em uma janela controlada, reinicie o serviço manualmente. Durante toda a correção: **não** recrie filas, **não** limpe o DynamoDB, **não** altere o `Visibility_Timeout` de 30 s nem a IAM, e **não** mude parâmetros de fila para mascarar o erro.

### Etapa 14.1 — Backup e versionamento antes de qualquer mudança

Execute no terminal do **Session Manager**, como `ec2-user`, dentro de `/home/ec2-user/amazon-sqs2`:

1. Registrar a revisão atualmente implantada (para rollback):

```bash
cd /home/ec2-user/amazon-sqs2
git rev-parse HEAD | tee /home/ec2-user/deploy-revisao-anterior.txt
```

- **O que faz:** grava o commit atualmente em execução em um arquivo. **Por quê:** permite voltar exatamente a esta versão se a correção apresentar problema. **Resultado esperado:** o arquivo `deploy-revisao-anterior.txt` contém o hash do commit atual.

2. Backup versionado do código e da configuração do serviço:

```bash
tar -czf /home/ec2-user/backup-amazon-sqs2-$(date +%Y%m%d-%H%M%S).tar.gz \
  -C /home/ec2-user amazon-sqs2
sudo cp /etc/systemd/system/lab-sqs.service \
  /home/ec2-user/lab-sqs.service.bak-$(date +%Y%m%d-%H%M%S)
```

- **O que faz:** cria um arquivo `.tar.gz` com o diretório do projeto e uma cópia da unit `systemd`, ambos com carimbo de data/hora. **Por quê:** backup imutável do estado anterior, independente do histórico Git. **Resultado esperado:** os dois arquivos de backup aparecem em `/home/ec2-user`.

> Como o laboratório **não** usa IaC, o versionamento da correção é feito pelo próprio Git (a correção vem em um branch/commit dedicado). Os recursos AWS **não** são alterados por esta implantação.

### Etapa 14.2 — Obter a correção sem reiniciar o serviço

1. Buscar a nova revisão sem aplicá-la ainda:

```bash
git fetch origin
git log --oneline origin/main -5
```

- **O que faz:** baixa as referências remotas e lista os últimos commits. **Por quê:** inspecionar a correção **antes** de aplicá-la. **Resultado esperado:** a lista mostra os commits da correção de resiliência.

2. Aplicar o código novo no diretório (o serviço em execução continua rodando o código antigo já carregado em memória — o `uvicorn` só passa a usar o código novo quando for reiniciado manualmente):

```bash
git checkout <BRANCH_OU_COMMIT_DA_CORRECAO>
git status
```

- **O que faz:** atualiza os arquivos do working tree para a revisão da correção. **Por quê:** deixar o código pronto no disco sem afetar o processo em execução. **Resultado esperado:** `git status` mostra o working tree limpo na revisão da correção. **Importante:** este passo **não** reinicia o serviço; o processo `uvicorn` ativo segue com o código anterior até o reinício manual (Etapa 14.4).

3. Atualizar dependências, se necessário (a correção **não** adiciona dependências novas — usa apenas a biblioteca padrão para o log estruturado):

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

- **O que faz:** garante que as dependências declaradas estão instaladas. **Por quê:** segurança; nada novo é esperado. **Resultado esperado:** `Requirement already satisfied` para os pacotes já presentes.

### Etapa 14.3 — Apresentar as alterações e pedir confirmação

Antes de reiniciar, revise e **apresente** as mudanças para aprovação:

```bash
git diff <REVISAO_ANTERIOR> HEAD --stat
```

- **O que faz:** mostra, por arquivo, o resumo das linhas alteradas entre a versão anterior e a correção. **Por quê:** evidenciar o escopo da mudança a quem aprova a implantação. **Resultado esperado:** a lista inclui os módulos de domínio, consumidores, `main.py`, API, frontend e documentação. **Não prossiga para o reinício sem a confirmação explícita.**

### Etapa 14.4 — Reiniciar o serviço em janela controlada (manual)

Somente após a confirmação, reinicie o serviço manualmente:

```bash
sudo systemctl restart lab-sqs.service
sudo systemctl status lab-sqs.service
journalctl -u lab-sqs.service -n 50 --no-pager
```

- **O que faz:** reinicia o serviço `systemd` para carregar o código novo e mostra o status e os últimos logs. **Por quê:** aplicar a correção de forma controlada. **Resultado esperado:** status **active (running)** e, nos logs, `Application startup complete`. Os eventos estruturados da correção (ex.: `iteracao_consumidor_falhou`, `consumidor_reiniciado`) aparecem em formato JSON pelo logger `sqs_lab.resiliencia` quando ocorrerem.

> **Rollback:** se algo falhar, volte ao estado anterior com `git checkout $(cat /home/ec2-user/deploy-revisao-anterior.txt)` seguido de `sudo systemctl restart lab-sqs.service`, ou restaure o `.tar.gz` de backup. Nenhum recurso AWS precisa ser tocado no rollback.

### Etapa 14.5 — Ligar/desligar a extensão opt-in de Visibility_Timeout

A extensão de Visibility_Timeout via `ChangeMessageVisibility` é **opt-in e vem desligada por padrão**. Ela é controlada pela **Config_Simulacao** da aplicação (não por atributo de fila) e **nunca** altera os 30 s configurados no Console AWS — age apenas sobre a mensagem recebida.

- **Manter desligada (padrão):** não é preciso fazer nada. Os cenários didáticos de expiração proposital (ex.: processamento de 45 s com Visibility_Timeout de 30 s) continuam demonstrando a reentrega.
- **Ligar (opcional):** ajuste a Config_Simulacao pelo Dashboard (área de configuração) ou via `PUT /api/config/simulacao`, informando o valor estendido dentro da faixa `[20, 43200]` segundos. A aplicação valida a faixa **antes** de qualquer chamada à AWS; um valor fora da faixa é rejeitado sem efeito colateral.
- **Permissão IAM:** se ligar a extensão, adicione a ação `sqs:ChangeMessageVisibility` à policy `lab-sqs-policy-minima` (Etapa 6), restrita às quatro filas. Com a flag desligada, essa permissão **não** é necessária.

> **Importante:** ligar a extensão **não** substitui os cenários de expiração — o cenário `duracao_superior_visibility` ignora a extensão de propósito, para que o teste didático de expiração continue válido.

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
   - **SQS** → cada fila: tempo limite de visibilidade (Visibility timeout) `30 s` (principais), Long_Polling `20 s`, SSE-SQS habilitado, retenção `4 dias` (principais) / `14 dias` (DLQs), Redrive_Policy com maxReceiveCount `2`, deduplicação por conteúdo nas FIFO.
   - **DynamoDB** → `lab-sqs-historico`: status **Ativa** (*Active*), chave `PK`/`SK`, índice `GSI1`.
   - **IAM** → `IAM_Role_EC2`: policies anexadas.
   - **EC2* → `lab-sqs-ec2`: estado **Em execução** (*Running*), perfil `IAM_Role_EC2`, Security Group `lab-sqs-sg` sem porta 22.

4. Teste funcional rápido: no Dashboard, envie uma mensagem para a fila Standard e verifique na área de monitoramento que a mensagem é recebida e processada; confirme que apareceram itens na tabela `lab-sqs-historico` do DynamoDB.
- **Resultado esperado:** a mensagem percorre o ciclo (enviada → recebida → processada) e o histórico registra os eventos.

5. Validar a saúde dos consumidores (correção de resiliência):

```
http://<IP_PUBLICO_DA_EC2>:8000/api/consumidores/saude
```

- **O que valida:** o endpoint inspeciona apenas o estado em memória dos slots (sem tocar na AWS) e reporta cada consumidor. **Resultado esperado:** JSON com `"iniciados": true`, `"algum_morto": false`, `"algum_degradado": false` e a lista `consumidores` com `"vivo": true` para os slots `standard` e `fifo`. Um consumidor com `"vivo": false` ou `"degradado": true` indica uma thread encerrada/em falha persistente, que o Dashboard sinaliza com a faixa amarela.

> **Tratamento de falha (Parte 7):** `saudavel: false` indica recurso ausente/inacessível — corrija o recurso indicado no Console AWS e recarregue. Dashboard inacessível pelo navegador costuma ser regra de entrada da porta `8000` no Security Group ou origem (IP) não autorizada.

---

## Parte 8 — Excluir todos os recursos ao final (evitar custo)

Ao encerrar o laboratório, **exclua todos os recursos** abaixo. A ordem respeita as dependências entre eles. Após a exclusão, confirme que nenhum recurso remanescente permanece ativo.

### Etapa 16 — Lista nominal de exclusão (nesta ordem)

1. **Instância EC2 `lab-sqs-ec2`** — EC2 → **Instâncias** (*Instances*) → selecionar → **Estado da instância** (*Instance state*) → **Encerrar instância** (*Terminate instance*). (Exclua a EC2 antes do Security Group, pois o SG está em uso pela instância.)
2. **Security Group `lab-sqs-sg`** — EC2 → **Grupos de segurança** (*Security Groups*) → selecionar → **Ações** (*Actions*) → **Excluir grupo de segurança** (*Delete security group*) — só é possível após a EC2 terminar.
3. **Fila_Principal_Standard `lab-sqs-standard`** — SQS → selecionar → **Excluir** (*Delete*).
4. **DLQ_Standard `lab-sqs-standard-dlq`** — SQS → selecionar → **Excluir** (*Delete*).
5. **Fila_Principal_FIFO `lab-sqs-fifo.fifo`** — SQS → selecionar → **Excluir** (*Delete*).
6. **DLQ_FIFO `lab-sqs-fifo-dlq.fifo`** — SQS → selecionar → **Excluir** (*Delete*).
7. **Tabela_Historico `lab-sqs-historico`** — DynamoDB → **Tabelas** (*Tables*) → selecionar → **Excluir** (*Delete*) — o índice `GSI1` é removido junto com a tabela.
8. **IAM_Role_EC2 `IAM_Role_EC2`** — IAM → **Perfis** (*Roles*) → selecionar → **Excluir** (*Delete*).
9. **Policy `lab-sqs-policy-minima`** — IAM → **Políticas** (*Policies*) → selecionar → **Excluir** (*Delete*) — após a role que a usa ser excluída.

### Etapa 17 — Confirmar ausência de recursos remanescentes

- Verifique em cada serviço (EC2, SQS, DynamoDB, IAM) que nenhum dos recursos acima aparece mais nas listas da região `us-east-1`.
- **Resultado esperado:** nenhum recurso da lista permanece ativo. Se algum permanecer (ex.: Security Group que não pôde ser excluído porque a EC2 ainda não terminou), repita a exclusão na ordem correta e registre o nome do recurso remanescente até removê-lo.

---

## Custo mensal estimado (USD)

Valores aproximados para a região `us-east-1`, considerando uso leve e descartável de laboratório. São estimativas de referência; os preços oficiais estão na calculadora e nas páginas de preços da AWS.

| Recurso | Premissa de uso | Custo mensal estimado (USD) |
|---------|-----------------|-----------------------------|
| EC2 `t3.micro` (On-Demand, Linux) | ~730 h/mês ligada; `us-east-1` | ~`7.50` (0 no Nível Gratuito (Free Tier) com `t2.micro`/`t3.micro` por 750 h) |
| EBS (volume raiz gp3, 8 GB) | 8 GB persistentes | ~`0.64` |
| Amazon SQS (4 filas) | Volume de laboratório, muito abaixo de 1 milhão de requisições/mês | ~`0.00` (1 milhão de requisições/mês no Free Tier) |
| DynamoDB `lab-sqs-historico` (On-Demand) | Poucos milhares de escritas/leituras no mês | ~`0.00`–`0.50` (dentro ou perto do Nível Gratuito (Free Tier)) |
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
14.1. (Correção) Fazer backup/versionamento, obter a correção sem reiniciar, apresentar as alterações, reiniciar manualmente em janela controlada e, opcionalmente, ligar/desligar a extensão de Visibility_Timeout.
15. Verificar pós-implantação (`/api/saude`, `/api/consumidores/saude`, Dashboard, Console AWS).
16. Excluir todos os recursos na ordem indicada.
17. Confirmar ausência de recursos remanescentes.
