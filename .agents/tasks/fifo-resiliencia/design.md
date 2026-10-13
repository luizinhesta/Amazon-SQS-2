# Design — Correção de resiliência dos consumidores SQS e tratamento de `DeleteMessage` falho

## Visão geral

O laboratório educacional reproduz os comportamentos de filas Amazon SQS Standard e FIFO. Durante os testes, um cenário real derrubou permanentemente o Consumidor_FIFO. Este documento projeta a correção em quatro frentes, mantendo intactos os cenários didáticos e as restrições de não tocar na AWS real: (A) tratamento estruturado do `DeleteMessage` falho com um novo evento/estado `exclusao_nao_confirmada`; (B) resiliência e supervisão das threads dos consumidores; (C) estratégia opcional e opt-in de extensão de Visibility_Timeout via `ChangeMessageVisibility`; (D) diferenciação de falhas no Dashboard (faixa laranja).

A pilha tecnológica permanece **Python ≥ 3.10, FastAPI, Uvicorn, boto3, DynamoDB, pytest + hypothesis + httpx**, servindo um frontend estático em `web/` com JavaScript puro (ES modules). Nenhuma dependência nova é adicionada: o módulo `logging` da biblioteca padrão cobre o registro estruturado. A pilha é fixa a partir da aprovação deste design.

### Causa raiz confirmada (estado exato do código atual)

O cenário que derruba o consumidor: fila `lab-sqs-fifo.fifo`, 3 mensagens, `MessageGroupId=grupo-A`, `processing_seconds=45`, Visibility_Timeout=30s, `maxReceiveCount=2`, cenário SUCESSO.

Hoje, em `app/consumidores/processamento.py`, o caminho de sucesso de `processar_mensagem` executa, **nesta ordem exata** (confirmado lendo o bloco `if sucesso:`):

1. registra `recebida` (seq=1);
2. registra `processamento_iniciado` (seq=2);
3. `sleep(processing_seconds)` — dorme 45s;
4. registra `processada` (seq=3) — **ANTES do delete**;
5. chama `sqs.delete_message(nome_fila, receipt_handle)`;
6. registra `excluida` (seq=4).

Com 45s de processamento e Visibility_Timeout de 30s, o `ReceiptHandle` já expirou quando o passo 5 executa. O `delete_message` do gateway repassa o erro bruto da AWS por design documentado (`botocore ClientError (InvalidParameterValue) "The receipt handle has expired"`). Como nenhuma camada do caminho `processar_mensagem → ConsumidorFifo.processar_uma_iteracao → ConsumidorFifo.executar → thread daemon do GerenciadorConsumidores` captura essa exceção (confirmei: nem `executar` nem `_executar_fifo` têm `try/except`), ela sobe e **mata a thread do consumidor permanentemente**. O Uvicorn segue vivo; as 3 mensagens voltam a ficar disponíveis sem consumidor.

O efeito combinado é duplo e é o cerne do problema:

- **(a)** a thread morre e para de consumir;
- **(b)** o evento `processada` (seq=3) **já foi persistido** no passo 4. Portanto, mesmo que a thread fosse recriada, numa reentrega `derivar_estado` retornaria `Estado.PROCESSADA` (regra terminal avaliada por `tipos_presentes`, confirmei em `state_deriver.py`), e `verificar_idempotencia` responderia `JA_PROCESSADO` — o consumidor pularia o efeito e **nunca excluiria** a mensagem, que ficaria em loop de reentrega até a DLQ, apesar de o negócio ter "concluído". Pior: o histórico diz "processada" enquanto a mensagem continua fisicamente na fila.

A correção precisa resolver **os dois** efeitos: não matar a thread (frentes A e B) e não gravar `processada` antes de a exclusão ser confirmada (frente A, reordenação).

## Decisão central: onde capturar a falha de `DeleteMessage`

Três candidatos poderiam capturar o `ClientError` do delete: o gateway (`sqs_gateway.delete_message`), a função de processamento (`processamento.processar_mensagem`) ou o laço do consumidor.

Escolho **capturar em `processar_mensagem`**, no bloco que hoje faz `sqs.delete_message(...)`. Justificativa:

- O gateway tem como regra de design documentada "repassar o erro bruto da AWS sem encapsular" (Req. 1.8, 4.12, 7.11, 8.8), e `test_sqs_gateway.py` depende disso. Alterar o gateway para engolir `ClientError` quebraria o contrato. O gateway permanece **inalterado** quanto ao `delete_message`.
- O laço do consumidor é o lugar da resiliência genérica (frente B), mas não tem o contexto do pedido (order_id, consumer_id, receive_count) para registrar o evento de auditoria correto. Colocar a regra de negócio do delete ali duplicaria lógica entre FIFO e Standard.
- `processar_mensagem` já é o dono da sequência `processada → delete → excluida` e tem acesso a todo o contexto e ao `HistoricoRepository`. É o único ponto onde a regra "nunca registrar `excluida` se o delete falhar" é garantida de forma centralizada para ambos os consumidores.

### Reordenação crítica: `processada` passa a ser gravado **após** o delete confirmado

A correção reordena o caminho de sucesso para gravar `processada` **somente após** o `delete_message` retornar com sucesso. A nova sequência passa a ser:

1. `recebida` (seq=1), `processamento_iniciado` (seq=2), `sleep` e `_aplicar_cenario` — inalterados;
2. `sqs.delete_message(...)`;
3. **se o delete teve sucesso**: registra `processada` (seq=3) e `excluida` (seq=4), nessa ordem;
4. **se o delete lançou `ClientError`/erro de infra reconhecido**: registra o novo evento `exclusao_nao_confirmada` (seq=3) com o erro original e **não** registra `processada` nem `excluida`.

Essa reordenação garante que:

- `excluida` nunca é registrado sem confirmação (regra dura);
- `processada` (que dispara a idempotência de "já processado") só existe quando a mensagem foi de fato removida;
- numa reentrega após falha de delete, a idempotência **não** vê `processada`, então o consumidor é autorizado a reprocessar — e o reprocessamento termina na reexecução do delete com o `ReceiptHandle` novo e válido, agora bem-sucedido, registrando enfim `processada` + `excluida`.

## Frente A — Tratamento do `DeleteMessage` falho

### Novo tipo de evento e novo estado (e os mapas obrigatórios)

Adicionar em `app/dominio/tipos.py`:

- `EventType.EXCLUSAO_NAO_CONFIRMADA = "exclusao_nao_confirmada"` — Evento_Observado: o processamento terminou, o delete foi tentado e **falhou** por erro de infraestrutura (tipicamente `ReceiptHandle` expirado).
- `Estado.EXCLUSAO_NAO_CONFIRMADA = "Exclusão não confirmada"` — novo estado derivado, **não terminal**.

Consequências obrigatórias (não condicionais — verificadas no código):

- `ESTADOS_PERMITIDOS = frozenset(Estado)` é derivado automaticamente — o novo estado entra sem mais ação.
- `ESTADOS_TERMINAIS` é explícito e contém apenas `{PROCESSADA, ENCAMINHADA_DLQ}`. O novo estado **NÃO** entra em `ESTADOS_TERMINAIS` (declarado explicitamente para evitar inclusão por engano).
- **`classificador_origem.py` tem um assert em tempo de importação: `assert set(ORIGEM_POR_TIPO) == set(EventType)`.** Adicionar o novo `EventType` **sem** adicionar a entrada em `ORIGEM_POR_TIPO` faz o import do módulo falhar e derruba a aplicação e toda a suíte. Portanto é **obrigatório** adicionar `EventType.EXCLUSAO_NAO_CONFIRMADA: EventSource.OBSERVADO` ao dicionário `ORIGEM_POR_TIPO`. Como `validar_origem`/`classificar_origem` tratam o novo tipo como OBSERVADO, o registro do evento com `event_source=OBSERVADO` passa na validação.
- `ORIGEM_OBRIGATORIA_POR_TIPO` (em `tipos.py`) **não** precisa de entrada nova: ele só fixa os tipos cuja origem é "travada" por exigência explícita do design (`PROVAVEL_EXPIRACAO_VISIBILITY`, `ENCAMINHADA_DLQ`). O novo tipo não entra ali, e o assert `ORIGEM_POR_TIPO[_tipo] is _origem_fixa` continua válido (itera só sobre as chaves de `ORIGEM_OBRIGATORIA_POR_TIPO`).
- No frontend, `monitoring.js` tem um mapa **fechado** `CLASSE_ESTADO` com fallback `|| "estado-neutro"`. É obrigatório adicionar a entrada `"Exclusão não confirmada": "estado-exclusao-nao-confirmada"` para não cair no neutro silencioso (ver frente D).

### Classificação do erro de delete

Criar em `app/acesso/sqs_gateway.py` um helper **puro** (sem capturar nada no gateway, apenas para o processamento classificar o erro):

```python
# app/acesso/sqs_gateway.py (adições; delete_message permanece intacto)
RECEIPT_HANDLE_EXPIRADO_CODES: frozenset[str] = frozenset(
    {"InvalidParameterValue", "ReceiptHandleIsInvalid"}
)

def e_receipt_handle_expirado(erro: BaseException) -> bool:
    """True se o erro do botocore indica ReceiptHandle expirado/inválido no DeleteMessage."""
    resposta = getattr(erro, "response", None)
    if not isinstance(resposta, dict):
        return False
    codigo = resposta.get("Error", {}).get("Code")
    mensagem = (resposta.get("Error", {}).get("Message", "") or "").lower()
    if codigo == "ReceiptHandleIsInvalid":
        return True
    return codigo == "InvalidParameterValue" and "receipt handle has expired" in mensagem

def e_erro_infra_sqs(erro: BaseException) -> bool:
    """True para qualquer erro de infraestrutura da AWS/botocore (ClientError/BotoCoreError).

    Classifica por estrutura/MRO, como `historico_repository._e_erro_transitorio`,
    para não importar botocore em tempo de módulo e manter os testes com fakes."""
    if getattr(erro, "response", None) is not None:  # ClientError expõe .response
        return True
    nomes = {base.__name__ for base in type(erro).__mro__}
    return "BotoCoreError" in nomes or "ClientError" in nomes
```

A verificação por estrutura (`.response["Error"]["Code"]`) segue o padrão já usado em `historico_repository._e_erro_transitorio`. O `InvalidParameterValue` só é tratado como handle expirado quando a mensagem contém "receipt handle has expired", para não mascarar outros usos de `InvalidParameterValue`.

### Assinatura e lógica de `processar_mensagem`

A assinatura pública ganha dois parâmetros **opcionais** com padrão que preserva 100% do comportamento atual (ver frente C). O `ResultadoProcessamento` ganha um campo novo com default (para não quebrar construções existentes, que são `@dataclass(frozen=True)`):

```python
@dataclass(frozen=True)
class ResultadoProcessamento:
    order_id: str
    cenario: CenarioFalha
    sucesso: bool            # True só quando processou E excluiu com confirmação
    excluida: bool           # True só quando o delete_message confirmou
    erro_detalhe: Optional[str] = None
    exclusao_nao_confirmada: bool = False  # NOVO (default preserva construções atuais)
```

Pseudocódigo do novo bloco de sucesso (substitui o `if sucesso:` atual; note a reordenação delete → processada):

```python
if sucesso:
    try:
        sqs.delete_message(nome_fila, mensagem.receipt_handle)
    except Exception as erro:  # noqa: BLE001 — classificamos abaixo
        if e_receipt_handle_expirado(erro) or e_erro_infra_sqs(erro):
            log_evento(logging.ERROR, "delete_message_falhou",
                       order_id=order_id_efetivo, fila=nome_fila,
                       consumer_id=consumer_id, erro=repr(erro))
            historico.registrar_evento(_novo_evento(
                order_id=order_id_efetivo, queue_name=nome_fila,
                event_type=EventType.EXCLUSAO_NAO_CONFIRMADA,
                event_source=EventSource.OBSERVADO, seq=3,
                consumer_id=consumer_id,
                receive_count=mensagem.approximate_receive_count,
                processing_seconds=processing_seconds,
                message_group_id=grupo, sqs_message_id=mensagem.message_id,
                error_detail=repr(erro)))
            return ResultadoProcessamento(
                order_id=order_id_efetivo, cenario=cenario,
                sucesso=False, excluida=False,
                erro_detalhe=repr(erro), exclusao_nao_confirmada=True)
        raise  # erro não reconhecido: a guarda do consumidor (frente B) trata
    # Delete confirmado: só agora grava processada + excluida.
    historico.registrar_evento(_novo_evento(... PROCESSADA, seq=3 ...))
    historico.registrar_evento(_novo_evento(... EXCLUIDA, seq=4 ...))
    return ResultadoProcessamento(order_id=..., sucesso=True, excluida=True)
```

A regra: **toda** falha de infraestrutura reconhecida no delete vira `exclusao_nao_confirmada` registrada e `ResultadoProcessamento(sucesso=False, excluida=False, exclusao_nao_confirmada=True)`; a mensagem é preservada na fila por natureza. Nenhuma exceção de delete reconhecida propaga para fora de `processar_mensagem` — isso impede que o erro mate a thread. Um erro genuinamente inesperado (não reconhecido como infra) ainda propaga, mas a guarda da frente B garante que o laço não morre.

### Logging estruturado

Introduzir `app/observabilidade/log_estruturado.py` (novo pacote `app/observabilidade/` com `__init__.py`):

```python
import json, logging
_log = logging.getLogger("sqs_lab.resiliencia")

def log_evento(nivel: int, evento: str, **campos) -> None:
    """Emite uma linha de log estruturada (JSON), preservando o erro original."""
    _log.log(nivel, json.dumps({"evento": evento, **campos}, ensure_ascii=False, default=str))
```

Nível `ERROR` para falha de delete e para consumidor encerrado por erro; `WARNING` para falhas pontuais de iteração recuperadas. O erro original é preservado tanto no log (`erro=repr(erro)`) quanto no `error_detail` do evento.

### StateDeriver: refletir o novo estado (precedência e caso de reentrega)

Em `app/dominio/state_deriver.py`, a precedência fica assim (confirmei a estrutura atual: DLQ e PROCESSADA são resolvidos por `tipos_presentes` **antes** do laço `reversed`; os demais estados são resolvidos **dentro** do laço `reversed`):

1. `Encaminhada para DLQ` (terminal) — inalterado, precedência máxima.
2. `Processada` (terminal) — inalterado (só existe quando o delete confirmou).
3. `Exclusão não confirmada` (**não terminal**) — retornado quando é o **desfecho mais recente do ciclo**, ou seja, quando o evento `EXCLUSAO_NAO_CONFIRMADA` é o mais recente na varredura reversa e **não** há `processamento_iniciado`/`recebida` posterior (reentrega ativa) que o sobreponha. Fica **abaixo** de DLQ e PROCESSADA — ambos terminais têm precedência sobre ele — e **não** entra em `ESTADOS_TERMINAIS`.
4. demais estados — avaliados no laço `reversed(lista)`.

**Invariante declarada (resolve finding 3):** no caminho de sucesso com delete falho, para aquela tentativa **nenhum** evento `FALHA` é gravado — grava-se **somente** `exclusao_nao_confirmada`. Logo `EXCLUSAO_NAO_CONFIRMADA` **nunca coexiste** com `FALHA` na mesma tentativa, eliminando qualquer ambiguidade de precedência entre os dois. (`FALHA` continua existindo apenas nos cenários de erro de processamento que não são "sucesso de negócio + delete falho".)

A regra do novo estado entra **dentro** do laço `reversed`, tratada como um dos desfechos do ciclo: ao encontrar um evento `EXCLUSAO_NAO_CONFIRMADA` durante a varredura reversa, retorna `Estado.EXCLUSAO_NAO_CONFIRMADA`. A posição exata e o comportamento de reentrega são **decisão explícita**:

- **Dependência de ordenação (resolvido na revisão anterior):** `seq` do novo evento é 3 (coerente com `FALHA`/`PROCESSADA` do mesmo nível). O StateDeriver **ignora `seq`** e depende da **ordem cronológica** da lista. Confirmei em `historico_repository.consultar_por_pedido`: a Query usa `ScanIndexForward=True` sobre a Sort Key `EVENTO#<app_time_ms>#<seq>`, portanto a lista chega em ordem cronológica crescente; `listar_eventos_cronologico` reforça com `ordenar_cronologicamente` (desempate estável por `seq`). O `agregador_estatisticas` e o `servico_monitoramento` já aplicam `ordenar_cronologicamente` antes de derivar. Logo, **não é preciso critério de desempate adicional**: a ordem cronológica é garantida.
- **Comportamento na reentrega ativa (resolvido na revisão anterior):** numa reentrega, a sequência cronológica é `...exclusao_nao_confirmada (1ª tentativa), recebida (2ª), processamento_iniciado (2ª)...`. Como o laço é `reversed`, o `processamento_iniciado` da 2ª tentativa é encontrado **antes** do `exclusao_nao_confirmada` e retorna `Estado.EM_PROCESSAMENTO`. **Isto é o comportamento desejado e declarado:** durante a reentrega ativa a mensagem está sendo reprocessada, logo `Em processamento` é correto; o estado só volta a `Exclusão não confirmada` se a reentrega **também** falhar o delete (gravando novo `exclusao_nao_confirmada` como evento mais recente). A nova regra, portanto, é apenas mais um `if tipo is EventType.EXCLUSAO_NAO_CONFIRMADA: return Estado.EXCLUSAO_NAO_CONFIRMADA` dentro do laço `reversed`, posicionado junto aos demais desfechos (depois de `PROCESSAMENTO_INICIADO` na ordem textual não importa, porque o `reversed` já garante que o evento cronologicamente mais recente vence). Um caso de teste explícito em `test_state_deriver.py` fixa esse comportamento de reentrega.

### Agregador de estatísticas

Em `app/dominio/agregador_estatisticas.py`, mensagens em `Exclusão não confirmada` **não** contam como `processadas` (ainda estão fisicamente na fila) nem como `com_falha` (o negócio concluiu). Adicionar:

- `EstatisticasFluxo.exclusao_nao_confirmada: int = 0` (dataclass mutável, default 0);
- no bloco de derivação por mensagem, `elif estado is Estado.EXCLUSAO_NAO_CONFIRMADA: destino.exclusao_nao_confirmada += 1`.

No schema de saída `EstatisticasFluxoSaida` (em `esquemas.py`), os contadores atuais usam `Field(..., ge=0)` (obrigatórios). **Para não quebrar serialização existente, o novo campo recebe default 0:** `exclusao_nao_confirmada: int = Field(0, ge=0, description=...)`. O mapeamento em `_fluxo_para_saida` (em `servico_monitoramento.py`) passa `exclusao_nao_confirmada=estat.exclusao_nao_confirmada`. O overview ganha uma linha distinta para esse contador. Isso evita apresentar como "processada" algo que a AWS ainda não confirmou removido (atende "não apresentar dado simulado como confirmado pela AWS").

### Contabilização do resultado nos consumidores (decisão do usuário = Opção A)

Esta é a contabilização **por mensagem** nas estatísticas internas de cada consumidor, distinta da agregação de estados do monitoramento acima. Com a nova regra, `ResultadoProcessamento(exclusao_nao_confirmada=True)` retorna `sucesso=False` e, sem tratamento próprio, cairia **silenciosamente** em `falhas_processamento` — embora o negócio tenha concluído. **Decisão explícita (Opção A, escolhida pelo usuário):** um contador **dedicado**, separado de `falhas_processamento`.

- Adicionar `EstatisticasConsumo.exclusoes_nao_confirmadas: int = 0` (`@dataclass` mutável) no `ConsumidorStandard`.
- Em `_tratar_mensagem`, introduzir um **ramo próprio** antes de cair em falhas:

  ```python
  if resultado.sucesso:
      self.estatisticas.processadas_com_sucesso += 1
  elif resultado.exclusao_nao_confirmada:
      self.estatisticas.exclusoes_nao_confirmadas += 1   # ramo dedicado — NÃO entra em falhas
  else:
      self.estatisticas.falhas_processamento += 1
  ```

  O `elif resultado.exclusao_nao_confirmada` **nunca** cai em `falhas_processamento`.
- Expor o contador por fluxo na API: o campo correspondente em `EstatisticasFluxoSaida` já é `exclusao_nao_confirmada` (`Field(0, ge=0)`, retrocompatível), definido acima.
- Vale para o **Standard** e, **se aplicável, para o FIFO**: onde o FIFO contabiliza o desfecho por mensagem (ex.: via `ResultadoIteracaoFifo`/totais de `ResultadoConsumoFifo`), aplicar o mesmo ramo dedicado — contar `exclusao_nao_confirmada` separado das falhas de processamento, nunca somando aos erros de iteração nem às falhas de negócio.
- No Dashboard (`web/js/areas/overview.js`), exibir uma **linha distinta "Exclusão não confirmada"** por fluxo, **separada da linha "Com falha"**, consumindo o campo `exclusao_nao_confirmada` do fluxo. Isso dá expectativa definida ao teste 10 e ao roteiro de validação.

## Idempotência e reentrega (item 2)

Com a reordenação, a sequência numa reentrega após `exclusao_nao_confirmada` é:

1. Mensagem reentregue (novo `ReceiptHandle` válido, `ApproximateReceiveCount` incrementado).
2. `verificar_idempotencia` consulta o histórico. Não há `processada` → `derivar_estado` não retorna `Estado.PROCESSADA` → `_pedido_ja_processado` é `False` → decisão `PROCESSAR`.
3. `processar_mensagem` roda de novo: `recebida`, `processamento_iniciado`, `sleep`, `_aplicar_cenario` (sucesso), `delete_message` com o handle novo e válido → **sucesso** → grava `processada` + `excluida`.

O "reprocessamento" na reentrega é apenas a simulação (um `sleep`) — não há efeito de negócio externo real no laboratório (nenhuma gravação idempotente de domínio além dos eventos de auditoria). Portanto a reexecução **não duplica operação de negócio** em sentido observável; ela re-registra os eventos de ciclo de vida (esperado num cenário de reentrega — é o que o SQS realmente faz) e finalmente confirma a exclusão.

**Decisão explícita (otimização rejeitada):** poderíamos, na reentrega, detectar `exclusao_nao_confirmada` e pular direto para o delete sem refazer o `sleep` (via uma decisão nova `CONFIRMAR_EXCLUSAO` na idempotência e um caminho "só excluir" no consumidor). Rejeito por complexidade e por quebrar a didática: ver a mensagem ser reprocessada por inteiro na reentrega demonstra o custo real da expiração de Visibility_Timeout. A correção fica mínima e o loop até a DLQ deixa de existir porque o delete passa a confirmar na reentrega quando o handle é válido. Observação importante: se o usuário mantiver `processing_seconds=45 > 30s`, cada reentrega continuará estourando o Visibility_Timeout e gerando novo `exclusao_nao_confirmada` até a DLQ — comportamento **didaticamente válido** e que **nunca mata a thread**. Quando `processing_seconds < Visibility_Timeout`, a primeira reentrega conclui.

## Frente B — Resiliência e supervisão dos consumidores

### Guarda por iteração (ambos os consumidores)

Requisito duro: uma exceção pontual (SQS/DynamoDB) deve ser logada e **não** encerrar o consumo; o laço continua na próxima iteração.

**FIFO** (`consumidor_fifo.py`). `ResultadoIteracaoFifo` é `@dataclass(frozen=True)` com defaults, e `ResultadoConsumoFifo` é frozen com totais `@property`. Para introduzir o contador de erros sem quebrar o padrão frozen (resolvido na revisão anterior):

- adicionar `erro: Optional[str] = None` a `ResultadoIteracaoFifo` (campo com default, compatível com frozen);
- adicionar `@property iteracoes_com_erro` a `ResultadoConsumoFifo`: `sum(1 for it in self.iteracoes if it.erro is not None)`;
- em `ConsumidorFifo.executar`, o laço hoje faz `iteracoes.append(self.processar_uma_iteracao())`. Trocar por uma guarda que **sempre anexa** um `ResultadoIteracaoFifo` (o append não pode ser pulado silenciosamente):

```python
try:
    iteracoes.append(self.processar_uma_iteracao())
except Exception as e:  # noqa: BLE001
    log_evento(logging.WARNING, "iteracao_consumidor_falhou",
               consumer_id=self.consumer_id, fila=self._nome_fila, erro=repr(e))
    iteracoes.append(ResultadoIteracaoFifo(
        recebidas=0, grupo_selecionado=None, erro=repr(e)))
    # NÃO re-levanta: o laço continua na próxima iteração.
```

**Standard** (`consumidor_standard.py`). `EstatisticasConsumo` é `@dataclass` mutável. Adicionar `iteracoes_com_erro: int = 0`. Em `executar`, hoje `self._executar_iteracao()` é chamado direto no laço `while`. Envolver essa chamada:

```python
try:
    self._executar_iteracao()
except Exception as e:  # noqa: BLE001
    self.estatisticas.iteracoes_com_erro += 1
    log_evento(logging.WARNING, "iteracao_consumidor_falhou",
               consumer_id=self.consumer_id, fila=self.nome_fila, erro=repr(e))
    # NÃO re-levanta.
```

Importante nos dois: capturar `Exception`, **não** `BaseException`. `InterrupcaoProcessamento` herda de `BaseException` de propósito (cenário didático de interrupção) e já é tratado dentro de `processar_mensagem`; a parada cooperativa (`_parar`/`criterio_parada`) não deve ser afetada.

Preservação da ordenação FIFO: a guarda é por iteração inteira. Se uma iteração falha no meio de um grupo, nada foi excluído fora de ordem; as mensagens não excluídas voltam após o Visibility_Timeout e `selecionar_grupo` reescolhe o primeiro grupo na ordem de recebimento. A ordem dentro do grupo continua preservada porque nunca excluímos fora de ordem.

### Supervisão de threads no `GerenciadorConsumidores`

Hoje (`app/main.py`) o `GerenciadorConsumidores` usa listas paralelas (`_threads`, `_consumidores_standard`, `_consumidores_fifo`), `iniciar()` com guarda `if self._threads: return`, `_executar_standard`/`_executar_fifo` chamam `consumidor.executar(...)` direto na thread, e `parar()` faz `_parada.set()` + `.parar()` nos Standard + `join`. O `ConsumidorFifo` **não tem** método `parar()` (para por `criterio_parada`/`max_iteracoes`).

Com a guarda por iteração, o laço praticamente não morre mais. Ainda assim, adicionar uma **camada de supervisão defensiva** que detecta e reinicia uma thread encerrada inesperadamente, sem duplicar consumidores. Esta é uma reescrita estrutural do gerenciador — especificada em detalhe para controlar o blast radius (resolvido na revisão anterior):

**Estrutura de dados.** Trocar as listas paralelas por uma lista de slots:

```python
@dataclass
class _SlotConsumidor:
    tipo: str                 # TIPO_CONSUMIDOR_STANDARD | TIPO_CONSUMIDOR_FIFO
    consumidor: object        # ConsumidorStandard | ConsumidorFifo
    thread: threading.Thread
    reinicios: int = 0
    marcos_reinicio: list[float] = field(default_factory=list)  # timestamps p/ janela
    degradado: bool = False
```

**Alvos de thread com guarda externa.** `_executar_standard`/`_executar_fifo` ganham um `try/except Exception` que loga `consumidor_encerrou_por_erro` (ERROR) e **retorna** (a thread termina normalmente, sem exceção subindo ao runtime). O FIFO roda com `criterio_parada=self._deve_parar`; o Standard com `continuar=lambda: not self._deve_parar()` — inalterado.

**Criação de slots.** `iniciar()` cria N slots por tipo (N vem de `estado_instancias.instancias(tipo)`). Idempotência preservada por `if self._slots: return`. Gerador de `consumer_id` por tipo: Standard via `criar_consumidor_standard` (gera id único internamente); FIFO via `ConsumidorFifo(consumer_id=gerar_consumer_id("consumidor-fifo"))`.

**Decisão sobre o `consumer_id` do FIFO (resolve finding 6):** reusamos `gerar_consumer_id("consumidor-fifo")` (de `consumidor_standard.py`) em vez do `_novo_consumer_id()` privado do `ConsumidorFifo`. Isto é **deliberado, não acidental**: (a) o `main.py` atual **já** cria o FIFO assim, então mantemos a convenção existente (sem regressão de formato de id em logs/histórico); (b) `gerar_consumer_id(prefixo=...)` combina um contador de processo com um sufixo aleatório, o que dá unicidade **entre reinícios de slot e entre processos**, enquanto o `_novo_consumer_id()` (hex12) é adequado mas não é o padrão adotado no gerenciador. O leve acoplamento de importar uma função de "standard" para id de FIFO é aceito e documentado aqui; alternativa (expor um helper neutro em módulo comum) fica como melhoria opcional de organização, fora do escopo desta correção.

**Thread supervisora.** Um método `supervisionar(self)` roda numa thread daemon própria (`name="supervisor-consumidores"`). A cada `INTERVALO_SUPERVISAO_SEGUNDOS` (padrão 5s, aguardado via `self._parada.wait(timeout=...)` para acordar imediatamente no shutdown), percorre `self._slots`; para cada slot cujo `not thread.is_alive()` **e** `not self._deve_parar()` **e** `not slot.degradado`, chama `self._recriar_slot(slot)`. A chave anti-duplicação: só substitui threads **já mortas** (nunca cria uma segunda thread para uma viva); cada slot tem no máximo uma thread viva.

**Premissa sobre Long_Polling (declarada):** as threads bloqueiam em `receive_messages` com Long_Polling de 20s (`WaitTimeSeconds=LONG_POLLING_SEGUNDOS=20`, confirmado em `constantes.py`). Uma thread "viva" presa no polling **não** é morta; `not thread.is_alive()` só dispara em morte real. Logo a supervisão não interfere com o polling normal, e o teto de reinícios (`MAX_REINICIOS_JANELA=5` em `JANELA_REINICIOS_SEGUNDOS=60`) só conta mortes reais — o intervalo de 20s do polling não gera falsos reinícios.

**Limite explícito da supervisão (resolve finding 5, parte b):** a supervisão recupera apenas threads **mortas** (`not is_alive()`). Uma thread **viva mas travada** (p.ex. bloqueada indefinidamente numa chamada que, por algum defeito, não respeite o timeout do boto3) **não** é detectada nem reiniciada — está **fora de escopo** desta correção. Justificativa: `is_alive()` não distingue "viva trabalhando" de "viva travada", e introduzir heartbeat/watchdog por iteração seria complexidade desproporcional ao problema real (que é a thread morta por exceção). A guarda por iteração (frente B) elimina a causa real conhecida — exceção que mata a thread —; travamento silencioso sem exceção é um modo de falha distinto e não observado neste laboratório. Declarado como limite conhecido.

**Política de reinício: imediato com teto.** `_recriar_slot` cria consumidor+thread novos (novo `consumer_id`) **no mesmo índice** do slot morto, registra o timestamp em `marcos_reinicio`, descarta marcos fora da janela de 60s e incrementa `reinicios`. Se os reinícios na janela excederem `MAX_REINICIOS_JANELA`, marca `slot.degradado = True`, loga `consumidor_em_falha_persistente` (ERROR) e **para de reiniciar** aquele slot — evita loop de reinício mascarando um problema real (restrição "não mascarar o erro"). O endpoint de saúde (frente D) expõe `degradado` e `reinicios`.

**Ordem de encerramento em `parar()`:** exatamente nesta ordem —
1. `self._parada.set()`;
2. `.parar()` em cada `ConsumidorStandard` (parada cooperativa; FIFO para via `criterio_parada`);
3. **encerrar e `join` da thread supervisora ANTES dos slots** (para ela não recriar um slot durante o shutdown);
4. `join(timeout=...)` em cada thread de slot;
5. limpar `self._slots`.

Como a supervisão checa `not self._deve_parar()` antes de recriar e é encerrada antes do join dos slots, não há recriação durante o shutdown. O `GerenciadorMonitorDlq` segue o padrão `_parada`/`join` e permanece **inalterado**.

**Timeout de join vs Long_Polling (resolve finding 5, parte a):** o `parar()` mantém a assinatura atual `parar(self, *, timeout: Optional[float] = 5.0)` e **continua** com `timeout=5.0` por padrão. Como o Long_Polling é de 20s, uma thread presa no meio de um `receive_messages` **pode não terminar dentro dos 5s** de join. **Decisão explícita:** isso é **aceitável e intencional** — as threads de consumidor e a supervisora são **daemon** (`daemon=True`), portanto não impedem o encerramento do processo mesmo que o join estoure; o `timeout` serve apenas para dar uma janela de encerramento cooperativo (o laço verifica `_parada` entre iterações) sem bloquear o shutdown da aplicação por até 20s. O design **não** eleva o timeout para ≥20s de propósito: elevar atrasaria o shutdown no caso comum (polling quase sempre retorna antes dos 20s) sem benefício real, já que as threads são daemon. Consequência para o **teste obrigatório 12**: o teste valida o **término cooperativo não-bloqueante** — após `parar()`, `_parada` está setado, a supervisora encerrou antes dos slots e nenhuma recriação ocorre; os testes usam fakes cujo laço retorna imediatamente (sem polling real de 20s), então o `join(5.0)` conclui determinísticamente. O comportamento com polling real de 20s é validado no roteiro manual (`TESTES.md`), não em teste unitário.

**Novas constantes em `constantes.py`:** `INTERVALO_SUPERVISAO_SEGUNDOS = 5`, `MAX_REINICIOS_JANELA = 5`, `JANELA_REINICIOS_SEGUNDOS = 60`.

## Frente C — Visibility Timeout (opcional, opt-in)

Decisão: implementar a extensão de Visibility_Timeout via `ChangeMessageVisibility` como recurso **opt-in e configurável, desligado por padrão**, para que os cenários didáticos de expiração proposital continuem valendo e os 30s do Console AWS nunca sejam alterados automaticamente.

- Adicionar ao `SqsGateway` o método (passthrough fiel ao contrato "erro bruto da AWS", **sem validação de faixa no gateway** — resolvido na revisão anterior):

```python
def change_message_visibility(self, nome_fila: str, receipt_handle: str,
                              visibility_timeout: int) -> Mapping[str, Any]:
    """ChangeMessageVisibility — ajusta o Visibility_Timeout de UMA mensagem recebida.
    NÃO altera o atributo da fila (os 30s do Console permanecem). Erro bruto da AWS repassado."""
    url = self.obter_url_fila(nome_fila)
    return self._sqs.change_message_visibility(
        QueueUrl=url, ReceiptHandle=receipt_handle, VisibilityTimeout=visibility_timeout)
```

- `processar_mensagem` ganha dois parâmetros opcionais com default que preserva o comportamento atual:
  - `estender_visibility: bool = False`
  - `visibility_timeout_estendido: Optional[int] = None`

  Quando `estender_visibility=True`, em vez de um `sleep(processing_seconds)` único, o processamento dorme em fatias de `HEARTBEAT_VISIBILITY_SEGUNDOS` e, entre as fatias, chama `change_message_visibility` renovando o prazo. A função `sleep` injetável continua sendo usada (os testes controlam o avanço sem espera real).

- **Owner único da validação (resolvido na revisão anterior):** a validação de faixa e de coerência flag/valor fica **exclusivamente** em `processar_mensagem` (ou numa função pura dedicada `validar_extensao_visibility(estender, valor)`), **não** no gateway. Regras: quando `estender_visibility=True`, `visibility_timeout_estendido` é obrigatório, inteiro, na faixa `[HEARTBEAT_VISIBILITY_SEGUNDOS, 43200]` (o SQS limita a visibilidade por mensagem a 12h = 43200s; o mínimo útil é um heartbeat). Fora da faixa ou flag ligada sem valor → `ValueError` **antes** de chamar a AWS. O gateway permanece passthrough puro, coerente com a Frente A.

- **Não quebrar os cenários didáticos:** a flag é propagada do consumidor, que a lê da `Config_Simulacao` (novo parâmetro opt-in `estender_visibility`, padrão `False`, validado pelo serviço de configuração em `PUT /api/config/simulacao`). A extensão só vale para `CenarioFalha.SUCESSO`; o cenário `DURACAO_SUPERIOR_VISIBILITY` **ignora** a extensão (continua deixando expirar de propósito). Assim o teste obrigatório 2 (45s com Visibility 30s) continua demonstrando a expiração com a flag desligada (padrão) e demonstra a mitigação quando o usuário liga a flag.

- **Nova constante:** `HEARTBEAT_VISIBILITY_SEGUNDOS` em `constantes.py` (ex.: 20).

Escopo: a correção da resiliência (frentes A e B) **não depende** da frente C. Mesmo com a flag desligada, a thread nunca mais morre e a reentrega converge. A frente C é melhoria de qualidade de vida para demonstrar processamentos longos sem reentrega.

## Frente D — Dashboard e monitoramento (faixa laranja)

### Causa do aviso (fatos verificados)

A faixa laranja ("Não foi possível atualizar os dados. Os últimos dados válidos foram preservados e podem estar desatualizados.") vem de `MENSAGEM_DADOS_DESATUALIZADOS` em `web/js/areas/refresh.js`, aplicada por `createObservabilidade.marcarFalha` com a classe CSS `global-status--stale`.

**Correção factual (resolve finding 1 da revisão atual — o diagnóstico anterior estava invertido):** a faixa **global** `#global-status`/`global-status--stale` é acionada por **um único caminho**: o `catch` do `createRefreshController.tick()` em `refresh.js`, isto é, quando `onTick` **lança**. Verifiquei por leitura direta de `web/js/main.js`: o `onTick` real faz `Promise.allSettled([api.obterEstatisticas(), api.obterMonitoramento()])`, considera o tick **SUCESSO** quando **ao menos uma** chamada resolve (`algumaOk`), e só **lança** (acionando a faixa global laranja) quando **ambas** rejeitam por erro real de rede/HTTP (`ApiError`). Portanto, hoje a faixa global laranja significa literalmente **"ambas as chamadas falharam por rede/HTTP"**.

O `timeout:true` (resposta HTTP 200) **não** aciona a faixa global: em `main.js` uma resposta resolvida com `timeout:true` conta como `fulfilled` ⇒ `algumaOk` ⇒ tick SUCESSO. O `timeout:true` é tratado **localmente** por `overview.js` (método `showFailure`, que escreve no banner **local** `this.nodes.failureBanner` de classe `ov-failure`, não na faixa global) e por tratamento equivalente em `monitoring.js`. Confirmei em `overview.js`: `refreshStats()` chama `showFailure(...)` quando `stats.timeout` é verdadeiro, operando só no banner local.

O backend (`servico_monitoramento.obter_estatisticas`/`obter_monitoramento`) responde **HTTP 200 com `timeout: true`** quando o cálculo excede o limite. O símbolo `TIMEOUT_PADRAO_SEGUNDOS` **existe** em `app/api/servico_monitoramento.py` e vale **`10.0` segundos**, e a função `executar_com_timeout` **existe** nesse mesmo módulo — ambos confirmados por leitura direta. O que de fato está desatualizado é a documentação em `app/api/app.py` (docstrings dizem "3 s"), enquanto o valor efetivo aplicado pelos endpoints (que chamam `obter_estatisticas(_historico(request))` sem passar `timeout_segundos`) é o default de 10s. As mensagens de timeout são `MENSAGEM_TIMEOUT_ESTATISTICAS`/`MENSAGEM_TIMEOUT_MONITORAMENTO` (em `esquemas.py`, com placeholder `{segundos}`).

Portanto a faixa laranja é **sintoma** da causa raiz (consumidor morto): com o consumidor derrubado, as mensagens acumulam eventos e as consultas ao histórico (quatro filas via GSI1 + agregação por order_id) ficam pesadas e passam de 10s. Com as frentes A e B o consumidor deixa de morrer, a pressão sobre o histórico cai e o timeout tende a desaparecer. Mesmo assim, o Dashboard precisa **diferenciar** as causas.

**Correção de documentação (incluída no escopo):** alinhar os docstrings de `app/api/app.py` que ainda dizem "3 s" para "10 s", refletindo o valor real de `TIMEOUT_PADRAO_SEGUNDOS`. O valor **permanece 10s** (não aumentar para mascarar lentidão).

### Diferenciação de estados (escopo mínimo viável, incremental)

**Fase 1 (obrigatória nesta correção).** O ponto central de orquestração é o `onTick` em `web/js/main.js` — é o único lugar que fala com a faixa global. A alteração é concreta e localizada:

1. **Estender `createObservabilidade` em `refresh.js`** com dois novos métodos, ao lado dos existentes `marcarSucesso`/`marcarFalha`, cada um escrevendo em `#global-status` com uma **classe CSS nomeada** (todas mutuamente exclusivas — cada método remove as outras classes antes de aplicar a sua):
   - `marcarIndisponivel()` → `global-status--error` (vermelha), texto "API indisponível. Verifique a conexão ou se o serviço está no ar.";
   - `marcarAtencao(texto)` → `global-status--warn` (amarela), texto como "Consumidor FIFO encerrado por erro." (texto injetado pelo chamador);
   - `marcarFalha()` → **permanece** `global-status--stale` (laranja); seu texto passa a distinguir timeout de cálculo quando acionado por esse motivo (ver abaixo), mas o default `MENSAGEM_DADOS_DESATUALIZADOS` é preservado.

   As classes CSS novas (`global-status--error`, `global-status--warn`) entram no CSS do Dashboard ao lado da existente `global-status--stale`. `marcarSucesso` passa a remover as três classes de estado.

2. **Reescrever o `onTick` em `main.js`** para distinguir as três situações a partir dos resultados do `allSettled` e do novo endpoint de saúde. **Decisão explícita de precedência** (de mais grave para menos grave): API indisponível > consumidor morto/degradado > timeout de cálculo > sucesso. O `onTick` passa a chamar **três** promessas no `allSettled` (a terceira é a saúde dos consumidores) e deixa de simplesmente lançar/retornar:

   ```js
   onTick: async () => {
     const [rEstat, rMon, rSaude] = await Promise.allSettled([
       api.obterEstatisticas(),
       api.obterMonitoramento(),
       api.obterSaudeConsumidores(),   // novo (api.js ganha este método)
     ]);
     // 1) API indisponível (vermelha): estatística E monitoramento rejeitaram por
     //    erro real de rede/HTTP (ApiError). A saúde não entra nesta decisão.
     const dadosFalharam = rEstat.status === "rejected" && rMon.status === "rejected";
     if (dadosFalharam) { obs.marcarIndisponivel(); return; }
     // 2) Consumidor morto/degradado (amarela): saúde resolveu e aponta problema.
     if (rSaude.status === "fulfilled" && temConsumidorEmFalha(rSaude.value)) {
       obs.marcarAtencao(descreverFalhaConsumidor(rSaude.value)); return;
     }
     // 3) Timeout de cálculo (laranja): alguma resolveu com timeout:true.
     const comTimeout = (rEstat.value && rEstat.value.timeout) ||
                        (rMon.value && rMon.value.timeout);
     if (comTimeout) { obs.marcarFalha(MENSAGEM_TIMEOUT_CALCULO); return; }
     // 4) Sucesso.
     obs.marcarSucesso();
   },
   ```

   Como o `onTick` passa a chamar `obs.*` diretamente e sempre `return` sem lançar, o `tick()` de `refresh.js` **não** aplicará mais `marcarFalha` automático no `catch` (o `catch` continua existindo como salvaguarda para exceções inesperadas do próprio `onTick`, mantendo `marcarFalha` como fallback). `MENSAGEM_TIMEOUT_CALCULO` é uma nova string exportada de `refresh.js` ("A atualização excedeu o tempo limite de cálculo. Os últimos dados válidos foram preservados."). **Decisão explícita de fonte do texto (resolve finding 6):** no caminho da faixa laranja de timeout de cálculo, o texto exibido é **sempre** a constante do frontend `MENSAGEM_TIMEOUT_CALCULO` (texto fixo); o campo `mensagem` vindo do backend (de `MENSAGEM_TIMEOUT_ESTATISTICAS`/`MENSAGEM_TIMEOUT_MONITORAMENTO` em `esquemas.py`) é **ignorado** nesse caminho. Assim há uma única fonte de verdade para o texto da faixa, independente de qual endpoint sinalizou `timeout:true`. `temConsumidorEmFalha`/`descreverFalhaConsumidor` são helpers puros em `main.js` sobre o payload de saúde (abaixo). A falha da própria chamada de saúde (status `rejected`) **não** aciona amarela — a saúde é informativa; se ela cair mas os dados estiverem OK, o tick é sucesso (não inventamos estado de consumidor).

3. **Novo método `api.obterSaudeConsumidores()` em `web/js/api.js`** chamando `GET /api/consumidores/saude` (segue o padrão dos demais métodos do cliente; herda o tratamento de `ApiError` status 0 vs HTTP já existente).

4. **Novo endpoint `GET /api/consumidores/saude`.** Serviço novo `app/api/servico_saude_consumidores.py` + rota em `app/api/app.py` + schema `RespostaSaudeConsumidores` em `esquemas.py`. Lê o estado do `GerenciadorConsumidores` via `request.app.state.gerenciador_consumidores` e devolve, por slot: `tipo` (standard/fifo), `consumer_id`, `vivo: bool` (= `thread.is_alive()`), `degradado: bool`, `reinicios: int`. Campos agregados de conveniência para o frontend: `algum_morto: bool`, `algum_degradado: bool`. `temConsumidorEmFalha` no frontend testa `algum_morto || algum_degradado`; `descreverFalhaConsumidor` monta o texto a partir do primeiro slot problemático (`tipo`+`consumer_id`). Como os consumidores vivem em threads do mesmo processo, o endpoint apenas inspeciona o estado em memória — **sem tocar na AWS**. Quando `gerenciador_consumidores is None` (verificação de inicialização falhou), devolve lista vazia com `iniciados: false` (indicador claro de "consumidores não iniciados"); o frontend não marca amarela nesse caso (não há dado para afirmar falha).

5. **Mensagens disponíveis vs em processamento.** Já derivado pelo StateDeriver (`Recebida`/`Aguardando` vs `Em processamento`) e exibido na tabela de monitoramento. A correção é só garantir que o novo `Estado.EXCLUSAO_NAO_CONFIRMADA` apareça com destaque próprio: adicionar `"Exclusão não confirmada": "estado-exclusao-nao-confirmada"` ao mapa fechado `CLASSE_ESTADO` em `monitoring.js` (senão cai no fallback neutro) e a classe correspondente no CSS.

6. **Falha de exclusão por ReceiptHandle expirado** fica visível de duas formas: o evento `exclusao_nao_confirmada` na linha do tempo do histórico (com `detalhe_erro` = erro original da AWS, já suportado por `EventoHistorico.detalhe_erro`) e o estado `Exclusão não confirmada` na tabela de monitoramento. O overview ganha o contador `exclusao_nao_confirmada` por fluxo.

**Fase 2 (recomendada, follow-up fora do escopo mínimo):** cabeçalho de status consolidado com badges "API funcionando / Consumidor Standard ativo / Consumidor FIFO ativo" combinando `GET /api/saude` + `GET /api/consumidores/saude`, atualizado pelo `createRefreshController`.

Regra transversal: **não esconder erros reais e não apresentar dado simulado como confirmado pela AWS.** O endpoint de saúde reflete `thread.is_alive()` real; o estado das mensagens é derivado de eventos reais; `exclusao_nao_confirmada` é Evento_Observado só gravado após a AWS rejeitar o delete. Nenhum número é inventado.

## Arquivos a modificar (resumo concreto)

- `app/dominio/tipos.py`: `EventType.EXCLUSAO_NAO_CONFIRMADA`, `Estado.EXCLUSAO_NAO_CONFIRMADA` (não entra em `ESTADOS_TERMINAIS`).
- `app/dominio/classificador_origem.py`: **obrigatório** adicionar `EventType.EXCLUSAO_NAO_CONFIRMADA: EventSource.OBSERVADO` a `ORIGEM_POR_TIPO` (senão o assert de import quebra).
- `app/dominio/state_deriver.py`: regra do novo estado no laço `reversed`; comportamento de reentrega documentado.
- `app/dominio/agregador_estatisticas.py`: contador `exclusao_nao_confirmada` por fluxo.
- `app/dominio/constantes.py`: `INTERVALO_SUPERVISAO_SEGUNDOS`, `MAX_REINICIOS_JANELA`, `JANELA_REINICIOS_SEGUNDOS`, `HEARTBEAT_VISIBILITY_SEGUNDOS`.
- `app/acesso/sqs_gateway.py`: `e_receipt_handle_expirado`, `e_erro_infra_sqs` (helpers puros) e `change_message_visibility` (frente C). `delete_message` **inalterado**.
- `app/consumidores/processamento.py`: reordenação delete→processada, captura de erro de delete, `exclusao_nao_confirmada`, campo novo de `ResultadoProcessamento`, parâmetros opt-in de visibility + validação (owner único), logging estruturado.
- `app/consumidores/consumidor_fifo.py`: guarda `try/except` por iteração em `executar` que sempre anexa `ResultadoIteracaoFifo`; campo `erro` em `ResultadoIteracaoFifo`; `@property iteracoes_com_erro` em `ResultadoConsumoFifo`; contabilização dedicada de `exclusao_nao_confirmada` separada das falhas, se o FIFO contabilizar o desfecho por mensagem (Opção A, aplicável ao FIFO).
- `app/consumidores/consumidor_standard.py`: guarda `try/except` por iteração em `executar`; campo `EstatisticasConsumo.iteracoes_com_erro`; campo dedicado `EstatisticasConsumo.exclusoes_nao_confirmadas` e ramo próprio `elif resultado.exclusao_nao_confirmada` em `_tratar_mensagem` (Opção A — não entra em `falhas_processamento`).
- `app/main.py`: `GerenciadorConsumidores` com slots + thread supervisora + política de reinício com teto; ordem de shutdown especificada; guarda externa nos alvos de thread.
- `app/observabilidade/__init__.py` + `app/observabilidade/log_estruturado.py`: novo módulo de logging estruturado.
- `app/api/servico_saude_consumidores.py` + `app/api/app.py` + `app/api/esquemas.py`: endpoint e schema de saúde dos consumidores; `exclusao_nao_confirmada` em `EstatisticasFluxoSaida` (default 0); alinhar docstrings "3 s" → "10 s" em `app.py`.
- `app/api/servico_monitoramento.py`: `_fluxo_para_saida` passa o novo contador.
- `web/js/areas/refresh.js`: `createObservabilidade` com `marcarIndisponivel`/`marcarAtencao` e classes `global-status--error`/`global-status--warn`; nova string exportada `MENSAGEM_TIMEOUT_CALCULO`; `marcarSucesso` passa a limpar as três classes de estado.
- `web/js/main.js`: reescrita do `onTick` para o `allSettled` de **três** chamadas (estatísticas, monitoramento, saúde) e a precatença de estados (API indisponível > consumidor em falha > timeout de cálculo > sucesso); helpers `temConsumidorEmFalha`/`descreverFalhaConsumidor`.
- `web/js/api.js`: novo método `obterSaudeConsumidores()` (`GET /api/consumidores/saude`); o ponto de distinção `ApiError` status 0 vs HTTP permanece inalterado e continua consumido indiretamente pelo `onTick`.
- `web/js/areas/monitoring.js`: entrada do novo estado em `CLASSE_ESTADO`.
- `web/js/areas/overview.js`: exibir uma linha distinta "Exclusão não confirmada" por fluxo (campo `exclusao_nao_confirmada`), separada da linha "Com falha".
- CSS do Dashboard: classes `estado-exclusao-nao-confirmada`, `global-status--error`, `global-status--warn`.
- Documentação: `README.md`, `ARQUITETURA.md`, `IMPLANTACAO.md`, `TESTES.md`.

## Tratamento de erros (concreto, por operação)

- **`delete_message` com ReceiptHandle expirado/inválido** (`InvalidParameterValue` com "receipt handle has expired" / `ReceiptHandleIsInvalid`): **recuperável**. O chamador (`processar_mensagem`) recebe `ResultadoProcessamento(sucesso=False, excluida=False, exclusao_nao_confirmada=True)`. Registra `exclusao_nao_confirmada` (OBSERVADO) com `error_detail=repr(erro)`. Log ERROR estruturado. Mensagem preservada na fila. Nunca registra `excluida`.
- **`delete_message` com outro erro de infra reconhecido** (`ClientError`/`BotoCoreError`): recuperável, mesmo tratamento (`exclusao_nao_confirmada`).
- **`delete_message` com erro inesperado não reconhecido**: re-levantado por `processar_mensagem`; capturado pela guarda por iteração do consumidor (frente B), logado WARNING, laço continua. Não mata a thread.
- **`receive_messages` falha (SQS indisponível)**: a exceção sobe de `processar_uma_iteracao`/`_executar_iteracao`; a guarda por iteração loga WARNING `iteracao_consumidor_falhou` e o laço continua. Recuperável.
- **`registrar_evento` falha ANTES do delete** (gravar `recebida`/`processamento_iniciado`/`exclusao_nao_confirmada`): `HistoricoRepository.registrar_evento` já levanta `FalhaRegistroEvento` (com retry interno para transitórios) com o erro original. A exceção sobe para a guarda por iteração (WARNING, laço continua). Como o `delete_message` **ainda não** ocorreu, a mensagem permanece na fila e **será reentregue** após o Visibility_Timeout — seguro, sem perda. Na idempotência, falhas de histórico já resultam em `ERRO_HISTORICO` (preserva a mensagem).
- **`registrar_evento` falha DEPOIS do delete confirmado** (gravar `processada`/`excluida` após o `delete_message` ter sucesso): **janela crítica aberta pela reordenação** (resolve finding 4). A mensagem **já saiu da fila** e **não** será reentregue; se a gravação do terminal falhar, o histórico fica sem `processada`/`excluida` para uma mensagem removida, violando a invariante "exatamente um `processada`/`excluida` por order_id". **Decisão explícita:** mitigar com retry dedicado + log ERROR, nesta ordem:
  1. O `HistoricoRepository.registrar_evento` já re-tenta transitórios internamente; confiamos nesse retry para a maioria dos casos.
  2. Em `processar_mensagem`, as gravações de `processada` e `excluida` que ocorrem **após** o delete confirmado são envolvidas em um bloco próprio. Se mesmo com o retry do repositório a gravação lançar `FalhaRegistroEvento`, emitimos um log **ERROR estruturado específico** `historico_pos_delete_falhou` com `order_id`, `fila`, `consumer_id`, `evento_pendente` (`processada`/`excluida`) e `erro=repr(erro)`, deixando rastro de que a mensagem foi removida da fila mas o terminal não foi persistido.
  3. A exceção **não** é re-levantada para a guarda por iteração neste caso (re-levantar não traz a mensagem de volta e poderia confundir o contador de erros de iteração com uma falha de consumo). `processar_mensagem` retorna `ResultadoProcessamento(sucesso=True, excluida=True)` (o delete de fato ocorreu) — o resultado reflete a realidade do SQS; o log ERROR é o canal de alerta. Esta janela é **risco conhecido e declarado** do laboratório: ocorre apenas se o DynamoDB falhar persistentemente nos ~milissegundos entre o delete e a gravação do terminal, e o `historico_pos_delete_falhou` a torna auditável. Um teste unitário (`test_processamento.py`) fixa esse comportamento: delete sucede, `registrar_evento(processada)` lança sempre → assert do log ERROR `historico_pos_delete_falhou` e `ResultadoProcessamento(sucesso=True, excluida=True)`, sem exceção propagada.
- **`change_message_visibility` falha** (frente C): recuperável; loga WARNING e continua o processamento sem renovar (a mensagem pode expirar — cai no fluxo normal de reentrega). Nunca aborta o processamento.
- **`visibility_timeout_estendido` inválido** (frente C): `ValueError` levantado por `processar_mensagem`/função de validação **antes** de qualquer chamada à AWS (owner único). Fatal para aquela chamada (configuração incorreta), tratado como erro de iteração pela guarda da frente B — não mata a thread.
- **Thread de consumidor encerra inesperadamente**: a supervisão recria o slot (novo `consumer_id`), com teto anti-tempestade. Acima do teto: marca `degradado`, loga ERROR `consumidor_em_falha_persistente`, expõe no endpoint de saúde. Fatal para aquele slot até intervenção, mas não derruba a aplicação.
- **Endpoint de estatísticas/monitoramento excede 10s**: já retorna `timeout:true` (200). Frontend mostra faixa laranja de "dados desatualizados" (texto ajustado para "cálculo excedeu o tempo limite"), preservando os últimos válidos. Não fatal.
- **`GET /api/consumidores/saude` com gerenciador ausente**: devolve lista vazia + indicador "consumidores não iniciados"; nunca 500.

## Validação de entradas externas

- **Erro do botocore no delete**: validar presença de `.response["Error"]["Code"]` antes de ler (via `e_receipt_handle_expirado`); ausência de `.response` → `e_erro_infra_sqs` decide por MRO. Falha reconhecida → `exclusao_nao_confirmada`.
- **`visibility_timeout_estendido`** (frente C): obrigatório quando `estender_visibility=True`; inteiro; faixa `[HEARTBEAT_VISIBILITY_SEGUNDOS, 43200]`. **Owner único: `processar_mensagem`/função pura de validação.** O gateway **não** valida (passthrough). Fora da faixa / flag sem valor → `ValueError` antes da AWS.
- **`estender_visibility` da Config_Simulacao**: booleano; owner da validação é o serviço de configuração (`PUT /api/config/simulacao`).
- **`consumer_id`** gerado internamente (contador + UUID): não é entrada externa.
- **`tipo` em `GET /api/consumidores/saude`**: não há parâmetro de rota variável (endpoint sem `{tipo}`); a resposta lista todos os slots.

## Invariantes e owners

- "Nunca `excluida` sem delete confirmado" — owner: `processar_mensagem` (único ponto que grava `excluida`, e só após `delete_message` retornar).
- "`processada` só após delete confirmado" — owner: `processar_mensagem` (reordenação).
- "Exatamente um `processada`/`excluida` por order_id ao final" — owner conjunto: idempotência (`verificar_idempotencia`, que bloqueia reprocessamento quando `processada` existe) + reordenação (que garante que `processada` só surge no sucesso confirmado).
- "Ordenação FIFO por MessageGroupId preservada" — owner: `ConsumidorFifo.selecionar_grupo` (inalterado) + guarda por iteração que nunca exclui fora de ordem.
- "Consumo não morre por exceção pontual" — owner: guarda por iteração em cada consumidor + supervisão no `GerenciadorConsumidores`.
- "Não duplicar consumidores" — owner: supervisão por slot (uma thread viva por slot; só recria mortas) + `iniciar()` idempotente (`if self._slots`).
- "Visibility de 30s do Console não é alterado automaticamente" — owner: nenhum código chama `set_queue_attributes`; `change_message_visibility` age só na mensagem recebida e só com opt-in.
- "Precedência de estados correta na reentrega" — owner: `derivar_estado`, dependente da ordem cronológica de `consultar_por_pedido` (garantida por `ScanIndexForward=True` + `ordenar_cronologicamente`).

## Testabilidade e cobertura dos 12 testes obrigatórios

Todos os testes usam fakes/dublês já estabelecidos (`HistoricoFake`, `SqsFake`, `SleepFake` em `tests/test_processamento.py`; fakes nos testes de consumidor), sem AWS real e sem espera real. Framework: pytest (+ hypothesis onde couber), já configurado em `pyproject.toml`.

### Extensão obrigatória dos dublês (resolve finding 2)

O `SqsFake` atual recebe um único `erro_ao_excluir: Exception | None` e, se definido, **lança sempre** em todo `delete_message` (confirmado nos três módulos de teste). Os testes 4, 5 e 10 exigem **desfecho por chamada** ("falha na 1ª, sucesso na 2ª"). Especificação concreta da extensão (retrocompatível):

- `SqsFake.__init__` ganha o parâmetro opcional `efeitos_delete: list[Exception | None] | None = None`. Quando fornecido, cada chamada de `delete_message` consome o próximo item em ordem: se for uma `Exception`, levanta-a; se for `None`, registra a exclusão e retorna `{}`. Esgotada a lista, o comportamento padrão (sucesso) prevalece. O parâmetro `erro_ao_excluir` **permanece** (lança sempre) para os testes atuais — os dois são mutuamente exclusivos; se ambos vierem, `efeitos_delete` tem precedência.
- Aplicar a mesma extensão aos `SqsFake` de `test_consumidor_fifo.py` e `test_consumidor_standard.py` (que hoje só registram exclusões), para modelar reentrega.

**Corpo das mensagens fake nos testes de consumidor (resolve finding 1).** Nos testes de consumidor (FIFO e Standard), as mensagens fake usam **corpo JSON de contrato com `order_id` explícito**, por exemplo:

```json
{"order_id": "pedido-1", "cenario": "sucesso", "processing_seconds": 5}
```

Isso é **obrigatório** porque o `ConsumidorStandard` deriva o `order_id` de `payload.order_id` **sem fallback para o corpo cru** (ao contrário de `consumidor_fifo._order_id_da_mensagem`). Se o corpo não for JSON com `order_id`, a idempotência do Standard decidiria `REJEITAR_VALIDACAO` e `processar_mensagem` nunca rodaria — não gravando `exclusao_nao_confirmada` e quebrando o teste 10. Com o corpo JSON de contrato acima, a idempotência decide **PROCESSAR** tanto no Standard quanto no FIFO, uniformizando os testes 4/5/10.

**Modelo de reentrega do `SqsFake` (resolve finding 2 — opção (a) escolhida).** O `SqsFake` **reentrega**: toda mensagem cujo `delete_message` **falhou** (ou que simplesmente **não foi excluída**) **PERMANECE** na fila do fake e é **devolvida no próximo `receive_messages`**, com `approximate_receive_count` **incrementado**. Só saem da fila do fake as mensagens cujo `delete_message` foi confirmado. Assim, os testes 4/5/10 que usam `executar(max_iteracoes=2)` com `efeitos_delete=[ERRO_HANDLE_EXPIRADO, None]` têm, de fato, uma **2ª entrega**: a 1ª iteração falha o delete (consome `ERRO_HANDLE_EXPIRADO`, mensagem permanece), a 2ª iteração recebe a mensagem de novo (handle válido, `approximate_receive_count` maior), consome o `None` de `efeitos_delete` e **confirma o delete** — fechando o ciclo exatamente como escrito. (A opção (b) — programar dois lotes manuais de `receive_messages` — foi descartada por tornar os testes mais verbosos e menos fiéis ao comportamento real do SQS.)

**Objeto de erro padrão nos testes** (formato botocore real, para exercitar `e_receipt_handle_expirado`/`e_erro_infra_sqs` de verdade): usar `botocore.exceptions.ClientError` construído como

```python
from botocore.exceptions import ClientError
ERRO_HANDLE_EXPIRADO = ClientError(
    {"Error": {"Code": "InvalidParameterValue",
               "Message": "The receipt handle has expired."}},
    "DeleteMessage",
)
```

Esse objeto expõe `.response["Error"]["Code"]` e `.response["Error"]["Message"]` no formato que os classificadores leem. `test_sqs_gateway.py` testa `e_receipt_handle_expirado(ERRO_HANDLE_EXPIRADO) is True`, `e_erro_infra_sqs(ERRO_HANDLE_EXPIRADO) is True`, e também um `ClientError` com outro `Code` (ex.: `ReceiptHandleIsInvalid` → expirado True; `AccessDenied` → expirado False, infra True) e um `Exception` comum (expirado False, infra False). `botocore` já é dependência do projeto, então importar `ClientError` nos testes não adiciona dependência nova.

### Atualização do teste de regressão existente (resolve finding 3)

`tests/test_processamento.py::test_erro_no_delete_message_propaga_erro_original` hoje faz `with pytest.raises(ErroAwsSimulado): _processar(cenario=SUCESSO, sqs=SqsFake(erro_ao_excluir=ErroAwsSimulado(...)))` e documenta "Req. 8.8: falha de infraestrutura propaga o erro". A nova regra muda isso: infra **reconhecida** (ClientError/ReceiptHandle) **não** propaga (vira `exclusao_nao_confirmada`). O `ErroAwsSimulado(Exception)` atual **não** tem `.response` nem herda de `ClientError`/`BotoCoreError`, logo cai em "erro não reconhecido" e **continuaria propagando por acaso** — frágil e contraditório com a intenção. **Decisão explícita:** substituir esse teste por **dois** testes nomeados sem ambiguidade:
- `test_delete_infra_reconhecida_vira_exclusao_nao_confirmada`: `SqsFake(erro_ao_excluir=ERRO_HANDLE_EXPIRADO)` → `processar_mensagem` **não** levanta; grava `exclusao_nao_confirmada`; `ResultadoProcessamento(sucesso=False, excluida=False, exclusao_nao_confirmada=True)`; nenhum `processada`/`excluida`.
- `test_delete_erro_inesperado_propaga_para_guarda`: `SqsFake(erro_ao_excluir=ErroAwsSimulado(...))` (sem `.response`) → `processar_mensagem` **propaga** (`pytest.raises(ErroAwsSimulado)`), e um teste de consumidor correspondente mostra que a guarda por iteração da Frente B captura, loga WARNING e o laço continua. Isso preserva a intenção original (erro inesperado sobe) sem contradizer a nova regra (infra reconhecida é tratada).

1. **FIFO sucesso em 5s, Visibility 30s** — `test_processamento.py`/`test_consumidor_fifo.py`: `processing_seconds=5`, cenário SUCESSO, `SqsFake.delete_message` sucesso; assert eventos na ordem `recebida, processamento_iniciado, processada, excluida` (regressão da **nova** ordem) e `ResultadoProcessamento(sucesso=True, excluida=True)`.
2. **Processamento 45s com Visibility 30s** — unitário determinístico: `SleepFake` registra `sleep(45)`; `SqsFake(erro_ao_excluir=ERRO_HANDLE_EXPIRADO)` (ClientError de ReceiptHandle expirado no delete); assert evento `exclusao_nao_confirmada`, **ausência** de `processada`/`excluida` (válido só após a reordenação), `exclusao_nao_confirmada=True`, nenhuma exceção propagada.
3. **Erro simulado de ReceiptHandle expirado** — `SqsFake(erro_ao_excluir=ERRO_HANDLE_EXPIRADO)`; assert `e_receipt_handle_expirado(erro) is True`, evento `exclusao_nao_confirmada` com `error_detail` contendo o erro, nenhuma exceção propagada por `processar_mensagem`.
4. **Consumidor segue operacional após a falha** — FIFO: `SqsFake(efeitos_delete=[ERRO_HANDLE_EXPIRADO, None])` (falha na 1ª chamada de delete, sucesso na 2ª); `executar(max_iteracoes=2, criterio_parada=…)`; assert 2 iterações completas, `resultado.iteracoes_com_erro`/estado coerente, laço não interrompido.
5. **Reentrega sem duplicar operação de negócio** — `HistoricoFake` persistente entre iterações + `SqsFake(efeitos_delete=[ERRO_HANDLE_EXPIRADO, None])`: 1ª iteração grava `exclusao_nao_confirmada` (sem `processada`); 2ª iteração (reentrega, handle válido) → idempotência `PROCESSAR` → grava `processada`+`excluida`; assert exatamente um `processada` e um `excluida` no total, derivação final `Estado.PROCESSADA`. Depende da ordem cronológica do repositório (garantida).
6. **Duas mensagens do mesmo MessageGroupId na ordem** — `test_consumidor_fifo.py`: lote `grupo-A: [m1, m2]`; assert `ordem_recebimento_do_grupo == [m1, m2]` e `processadas_do_grupo` na mesma ordem (regressão).
7. **Grupos diferentes permitem paralelismo** — unitário (resolve finding 7): a **propriedade testável automaticamente** é "na iteração de um grupo selecionado, mensagens de **outros** grupos NÃO são excluídas". O teste programa um lote com `grupo-A` e `grupo-B`, `executar` uma iteração, e assert que apenas os `receipt_handle` do grupo selecionado aparecem em `SqsFake.exclusoes` (nenhum do outro grupo). Essa propriedade, **somada a N≥2 instâncias** do `GerenciadorConsumidores`, é exatamente o que habilita o paralelismo entre grupos (cada instância pega um grupo distinto sem interferir no outro). O **paralelismo concorrente real** entre threads é intrinsecamente não-determinístico e fica declarado como validação de integração/roteiro manual no Dashboard — o item 7 **não** fica parcialmente coberto: a propriedade automatizável acima é a condição necessária e suficiente no nível de unidade.
8. **Encaminhamento automático para DLQ após limite** — `test_state_deriver.py`/`test_monitor_dlq.py`: eventos com `receive_count >= MAX_RECEIVE_COUNT` + `encaminhada_dlq` OBSERVADO → `Estado.ENCAMINHADA_DLQ`; garante que **DLQ tem precedência** sobre `exclusao_nao_confirmada` (DLQ é resolvido por `_dlq_confirmada` antes do laço).
9. **Reprocessamento manual da DLQ** — `redrive_from_dlq` já coberto; teste de regressão garantindo que a reordenação não quebrou o redrive.
10. **Standard permanece funcional após falhas semelhantes** — espelho do teste 4 para `ConsumidorStandard`, com mensagens fake de corpo JSON de contrato (`order_id` explícito) e `SqsFake(efeitos_delete=[ERRO_HANDLE_EXPIRADO, None])` reentregando a mensagem na 2ª iteração: assert laço continua; após a 1ª iteração `estatisticas.exclusoes_nao_confirmadas == 1` e `estatisticas.falhas_processamento == 0` (contador dedicado da Opção A — exclusão não confirmada **não** conta como falha); após a 2ª (reentrega, delete confirmado) `processadas_com_sucesso == 1`.
11. **Histórico nunca registra exclusão sem confirmação** — propriedade (hypothesis): para qualquer sequência de desfechos de `delete_message` (via `efeitos_delete`), o número de eventos `excluida` é ≤ número de deletes confirmados, e **nunca** há `excluida` após um delete que lançou. Owner: `test_processamento.py`. Invariante central. Teste complementar da janela pós-delete (resolve finding 4): delete sucede mas `registrar_evento(processada)` lança → assert log ERROR `historico_pos_delete_falhou`, `ResultadoProcessamento(sucesso=True, excluida=True)`, sem exceção propagada.
12. **Encerramento limpo e recuperação segura** — `test_main_composicao.py`: `GerenciadorConsumidores.iniciar()`/`parar()` com fakes; assert threads terminam no `parar()` na ordem (supervisor antes dos slots); teste de supervisão: um slot cuja thread morre (fake que levanta e retorna) → supervisão recria **uma** reposição com novo `consumer_id`, sem duplicar; respeita `_parada` (não recria após `parar()`); teto de reinícios marca `degradado`.

Testes de apoio: `test_tipos`/`test_classificador_origem` para o novo tipo/estado e o assert de import; `test_state_deriver` para a precedência do `Exclusão não confirmada` **e o caso de reentrega ativa** (constatação 6); `test_sqs_gateway` para `e_receipt_handle_expirado`, `e_erro_infra_sqs` e `change_message_visibility` (cliente boto3 fake); `test_api_*` para `GET /api/consumidores/saude` e para o contador `exclusao_nao_confirmada` em `EstatisticasFluxoSaida`.

Limites de teste: a supervisão real depende de temporização de threads; os testes injetam/fazem `join` determinístico e usam fakes que terminam o laço de imediato, evitando `sleep` real. O comportamento concorrente verdadeiro (reinício sob carga, paralelismo FIFO entre grupos) é validável por teste de integração manual (roteiro no Dashboard/Console), documentado em `TESTES.md`.

## Fora de escopo (confirmado)

Não recriar filas, não excluir mensagens, não limpar DynamoDB, não alterar atributos de fila (incluindo o Visibility de 30s) nem IAM. Não reiniciar o serviço automaticamente durante a análise. A paginação/limite temporal das consultas de monitoramento e o cabeçalho de status consolidado (Fase 2 da frente D) ficam como follow-up recomendado, não implementados nesta correção.

---

## Respostas às constatações da revisão (`design-review.json` / `design-review.md`)

Veredito da revisão atual (iteração mais recente): CHANGES_REQUESTED (0 HIGH, 4 MEDIUM, 2 NIT). Todas as constatações foram **verificadas por leitura direta do código** e endereçadas; as decisões abaixo foram incorporadas às seções correspondentes deste design. Resposta item a item:

- **Finding 1 (MEDIUM) — formato do corpo dos fakes vs seletor de `order_id` do Standard.** ENDEREÇADO. Na subseção "Extensão obrigatória dos dublês" ficou fixado que as mensagens fake dos testes de consumidor usam **corpo JSON de contrato com `order_id` explícito** (ex.: `{"order_id":"pedido-1","cenario":"sucesso","processing_seconds":5}`), porque o `ConsumidorStandard` usa `payload.order_id` **sem fallback de corpo cru**; assim a idempotência decide **PROCESSAR** tanto no Standard quanto no FIFO, e os testes 4/5/10 fecham como escritos.

- **Finding 2 (MEDIUM) — modelo de reentrega do `SqsFake`.** ENDEREÇADO. Na mesma subseção foi especificada a **opção (a)**: mensagens cujo `delete_message` falhou (ou não foram excluídas) **permanecem** na fila do fake e são **devolvidas no próximo `receive_messages`** com `approximate_receive_count` incrementado. Com isso, os testes 4/5/10 com `executar(max_iteracoes=2)` e `efeitos_delete=[ERRO_HANDLE_EXPIRADO, None]` têm uma **2ª entrega** que consome o `None` e confirma o delete.

- **Finding 3 (MEDIUM) — `EXCLUSAO_NAO_CONFIRMADA` na precedência do StateDeriver e convivência com `FALHA`.** ENDEREÇADO. Na seção "StateDeriver" o novo estado foi adicionado **explicitamente** à lista de precedência (item 3: desfecho mais recente do ciclo, sem `processamento_iniciado`/`recebida` posterior; **não terminal**; DLQ e PROCESSADA têm precedência). Declarada a **invariante**: no caminho de sucesso com delete falho, **nenhum** `FALHA` é gravado para aquela tentativa, só `exclusao_nao_confirmada`, eliminando a coexistência com `FALHA`.

- **Finding 4 (MEDIUM) — contabilização de `exclusao_nao_confirmada` nas estatísticas do consumidor (Opção A).** ENDEREÇADO. Nova subseção "Contabilização do resultado nos consumidores": contador **dedicado** `EstatisticasConsumo.exclusoes_nao_confirmadas` + campo correspondente em `EstatisticasFluxoSaida` (`Field(0, ge=0)`, retrocompatível), tratado em `_tratar_mensagem` com ramo próprio (`elif resultado.exclusao_nao_confirmada: self.estatisticas.exclusoes_nao_confirmadas += 1`), **sem** cair em `falhas_processamento`. No Dashboard (`overview.js`), linha distinta "Exclusão não confirmada" por fluxo, separada de "Com falha". Vale para Standard e, se aplicável, FIFO.

- **Finding 5 (NIT) — numeração duplicada na Frente D (Fase 1).** ENDEREÇADO. Os itens da Fase 1 foram **renumerados em sequência contínua (1..6)**.

- **Finding 6 (NIT) — fonte do texto da faixa laranja de timeout de cálculo.** ENDEREÇADO. Declarado que a faixa laranja de timeout de cálculo usa a constante do frontend `MENSAGEM_TIMEOUT_CALCULO` como **texto fixo**, **ignorando** o campo `mensagem` do backend nesse caminho.
