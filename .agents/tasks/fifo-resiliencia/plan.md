# Plano de Implementação — Resiliência dos consumidores SQS e `DeleteMessage` falho

Fonte de verdade: `.agents/tasks/fifo-resiliencia/design.md` (APROVADO). Tudo acontece DENTRO do worktree
`c:\github\amazon-sqs2\.worktrees\fix-fifo-resiliencia` (branch `fix/fifo-resiliencia-consumidores`). Não criar worktree,
não fazer merge/push, não tocar na AWS real, não reiniciar serviços.

## Ambiente e comandos de verificação

- Python 3.13 no sistema; as dependências de desenvolvimento (`fastapi`, `uvicorn`, `pytest`, `hypothesis`, `httpx`, `boto3`)
  NÃO estão instaladas no interpretador atual. Antes de rodar os testes é obrigatório instalá-las uma vez:
  `python -m pip install -e ".[dev]"` executado em `c:\github\amazon-sqs2\.worktrees\fix-fifo-resiliencia`.
- Suíte completa: `python -m pytest` (de dentro do worktree; `pyproject.toml` já define `testpaths=["tests"]`).
- Teste de um módulo: `python -m pytest tests/test_processamento.py` (idem para os demais).
- Não existe harness de teste JS no projeto; o frontend (`web/`) é validado manualmente pelo roteiro em `TESTES.md`.
  As mudanças de JS/CSS são verificadas por inspeção e pelos testes de API/schema do backend que alimentam o Dashboard.

## Ordem e dependências

Os itens 1–5 (domínio/tipos) são a base e vêm primeiro, pois tudo depende do novo `EventType`/`Estado` e das constantes.
Os itens 6–7 (gateway helpers e `processamento.py`) dependem de 1–5. Os itens 8–10 (consumidores e `main.py`) dependem de 6–7.
Os itens 11–13 (API: schema, serviço de saúde, monitoramento) dependem de 1–10. Os itens 14–18 (frontend) dependem de 11–13.
Os itens 19–30 (testes) seguem os módulos correspondentes. Os itens 31 (docs) ficam por último. Cada item deixa o repositório
compilável; os testes de regressão existentes só são ajustados nos itens que mudam o contrato correspondente.

---

- [ ] 1. Adicionar o novo tipo de evento e o novo estado ao domínio.
      Em `tipos.py`: `EventType.EXCLUSAO_NAO_CONFIRMADA = "exclusao_nao_confirmada"` e
      `Estado.EXCLUSAO_NAO_CONFIRMADA = "Exclusão não confirmada"`. NÃO incluir o novo `Estado` em `ESTADOS_TERMINAIS`
      (mantém apenas `{PROCESSADA, ENCAMINHADA_DLQ}`). NÃO adicionar entrada em `ORIGEM_OBRIGATORIA_POR_TIPO`.
      `ESTADOS_PERMITIDOS = frozenset(Estado)` absorve o novo estado automaticamente.
      Arquivos: `app/dominio/tipos.py`
      Verificar: `python -m pytest tests/test_contratos_base.py` — passa; `python -c "from app.dominio.tipos import EventType, Estado; print(EventType.EXCLUSAO_NAO_CONFIRMADA, Estado.EXCLUSAO_NAO_CONFIRMADA)"`.

- [ ] 2. Mapear a origem do novo evento no classificador (obrigatório para o assert de import não quebrar).
      Em `classificador_origem.py`, adicionar `EventType.EXCLUSAO_NAO_CONFIRMADA: EventSource.OBSERVADO` ao dicionário
      `ORIGEM_POR_TIPO`. O assert `set(ORIGEM_POR_TIPO) == set(EventType)` volta a passar.
      Arquivos: `app/dominio/classificador_origem.py`
      Verificar: `python -m pytest tests/test_classificador_origem.py` — passa (o import do módulo não levanta AssertionError).

- [ ] 3. Adicionar as constantes de supervisão e de heartbeat de visibility.
      Em `constantes.py`: `INTERVALO_SUPERVISAO_SEGUNDOS = 5`, `MAX_REINICIOS_JANELA = 5`, `JANELA_REINICIOS_SEGUNDOS = 60`,
      `HEARTBEAT_VISIBILITY_SEGUNDOS = 20`. NÃO alterar `VISIBILITY_TIMEOUT_SEGUNDOS` (30) nem `MAX_RECEIVE_COUNT` (2).
      Arquivos: `app/dominio/constantes.py`
      Verificar: `python -m pytest tests/test_contratos_base.py` — passa.

- [ ] 4. Refletir o novo estado no StateDeriver (precedência e caso de reentrega).
      Em `derivar_estado`, dentro do laço `reversed(lista)`, adicionar o ramo
      `if tipo is EventType.EXCLUSAO_NAO_CONFIRMADA: return Estado.EXCLUSAO_NAO_CONFIRMADA`, posicionado junto aos demais
      desfechos do ciclo (depois dos ramos de DLQ e PROCESSADA, que são resolvidos antes do laço por `_dlq_confirmada`/
      `tipos_presentes`). O estado é NÃO terminal e fica abaixo de DLQ e PROCESSADA. Comportamento de reentrega:
      como o laço é `reversed`, um `processamento_iniciado`/`recebida` posterior vence o `exclusao_nao_confirmada` anterior
      (retorna `EM_PROCESSAMENTO`/`RECEBIDA`), que é o desejado.
      Arquivos: `app/dominio/state_deriver.py`
      Verificar: `python -m pytest tests/test_state_deriver.py` — passa (inclui os novos casos do item 23).

- [ ] 5. Contar `exclusao_nao_confirmada` por fluxo no agregador de estatísticas.
      Em `agregador_estatisticas.py`: adicionar `EstatisticasFluxo.exclusao_nao_confirmada: int = 0` e, no bloco de derivação
      por mensagem, `elif estado is Estado.EXCLUSAO_NAO_CONFIRMADA: destino.exclusao_nao_confirmada += 1`. Esse estado NÃO
      conta como `processadas` nem como `com_falha`.
      Arquivos: `app/dominio/agregador_estatisticas.py`
      Verificar: `python -m pytest tests/test_agregador_estatisticas.py` — passa (inclui o novo caso do item 24).

- [ ] 6. Introduzir o módulo de logging estruturado.
      Criar o pacote `app/observabilidade/` com `__init__.py` e `log_estruturado.py` expondo
      `log_evento(nivel: int, evento: str, **campos) -> None` que emite uma linha JSON via `logging.getLogger("sqs_lab.resiliencia")`
      (`json.dumps({"evento": evento, **campos}, ensure_ascii=False, default=str)`).
      Arquivos: `app/observabilidade/__init__.py`, `app/observabilidade/log_estruturado.py`
      Verificar: `python -c "import logging; from app.observabilidade.log_estruturado import log_evento; log_evento(logging.ERROR, 'teste', a=1)"` — não lança.

- [ ] 7. Adicionar os classificadores de erro de delete e o `change_message_visibility` ao gateway (sem tocar `delete_message`).
      Em `sqs_gateway.py`: `RECEIPT_HANDLE_EXPIRADO_CODES = frozenset({"InvalidParameterValue", "ReceiptHandleIsInvalid"})`;
      `e_receipt_handle_expirado(erro)` (lê `.response["Error"]["Code"]`/`["Message"]`, estrutural, trata `InvalidParameterValue`
      só quando a mensagem contém "receipt handle has expired"); `e_erro_infra_sqs(erro)` (True se `.response is not None`, ou se
      `BotoCoreError`/`ClientError` no MRO) — padrão do `_e_erro_transitorio`, sem importar botocore no topo do módulo;
      `change_message_visibility(nome_fila, receipt_handle, visibility_timeout)` passthrough puro (resolve URL + chama o cliente),
      SEM validação de faixa (owner da validação é `processamento.py`). `delete_message` permanece intacto.
      Arquivos: `app/acesso/sqs_gateway.py`
      Verificar: `python -m pytest tests/test_sqs_gateway.py` — passa (inclui os novos casos do item 25).

- [ ] 8. Reordenar e endurecer o caminho de sucesso de `processar_mensagem` (frente A) e adicionar a extensão opt-in (frente C).
      Em `processamento.py`: adicionar `ResultadoProcessamento.exclusao_nao_confirmada: bool = False` (default preserva construções).
      Reordenar o bloco `if sucesso:` para: (1) `sqs.delete_message(...)`; (2) em `except Exception as erro`, se
      `e_receipt_handle_expirado(erro) or e_erro_infra_sqs(erro)` → `log_evento(ERROR, "delete_message_falhou", ...)`,
      registrar `EXCLUSAO_NAO_CONFIRMADA` (seq=3, OBSERVADO, `error_detail=repr(erro)`) e retornar
      `ResultadoProcessamento(sucesso=False, excluida=False, exclusao_nao_confirmada=True, erro_detalhe=repr(erro))`; senão `raise`;
      (3) só após delete confirmado, registrar `PROCESSADA` (seq=3) e `EXCLUIDA` (seq=4), nessa ordem, e retornar `sucesso=True`.
      Envolver as gravações pós-delete (`processada`/`excluida`) em bloco próprio: se lançarem `FalhaRegistroEvento`, emitir
      `log_evento(ERROR, "historico_pos_delete_falhou", order_id=..., fila=..., consumer_id=..., evento_pendente=..., erro=repr(erro))`,
      NÃO re-levantar, e retornar `ResultadoProcessamento(sucesso=True, excluida=True)`. Frente C: adicionar parâmetros opcionais
      `estender_visibility: bool = False` e `visibility_timeout_estendido: Optional[int] = None`; função pura
      `validar_extensao_visibility(estender, valor)` (owner único) — quando `estender_visibility=True`, valor obrigatório, inteiro,
      faixa `[HEARTBEAT_VISIBILITY_SEGUNDOS, 43200]`, senão `ValueError` antes de chamar a AWS; quando ligado e cenário SUCESSO,
      dormir em fatias de `HEARTBEAT_VISIBILITY_SEGUNDOS` chamando `change_message_visibility` entre fatias (falha de CMV → WARNING,
      segue sem renovar). `DURACAO_SUPERIOR_VISIBILITY` ignora a extensão. Importar `log_evento`, `e_receipt_handle_expirado`,
      `e_erro_infra_sqs`, `FalhaRegistroEvento`.
      Arquivos: `app/consumidores/processamento.py`
      Verificar: `python -m pytest tests/test_processamento.py` — passa (inclui os itens 19/20/21/26/27 e substitui o teste de regressão).

- [ ] 9. Adicionar a guarda por iteração e o contador dedicado de exclusão não confirmada ao Consumidor_Standard.
      Em `consumidor_standard.py`: adicionar `EstatisticasConsumo.iteracoes_com_erro: int = 0` e
      `EstatisticasConsumo.exclusoes_nao_confirmadas: int = 0`. Em `executar`, envolver `self._executar_iteracao()` em
      `try/except Exception` (NÃO `BaseException`) que incrementa `iteracoes_com_erro`, loga
      `log_evento(WARNING, "iteracao_consumidor_falhou", consumer_id=..., fila=self.nome_fila, erro=repr(e))` e NÃO re-levanta.
      Em `_tratar_mensagem`, trocar o `if/else` final por ramo dedicado:
      `if resultado.sucesso: ... processadas_com_sucesso` / `elif resultado.exclusao_nao_confirmada: ... exclusoes_nao_confirmadas`
      / `else: ... falhas_processamento`.
      Arquivos: `app/consumidores/consumidor_standard.py`
      Verificar: `python -m pytest tests/test_consumidor_standard.py` — passa (inclui o item 28).

- [ ] 10. Adicionar a guarda por iteração ao Consumidor_FIFO e contabilizar exclusão não confirmada separada das falhas.
      Em `consumidor_fifo.py`: adicionar `ResultadoIteracaoFifo.erro: Optional[str] = None` e
      `@property ResultadoConsumoFifo.iteracoes_com_erro` (`sum(1 for it in self.iteracoes if it.erro is not None)`).
      Em `executar`, trocar `iteracoes.append(self.processar_uma_iteracao())` por `try/except Exception` que, no erro, loga
      `log_evento(WARNING, "iteracao_consumidor_falhou", ...)` e anexa `ResultadoIteracaoFifo(recebidas=0, grupo_selecionado=None, erro=repr(e))`
      (sempre anexa, nunca pula), sem re-levantar. Onde o FIFO contabilizar o desfecho por mensagem, somar `exclusao_nao_confirmada`
      separado das falhas de processamento.
      Arquivos: `app/consumidores/consumidor_fifo.py`
      Verificar: `python -m pytest tests/test_consumidor_fifo.py` — passa (inclui os itens 22 e 29).

- [ ] 11. Reescrever o `GerenciadorConsumidores` com slots + thread supervisora + política de reinício com teto.
      Em `main.py`: substituir as listas paralelas por `@dataclass _SlotConsumidor(tipo, consumidor, thread, reinicios=0,
      marcos_reinicio=[], degradado=False)` e `self._slots: list[_SlotConsumidor]`. `iniciar()` idempotente por `if self._slots: return`,
      criando N slots por tipo (N de `estado_instancias.instancias(tipo)`); FIFO continua usando `gerar_consumer_id("consumidor-fifo")`.
      Alvos `_executar_standard`/`_executar_fifo` ganham `try/except Exception` externo que loga
      `consumidor_encerrou_por_erro` (ERROR) e retorna (thread termina sem exceção subir). Método `supervisionar()` em thread daemon
      `name="supervisor-consumidores"`: a cada `INTERVALO_SUPERVISAO_SEGUNDOS` (via `self._parada.wait(timeout=...)`), para cada slot
      com `not thread.is_alive() and not self._deve_parar() and not slot.degradado` chama `_recriar_slot(slot)`. `_recriar_slot` cria
      consumidor+thread novos (novo `consumer_id`) no mesmo índice, registra timestamp em `marcos_reinicio`, descarta marcos fora de
      `JANELA_REINICIOS_SEGUNDOS`, incrementa `reinicios`; acima de `MAX_REINICIOS_JANELA` marca `slot.degradado=True`, loga
      `consumidor_em_falha_persistente` (ERROR) e para de reiniciar. `parar(*, timeout=5.0)` na ordem: `_parada.set()` → `.parar()` nos
      Standard → encerrar/join da supervisora ANTES dos slots → `join(timeout)` nos slots → limpar `self._slots`. Threads daemon.
      `quantidade_threads` segue contando as threads de slot vivas. Expor acesso ao estado dos slots para o endpoint de saúde (item 12).
      Arquivos: `app/main.py`
      Verificar: `python -m pytest tests/test_main_composicao.py` — passa (inclui o item 30; `test_gerenciador_inicia_threads_por_instancia_configurada` continua válido).

- [ ] 12. Criar o endpoint `GET /api/consumidores/saude` + schema + serviço.
      Adicionar `RespostaSaudeConsumidores` e um item por slot (`tipo`, `consumer_id`, `vivo`, `degradado`, `reinicios`) em
      `esquemas.py`, com agregados `algum_morto`, `algum_degradado` e `iniciados`. Criar `app/api/servico_saude_consumidores.py` que lê
      `request.app.state.gerenciador_consumidores`: se `None`, devolve `iniciados=False` com lista vazia; senão inspeciona os slots em
      memória (`thread.is_alive()`, `degradado`, `reinicios`) sem tocar na AWS. Registrar a rota em `app.py` dentro do roteador
      `/api/consumidores` (ou um novo roteador), passando o `request`.
      Arquivos: `app/api/esquemas.py`, `app/api/servico_saude_consumidores.py`, `app/api/app.py`
      Verificar: `python -m pytest tests/test_api_config_saude.py tests/test_main_composicao.py` — passam (inclui o item 30 para saúde).

- [ ] 13. Expor `exclusao_nao_confirmada` por fluxo na API e alinhar o docstring de timeout.
      Em `esquemas.py`, adicionar `EstatisticasFluxoSaida.exclusao_nao_confirmada: int = Field(0, ge=0, description=...)` (default 0,
      retrocompatível). Em `servico_monitoramento.py`, `_fluxo_para_saida` passa `exclusao_nao_confirmada=estat.exclusao_nao_confirmada`.
      Em `app.py`, alinhar os docstrings que dizem "3 s" para "10 s" (valor real de `TIMEOUT_PADRAO_SEGUNDOS`); NÃO alterar o valor.
      Arquivos: `app/api/esquemas.py`, `app/api/servico_monitoramento.py`, `app/api/app.py`
      Verificar: `python -m pytest tests/test_api_estatisticas_monitoramento.py` — passa (inclui o item 24 para a saída da API).

- [ ] 14. Estender `createObservabilidade`/`refresh.js` para três estados de faixa + nova string de timeout.
      Em `web/js/areas/refresh.js`: adicionar `marcarIndisponivel()` (classe `global-status--error`, texto "API indisponível...") e
      `marcarAtencao(texto)` (classe `global-status--warn`, texto injetado); cada método remove as outras classes antes de aplicar a sua.
      `marcarSucesso` passa a remover as três classes (`--stale`, `--error`, `--warn`). Exportar a constante
      `MENSAGEM_TIMEOUT_CALCULO` ("A atualização excedeu o tempo limite de cálculo. Os últimos dados válidos foram preservados.").
      `marcarFalha` continua usando `global-status--stale` com texto default `MENSAGEM_DADOS_DESATUALIZADOS`.
      Arquivos: `web/js/areas/refresh.js`
      Verificar: inspeção (sem harness JS); confirmar que os métodos novos existem e que `marcarSucesso` limpa as três classes.

- [ ] 15. Adicionar o método `obterSaudeConsumidores()` ao cliente de API do frontend.
      Em `web/js/api.js`, adicionar `obterSaudeConsumidores()` chamando `GET /api/consumidores/saude`, no padrão dos demais métodos
      (herda o tratamento de `ApiError` status 0 vs HTTP).
      Arquivos: `web/js/api.js`
      Verificar: inspeção; confirmar o novo método apontando para `/consumidores/saude`.

- [ ] 16. Reescrever o `onTick` de `main.js` com a precedência de estados.
      Em `web/js/main.js`, o `onTick` passa a `Promise.allSettled([obterEstatisticas, obterMonitoramento, obterSaudeConsumidores])` e
      aplica a precedência: (1) ambas estatística e monitoramento rejeitadas → `obs.marcarIndisponivel()`; (2) saúde resolvida e
      `temConsumidorEmFalha(saude)` → `obs.marcarAtencao(descreverFalhaConsumidor(saude))`; (3) alguma com `timeout:true` →
      `obs.marcarFalha(MENSAGEM_TIMEOUT_CALCULO)`; (4) senão `obs.marcarSucesso()`. O `onTick` chama `obs.*` e sempre `return` sem lançar
      (o `catch` de `refresh.js` fica como salvaguarda). Adicionar os helpers puros `temConsumidorEmFalha` (`algum_morto||algum_degradado`)
      e `descreverFalhaConsumidor` (texto do primeiro slot problemático: `tipo`+`consumer_id`). A falha da chamada de saúde (rejected)
      NÃO aciona amarela. Obter a instância de observabilidade (via `createObservabilidade`) e passá-la ao `createRefreshController`.
      Arquivos: `web/js/main.js`
      Verificar: inspeção; conferir a cadeia de precedência e que nenhum caminho lança.

- [ ] 17. Destacar o novo estado na tabela de monitoramento e nas estatísticas do overview.
      Em `web/js/areas/monitoring.js`, adicionar `"Exclusão não confirmada": "estado-exclusao-nao-confirmada"` ao mapa fechado
      `CLASSE_ESTADO`. Em `web/js/areas/overview.js`, adicionar uma linha distinta "Exclusão não confirmada" por fluxo (chave
      `exclusao_nao_confirmada`), SEPARADA da linha "Com falha" (incluir em `STAT_LABELS` ou renderizar como linha extra).
      Arquivos: `web/js/areas/monitoring.js`, `web/js/areas/overview.js`
      Verificar: inspeção; confirmar o novo rótulo e a chave consumida.

- [ ] 18. Adicionar as classes CSS dos novos estados/faixas.
      Em `web/styles.css`, adicionar `.estado-exclusao-nao-confirmada`, `.global-status--error` (vermelha) e `.global-status--warn`
      (amarela), ao lado da `.global-status--stale` (laranja) existente.
      Arquivos: `web/styles.css`
      Verificar: inspeção; confirmar as três classes novas.

- [ ] 19. Teste 1 — FIFO/sucesso em 5s, Visibility 30s (nova ordem de eventos).
      Em `test_processamento.py` (e espelho em `test_consumidor_fifo.py` quando couber): `processing_seconds=5`, cenário SUCESSO,
      `SqsFake` com delete bem-sucedido; assert eventos na ordem `recebida, processamento_iniciado, processada, excluida` e
      `ResultadoProcessamento(sucesso=True, excluida=True)`. Atualizar `test_sucesso_executa_delete_message_e_registra_excluida`
      para a nova ordem (delete antes de `processada`).
      Arquivos: `tests/test_processamento.py`, `tests/test_consumidor_fifo.py`
      Verificar: `python -m pytest tests/test_processamento.py tests/test_consumidor_fifo.py` — passam.

- [ ] 20. Teste 2 — processamento 45s com Visibility 30s (delete falha, vira exclusão não confirmada).
      `SleepFake` registra `sleep(45)`; `SqsFake(erro_ao_excluir=ERRO_HANDLE_EXPIRADO)` (ClientError de ReceiptHandle expirado);
      assert evento `exclusao_nao_confirmada` presente, AUSÊNCIA de `processada`/`excluida`, `exclusao_nao_confirmada=True`,
      nenhuma exceção propagada.
      Arquivos: `tests/test_processamento.py`
      Verificar: `python -m pytest tests/test_processamento.py` — passa.

- [ ] 21. Teste 3 — erro simulado de ReceiptHandle expirado + substituição do teste de regressão.
      Definir o objeto padrão `ERRO_HANDLE_EXPIRADO = ClientError({"Error": {"Code": "InvalidParameterValue", "Message": "The receipt handle has expired."}}, "DeleteMessage")`
      (import de `botocore.exceptions`, já é dependência). Remover `test_erro_no_delete_message_propaga_erro_original` e criar DOIS testes:
      `test_delete_infra_reconhecida_vira_exclusao_nao_confirmada` (`erro_ao_excluir=ERRO_HANDLE_EXPIRADO` → não levanta; grava
      `exclusao_nao_confirmada`; `sucesso=False, excluida=False, exclusao_nao_confirmada=True`; sem `processada`/`excluida`) e
      `test_delete_erro_inesperado_propaga_para_guarda` (`erro_ao_excluir=ErroAwsSimulado(...)` sem `.response` → `pytest.raises`).
      Arquivos: `tests/test_processamento.py`
      Verificar: `python -m pytest tests/test_processamento.py` — passa.

- [ ] 22. Teste 4 — consumidor FIFO segue operacional após a falha (desfecho por chamada).
      Estender o `SqsFake` de `test_consumidor_fifo.py`: `__init__` ganha `efeitos_delete: list[Exception|None]|None = None`
      (consome um item por `delete_message`: `Exception`→levanta; `None`→registra exclusão e `{}`); `erro_ao_excluir` continua
      lançando sempre; `efeitos_delete` tem precedência. Modelo de reentrega: mensagens com delete falho (ou não excluídas)
      PERMANECEM na fila do fake e são devolvidas no próximo `receive_messages` com `approximate_receive_count` incrementado.
      Teste: `efeitos_delete=[ERRO_HANDLE_EXPIRADO, None]`, `executar(max_iteracoes=2, ...)`; assert 2 iterações, `iteracoes_com_erro`
      coerente, laço não interrompido.
      Arquivos: `tests/test_consumidor_fifo.py`
      Verificar: `python -m pytest tests/test_consumidor_fifo.py` — passa.

- [ ] 23. Teste 5 (reentrega sem duplicar) + casos do StateDeriver.
      Em `test_processamento.py` (ou `test_consumidor_fifo.py`): `HistoricoFake` persistente entre iterações +
      `SqsFake(efeitos_delete=[ERRO_HANDLE_EXPIRADO, None])`; assert exatamente um `processada` e um `excluida` no total e derivação
      final `Estado.PROCESSADA`. Em `test_state_deriver.py`: adicionar caso em que `exclusao_nao_confirmada` é o evento mais recente →
      `Estado.EXCLUSAO_NAO_CONFIRMADA`; e o caso de reentrega ativa (`...exclusao_nao_confirmada, recebida, processamento_iniciado`) →
      `Estado.EM_PROCESSAMENTO`; e precedência DLQ/PROCESSADA sobre o novo estado.
      Arquivos: `tests/test_processamento.py`, `tests/test_state_deriver.py`
      Verificar: `python -m pytest tests/test_processamento.py tests/test_state_deriver.py` — passam.

- [ ] 24. Testes do agregador e da saída da API para o novo contador.
      Em `test_agregador_estatisticas.py`: mensagem derivando `EXCLUSAO_NAO_CONFIRMADA` incrementa `exclusao_nao_confirmada` e NÃO
      incrementa `processadas`/`com_falha`. Em `test_api_estatisticas_monitoramento.py`: `EstatisticasFluxoSaida` serializa
      `exclusao_nao_confirmada` (default 0 quando ausente).
      Arquivos: `tests/test_agregador_estatisticas.py`, `tests/test_api_estatisticas_monitoramento.py`
      Verificar: `python -m pytest tests/test_agregador_estatisticas.py tests/test_api_estatisticas_monitoramento.py` — passam.

- [ ] 25. Testes do gateway para os classificadores e `change_message_visibility`.
      Em `test_sqs_gateway.py`: `e_receipt_handle_expirado(ERRO_HANDLE_EXPIRADO) is True`; `ReceiptHandleIsInvalid` → expirado True;
      `AccessDenied` → expirado False, infra True; `Exception` comum → ambos False; `e_erro_infra_sqs(ERRO_HANDLE_EXPIRADO) is True`;
      `change_message_visibility` chama o cliente boto3 fake com `QueueUrl/ReceiptHandle/VisibilityTimeout` e repassa erro bruto.
      Arquivos: `tests/test_sqs_gateway.py`
      Verificar: `python -m pytest tests/test_sqs_gateway.py` — passa.

- [ ] 26. Teste 11 — histórico nunca registra exclusão sem confirmação (propriedade + janela pós-delete).
      Em `test_processamento.py`: teste de propriedade (hypothesis) sobre sequências de `efeitos_delete` garantindo que o número de
      eventos `excluida` ≤ número de deletes confirmados e que nunca há `excluida` após um delete que lançou. Teste complementar da
      janela pós-delete: delete sucede mas `registrar_evento(processada)` lança sempre → assert log ERROR `historico_pos_delete_falhou`,
      `ResultadoProcessamento(sucesso=True, excluida=True)`, sem exceção propagada (usar `caplog`).
      Arquivos: `tests/test_processamento.py`
      Verificar: `python -m pytest tests/test_processamento.py` — passa.

- [ ] 27. Teste da frente C — extensão de visibility opt-in (validação e heartbeat).
      Em `test_processamento.py`: `estender_visibility=True` com `visibility_timeout_estendido` fora de `[HEARTBEAT, 43200]` ou ausente →
      `pytest.raises(ValueError)` antes de qualquer chamada à AWS; com flag ligada e cenário SUCESSO, o `SleepFake` recebe fatias e o
      `SqsFake` registra chamadas de `change_message_visibility`; flag desligada (default) preserva 100% o comportamento atual.
      Arquivos: `tests/test_processamento.py`
      Verificar: `python -m pytest tests/test_processamento.py` — passa.

- [ ] 28. Teste 10 — Standard permanece funcional após falhas semelhantes.
      Estender o `SqsFake` de `test_consumidor_standard.py` com `efeitos_delete` e o modelo de reentrega (igual ao item 22). Mensagens
      fake com corpo JSON de contrato (`{"order_id":"pedido-1","cenario":"sucesso","processing_seconds":5}`), pois o Standard deriva
      `order_id` de `payload.order_id` sem fallback cru. `efeitos_delete=[ERRO_HANDLE_EXPIRADO, None]`: após a 1ª iteração
      `exclusoes_nao_confirmadas == 1` e `falhas_processamento == 0`; após a 2ª (reentrega, delete confirmado)
      `processadas_com_sucesso == 1`; laço continua.
      Arquivos: `tests/test_consumidor_standard.py`
      Verificar: `python -m pytest tests/test_consumidor_standard.py` — passa.

- [ ] 29. Testes 6 e 7 — ordem FIFO por grupo e isolamento entre grupos.
      Em `test_consumidor_fifo.py`: (6) lote `grupo-A:[m1,m2]` → `ordem_recebimento_do_grupo == [m1,m2]` e `processadas_do_grupo` na
      mesma ordem; (7) lote com `grupo-A` e `grupo-B`, uma iteração, assert que apenas os `receipt_handle` do grupo selecionado
      aparecem em `SqsFake.exclusoes` (nenhum do outro grupo). Garantir corpo JSON de contrato nas mensagens fake.
      Arquivos: `tests/test_consumidor_fifo.py`
      Verificar: `python -m pytest tests/test_consumidor_fifo.py` — passa.

- [ ] 30. Testes 8, 9 e 12 — DLQ, redrive e encerramento/recuperação.
      (8) em `test_state_deriver.py`/`test_monitor_dlq.py`: `encaminhada_dlq` OBSERVADO com `receive_count >= MAX_RECEIVE_COUNT` →
      `ENCAMINHADA_DLQ` com precedência sobre `exclusao_nao_confirmada`. (9) em `test_monitor_dlq.py`/`test_sqs_gateway.py`: regressão do
      `redrive_from_dlq` após a reordenação. (12) em `test_main_composicao.py`: `iniciar()`/`parar()` com fakes → threads terminam na
      ordem (supervisor antes dos slots); supervisão recria UMA reposição com novo `consumer_id` sem duplicar quando um slot morre;
      respeita `_parada` (não recria após `parar()`); teto de reinícios marca `degradado`; `GET /api/consumidores/saude` reflete o estado.
      Arquivos: `tests/test_state_deriver.py`, `tests/test_monitor_dlq.py`, `tests/test_main_composicao.py`
      Verificar: `python -m pytest tests/test_state_deriver.py tests/test_monitor_dlq.py tests/test_main_composicao.py` — passam.

- [ ] 31. Atualizar a documentação.
      `README.md`, `ARQUITETURA.md`, `IMPLANTACAO.md` (instruções de implantar a correção na EC2 sem reiniciar automaticamente;
      backup/versionamento antes da implantação) e `TESTES.md` (roteiro de validação prática pelo Dashboard/Console: novo estado
      "Exclusão não confirmada", endpoint de saúde, faixa vermelha>amarela>laranja>sucesso, cenário de 45s com flag opt-in on/off,
      recuperação de thread). Documentar o novo evento/estado, o endpoint `GET /api/consumidores/saude`, a flag opt-in de
      `ChangeMessageVisibility` desligada por padrão e os limites conhecidos (thread viva-mas-travada fora de escopo; janela pós-delete).
      Arquivos: `README.md`, `ARQUITETURA.md`, `IMPLANTACAO.md`, `TESTES.md`
      Verificar: `python -m pytest` (suíte completa) — passa; revisão de que a doc reflete o comportamento implementado.

## Verificação final

Rodar a suíte completa a partir do worktree: `python -m pytest`. Todos os testes devem passar, incluindo os 12 obrigatórios e os de
regressão ajustados. Nenhuma chamada à AWS real; nenhum serviço reiniciado.
