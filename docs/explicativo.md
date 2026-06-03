# GEX Webhook Pipeline Documento Explicativo

## Visão Geral do Fluxo

```
Gateway (grummer / lous)
        │ HTTP POST /webhooks/{gateway}
        ▼
  ┌────────────────────────────────────────┐
  │ gex_receiver (FastAPI)                 │
  │                                        │
  │ 1. decrypt (AES-256-CBC se grummer)    │
  │ 2. validate schema (Pydantic v2)       │
  │ 3. check idempotency (processed_events)│
  │ 4. INSERT raw_payloads                 │
  │ 5. publish lead.received (se approved) │
  └──────────┬─────────────────────────────┘
             │ lead.received (RabbitMQ)
             ▼
  ┌──────────────────────────────────────┐
  │ gex_worker (FastStream)              │
  │                                      │
  │ 1. sp_insert_lead (stored proc)      │
  │ 2. CREATE 4 distribution_status rows │
  │ 3. publish 4 distribution queues     │
  │ 4. retry 3× com backoff se falha     │
  │ 5. DLQ em caso de exaustão           │
  └──────────┬───────────────────────────┘
             │ dist.sms / dist.email / dist.callcenter / dist.whatsapp
             ▼
        distribuidores (sms implementado, demais placeholders)
```

O diagrama Mermaid completo está no `README.md` raiz. O fluxo acima resume as duas camadas: **receiver** (FastAPI, síncrono por request) e **worker** (FastStream, event-driven assíncrono).

---

## Escolha de Bibliotecas e Linguagem

**Python 3.14** por ser a versão estável mais recente e incluir `uuid.uuid7()` na stdlib.

| Componente | Escolha | Alternativa considerada | Por que esta |
|---|---|---|---|
| HTTP framework | FastAPI | Flask, Django Ninja | Validação integrada com Pydantic v2, `Annotated` DI nativa no 3.14, performance via Starlette |
| Consumer assíncrono | FastStream | aio-pika puro, arq, Celery | Declaração declarativa de filas/exchanges, middleware nativo (`BaseMiddleware`), integração com Pydantic |
| Camada de dados | SQLAlchemy Core (text()) | ORM, Tortoise-ORM | Challenge exige SQL puro com stored procedures. Core dá controle total sobre SQL sem abrir mão de async session + pool |
| Driver MySQL async | asyncmy | aiomysql, asyncmy | Único driver async compatível com SQLAlchemy 2.1+ e Python 3.14 |
| Cripto | cryptography (Fernet + AES-CBC manual) |  | Standard da indústria; suporta AES-256-CBC PKCS7 que o gateway grummer exige |
| Logging | structlog + stdlib logging |  | Logs estruturados JSON, contextvars para correlation_id, integração com uvicorn |
| Testes | pytest + testcontainers |  | Testcontainers sobe RabbitMQ real para testes de integração; pytest markers separam unit/integration |
| Gerenciamento de projeto | uv workspace | PDM, Poetry | Resolução mais rápida, workspace nativo, `uv sync --package` para imagens modulares |

**Por que NÃO ORM:** O challenge pede stored procedures e SQL puro. ORM adicionaria complexidade sem benefício.

**Por que NÃO aio-pika diretamente:** FastStream gerencia declaração de topologia (exchanges/queues/bindings), retry middleware, e DLQ routing. aio-pika puro exigiria reimplementar tudo isso.

---

## Decisões Importantes de Arquitetura

### 1. Idempotência em Duas Camadas

- **Receiver layer:** `processed_events` com `UNIQUE(transaction_id, event)`. O receiver faz `INSERT ... ON DUPLICATE KEY UPDATE` se o par já existe, retorna `duplicate` (HTTP 200) sem publicar na fila.
- **Worker layer:** `lead_events` com `UNIQUE(order_id, event)`. Mesmo se uma mensagem chegar duas vezes (ex.: consumer reiniciou antes do ack), o segundo INSERT falha silenciosamente e o worker ignora.

**Por que duas camadas?** A primeira evita trabalho desnecessário (decrypt, publish, fila). A segunda é rede de segurança contra entrega duplicada do RabbitMQ.

### 2. Chave Natural de Idempotência: `(transaction_id, event)`

Alinhado com a especificação seção 1(f): "A chave natural é **transaction_id + event** (ou seja, **order + event**)."

### 3. UUIDv7 (via `uuid.uuid7()` Python 3.14)

Todas as PKs usam UUIDv7, ordenado por timestamp, evitando fragmentação de índice no InnoDB (vs UUIDv4 aleatório). O sort key é o timestamp embutido, então `ORDER BY id` equivale a `ORDER BY created_at` sem índice extra.

### 4. Modularização Preservada nas Imagens Docker

Cada aplicação tem seu próprio Dockerfile (`apps/receiver/Dockerfile`, `apps/worker/Dockerfile`) que instala **apenas** seu pacote via `uv sync --package gex-<app> --frozen --no-editable`. O worker image contém `gex_common` + `gex_worker`; o receiver contém `gex_common` + `gex_receiver`. Um rebuild do worker não afeta o receiver e vice-versa.

### 5. Health Check em Dois Níveis

- `GET /health` - verifica se o processo responde (liveness)
- `GET /health/ready` - verifica DB + RabbitMQ (readiness)

Usado pelo Docker Compose (`depends_on: service_healthy`) para garantir ordem de inicialização.

---

## Estratégia de Retry e DLQ

**Backoffs:** `[1000ms, 4000ms, 16000ms]` progressão geométrica ×4.

**Implementação:** `RetryMiddleware` (FastStream `BaseMiddleware`) no worker. Em cada falha, incrementa `retry-count`. Quando excede 3 tentativas, `DlqMiddleware.after_processed()` publica a mensagem na DLQ apropriada e retorna `False` para não suprimir a exceção (permite nack).

**DLQ layout:**

| DLQ | Origem | Motivo |
|---|---|---|
| `lead.dead.decrypt_failed` | receiver | AES-256-CBC falhou |
| `lead.dead.schema_failed` | receiver | Validação Pydantic falhou |
| `lead.dead.consumer_failed` | worker | `sp_insert_lead` ou publish falhou após 3 retries |
| `dist.dead.sms` | worker (dist.sms) | POST ao webhook.site falhou após 3 retries |

A DLQ é persistente no RabbitMQ. Não há consumer automático da DLQ o reprocessamento é manual ou via script externo.

---

## Premissas e Limitações Conhecidas

1. **Webhook.site rate limiting (429):** O `dist.sms` consumer sofre 100% de falha no ambiente E2E porque o webhook.site retorna 429 após algumas requisições Em produção, usaríamos um provedor SMS real sem rate limit tão agressivo.

2. **Sem circuit breaker:** O `dist.sms` não implementa circuit breaker, apenas retry exponencial + DLQ são suficientes para o escopo do desafio. Em produção, um circuit breaker por canal evitaria que 90% de falha no SMS consuma recursos dos outros canais.

3. **Outros Providers:** Poderíamos também implementar a mesma integração, mas usando outro provedor. garantinhdo que a gente tentasse 1 provedor, caso erro, usar o outro provedor, fallback erro deadletter queue para reprocessamento, seria apenas uma maneira de mitigar erros ocasionados por provedores.

4. **Sem tracing distribuído (OpenTelemetry):** O `correlation_id` atravessa toda a pipeline (HTTP → RabbitMQ → worker → DB), mas não há exportação OTLP.

5. **Apenas distribuidor SMS implementado:** `dist.email`, `dist.callcenter`, e `dist.whatsapp` são placeholders, o consumer de `lead.received` publica nas 4 filas, mas só `dist.sms` tem um consumidor real.

---

## Justificativa dos Índices

Os índices abaixo foram analisados com `EXPLAIN FORMAT=JSON` contra MySQL 8.4 com 200 registros reais (125 leads, 125 orders, 125 lead_events, 500 distribution_status, 200 raw_payloads). O output completo está em `docs/_explain/explain_output.txt`.

### Chaves Únicas (Dedup e Idempotência)

**`uk_processed_events_natural(transaction_id, event)`**  
- A base da idempotência do receiver. O INSERT ON DUPLICATE KEY depende deste índice para detectar duplicatas. Access type `const` significa lookup direto, a combinação exata resolve em uma única leitura de página. Sem ele, cada webhook exigiria um `SELECT` antes do `INSERT` (race condition) ou uma tabela de bloqueio.

**`uk_leads_email(email)`** 
- Dedup de customer. O worker normaliza o email (trim + lowercase) antes de inserir. Sem este índice, um cliente comprando por dois gateways teria dois registros em `leads`.

**`uk_orders_gateway_txn(gateway, transaction_id)`**
- Garante uma ordem por (gateway, pedido). Sem ele, retry do webhook criaria ordens duplicadas.

**`uk_lead_events_order_event(order_id, event)`**
- Segunda camada de idempotência no worker. Protege contra entrega duplicada de mensagens RabbitMQ.

**`uk_dist_order_channel(order_id, channel)`**
- Exatamente uma linha de status por (order, canal). Sem ele, retry no distribuidor criaria múltiplas linhas `pending`.

### Índices de Auditoria e Consulta

**`idx_dist_pending_since(status, created_at)`**
- Audit query #2: "leads pending > 5 minutos". O access type `range` com `status='pending'` (equality) + `created_at < NOW() - 5min` (range) escaneia apenas a porção do índice com status pendente. Sem este índice, uma full table scan de distribution_status.

**`idx_lead_events_event_time(event, gateway_timestamp)`**
- Audit query #1: lag por gateway nas últimas 24h. A ordem (event equality, gateway_timestamp range) permite seek no índice para `event='order.approved'` e range scan temporal exato. O optimizador escolheu este índice em vez do `idx_lead_events_event` (que só tem a coluna event) comprovando que o range scan é mais eficiente.

**`idx_raw_gateway_time(gateway, received_at)`**
- O optimizador ignorou este índice com 200 linhas porque estimou que 111 das 200 linhas correspondem a `gateway='grummer'`, a varredura de tabela é mais barata que lookup do índice + busca de linhas. **Em produção com milhões de registros**, este índice será essencial: a seletividade de `gateway` é ~55%, e uma full scan em 1M+ linhas custa ordens de grandeza mais que um range scan via índice.

**`idx_raw_status(processing_status)`**
- GROUP BY processing_status sem filesort (usando o índice como covering index). O access type `range` com `IN('accepted','duplicate')` escaneia apenas a porção relevante do índice.

**`idx_dlq_origin_time(origin, created_at)`**
- DLQ count por origem nas últimas 24h. Em produção com DLQ acumulada, a combinação origin equality + created_at range torna-se um range scan eficiente.

**`idx_dist_channel_status_time(channel, status, created_at)`**
- Per-channel status breakdown. Índice covering (USING INDEX) toda a consulta resolve sem tocar na tabela.

### Índices de Correlação (Tracing)

`idx_processed_events_correlation(correlation_id)`, `idx_raw_correlation(correlation_id)`, `idx_orders_correlation(correlation_id)`, `idx_lead_events_correlation(correlation_id)`, `idx_dlq_correlation(correlation_id)`. Permitem rastrear um correlation_id ponta a ponta (HTTP → raw_payloads → processed_events → orders → lead_events → DLQ) sem table scans.

---