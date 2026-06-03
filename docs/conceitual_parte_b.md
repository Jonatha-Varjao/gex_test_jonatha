# GEX Webhook Pipeline - Parte B: Decisões de Arquitetura

## 1. Idempotência: `transaction_id + event` vs `transaction_id` only

Um pedido (`transaction_id`) tem vida longa: pode gerar
`order.approved` hoje, `order.refunded` em 30 dias e
`order.charged_back` em 60. Cada evento dispara uma campanha
diferente (recuperação, retenção, jurídico).

Chave só por `transaction_id` colapsaria todos no primeiro INSERT:
o refund chegaria como "duplicate" e nunca alcançaria o call center.

`(transaction_id, event)` permite:
- **Múltiplos eventos por pedido** processados normalmente
- **Replay do mesmo evento** é idempotente 

**Trade-off aceito:** se o gateway reemitir o mesmo evento com
dados atualizados (ex.: `payment_status` muda de `pending` para
`approved` na mesma linha), nossa chave enxerga como duplicado.
Aceitável porque o spec modela isso como **novo evento**
(`order.payment_updated`), não como update in-place.

---

## 2. Cripto: AES-256-CBC vs AES-256-GCM

**CBC** (usado no grummer): padding PKCS7, **sem autenticação**.
Vulnerabilidades:
- **Padding oracle:** atacante observa respostas de erro →
  decifra qualquer ciphertext sem a chave
- **Determinístico:** mesmo IV + key + plaintext = mesmo ciphertext
  (vaza padrões)
- **Replay:** ciphertext arbitrário aceito sem validação de freshness

**GCM** (AEAD): cifragem + autenticação (GMAC) num passo só.
Paralelizável via AES-NI, aumentado o throughput.
Único requisito: **nunca reutilizar `(key, nonce)`**.

**Decisão:** usar **GCM para qualquer novo webhook**. Manter CBC
para grummer por interoperabilidade com o gateway legado.

---

## 3. Backpressure quando SMS falha 90%

**Por que RabbitMQ + retry exponencial sozinho não basta:**

Cada mensagem condenada consome 21 s de retry (1 + 4 + 16) + 3
slots no broker. Com 90% de falha, o throughput efetivo cai ~10×,
a latência explode, e a DLQ enche. DLQ cheia = publisher
backpressure no exchange `lead` -> **toda a esteira trava**,
incluindo `email`/`callcenter`/`whatsapp` que estão saudáveis.

**Solução em 3 camadas:**

1. **Circuit breaker por canal (pybreaker + Redis):** após 5 falhas
   consecutivas em `dist.sms`, ABRIR. Mensagens ACKadas e movidas
   para `dist.sms.circuit_open` (fila dedicada, não a DLQ final).
   HALF_OPEN a cada 30 s: tenta 1, sucesso = CLOSE, falha = OPEN.
2. **Isolamento por fila independente** já temos 4 filas `dist.*`.
3. **Rate limit + dead-letter metering:** máximo 100 SMS/s no
   consumer.

---

## 4. Sinais para migrar receiver + decrypt de Python para Go

### Vale a pena migrar se:
1. **CPU-bound comprovado no hot path.** Profile mostra
   Python (Pydantic marshal, GC, asyncio overhead) > 30% do p95 do
   request. Go provavelmente cortaria bastante esse overhead.
2. **Latência p95 viola SLA.** SLA < 100 ms e o GIL + GC pauses
   do Python são a causa raiz. Go elimina ambos por construção.

### NÃO vale a pena migrar se:
1. **Time sem fluência em Go.** Seis meses até paridade de
   cobertura de testes e observabilidade. Custo de manutenção
   domina o savings de CPU.
2. **I/O domina o hot path.** `await session.execute(text(...))`+ `await broker.publish(...)`. Otimizar queries a nível de Banco primeiro.
