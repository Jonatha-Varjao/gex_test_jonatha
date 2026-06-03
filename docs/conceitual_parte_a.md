# GEX Webhook Pipeline - Parte A: Resolução de Incidente

## Cenário

Sexta 14h: gateway reporta 1.587 transações aprovadas ($1.3M). No
`lead_events` da GEX, apenas 421 com `event = 'order.approved'`.
Call center sem leads há 4h. Gap: **1.166 pedidos perdidos**.

---

## 1. Primeira ação (antes de tocar em prod)

1. **Acuse o WhatsApp por escrito:** "Recebido. Abrindo SEV-1, time
   de plantão notificado, ETA 30min para diagnóstico inicial. Não
   mexo em prod até identificar a causa raiz."
2. Abrir ticket de incidente com timestamp e snapshot dos dashboards
   (leads/hora, profundidade fila, error rate, replicação MySQL).
3. Pagar on-call backend + DBA; abrir war room no Slack.
4. Coletar logs e rows de `processed_events` e `raw_payloads` por
   hora, profundidade das filas RMQ, logs do último ack do consumer.

---

## 2. Cinco hipóteses ranqueadas

| # | Hipótese | Prob. | Sinal característico |
|---|----------|-------|----------------------|
| 1 | Consumer/worker travou (pool MySQL exaurido, deadlock, OOM) | **Alta** | `lead.received` com `messages_ready > 0` e `consumers = 0` ou stuck `messages_unacknowledged` crescendo |
| 2 | Gateway reteve/falhou envio (5xx, circuit breaker deles) | Alta | `raw_payloads.received_at` do gateway grummer cessa antes da janela do gap |
| 3 | Pico decrypt_failed (chave rotacionada, IV malformado, header `X-GR-Encrypted` ausente) | Média | `raw_payloads.processing_status = 'decrypt_failed'` > 5%; `lead_dead_letter` cheio em `lead.dead.decrypt_failed` |
| 4 | Deadlock/rollback silencioso no `sp_insert_lead` (exceção não propagada) | Média | `orders` populado mas `lead_events` vazio; `Innodb_row_lock_waits` alto |
| 5 | Validation rate de schema explodiu (novo campo, país não normalizado) | Baixa | `lead.dead.schema_failed` crescendo, `processing_status = 'schema_failed'` > 1% |

H1 e H2 cobrem ~80% do cenário. H4 é a mais perigosa por ser
silenciosa por não crescer a fila `lead_dead_letter`.

---

## 3. Dados, logs e queries

```sql
-- (a) Chegada por gateway e hora
SELECT gateway, DATE_FORMAT(received_at,'%Y-%m-%d %H:00') hr,
       COUNT(*) raw_count,
       SUM(processing_status = 'accepted')       accepted,
       SUM(processing_status = 'duplicate')      duplicate,
       SUM(processing_status = 'decrypt_failed') decrypt_failed,
       SUM(processing_status = 'schema_failed')  schema_failed
FROM raw_payloads
WHERE received_at BETWEEN '2026-05-29 14:00' AND '2026-06-01 14:00'
GROUP BY gateway, hr
ORDER BY hr;

-- (b) DLQ por origem e hora
SELECT origin, DATE_FORMAT(created_at,'%Y-%m-%d %H:00') hr, COUNT(*)
FROM lead_dead_letter
WHERE created_at >= NOW() - INTERVAL 72 HOUR
GROUP BY origin, hr;

-- (c) Orders sem lead_events (gap candidates)
SELECT o.gateway, COUNT(*) orders_without_event
FROM orders o
LEFT JOIN lead_events le ON le.order_id = o.id
WHERE o.event = 'order.approved'
  AND o.transaction_time BETWEEN '2026-05-29 14:00' AND '2026-06-01 14:00'
  AND le.id IS NULL;
```

```bash
# RabbitMQ
rabbitmqctl list_queues name messages messages_ready messages_unacknowledged consumers
rabbitmqctl list_queues | grep -E 'dead|received|sms'
rabbitmqadmin get queue=lead.received count=5 ackmode=ack_requeue_true
rabbitmqadmin get queue=lead.dead.consumer_failed count=5

# Logs por correlation_id de amostra
grep '<corr_id_amostra>' /var/log/gex/{receiver,worker}.log
```

---

## 4. Diferenciação entre cenários (a/b/c/d)

| Cenário | Sinal primário | Onde olhar |
|---------|---------------|------------|
| (a) Gateway nunca enviou | `raw_payloads` vazio para `transaction_id` do gap; `orders` vazio | `raw_payloads` por `transaction_id`; log do gateway lado a lado |
| (b) Decrypt falhou | `raw_payloads.processing_status = 'decrypt_failed'`; `lead_dead_letter` com payload original | `lead_dead_letter` + contagem `decrypt_failed` |
| (c) Publicado mas consumer travou | `processing_status = 'accepted'`; `lead.received` depth > 0; `consumers = 0` | `rabbitmqctl list_queues` + healthcheck do worker |
| (d) Consumer publicou mas distribuidor parou | `lead_events` existe; `distribution_status.status = 'pending'` > 5 min; `dist.sms` depth > 0 | `audit_queries.sql` Q2 + fila `dist.sms` |

---

## 5. Reprocessamento dos 1.166 sem duplicar os 421

**Pré-condição:** causa raiz corrigida e workers parados.

```sql
-- Identificar o gap (~1.166 linhas)
SELECT rp.id, rp.gateway,
       rp.headers->>'$.x-correlation-id' AS original_corr,
       rp.received_at,
       rp.body_decrypted
FROM raw_payloads rp
WHERE rp.processing_status IN ('accepted', 'duplicate')
  AND rp.received_at BETWEEN '2026-05-29 14:00' AND '2026-06-01 14:00'
  AND NOT EXISTS (
      SELECT 1 FROM lead_events le
      JOIN orders o ON o.id = le.order_id
      WHERE o.transaction_id = rp.transaction_id
        AND le.event = 'order.approved'
  );
```

**Estratégia:** script externo `scripts/replay_missing.py` lê o SELECT
acima e re-publica cada registro em `lead.received` com o mesmo
`x-correlation-id`.

**Por que não duplica:** ao reentrar no receiver, o
`processed_events.UNIQUE(transaction_id, event)` filtra os 421 que
já viraram `lead_events`. Os 1.166 passam como `accepted`. O
`sp_insert_lead` é protegido por `lead_events.UNIQUE(order_id, event)` como segunda camada, qualquer race residual vira no-op silencioso.

---

## 6. Medida preventiva

1.  **Alerta de DLQ acumulando:** `lead_dead_letter_count{origin =
   "decrypt_failed"} > 50` em 15 min..

2. **Implementar Observabiliade:** Unificar logs vai facilitar a DX para captura do bug e tracing distribuído para gente saber o fluxo de cada correlation-id e também serve para identificar gargalos dos processos.