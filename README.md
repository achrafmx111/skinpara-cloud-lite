# SkinPara Cloud Lite

Minimal isolated package for the proven sandbox flow:

`Kapso -> POST /api/kapso/webhook -> Control Center -> Redis -> AI Bridge -> Supabase history -> Redis -> Control Center -> Kapso`

Only the signed Kapso inbound POST path is routed by the gateway. Every other path returns 404, except `/healthz`.

## Included runtime services

- `gateway`: restricted Caddy ingress.
- `skinpara-control-center`: Kapso HMAC, allowlist, dedupe, direct queue publisher and sandbox outbound adapter.
- `skinpara-ai-bridge`: queue, RAG integration and Professional Advisor.
- `skinpara-catalog-service`: read-only Supabase catalog API.
- `redis`: isolated queue, locks and dedupe state.

## External services

- Kapso sandbox API and webhook registration.
- OpenRouter free-model API.
- Supabase catalog/customer-memory project.
- Shopify is optional and disabled in this package. Order mode is fixed to `test`.

## Safety defaults

- `SKINPARA_ORDER_MODE=test`
- `KAPSO_SANDBOX_OUTBOUND_ENABLED=false`
- WhatsApp production/live=false
- Campaign live=false
- Delivery live=false
- Shopify integration=false
- Existing Chatwoot code path remains preserved but is not used by `kapso_direct`.

## Durable direct-channel state

Review and apply `supabase/001_kapso_direct_messages.sql` before deployment. It is intentionally not auto-applied. Redis carries runtime jobs only; Supabase stores ordered conversation history with non-PII HMAC keys. Medical-risk rows are marked `handoff_required=true`; this package does not invent a replacement human-handoff system.

## Blitz preparation

1. Configure the variables listed in `.env.example` using Blitz secret/environment storage. Do not upload an `.env` file.
2. Deploy the Compose package with 2 GB RAM or more.
3. Route the Blitz public hostname to gateway port `8080`.
4. Configure only the Kapso sandbox webhook after deployment approval: `https://HOST/api/kapso/webhook`.
5. Keep sandbox outbound false until a separately approved acceptance window.

## Expected memory

Observed local steady-state application usage was approximately 150 MB total before gateway. Compose limits reserve at most 1,024 MB across all five services, leaving substantial headroom under a 2 GB limit.
