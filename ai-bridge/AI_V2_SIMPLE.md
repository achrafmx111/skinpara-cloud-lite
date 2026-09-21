# SkinPara AI V2 — Simple Production Shape

Goal: keep the customer experience simple and keep hard business facts deterministic.

## Runtime flow

WhatsApp/Kapso -> Control Center -> AI Bridge -> conversation history + catalog -> one model -> safety check -> WhatsApp

The model owns natural conversation. Code owns hard boundaries.

## Model responsibilities
- Understand Darija Arabic, Latin Darija, French and English.
- Use recent conversation history naturally.
- Ask the next useful question without repeating answered questions.
- Handle topic changes.
- Explain and recommend only from supplied catalog context.

## Deterministic responsibilities
- Product/catalog lookup.
- Price/variant facts.
- Stock confirmation.
- Order state and ORDER_MODE=test guard.
- Medical-risk escalation.
- Human handoff.
- Reasoning/mojibake/output safety.

## Deliberately removed from the main conversation path
- Deterministic skin questionnaire interception.
- Three free-model routing array.
- Repeated patch-per-utterance behavior.

## Current model
Configured with OPENROUTER_MODEL. Default is nvidia/nemotron-3-super-120b-a12b:free for development only.

Before production, choose a stable model/provider after a small real-conversation comparison. Do not compensate for an unreliable provider by adding business-logic complexity.

## Safety
SKINPARA_ORDER_MODE stays test. No real Shopify order, production campaign, delivery action or live-mode switch is authorized by this change.
