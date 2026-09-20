import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { createRequire } from "node:module";
import {
  buildDirectInboundJob,
  directOutboundGate,
  stableDirectKeys
} from "../direct-channel.js";
import {
  parseKapsoInbound,
  validateAllowedTestNumber,
  verifyKapsoSignature
} from "../kapso-adapter.js";

const require = createRequire(import.meta.url);
const { createDirectProcessor } = require("../../ai-bridge/direct-channel.cjs");

class MemoryStore {
  constructor() { this.rows = []; }
  async insertMessage(row) {
    if (this.rows.some(x => x.event_key === row.event_key || (row.external_message_id && x.external_message_id === row.external_message_id))) return false;
    this.rows.push({ ...row, id: this.rows.length + 1 });
    return true;
  }
  async getHistory(key) { return this.rows.filter(x => x.conversation_key === key).sort((a, b) => a.id - b.id); }
  async hasEvent(key) { return this.rows.some(x => x.event_key === key); }
  async markHandoff(key, reason) {
    const row = this.rows.find(x => x.event_key === key);
    row.handoff_required = true;
    row.handoff_reason = reason;
  }
}

function kapsoPayload({ phone = "212600000000", id = "wamid.test-1", text = "Salam" } = {}) {
  return {
    type: "whatsapp.message.received",
    phone_number_id: "sandbox-phone-id",
    conversation: { phone_number: phone },
    message: { kapso: { id, content: text } }
  };
}

test("signed inbound parses and invalid signature is rejected", () => {
  const secret = "test-only-signing-secret";
  const raw = Buffer.from(JSON.stringify(kapsoPayload()));
  const signature = crypto.createHmac("sha256", secret).update(raw).digest("hex");
  assert.equal(verifyKapsoSignature(raw, signature, secret), true);
  assert.equal(parseKapsoInbound(JSON.parse(raw)).ok, true);
  assert.equal(verifyKapsoSignature(raw, "0".repeat(64), secret), false);
});

test("allowlist drops a different number", () => {
  const old = process.env.KAPSO_SANDBOX_ALLOWED_TO;
  process.env.KAPSO_SANDBOX_ALLOWED_TO = "212600000000";
  assert.equal(validateAllowedTestNumber("212600000001"), false);
  process.env.KAPSO_SANDBOX_ALLOWED_TO = old;
});

test("stable keys contain no phone and are deterministic", () => {
  const a = stableDirectKeys({ phone: "212600000000", phoneNumberId: "sandbox", keySecret: "test-key-secret" });
  const b = stableDirectKeys({ phone: "212600000000", phoneNumberId: "sandbox", keySecret: "test-key-secret" });
  assert.deepEqual(a, b);
  assert.equal(JSON.stringify(a).includes("212600000000"), false);
});

test("direct processor persists ordered history, supplies prior context, dedupes, and queues reply", async () => {
  const store = new MemoryStore();
  const outbound = [];
  const seenHistories = [];
  const processJob = createDirectProcessor({
    store,
    enqueueOutbound: async x => outbound.push(x),
    searchCatalog: async () => ({ context: "PRODUCT 1\nTitle: First\nPRODUCT 2\nTitle: Second" }),
    searchProducts: async () => [],
    callAdvisor: async args => {
      seenHistories.push(args.history.map(x => x.content));
      return args.userMessage.includes("second") ? "The second one is Second." : "1. First\n2. Second";
    }
  });
  const base = buildDirectInboundJob({ senderPhone: "212600000000", phoneNumberId: "sandbox", messageId: "m1", textContent: "Compare two" }, "test-key-secret");
  assert.equal((await processJob(base)).handled, true);
  assert.equal((await processJob(base)).reason, "duplicate_message_id");
  await processJob({ ...base, messageId: "m2", textContent: "Explain the second one" });
  assert.deepEqual(store.rows.map(x => x.role), ["user", "assistant", "user", "assistant"]);
  assert.ok(seenHistories[1].includes("1. First\n2. Second"));
  assert.equal(outbound.length, 2);
  assert.equal(outbound[1].messageType, "outgoing");
});

test("outbound is blocked while sandbox outbound is false", () => {
  const result = directOutboundGate({ channel: "kapso_direct", messageType: "outgoing", private: false, content: "safe" }, {
    SKINPARA_CHANNEL_MODE: "kapso_direct",
    KAPSO_SANDBOX_ENABLED: "true",
    KAPSO_SANDBOX_OUTBOUND_ENABLED: "false",
    SKINPARA_ORDER_MODE: "test",
    KAPSO_SANDBOX_PHONE_NUMBER_ID: "sandbox",
    KAPSO_SANDBOX_ALLOWED_TO: "212600000000"
  });
  assert.deepEqual(result, { ok: false, reason: "outbound_disabled" });
});

test("medical risk is durable handoff_required and queues only the safe reply", async () => {
  const store = new MemoryStore();
  const outbound = [];
  const processJob = createDirectProcessor({
    store,
    enqueueOutbound: async x => outbound.push(x),
    searchCatalog: async () => ({ context: "" }),
    searchProducts: async () => [],
    callAdvisor: async () => { throw new Error("advisor_must_not_run"); }
  });
  const job = buildDirectInboundJob({ senderPhone: "212600000000", phoneNumberId: "sandbox", messageId: "risk-1", textContent: "I have trouble breathing and swelling" }, "test-key-secret");
  const result = await processJob(job);
  assert.equal(result.handoff_required, true);
  assert.equal(store.rows[0].handoff_required, true);
  assert.equal(store.rows[1].handoff_required, true);
  assert.equal(outbound[0].handoffRequired, true);
});
