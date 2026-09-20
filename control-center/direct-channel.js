import crypto from "node:crypto";

export function stableDirectKeys({ phone, phoneNumberId, keySecret }) {
  if (!phone || !phoneNumberId || !keySecret) throw new Error("direct_key_inputs_missing");
  const normalized = String(phone).replace(/\D/g, "");
  const digest = value => crypto.createHmac("sha256", keySecret).update(value).digest("hex");
  const customerKey = `cust_${digest(`kapso:${normalized}`).slice(0, 40)}`;
  return {
    customerKey,
    conversationKey: `conv_${digest(`kapso:${phoneNumberId}:${customerKey}`).slice(0, 40)}`
  };
}

export function buildDirectInboundJob(inbound, keySecret) {
  const keys = stableDirectKeys({
    phone: inbound.senderPhone,
    phoneNumberId: inbound.phoneNumberId,
    keySecret
  });
  return {
    channel: "kapso_direct",
    messageId: String(inbound.messageId),
    textContent: String(inbound.textContent),
    customerKey: keys.customerKey,
    conversationKey: keys.conversationKey,
    receivedAt: new Date().toISOString()
  };
}

export function directOutboundEligible(event) {
  return Boolean(
    event?.channel === "kapso_direct" &&
    event?.messageType === "outgoing" &&
    event?.private !== true &&
    typeof event?.content === "string" &&
    event.content.trim()
  );
}

export function directOutboundGate(event, env = process.env) {
  if (!directOutboundEligible(event)) return { ok: false, reason: "ineligible_direct_outbound" };
  if (env.SKINPARA_CHANNEL_MODE !== "kapso_direct") return { ok: false, reason: "direct_mode_disabled" };
  if (env.KAPSO_SANDBOX_ENABLED !== "true" || env.SKINPARA_ORDER_MODE !== "test") return { ok: false, reason: "sandbox_disabled" };
  if (env.KAPSO_SANDBOX_OUTBOUND_ENABLED !== "true") return { ok: false, reason: "outbound_disabled" };
  if (!env.KAPSO_SANDBOX_PHONE_NUMBER_ID || !env.KAPSO_SANDBOX_ALLOWED_TO) return { ok: false, reason: "sandbox_destination_missing" };
  return { ok: true };
}
