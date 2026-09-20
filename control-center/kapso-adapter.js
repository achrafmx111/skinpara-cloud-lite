import crypto from "node:crypto";

export function isSandboxGated() {
  const enabled = process.env.KAPSO_SANDBOX_ENABLED === "true";
  const testMode = process.env.SKINPARA_ORDER_MODE === "test";
  return enabled && testMode;
}

export function normalizePhoneForAllowlist(phone) {
  if (!phone) return "";
  // Strip everything except digits
  let digits = String(phone).replace(/\D/g, "");
  // Standardize Moroccan prefix variations just in case, though the allowlist should be exact
  if (digits.startsWith("00212")) digits = digits.slice(2);
  if (/^0[67]\d{8}$/.test(digits)) digits = `212${digits.slice(1)}`;
  if (/^[67]\d{8}$/.test(digits)) digits = `212${digits}`;
  return digits;
}

export function validateAllowedTestNumber(number) {
  const allowed = process.env.KAPSO_SANDBOX_ALLOWED_TO;
  if (!allowed) return false;
  
  const normInput = normalizePhoneForAllowlist(number);
  const normAllowed = normalizePhoneForAllowlist(allowed);
  
  if (!normInput || !normAllowed) return false;
  return normInput === normAllowed;
}

export function verifyKapsoSignature(rawBodyBuffer, signature, secret) {
  if (!rawBodyBuffer || !signature || !secret) return false;
  if (typeof signature !== "string") return false;
  
  try {
    const expectedHash = crypto
      .createHmac("sha256", secret)
      .update(rawBodyBuffer)
      .digest("hex");
      
    // Must be valid hex strings of correct length to convert to buffers safely for timingSafeEqual
    if (signature.length !== expectedHash.length) return false;
    
    const sigBuffer = Buffer.from(signature, "hex");
    const expBuffer = Buffer.from(expectedHash, "hex");
    
    // Fallback length check just in case Buffer.from shortens invalid hex
    if (sigBuffer.length !== expBuffer.length) return false;
    
    return crypto.timingSafeEqual(sigBuffer, expBuffer);
  } catch (err) {
    return false; // Fail closed safely
  }
}

export function parseKapsoInbound(payload) {
  if (!payload) return { ok: false, reason: "unsupported_payload" };
  
  // Defensively handle batch
  if (payload.batch === true) {
    // For Phase 1, we just fail safely or explicitly reject batch for now to prevent loops/duplicates
    return { ok: false, reason: "unsupported_payload", detail: "batch_not_supported_yet" };
  }
  
  const type = payload.type;
  if (type !== "whatsapp.message.received") {
    return { ok: false, reason: "unsupported_payload", detail: "not_message_received" };
  }
  
  const phoneNumberId = payload.phone_number_id;
  const conversation = payload.conversation || {};
  const message = payload.message || {};
  const kapsoMeta = message.kapso || {};
  
  const senderPhone = conversation.phone_number;
  const messageId = kapsoMeta.id || message.id; // defensive
  
  // The exact text leaf is unproven
  const textContent = kapsoMeta.content || kapsoMeta.text || message.text?.body;
  
  if (!phoneNumberId || !senderPhone || !messageId) {
    return { ok: false, reason: "unsupported_payload", detail: "missing_identifiers" };
  }
  
  if (!textContent || typeof textContent !== "string") {
    return { ok: false, reason: "unsupported_payload", detail: "not_text_message" };
  }
  
  return {
    ok: true,
    data: {
      phoneNumberId,
      senderPhone,
      messageId,
      textContent
    }
  };
}

export async function deduplicateKapsoMessage(redisClient, messageId) {
  if (!redisClient || !messageId) return false; // Fail closed if redis missing
  
  try {
    const key = `kapso:sandbox:dedupe:${messageId}`;
    // SET NX (only if not exists), EX (expire in 86400 seconds / 24h)
    const result = await redisClient.set(key, "1", {
      NX: true,
      EX: 86400
    });
    
    // result is 'OK' if set, null if it already existed
    return result === 'OK';
  } catch (err) {
    // If Redis fails, FAIL CLOSED to prevent duplicates
    return false;
  }
}

export function translateKapsoToChatwoot(inboundData) {
  return {
    content: inboundData.textContent,
    message_type: "incoming", // explicitly inbound for Chatwoot
    content_type: "text",
    private: false
  };
}

export function translateChatwootToKapso(chatwootPayload, destinationPhone) {
  // Chatwoot payload from outgoing webhook
  if (chatwootPayload.message_type !== "outgoing" || chatwootPayload.private) {
    return null;
  }
  if (!chatwootPayload.content) {
    return null; // Skip non-text
  }
  
  return {
    messaging_product: "whatsapp",
    to: destinationPhone,
    text: { body: chatwootPayload.content },
    type: "text"
  };
}

export function buildKapsoOutboundRequest(kapsoPayload, phoneNumberId, apiKey) {
  // Makes the transport mockable by just building the request parameters
  return {
    method: "POST",
    url: `https://api.kapso.ai/meta/whatsapp/v24.0/${encodeURIComponent(phoneNumberId)}/messages`,
    headers: {
      "X-API-Key": apiKey,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(kapsoPayload)
  };
}
