import crypto from "node:crypto";

const STATUS_RANK = { queued: 0, submitted: 1, sent: 2, delivered: 3, read: 4, failed: 5 };
const iso = value => new Date(value || Date.now()).toISOString();
const first = rows => Array.isArray(rows) ? rows[0] || null : null;

export function normalizeMoroccanPhone(input) {
  let digits = String(input || "").replace(/\D/g, "");
  if (digits.startsWith("00212")) digits = digits.slice(2);
  if (/^0[67]\d{8}$/.test(digits)) digits = `212${digits.slice(1)}`;
  if (/^[67]\d{8}$/.test(digits)) digits = `212${digits}`;
  if (!/^212[67]\d{8}$/.test(digits)) throw new Error("invalid_moroccan_phone");
  return `+${digits}`;
}

export function sessionWindowDecision(lastInboundAt, at = Date.now()) {
  if (!lastInboundAt) return { inside: false, message_type: "template", reason: "no_inbound_session" };
  const ageMs = new Date(at).getTime() - new Date(lastInboundAt).getTime();
  const inside = ageMs >= 0 && ageMs <= 24 * 60 * 60 * 1000;
  return { inside, message_type: inside ? "session" : "template", reason: inside ? "inside_24h_window" : "outside_24h_window" };
}

export function validateTemplateVariables(required = [], variables = {}) {
  for (const name of required) {
    if (variables[name] === null || variables[name] === undefined || variables[name] === "") {
      return { ok: false, reason: `missing_variable:${name}` };
    }
  }
  return { ok: true };
}

export function marketingPermission(preference) {
  const allowed = preference?.marketing_opt_out !== true && preference?.whatsapp_marketing_allowed === true;
  return { ok: allowed, reason: allowed ? "marketing_allowed" : "marketing_not_allowed" };
}

export function createWhatsAppProvider({ supabaseFetch, log, mode = "disabled", liveEnabled = false, verifyToken = "" }) {
  const provider = mode === "cloud" ? "meta_cloud" : mode;

  function health() {
    return {
      ok: true,
      provider,
      mode,
      live_enabled: liveEnabled,
      outbound_enabled: mode === "cloud" && liveEnabled,
      webhook_owner: "chatwoot",
      skinpara_webhook_role: "readiness_audit_only"
    };
  }

  function validateConfig() {
    const validMode = ["disabled", "dry-run", "test", "cloud"].includes(mode);
    return { ok: validMode, provider, live_enabled: liveEnabled, fail_closed: !liveEnabled };
  }

  function assertNoLive(requestedMode) {
    if (requestedMode === "live" || mode === "cloud" || liveEnabled) throw new Error("blocked_live_mode");
  }

  async function auditDecision(input, status, reason) {
    const key = String(input.dedupe_key || `outbound:${input.contact_id || 0}:${input.campaign_id || input.automation_run_id || Date.now()}:${input.template_id || input.message_type || "message"}`);
    const event = {
      provider,
      provider_event_id: key,
      phone_number_id: null,
      wa_id: null,
      contact_id: input.contact_id ? Number(input.contact_id) : null,
      conversation_id: input.conversation_id ? Number(input.conversation_id) : null,
      message_id: null,
      direction: "outbound",
      event_type: "outbound_decision",
      status,
      event_at: iso(),
      payload_hash: crypto.createHash("sha256").update(JSON.stringify({ campaign_id: input.campaign_id || null, automation_run_id: input.automation_run_id || null, template_id: input.template_id || null, mode })).digest("hex"),
      dedupe_key: key,
      metadata: { campaign_id: input.campaign_id || null, automation_run_id: input.automation_run_id || null, template_id: input.template_id || null, mode, reason }
    };
    const rows = await supabaseFetch("/skinpara_whatsapp_events?on_conflict=dedupe_key", { method: "POST", headers: { Prefer: "resolution=ignore-duplicates,return=representation" }, body: JSON.stringify(event) });
    return { duplicate: !rows?.length };
  }

  async function sendSessionMessage(input) {
    assertNoLive(input.mode);
    const decision = sessionWindowDecision(input.last_inbound_at);
    if (!decision.inside) {
      await auditDecision(input, "blocked", decision.reason);
      throw new Error("template_required_outside_24h");
    }
    const audit = await auditDecision(input, mode === "test" ? "test" : "blocked", mode === "test" ? "test_simulation" : "provider_disabled");
    return { ok: true, mode, simulated: mode === "test", outbound: false, ...audit };
  }

  async function sendTemplateMessage(input) {
    assertNoLive(input.mode);
    const template = first(await supabaseFetch(`/skinpara_message_templates?id=eq.${encodeURIComponent(input.template_id)}&limit=1`));
    if (!template) throw new Error("template_not_found");
    if (template.provider_status !== "approved" || template.status !== "approved") {
      await auditDecision(input, "blocked", "template_not_approved");
      throw new Error("template_not_approved");
    }
    const variables = input.variables || {};
    const variableCheck = validateTemplateVariables(template.variables || [], variables);
    if (!variableCheck.ok) {
      await auditDecision(input, "blocked", variableCheck.reason);
      throw new Error(variableCheck.reason);
    }
    if (template.marketing) {
      const pref = first(await supabaseFetch(`/skinpara_communication_preferences?contact_id=eq.${Number(input.contact_id)}&limit=1`));
      if (!marketingPermission(pref).ok) {
        await auditDecision(input, "blocked", "marketing_not_allowed");
        throw new Error("marketing_not_allowed");
      }
    }
    const audit = await auditDecision(input, mode === "test" ? "test" : "blocked", mode === "test" ? "test_simulation" : "provider_disabled");
    return { ok: true, mode, simulated: mode === "test", outbound: false, ...audit };
  }

  function verifyWebhook(query) {
    const modeParam = query.get("hub.mode");
    const token = query.get("hub.verify_token") || "";
    const challenge = query.get("hub.challenge") || "";
    if (!verifyToken || modeParam !== "subscribe") throw new Error("webhook_verification_failed");
    const a = Buffer.from(token), b = Buffer.from(verifyToken);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) throw new Error("webhook_verification_failed");
    return challenge;
  }

  function normalizeWebhook(payload) {
    const events = [];
    for (const entry of payload?.entry || []) {
      for (const change of entry?.changes || []) {
        const value = change?.value || {};
        const phoneNumberId = value?.metadata?.phone_number_id || null;
        for (const message of value.messages || []) {
          const at = iso(Number(message.timestamp || 0) * 1000 || Date.now());
          events.push({ provider_event_id: `message:${message.id}`, phone_number_id: phoneNumberId, wa_id: message.from || null, message_id: message.id, direction: "inbound", event_type: "incoming_message", status: "received", event_at: at, message_type: message.type || "unknown", error_code: null, error_message: null });
        }
        for (const status of value.statuses || []) {
          const at = iso(Number(status.timestamp || 0) * 1000 || Date.now());
          const error = status.errors?.[0] || null;
          events.push({ provider_event_id: `status:${status.id}:${status.status}:${status.timestamp || "0"}`, phone_number_id: phoneNumberId, wa_id: status.recipient_id || null, message_id: status.id, direction: "outbound", event_type: "message_status", status: status.status, event_at: at, message_type: "unknown", error_code: error?.code ? String(error.code) : null, error_message: error?.title || error?.message || null });
        }
      }
    }
    return events;
  }

  async function handleWebhook(payload) {
    const normalized = normalizeWebhook(payload);
    let inserted = 0, duplicates = 0;
    for (const event of normalized) {
      let phone = null;
      try { if (event.wa_id) phone = normalizeMoroccanPhone(event.wa_id); } catch {}
      const hash = crypto.createHash("sha256").update(JSON.stringify({ id: event.provider_event_id, status: event.status, at: event.event_at })).digest("hex");
      const rows = await supabaseFetch("/skinpara_whatsapp_events?on_conflict=dedupe_key", { method: "POST", headers: { Prefer: "resolution=ignore-duplicates,return=representation" }, body: JSON.stringify({ provider: "meta_cloud", provider_event_id: event.provider_event_id, phone_number_id: event.phone_number_id, wa_id: event.wa_id, contact_id: null, conversation_id: null, message_id: event.message_id, direction: event.direction, event_type: event.event_type, status: event.status, event_at: event.event_at, payload_hash: hash, dedupe_key: event.provider_event_id, metadata: { message_type: event.message_type, error_code: event.error_code, error_message: event.error_message } }) });
      if (!rows?.length) { duplicates++; continue; }
      inserted++;
      const existing = first(await supabaseFetch(`/skinpara_whatsapp_messages?provider_message_id=eq.${encodeURIComponent(event.message_id)}&limit=1`));
      const shouldUpdate = !existing || event.direction === "inbound" || event.status === "failed" || (STATUS_RANK[event.status] ?? -1) >= (STATUS_RANK[existing.status] ?? -1);
      if (shouldUpdate) {
        await supabaseFetch("/skinpara_whatsapp_messages?on_conflict=provider_message_id", { method: "POST", headers: { Prefer: "resolution=merge-duplicates,return=minimal" }, body: JSON.stringify({ provider_message_id: event.message_id, provider: "meta_cloud", phone_number_id: event.phone_number_id, wa_id: event.wa_id, normalized_phone: phone, direction: event.direction, message_type: event.message_type, status: event.direction === "inbound" ? "received" : event.status, last_status_at: event.event_at, error_code: event.error_code, error_message: event.error_message, updated_at: nowIsoSafe() }) });
      }
      if (event.direction === "inbound" && event.wa_id && phone) {
        await supabaseFetch("/skinpara_whatsapp_contacts?on_conflict=wa_id", { method: "POST", headers: { Prefer: "resolution=merge-duplicates,return=minimal" }, body: JSON.stringify({ provider: "meta_cloud", wa_id: event.wa_id, normalized_phone: phone, last_inbound_at: event.event_at, updated_at: nowIsoSafe() }) });
      }
    }
    log("whatsapp_webhook_normalized", { events: normalized.length, inserted, duplicates });
    return { ok: true, events: normalized.length, inserted, duplicates };
  }

  async function readiness() {
    const [events, messages, templates] = await Promise.all([
      supabaseFetch("/skinpara_whatsapp_events?order=created_at.desc&limit=20"),
      supabaseFetch("/skinpara_whatsapp_messages?order=updated_at.desc&limit=20"),
      supabaseFetch("/skinpara_message_templates")
    ]);
    const counts = {};
    for (const template of templates || []) counts[template.status] = (counts[template.status] || 0) + 1;
    return { ...health(), config: validateConfig(), template_counts: counts, recent_events: events || [], recent_messages: messages || [] };
  }

  return { health, validateConfig, sendSessionMessage, sendTemplateMessage, verifyWebhook, normalizeWebhook, handleWebhook, readiness, sessionWindowDecision };
}

function nowIsoSafe() { return new Date().toISOString(); }


