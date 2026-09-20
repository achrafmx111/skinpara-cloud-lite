import crypto from "node:crypto";

const VALID = new Set(["draft", "ready_to_ship", "submitted", "picked_up", "in_transit", "out_for_delivery", "delivered", "delivery_failed", "returned", "cancelled"]);
const STATUS_MAP = {
  draft: "draft", ready: "ready_to_ship", ready_to_ship: "ready_to_ship",
  submitted: "submitted", created: "submitted", picked_up: "picked_up", pickup: "picked_up",
  in_transit: "in_transit", transit: "in_transit", out_for_delivery: "out_for_delivery",
  delivered: "delivered", failed: "delivery_failed", delivery_failed: "delivery_failed",
  returned: "returned", return: "returned", cancelled: "cancelled", canceled: "cancelled"
};
const first = rows => Array.isArray(rows) ? rows[0] || null : rows || null;
const enc = value => encodeURIComponent(String(value));
const nowIso = () => new Date().toISOString();
const hash = value => crypto.createHash("sha256").update(String(value)).digest("hex");

export function normalizeDeliveryStatus(value) {
  const raw = String(value || "").trim().toLowerCase().replace(/[\s-]+/g, "_");
  return { raw, normalized: STATUS_MAP[raw] || "unknown", known: Boolean(STATUS_MAP[raw]) };
}

export class ManualDeliveryProvider {
  constructor() { this.id = "manual"; }
  health() { return { ok: true, provider: this.id, external_calls: false }; }
  validateConfig() { return { ok: true, mode: "manual", credentials_required: false }; }
  async createShipment(input) { return { provider_shipment_id: `MANUAL-${hash(input.request_key).slice(0, 12).toUpperCase()}`, tracking_number: input.tracking_number || null, status: "ready_to_ship" }; }
  async getShipment(shipment) { return shipment; }
  async cancelShipment() { return { status: "cancelled" }; }
  normalizeWebhook(payload) { return payload; }
}

export class MockDeliveryProvider extends ManualDeliveryProvider {
  constructor() { super(); this.id = "mock"; }
  validateConfig() { return { ok: true, mode: "test", credentials_required: false }; }
  async createShipment(input) {
    const token = hash(input.request_key).slice(0, 12).toUpperCase();
    return { provider_shipment_id: `MOCK-SHP-${token}`, tracking_number: `MOCK-TRK-${token}`, status: "submitted" };
  }
}

export function createDeliveryService({ supabaseFetch, log, providerName = "manual", liveEnabled = false }) {
  const providers = { manual: new ManualDeliveryProvider(), mock: new MockDeliveryProvider(), test: new MockDeliveryProvider() };
  const configuredProvider = providers[providerName] || null;

  function assertSafe(requestedMode, requestedProvider) {
    const isReal = !["manual", "mock", "test"].includes(String(requestedProvider || providerName));
    if (requestedMode === "live" || liveEnabled || isReal) throw new Error("blocked_live_mode");
    if (!configuredProvider) throw new Error("delivery_provider_not_supported");
  }

  function health() {
    return { ok: true, provider: providerName, mode: providerName === "manual" ? "manual" : "test", live_enabled: liveEnabled, outbound_enabled: false, external_calls: false, fail_closed: true };
  }

  async function createShipment(input) {
    assertSafe(input.mode, input.provider);
    const orderKey = String(input.order_id || "").trim();
    const requestKey = String(input.request_key || "").trim();
    if (!orderKey || !requestKey) throw new Error("order_id_and_request_key_required");
    const existing = first(await supabaseFetch(`/skinpara_shipments?client_request_key=eq.${enc(requestKey)}&limit=1`));
    if (existing) return { ok: true, duplicate: true, shipment: existing };
    const order = first(await supabaseFetch(`/skinpara_orders?order_key=eq.${enc(orderKey)}&limit=1`));
    if (!order) throw new Error("durable_order_not_found");
    if (order.test_mode !== true) throw new Error("test_order_required");
    const result = await configuredProvider.createShipment({ ...input, request_key: requestKey });
    const row = {
      order_id: order.order_key, contact_id: order.contact_id, provider: configuredProvider.id,
      provider_shipment_id: result.provider_shipment_id, tracking_number: result.tracking_number,
      status: VALID.has(result.status) ? result.status : "draft", mode: "test",
      recipient_name: input.recipient_name || null, normalized_phone: input.normalized_phone || null,
      city: input.city || null, address_summary: input.address_summary || null,
      cod_amount: Number(order.order_value || 0), currency: "MAD", client_request_key: requestKey,
      submitted_at: result.status === "submitted" ? nowIso() : null, updated_at: nowIso()
    };
    const shipment = first(await supabaseFetch("/skinpara_shipments", { method: "POST", headers: { Prefer: "return=representation" }, body: JSON.stringify(row) }));
    log("delivery_shipment_created", { shipment_id: shipment?.id, order_id: order.order_key, provider: configuredProvider.id, mode: "test" });
    return { ok: true, duplicate: false, shipment };
  }

  async function getShipment(id) {
    const shipment = first(await supabaseFetch(`/skinpara_shipments?id=eq.${Number(id)}&limit=1`));
    if (!shipment) throw new Error("shipment_not_found");
    return { ok: true, shipment };
  }

  async function listShipments() {
    return { ok: true, ...health(), shipments: await supabaseFetch("/skinpara_shipments?order=updated_at.desc&limit=100") };
  }

  async function handleStatusEvent(input) {
    assertSafe(input.mode, input.provider);
    const shipmentId = Number(input.shipment_id || 0);
    if (!shipmentId) throw new Error("shipment_id_required");
    const mapped = normalizeDeliveryStatus(input.provider_status);
    const providerEventId = String(input.provider_event_id || "").trim();
    const fallback = hash(JSON.stringify({ shipment_id: shipmentId, status: mapped.raw, event_at: input.event_at || null }));
    const dedupeKey = `${providerName}:${providerEventId || fallback}`;
    const result = await supabaseFetch("/rpc/skinpara_process_delivery_event", {
      method: "POST", headers: { Prefer: "return=representation" }, body: JSON.stringify({
        p_shipment_id: shipmentId, p_provider_event_id: providerEventId || null,
        p_event_type: String(input.event_type || "status"), p_provider_status: mapped.raw || "unknown",
        p_normalized_status: mapped.normalized, p_event_at: input.event_at || nowIso(),
        p_dedupe_key: dedupeKey, p_payload_hash: hash(JSON.stringify({ status: mapped.raw, reason: input.reason || null })),
        p_error_class: input.error_class || null, p_error_message: input.reason || null
      })
    });
    log("delivery_status_event", { shipment_id: shipmentId, normalized_status: mapped.normalized, known: mapped.known, duplicate: Boolean(result?.duplicate) });
    return { ok: true, known_status: mapped.known, ...result };
  }

  async function cancelShipment(input) {
    assertSafe(input.mode, input.provider);
    return handleStatusEvent({ ...input, provider_status: "cancelled", event_type: "cancelled" });
  }

  async function readiness() {
    const [shipments, events] = await Promise.all([
      supabaseFetch("/skinpara_shipments?order=updated_at.desc&limit=20"),
      supabaseFetch("/skinpara_delivery_events?order=created_at.desc&limit=20")
    ]);
    return { ...health(), config: configuredProvider?.validateConfig() || { ok: false }, recent_shipments: shipments || [], recent_events: events || [] };
  }

  return { health, createShipment, getShipment, listShipments, handleStatusEvent, cancelShipment, readiness };
}


