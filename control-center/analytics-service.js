const DAY = 86400000;
const num = value => Number(value || 0);
const pct = (a, b) => b ? Number((a * 100 / b).toFixed(2)) : 0;
const uniq = values => new Set(values.filter(value => value !== null && value !== undefined));

function moroccoDate(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", { timeZone: "Africa/Casablanca", year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(date);
  return Object.fromEntries(parts.filter(x => x.type !== "literal").map(x => [x.type, Number(x.value)]));
}

function zonedStart(year, month, day) {
  let guess = Date.UTC(year, month - 1, day, 0, 0, 0);
  for (let i = 0; i < 3; i++) {
    const shown = new Intl.DateTimeFormat("en-CA", { timeZone: "Africa/Casablanca", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" }).formatToParts(new Date(guess));
    const p = Object.fromEntries(shown.filter(x => x.type !== "literal").map(x => [x.type, Number(x.value)]));
    const represented = Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second);
    guess += Date.UTC(year, month - 1, day) - represented;
  }
  return new Date(guess);
}

export function analyticsRange(params, now = new Date()) {
  const preset = String(params.get("range") || "last_30_days");
  const d = moroccoDate(now);
  let start, end;
  if (preset === "custom") {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(params.get("from") || "") || !/^\d{4}-\d{2}-\d{2}$/.test(params.get("to") || "")) throw new Error("custom_range_requires_from_and_to");
    const [fy, fm, fd] = params.get("from").split("-").map(Number), [ty, tm, td] = params.get("to").split("-").map(Number);
    start = zonedStart(fy, fm, fd); end = new Date(zonedStart(ty, tm, td).getTime() + DAY);
  } else {
    const today = zonedStart(d.year, d.month, d.day);
    if (preset === "today") { start = today; end = new Date(today.getTime() + DAY); }
    else if (preset === "yesterday") { start = new Date(today.getTime() - DAY); end = today; }
    else if (preset === "last_7_days") { start = new Date(today.getTime() - 6 * DAY); end = new Date(today.getTime() + DAY); }
    else if (preset === "this_month") { start = zonedStart(d.year, d.month, 1); end = zonedStart(d.month === 12 ? d.year + 1 : d.year, d.month === 12 ? 1 : d.month + 1, 1); }
    else if (preset === "previous_month") { const py = d.month === 1 ? d.year - 1 : d.year, pm = d.month === 1 ? 12 : d.month - 1; start = zonedStart(py, pm, 1); end = zonedStart(d.year, d.month, 1); }
    else { start = new Date(today.getTime() - 29 * DAY); end = new Date(today.getTime() + DAY); }
  }
  if (start >= end) throw new Error("invalid_date_range");
  return { preset, timezone: "Africa/Casablanca", from: start.toISOString(), to_exclusive: end.toISOString() };
}

export function createAnalyticsService({ supabaseFetch, getLiveConversations, getOperationalHealth }) {
  const inRange = (row, field, range) => { const t = new Date(row?.[field] || 0).getTime(); return t >= new Date(range.from).getTime() && t < new Date(range.to_exclusive).getTime(); };
  async function rows(table, select = "*") { return await supabaseFetch(`/${table}?select=${encodeURIComponent(select)}&limit=10000`); }
  async function rangedRows(table, field, range) { return await supabaseFetch(`/${table}?${field}=gte.${encodeURIComponent(range.from)}&${field}=lt.${encodeURIComponent(range.to_exclusive)}&limit=10000`); }
  async function load(range) {
    const [ordersCreated, ordersDelivered, customers, stats, shipments, terminalShipments, deliveryEvents, campaigns, recipients, wallets, ledger, referrals, aiEvents] = await Promise.all([
      rangedRows("skinpara_orders", "created_at", range), rangedRows("skinpara_orders", "delivered_at", range),
      rows("skinpara_customers"), rows("skinpara_customer_stats"), rangedRows("skinpara_shipments", "created_at", range),
      supabaseFetch("/skinpara_shipments?status=in.(delivery_failed,returned,cancelled)&select=order_id,status&limit=10000"),
      rangedRows("skinpara_delivery_events", "event_at", range), rangedRows("skinpara_campaigns", "created_at", range),
      rangedRows("skinpara_campaign_recipients", "created_at", range), rows("skinpara_wallet_accounts"),
      rangedRows("skinpara_wallet_ledger", "created_at", range), rangedRows("skinpara_referrals", "created_at", range),
      rangedRows("skinpara_ai_events", "created_at", range)
    ]);
    const orderMap = new Map([...ordersCreated, ...ordersDelivered].map(order => [order.order_key, order]));
    return { range, orders: [...orderMap.values()], customers, stats, shipments, terminalShipments, deliveryEvents, campaigns, recipients, wallets, ledger, referrals, aiEvents };
  }

  function sales(d) {
    const orders = d.orders.filter(x => inRange(x, "created_at", d.range));
    const nonRevenueOrderIds = uniq(d.terminalShipments.map(x => x.order_id));
    const delivered = orders.filter(x => x.status === "delivered" && !nonRevenueOrderIds.has(x.order_key));
    const cancelled = orders.filter(x => ["cancelled", "rejected"].includes(x.status));
    const returnedIds = uniq(d.shipments.filter(x => x.status === "returned" && inRange(x, "returned_at", d.range)).map(x => x.order_id));
    const deliveredCustomers = uniq(delivered.map(x => x.contact_id));
    const repeat = d.stats.filter(x => x.repeat_customer && deliveredCustomers.has(x.contact_id)).length;
    const newCustomers = d.customers.filter(x => inRange(x, "first_seen_at", d.range)).length;
    const revenue = orders.reduce((s, x) => s + num(x.order_value), 0), deliveredRevenue = delivered.reduce((s, x) => s + num(x.order_value), 0);
    return { source: "supabase:skinpara_orders,skinpara_customer_stats", mode: "TEST/LOCAL", total_orders: orders.length, delivered_orders: delivered.length, cancelled_rejected_orders: cancelled.length, returned_orders: returnedIds.size, total_order_value: revenue, delivered_revenue: deliveredRevenue, average_order_value: orders.length ? Number((revenue / orders.length).toFixed(2)) : 0, delivered_aov: delivered.length ? Number((deliveredRevenue / delivered.length).toFixed(2)) : 0, repeat_customer_count: repeat, repeat_customer_rate: pct(repeat, deliveredCustomers.size), new_customer_count: newCustomers, total_customers: d.customers.length, vip_customers: d.stats.filter(x => x.vip).length };
  }

  function funnel(d, live = []) {
    const cohort = d.orders.filter(x => inRange(x, "created_at", d.range));
    const rank = status => ({ created: 1, ready_to_ship: 2, shipped: 3, delivered: 4, rejected: 0, cancelled: 0 }[status] || 0);
    const stages = [
      { id: "order_created", count: cohort.length },
      { id: "ready_to_ship", count: cohort.filter(x => rank(x.status) >= 2).length },
      { id: "shipped", count: cohort.filter(x => rank(x.status) >= 3).length },
      { id: "delivered", count: cohort.filter(x => rank(x.status) >= 4).length }
    ].map(x => ({ ...x, source: "supabase:skinpara_orders_normalized_state" }));
    const leads = uniq(live.map(x => x.id)).size;
    const liveStages = [{ id: "conversation", count: leads, source: "chatwoot_live", reliability: "operational_not_historical" }];
    return { source: "mixed_explicit", limitations: ["interested/pending-stock/stock-confirmed are omitted because durable historical events are not reliable", "Order stages use the normalized durable order state cohort to remain monotonic when old fixtures lack intermediate events"], stages: [...liveStages, ...stages].map((x, i, all) => ({ ...x, from_previous_percent: i ? pct(x.count, all[i - 1].count) : 100, from_top_percent: pct(x.count, all[0].count) })) };
  }

  function delivery(d) {
    const shipments = d.shipments.filter(x => inRange(x, "created_at", d.range));
    const shipmentIds = uniq(shipments.map(x => x.id));
    const experienced = status => uniq(d.deliveryEvents.filter(x => shipmentIds.has(x.shipment_id) && x.normalized_status === status).map(x => x.shipment_id)).size;
    const shipped = shipments.filter(x => x.shipped_at), delivered = shipments.filter(x => x.status === "delivered");
    const avgHours = (items, a, b) => { const values = items.map(x => (new Date(x[b]) - new Date(x[a])) / 3600000).filter(Number.isFinite); return values.length ? Number((values.reduce((s, x) => s + x, 0) / values.length).toFixed(2)) : null; };
    return { source: "supabase:skinpara_shipments,skinpara_delivery_events", data_mode: "TEST/LOCAL/MANUAL PROVIDER", shipments_created: shipments.length, shipped: shipped.length, delivered: delivered.length, delivery_failed: experienced("delivery_failed"), returned: experienced("returned"), cancelled: experienced("cancelled"), delivery_success_rate: pct(delivered.length, shipments.length), failure_rate: pct(experienced("delivery_failed"), shipments.length), return_rate: pct(experienced("returned"), shipments.length), average_ready_to_ship_to_shipped_hours: avgHours(shipped, "created_at", "shipped_at"), average_shipped_to_delivered_hours: avgHours(delivered.filter(x => x.shipped_at), "shipped_at", "delivered_at") };
  }

  function campaigns(d) {
    const campaigns = d.campaigns.filter(x => inRange(x, "created_at", d.range)), ids = uniq(campaigns.map(x => x.id)), recipients = d.recipients.filter(x => ids.has(x.campaign_id));
    const skip = reason => recipients.filter(x => String(x.skip_reason || "").includes(reason)).length;
    const status = value => recipients.filter(x => x.status === value).length;
    return { source: "supabase:skinpara_campaigns,skinpara_campaign_recipients", data_mode: "DRY-RUN/TEST", campaigns_created: campaigns.length, audience_count: recipients.length, eligible: recipients.filter(x => x.eligibility_status === "eligible").length, skipped: recipients.filter(x => x.eligibility_status === "skipped" || x.status === "skipped").length, simulated_sent: status("simulated"), failed: status("failed"), opt_out_skips: skip("opt_out"), consent_skips: skip("consent"), frequency_cap_skips: skip("frequency"), quiet_hours_deferrals: recipients.filter(x => x.eligibility_status === "deferred" || String(x.skip_reason || "").includes("quiet")).length, sent: status("sent"), delivered: status("delivered"), read: status("read"), conversion: null, revenue_attributed: null, limitations: ["No real provider conversion/revenue attribution is available"] };
  }

  function loyalty(d) {
    const ledger = d.ledger.filter(x => inRange(x, "created_at", d.range)), referrals = d.referrals.filter(x => inRange(x, "created_at", d.range));
    const sumType = type => ledger.filter(x => x.type === type).reduce((s, x) => s + num(x.amount), 0);
    const rewardCost = ledger.filter(x => x.reason === "referral_reward" && num(x.amount) > 0).reduce((s, x) => s + num(x.amount), 0);
    const referredIds = uniq(referrals.map(x => x.referred_contact_id));
    const excluded = uniq(d.terminalShipments.map(x => x.order_id));
    const linkedRevenue = d.orders.filter(x => referredIds.has(x.contact_id) && x.status === "delivered" && !excluded.has(x.order_key)).reduce((s, x) => s + num(x.order_value), 0);
    return { source: "supabase:skinpara_referrals,skinpara_wallet_ledger,skinpara_wallet_accounts", data_mode: "TEST/LOCAL", total_referral_codes: d.customers.filter(x => x.referral_code).length, attributed_referrals: referrals.filter(x => x.status === "attributed").length, qualified_referrals: referrals.filter(x => ["qualified","rewarded","reversed"].includes(x.status)).length, rewarded_referrals: referrals.filter(x => ["rewarded","reversed"].includes(x.status)).length, referral_conversion_rate: pct(referrals.filter(x => ["qualified","rewarded","reversed"].includes(x.status)).length, referrals.length), referral_reward_value: rewardCost, wallet_credits: sumType("credit"), wallet_debits: Math.abs(sumType("debit")), wallet_reversals: Math.abs(sumType("reversal")), outstanding_wallet_liability: d.wallets.reduce((s, x) => s + num(x.balance), 0), customers_with_wallet_balance: d.wallets.filter(x => num(x.balance) > 0).length, revenue_linked_to_referred_customers: linkedRevenue, net_contribution_before_product_margin: linkedRevenue - rewardCost, roi: null, limitations: ["Product margin/cost is unavailable; ROI is intentionally not calculated"] };
  }

  function ai(d, live = []) {
    const events = d.aiEvents.filter(x => inRange(x, "created_at", d.range));
    return { source: "supabase:skinpara_ai_events + chatwoot_live", attribution: "partial", ai_response_attempts: events.length, ai_handled_conversations: uniq(events.map(x => x.conversation_id)).size, human_handoffs_live: live.filter(x => (x.labels || []).includes("human-handoff")).length, ai_errors: null, provider_failures: null, openrouter_quota_rate_failures: null, rag_search_count: null, rag_success: null, rag_fallback: null, intent_distribution: {}, limitations: ["AI failures, RAG outcome and intent are not stored as structured durable fields yet", "No AI-vs-human sales causation is claimed"] };
  }

  function products(d) {
    const excluded = uniq(d.terminalShipments.map(x => x.order_id));
    const delivered = d.orders.filter(x => x.status === "delivered" && !excluded.has(x.order_key) && inRange(x, "delivered_at", d.range));
    const map = new Map();
    for (const order of delivered) { const name = order.product_name || "Unknown product"; const item = map.get(name) || { product: name, delivered_orders: 0, delivered_revenue: 0, customers: new Set() }; item.delivered_orders++; item.delivered_revenue += num(order.order_value); item.customers.add(order.contact_id); map.set(name, item); }
    return { source: "supabase:skinpara_orders", top_products: [...map.values()].map(x => ({ product: x.product, delivered_orders: x.delivered_orders, delivered_revenue: x.delivered_revenue, unique_customers: x.customers.size })).sort((a,b) => b.delivered_revenue - a.delivered_revenue).slice(0,20), top_brands: [], limitations: ["Brand is not stored structurally on historical orders; unsafe title parsing is not used"] };
  }

  async function report(kind, params) {
    const range = analyticsRange(params), d = await load(range);
    const live = ["overview","funnel","ai","agents"].includes(kind) ? await getLiveConversations() : [];
    if (kind === "sales") return { ok: true, range, sales: sales(d) };
    if (kind === "funnel") return { ok: true, range, funnel: funnel(d, live) };
    if (kind === "delivery") return { ok: true, range, delivery: delivery(d) };
    if (kind === "campaigns") return { ok: true, range, campaigns: campaigns(d) };
    if (kind === "loyalty") return { ok: true, range, loyalty: loyalty(d) };
    if (kind === "ai") return { ok: true, range, ai: ai(d, live) };
    if (kind === "products") return { ok: true, range, products: products(d) };
    if (kind === "agents") return { ok: true, range, agents: { source: "chatwoot_live", attribution: "operational_only", open_conversations: live.length, assigned_conversations: live.filter(x => x.assignee).length, limitations: ["Historical response/resolution-time attribution is not durably available; no sales ranking is produced"] } };
    if (kind === "operational") return { ok: true, range, operational: await getOperationalHealth() };
    return { ok: true, range, sales: sales(d), funnel: funnel(d, live), delivery: delivery(d), campaigns: campaigns(d), loyalty: loyalty(d), ai: ai(d, live), products: products(d) };
  }
  return { report };
}


