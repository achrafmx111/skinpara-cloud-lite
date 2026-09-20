import { deferForQuietHours } from "./automation-engine.js";

const nowIso = () => new Date().toISOString();
const first = rows => Array.isArray(rows) ? rows[0] || null : null;
const enc = value => encodeURIComponent(String(value));

export function createCampaignEngine({ supabaseFetch, log, liveEnabled = false }) {
  async function table(path) { return await supabaseFetch(path); }
  async function rpcPost(path, body, prefer = "return=representation") {
    return await supabaseFetch(path, {
      method: "POST",
      headers: { Prefer: prefer },
      body: JSON.stringify(body)
    });
  }
  async function patch(path, body) {
    return await supabaseFetch(path, {
      method: "PATCH",
      headers: { Prefer: "return=representation" },
      body: JSON.stringify(body)
    });
  }

  function assertSafeMode(mode) {
    if (mode === "live" && !liveEnabled) throw new Error("blocked_live_mode");
    if (!["draft", "dry-run", "test", "live"].includes(mode)) throw new Error("invalid_campaign_mode");
  }

  async function saveSegment(input) {
    const row = {
      id: String(input.id || `segment_${Date.now()}`),
      name: String(input.name || "Segment"),
      description: input.description || null,
      enabled: input.enabled === true,
      definition: input.definition || {},
      updated_at: nowIso()
    };
    return first(await rpcPost("/skinpara_segments?on_conflict=id", row, "resolution=merge-duplicates,return=representation"));
  }

  async function saveTemplate(input) {
    if (input.provider_template_id && input.status !== "approved") throw new Error("provider_id_requires_real_approval");
    const row = {
      id: String(input.id || `template_${Date.now()}`),
      name: String(input.name || "Template"),
      channel: "whatsapp",
      language: String(input.language || "ar_MA"),
      category: input.category === "transactional" ? "transactional" : "marketing",
      provider_template_name: input.provider_template_name || null,
      provider_template_id: input.provider_template_id || null,
      status: ["draft","pending_approval","approved","rejected","disabled"].includes(input.status) ? input.status : "draft",
      body: String(input.body || ""),
      variables: Array.isArray(input.variables) ? input.variables : [],
      marketing: input.category !== "transactional",
      active: input.active === true,
      updated_at: nowIso()
    };
    return first(await rpcPost("/skinpara_message_templates?on_conflict=id", row, "resolution=merge-duplicates,return=representation"));
  }

  async function saveCampaign(input) {
    const mode = String(input.mode || "dry-run");
    assertSafeMode(mode);
    const row = {
      id: String(input.id || `campaign_${Date.now()}`),
      name: String(input.name || "Campaign"),
      channel: "whatsapp",
      campaign_type: input.campaign_type === "transactional" ? "transactional" : "marketing",
      segment_id: input.segment_id || null,
      template_id: input.template_id || null,
      status: ["draft","scheduled","preparing","ready","running","paused","completed","cancelled","failed"].includes(input.status) ? input.status : "draft",
      mode,
      scheduled_for: input.scheduled_for || null,
      max_recipients: Math.max(1, Number(input.max_recipients || 100)),
      daily_cap: Math.max(1, Number(input.daily_cap || 100)),
      hourly_cap: Math.max(1, Number(input.hourly_cap || 20)),
      per_customer_24h: Math.max(1, Number(input.per_customer_24h || 1)),
      per_customer_7d: Math.max(1, Number(input.per_customer_7d || 3)),
      quiet_hours: input.quiet_hours || { timezone: "Africa/Casablanca", start: "22:00", end: "08:00" },
      cap_policy: input.cap_policy === "defer" ? "defer" : "skip",
      created_by: input.created_by || "admin",
      updated_at: nowIso()
    };
    return first(await rpcPost("/skinpara_campaigns?on_conflict=id", row, "resolution=merge-duplicates,return=representation"));
  }

  async function loadAudienceData() {
    const [customers, memory, stats, wallets, referrals, preferences, reorderRuns] = await Promise.all([
      table("/skinpara_customers?order=contact_id.asc"),
      table("/skinpara_customer_memory"),
      table("/skinpara_customer_stats"),
      table("/skinpara_wallet_accounts"),
      table("/skinpara_referrals"),
      table("/skinpara_communication_preferences"),
      table("/skinpara_automation_runs?action_type=eq.schedule_reorder&status=in.(pending,retry)&select=contact_id,scheduled_for")
    ]);
    const index = rows => new Map((rows || []).map(row => [Number(row.contact_id), row]));
    const referralCounts = new Map();
    for (const referral of referrals || []) {
      const id = Number(referral.referrer_contact_id);
      const value = referralCounts.get(id) || { total: 0, rewarded: 0 };
      value.total++;
      if (referral.status === "rewarded") value.rewarded++;
      referralCounts.set(id, value);
    }
    const reorder = new Set((reorderRuns || []).filter(x => new Date(x.scheduled_for) <= new Date()).map(x => Number(x.contact_id)));
    return { customers: customers || [], memory: index(memory), stats: index(stats), wallets: index(wallets), preferences: index(preferences), referralCounts, reorder };
  }

  function matches(def, customer, data) {
    const id = Number(customer.contact_id);
    const memory = data.memory.get(id) || {};
    const stats = data.stats.get(id) || {};
    const wallet = data.wallets.get(id) || {};
    if (Array.isArray(def.contact_ids) && !def.contact_ids.map(Number).includes(id)) return false;
    if (def.skin_type && String(memory.skin_type || "").toLowerCase() !== String(def.skin_type).toLowerCase()) return false;
    if (def.skin_concern && !String(memory.skin_concern || "").toLowerCase().includes(String(def.skin_concern).toLowerCase())) return false;
    if (def.preferred_language && memory.preferred_language !== def.preferred_language) return false;
    if (def.repeat_customer !== undefined && Boolean(stats.repeat_customer) !== Boolean(def.repeat_customer)) return false;
    if (def.vip !== undefined && Boolean(stats.vip) !== Boolean(def.vip)) return false;
    if (def.wallet_balance_gt !== undefined && Number(wallet.balance || 0) <= Number(def.wallet_balance_gt)) return false;
    if (def.min_delivered_orders !== undefined && Number(stats.delivered_orders || 0) < Number(def.min_delivered_orders)) return false;
    if (def.min_delivered_value !== undefined && Number(stats.delivered_order_value || 0) < Number(def.min_delivered_value)) return false;
    if (def.min_aov !== undefined && Number(stats.average_order_value || 0) < Number(def.min_aov)) return false;
    if (def.last_product_contains && !String(stats.last_product || "").toLowerCase().includes(String(def.last_product_contains).toLowerCase())) return false;
    if (def.inactive_days && new Date(customer.last_seen_at).getTime() > Date.now() - Number(def.inactive_days) * 86400000) return false;
    if (def.reorder_due === true && !data.reorder.has(id)) return false;
    return true;
  }

  async function evaluateSegment(segmentId) {
    const segment = first(await table(`/skinpara_segments?id=eq.${enc(segmentId)}&limit=1`));
    if (!segment) throw new Error("segment_not_found");
    const data = await loadAudienceData();
    const customers = data.customers.filter(customer => matches(segment.definition || {}, customer, data));
    await patch(`/skinpara_segments?id=eq.${enc(segment.id)}`, { estimated_count: customers.length, last_evaluated_at: nowIso(), updated_at: nowIso() });
    return { segment, customers, data };
  }

  function renderTemplate(template, snapshot) {
    let body = template.body;
    for (const variable of template.variables || []) {
      const value = snapshot[variable];
      if (value === null || value === undefined || value === "") return { ok: false, reason: `missing_variable:${variable}` };
      body = body.replaceAll(`{{${variable}}}`, String(value));
    }
    if (/{{[^}]+}}/.test(body)) return { ok: false, reason: "undefined_placeholder" };
    return { ok: true, body };
  }

  async function snapshotCampaign(id) {
    const campaign = first(await table(`/skinpara_campaigns?id=eq.${enc(id)}&limit=1`));
    if (!campaign) throw new Error("campaign_not_found");
    assertSafeMode(campaign.mode);
    if (["cancelled","completed"].includes(campaign.status)) throw new Error("campaign_not_snapshotable");
    const template = first(await table(`/skinpara_message_templates?id=eq.${enc(campaign.template_id)}&limit=1`));
    if (!template || !template.active) throw new Error("template_inactive_or_missing");
    const evaluated = await evaluateSegment(campaign.segment_id);
    const candidates = evaluated.customers.slice(0, campaign.max_recipients);
    const recent = await table(`/skinpara_campaign_recipients?sent_at=gte.${enc(new Date(Date.now()-7*86400000).toISOString())}&status=in.(simulated,sent)`);
    const decisions = [];
    for (const customer of candidates) {
      const contactId = Number(customer.contact_id);
      const pref = evaluated.data.preferences.get(contactId);
      const stats = evaluated.data.stats.get(contactId) || {};
      const wallet = evaluated.data.wallets.get(contactId) || {};
      const referrals = evaluated.data.referralCounts.get(contactId) || { total: 0, rewarded: 0 };
      const snapshot = { contact_id: contactId, customer_name: customer.customer_name, phone_number: customer.phone_number, preferred_language: evaluated.data.memory.get(contactId)?.preferred_language || null, wallet_balance: Number(wallet.balance || 0), total_orders: Number(stats.total_orders || 0), delivered_orders: Number(stats.delivered_orders || 0), last_product: stats.last_product || null, referral_count: referrals.total, rewarded_referrals: referrals.rewarded };
      let reason = null;
      if (campaign.campaign_type === "marketing" && pref?.marketing_opt_out) reason = "marketing_opt_out";
      else if (campaign.campaign_type === "marketing" && pref?.whatsapp_marketing_allowed !== true) reason = "whatsapp_marketing_not_allowed";
      const sent = (recent || []).filter(x => Number(x.contact_id) === contactId);
      const count24 = sent.filter(x => new Date(x.sent_at) >= new Date(Date.now()-86400000)).length;
      if (!reason && count24 >= campaign.per_customer_24h) reason = "frequency_cap_24h";
      else if (!reason && sent.length >= campaign.per_customer_7d) reason = "frequency_cap_7d";
      const rendered = renderTemplate(template, snapshot);
      if (!reason && !rendered.ok) reason = rendered.reason;
      const quiet = deferForQuietHours(campaign.scheduled_for || Date.now(), campaign.quiet_hours);
      const status = reason ? "skipped" : quiet.deferred ? "deferred" : "pending";
      const rows = await rpcPost("/skinpara_campaign_recipients?on_conflict=dedupe_key", { campaign_id: campaign.id, contact_id: contactId, snapshot_data: snapshot, eligibility_status: reason ? "skipped" : quiet.deferred ? "deferred" : "eligible", skip_reason: reason || (quiet.deferred ? "quiet_hours_deferred" : null), status, scheduled_for: quiet.scheduled_for, dedupe_key: `${campaign.id}:${contactId}`, rendered_body: rendered.ok ? rendered.body : null, updated_at: nowIso() }, "resolution=ignore-duplicates,return=representation");
      decisions.push({ contact_id: contactId, status, reason: reason || (quiet.deferred ? "quiet_hours_deferred" : null), duplicate: !rows?.length });
    }
    await patch(`/skinpara_campaigns?id=eq.${enc(id)}`, { status: "ready", updated_at: nowIso() });
    return { ok: true, campaign_id: id, matched_count: evaluated.customers.length, snapshot_count: candidates.length, decisions };
  }

  async function executeCampaign(id) {
    const campaign = first(await table(`/skinpara_campaigns?id=eq.${enc(id)}&limit=1`));
    if (!campaign) throw new Error("campaign_not_found");
    assertSafeMode(campaign.mode);
    if (campaign.status === "paused") return { ok: true, stopped: true, reason: "campaign_paused" };
    if (campaign.status === "cancelled") return { ok: true, stopped: true, reason: "campaign_cancelled" };
    let recipients = await table(`/skinpara_campaign_recipients?campaign_id=eq.${enc(id)}&status=in.(pending,deferred)&scheduled_for=lte.${enc(nowIso())}&order=id.asc`);
    const all = await table(`/skinpara_campaign_recipients?campaign_id=eq.${enc(id)}`);
    if (!(all || []).length) { await snapshotCampaign(id); recipients = await table(`/skinpara_campaign_recipients?campaign_id=eq.${enc(id)}&status=pending`); }
    await patch(`/skinpara_campaigns?id=eq.${enc(id)}`, { status: "running", started_at: campaign.started_at || nowIso(), updated_at: nowIso() });
    let simulated = 0, skipped = 0, failed = 0;
    const sentHour = (all || []).filter(x => x.sent_at && new Date(x.sent_at) >= new Date(Date.now()-3600000)).length;
    const sentDay = (all || []).filter(x => x.sent_at && new Date(x.sent_at) >= new Date(Date.now()-86400000)).length;
    let hourCount = sentHour, dayCount = sentDay;
    for (const recipient of recipients || []) {
      if (hourCount >= campaign.hourly_cap || dayCount >= campaign.daily_cap) {
        const reason = hourCount >= campaign.hourly_cap ? "campaign_hourly_cap" : "campaign_daily_cap";
        if (campaign.cap_policy === "defer") await patch(`/skinpara_campaign_recipients?id=eq.${recipient.id}`, { status: "deferred", eligibility_status: "deferred", skip_reason: reason, scheduled_for: new Date(Date.now()+3600000).toISOString(), updated_at: nowIso() });
        else { await patch(`/skinpara_campaign_recipients?id=eq.${recipient.id}`, { status: "skipped", eligibility_status: "skipped", skip_reason: reason, updated_at: nowIso() }); skipped++; }
        continue;
      }
      try {
        if (!recipient.rendered_body) throw new Error("rendered_body_missing");
        const at = nowIso();
        await patch(`/skinpara_campaign_recipients?id=eq.${recipient.id}&status=in.(pending,deferred)`, { status: "simulated", sent_at: at, updated_at: at });
        await rpcPost("/skinpara_campaign_events", { campaign_id: id, recipient_id: recipient.id, event_type: campaign.mode === "dry-run" ? "dry_run_rendered" : "test_simulated_send", provider_message_id: null, payload: { outbound: false, adapter: campaign.mode === "dry-run" ? "DryRunAdapter" : "TestAdapter" } }, "return=minimal");
        simulated++; hourCount++; dayCount++;
      } catch (error) {
        await patch(`/skinpara_campaign_recipients?id=eq.${recipient.id}`, { status: "failed", failed_at: nowIso(), error: error.message.slice(0,300), updated_at: nowIso() }); failed++;
      }
    }
    const remaining = await table(`/skinpara_campaign_recipients?campaign_id=eq.${enc(id)}&status=in.(pending,deferred)&limit=1`);
    await patch(`/skinpara_campaigns?id=eq.${enc(id)}`, { status: remaining?.length ? "ready" : "completed", completed_at: remaining?.length ? null : nowIso(), updated_at: nowIso() });
    log("campaign_simulation", { campaign_id: id, mode: campaign.mode, simulated, skipped, failed, outbound: false });
    return { ok: true, simulated, skipped, failed, outbound_messages: 0 };
  }

  async function setStatus(id, status) {
    if (!['paused','cancelled'].includes(status)) throw new Error("invalid_campaign_control");
    const campaign = first(await patch(`/skinpara_campaigns?id=eq.${enc(id)}`, { status, updated_at: nowIso() }));
    if (status === "cancelled") await patch(`/skinpara_campaign_recipients?campaign_id=eq.${enc(id)}&status=in.(pending,deferred)`, { status: "cancelled", updated_at: nowIso() });
    return { ok: true, campaign };
  }

  async function listCampaigns() {
    const [campaigns, recipients, segments, templates] = await Promise.all([table("/skinpara_campaigns?order=created_at.desc"),table("/skinpara_campaign_recipients"),table("/skinpara_segments?order=name.asc"),table("/skinpara_message_templates?order=name.asc")]);
    return { campaigns: (campaigns || []).map(c => { const rs=(recipients||[]).filter(r=>r.campaign_id===c.id); return {...c,audience_count:rs.length,eligible_count:rs.filter(r=>r.eligibility_status==='eligible').length,skipped_count:rs.filter(r=>r.status==='skipped').length,simulated_sent:rs.filter(r=>r.status==='simulated').length,failed_count:rs.filter(r=>r.status==='failed').length}; }), segments, templates };
  }

  async function runDueCampaigns() {
    const campaigns = await table(
      `/skinpara_campaigns?status=in.(scheduled,ready,running)&or=(scheduled_for.is.null,scheduled_for.lte.${enc(nowIso())})&order=created_at.asc&limit=20`
    );
    const results = [];
    for (const campaign of campaigns || []) {
      try {
        results.push({ campaign_id: campaign.id, ...(await executeCampaign(campaign.id)) });
      } catch (error) {
        results.push({ campaign_id: campaign.id, ok: false, error: error.message });
      }
    }
    return { ok: true, checked: (campaigns || []).length, results };
  }

  return { saveSegment, saveTemplate, saveCampaign, evaluateSegment, snapshotCampaign, executeCampaign, setStatus, listCampaigns, runDueCampaigns };
}


