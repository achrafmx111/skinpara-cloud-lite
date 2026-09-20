const DEFAULT_QUIET_HOURS = {
  timezone: "Africa/Casablanca",
  start: "22:00",
  end: "08:00"
};

function first(rows) {
  return Array.isArray(rows) ? rows[0] || null : null;
}

function iso(value = Date.now()) {
  return new Date(value).toISOString();
}

function localMinutes(date, timezone) {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: timezone,
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23"
  }).formatToParts(date);
  const values = Object.fromEntries(parts.map(x => [x.type, x.value]));
  return Number(values.hour) * 60 + Number(values.minute);
}

function clockMinutes(value, fallback) {
  const match = String(value || fallback).match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return clockMinutes(fallback, "00:00");
  return Math.min(23, Number(match[1])) * 60 + Math.min(59, Number(match[2]));
}

export function deferForQuietHours(value, config = {}) {
  const quiet = { ...DEFAULT_QUIET_HOURS, ...(config || {}) };
  const date = new Date(value);
  const current = localMinutes(date, quiet.timezone);
  const start = clockMinutes(quiet.start, DEFAULT_QUIET_HOURS.start);
  const end = clockMinutes(quiet.end, DEFAULT_QUIET_HOURS.end);
  const inside = start < end
    ? current >= start && current < end
    : current >= start || current < end;
  if (!inside) return { scheduled_for: date.toISOString(), deferred: false };
  const minutes = current < end
    ? end - current
    : (24 * 60 - current) + end;
  return {
    scheduled_for: new Date(date.getTime() + minutes * 60000).toISOString(),
    deferred: true
  };
}

export function createAutomationEngine({
  supabaseFetch,
  log,
  getConversation,
  catalogUrl = "http://skinpara-catalog-service:8792",
  allowLiveMessaging = false
}) {
  async function listRules() {
    const rules = await supabaseFetch(
      "/skinpara_automation_rules?order=priority.desc,created_at.asc"
    );
    const runs = await supabaseFetch(
      "/skinpara_automation_runs?select=rule_id,status,scheduled_for,executed_at,created_at&order=created_at.desc&limit=500"
    );
    return (rules || []).map(rule => {
      const related = (runs || []).filter(run => run.rule_id === rule.id);
      return {
        ...rule,
        last_run: related[0] || null,
        next_scheduled_run:
          related.find(run => ["pending", "retry"].includes(run.status))
            ?.scheduled_for || null
      };
    });
  }

  async function saveRule(input) {
    const now = iso();
    const rule = {
      id: String(input.id || `auto_${Date.now()}`),
      name: String(input.name || "Automation"),
      event_type: String(input.event_type || input.if_label || "new-lead").replaceAll("-", "_"),
      enabled: input.enabled === true,
      conditions: input.conditions || {},
      action_type: String(input.action_type || "audit_only"),
      action_config: input.action_config || {
        value: String(input.action_value || ""),
        delay_seconds: Math.max(0, Number(input.after_minutes || 0) * 60)
      },
      message_category: ["transactional", "marketing", "internal"].includes(input.message_category)
        ? input.message_category : "transactional",
      cooldown_seconds: Math.max(0, Number(input.cooldown_seconds || 0)),
      priority: Number(input.priority || 100),
      quiet_hours: input.quiet_hours || DEFAULT_QUIET_HOURS,
      max_attempts: Math.max(1, Number(input.max_attempts || 3)),
      mode: ["disabled", "dry-run", "test", "live"].includes(input.mode)
        ? input.mode : "dry-run",
      created_at: input.created_at || now,
      updated_at: now
    };
    const rows = await supabaseFetch(
      "/skinpara_automation_rules?on_conflict=id",
      {
        method: "POST",
        headers: { Prefer: "resolution=merge-duplicates,return=representation" },
        body: JSON.stringify(rule)
      }
    );
    return first(rows) || rule;
  }

  async function listRuns(limit = 100) {
    return await supabaseFetch(
      `/skinpara_automation_runs?order=created_at.desc&limit=${Math.min(500, Math.max(1, Number(limit)))}`
    );
  }

  async function preferences(contactId) {
    if (!contactId) return null;
    return first(await supabaseFetch(
      `/skinpara_communication_preferences?contact_id=eq.${Number(contactId)}&limit=1`
    ));
  }

  function skipReason(rule, pref) {
    if (!rule.enabled || rule.mode === "disabled") return "rule_disabled";
    if (rule.message_category === "internal") return null;
    if (pref?.automation_opt_out) return "automation_opt_out";
    if (rule.message_category === "marketing" &&
        (pref?.marketing_opt_out || pref?.whatsapp_marketing_allowed !== true)) {
      return "marketing_not_allowed";
    }
    return null;
  }

  async function reorderDelaySeconds(event, rule) {
    const fallback = Math.max(1, Number(rule.action_config?.default_reorder_days || 60));
    if (!event.product_name) return fallback * 86400;
    try {
      const response = await fetch(
        `${catalogUrl.replace(/\/+$/, "")}/catalog/search?q=${encodeURIComponent(event.product_name)}&limit=1`
      );
      if (!response.ok) throw new Error(`catalog_${response.status}`);
      const result = await response.json();
      const days = Number(result?.products?.[0]?.reorder_days || fallback);
      return Math.max(1, days) * 86400;
    } catch (error) {
      log("automation_reorder_catalog_fallback", {
        event_key: event.event_key,
        error: error.message
      });
      return fallback * 86400;
    }
  }

  async function cancelNoReply(event, reason = "customer_replied") {
    if (!event.conversation_id) return [];
    return await supabaseFetch(
      `/skinpara_automation_runs?conversation_id=eq.${Number(event.conversation_id)}&action_type=eq.no_reply_followup&status=in.(pending,retry)`,
      {
        method: "PATCH",
        headers: { Prefer: "return=representation" },
        body: JSON.stringify({
          status: "cancelled",
          cancelled_at: iso(),
          skip_reason: reason,
          updated_at: iso()
        })
      }
    );
  }

  async function scheduleEvent(event) {
    if (event.event_type === "customer_replied") {
      await cancelNoReply(event);
    }
    const type = String(event.event_type).replaceAll("-", "_");
    const rules = await supabaseFetch(
      `/skinpara_automation_rules?event_type=eq.${encodeURIComponent(type)}&order=priority.desc`
    );
    const pref = await preferences(event.contact_id);
    const decisions = [];

    for (const rule of rules || []) {
      const dedupeKey = `${rule.id}:${event.event_key}:${event.contact_id || 0}:${event.order_id || 0}`;
      let reason = skipReason(rule, pref);
      let delay = Math.max(0, Number(rule.action_config?.delay_seconds || 0));
      if (rule.action_type === "schedule_reorder") {
        delay = await reorderDelaySeconds(event, rule);
      }
      if (!reason && rule.cooldown_seconds > 0 && event.contact_id) {
        const cutoff = iso(Date.now() - rule.cooldown_seconds * 1000);
        const recent = await supabaseFetch(
          `/skinpara_automation_runs?rule_id=eq.${encodeURIComponent(rule.id)}&contact_id=eq.${Number(event.contact_id)}&created_at=gte.${encodeURIComponent(cutoff)}&status=in.(dry_run,executed,pending,retry)&limit=1`
        );
        if ((recent || []).length) reason = "cooldown";
      }
      const base = new Date(event.event_at || Date.now()).getTime() + delay * 1000;
      const quiet = deferForQuietHours(base, rule.quiet_hours);
      const status = reason ? "skipped" : "pending";
      const rows = await supabaseFetch(
        "/skinpara_automation_runs?on_conflict=dedupe_key",
        {
          method: "POST",
          headers: { Prefer: "resolution=ignore-duplicates,return=representation" },
          body: JSON.stringify({
            rule_id: rule.id,
            rule_name: rule.name,
            event_type: rule.event_type,
            contact_id: event.contact_id || null,
            conversation_id: event.conversation_id || null,
            order_id: event.order_id || null,
            source_event_id: event.source_event_id || String(event.id || event.event_key),
            event_key: event.event_key,
            dedupe_key: dedupeKey,
            action_type: rule.action_type,
            action_config: rule.action_config,
            message_category: rule.message_category,
            mode: rule.mode,
            status,
            max_attempts: rule.max_attempts,
            priority: rule.priority,
            scheduled_for: quiet.scheduled_for,
            skip_reason: reason || (quiet.deferred ? "quiet_hours_deferred" : null),
            updated_at: iso()
          })
        }
      );
      const run = first(rows);
      decisions.push({ rule_id: rule.id, matched: !reason, status, duplicate: !run, run });
      log("automation_decision", {
        rule_id: rule.id,
        rule_name: rule.name,
        event_type: type,
        source_event_id: event.source_event_id || event.id || null,
        contact_id: event.contact_id || null,
        order_id: event.order_id || null,
        matched: !reason,
        status,
        scheduled_for: quiet.scheduled_for,
        skip_reason: reason || (quiet.deferred ? "quiet_hours_deferred" : null)
      });
    }
    return decisions;
  }

  async function ingestEvents() {
    const events = await supabaseFetch(
      "/skinpara_automation_events?order=id.asc&limit=500"
    );
    let scheduled = 0;
    for (const event of events || []) {
      const decisions = await scheduleEvent(event);
      scheduled += decisions.filter(x => !x.duplicate).length;
    }
    return scheduled;
  }

  async function executeDueRuns() {
    const due = await supabaseFetch(
      `/skinpara_automation_runs?status=in.(pending,retry)&scheduled_for=lte.${encodeURIComponent(iso())}&order=priority.desc,scheduled_for.asc&limit=50`
    );
    let executed = 0;
    for (const candidate of due || []) {
      const claimed = first(await supabaseFetch(
        `/skinpara_automation_runs?id=eq.${candidate.id}&status=in.(pending,retry)`,
        {
          method: "PATCH",
          headers: { Prefer: "return=representation" },
          body: JSON.stringify({ status: "running", locked_at: iso(), attempts: candidate.attempts + 1, updated_at: iso() })
        }
      ));
      if (!claimed) continue;
      try {
        const pref = await preferences(claimed.contact_id);
        let reason = skipReason({
          enabled: true,
          mode: claimed.mode,
          message_category: claimed.message_category
        }, pref);
        if (!reason && claimed.action_type === "no_reply_followup" && claimed.conversation_id) {
          const conversation = await getConversation(claimed.conversation_id);
          if (["resolved", "closed"].includes(String(conversation?.status))) {
            reason = "conversation_resolved";
          }
        }
        let status = "dry_run";
        if (reason) status = "skipped";
        else if (claimed.mode === "live" && allowLiveMessaging) status = "executed";
        else if (claimed.mode === "live") reason = "live_messaging_blocked";
        await supabaseFetch(`/skinpara_automation_runs?id=eq.${claimed.id}`, {
          method: "PATCH",
          body: JSON.stringify({
            status: reason ? "skipped" : status,
            skip_reason: reason,
            executed_at: iso(),
            updated_at: iso()
          })
        });
        executed++;
      } catch (error) {
        // The claim already increments attempts, so compare the persisted value directly.
        const retry = claimed.attempts < claimed.max_attempts;
        await supabaseFetch(`/skinpara_automation_runs?id=eq.${claimed.id}`, {
          method: "PATCH",
          body: JSON.stringify({
            status: retry ? "retry" : "failed",
            scheduled_for: iso(Date.now() + 60000),
            error: error.message.slice(0, 500),
            updated_at: iso()
          })
        });
      }
    }
    return executed;
  }

  async function runOnce() {
    const scheduled = await ingestEvents();
    const executed = await executeDueRuns();
    return { ok: true, scheduled, executed };
  }

  return {
    listRules,
    saveRule,
    listRuns,
    scheduleEvent,
    cancelNoReply,
    runOnce,
    executeDueRuns
  };
}


