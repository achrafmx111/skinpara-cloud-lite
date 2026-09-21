const crypto = require("crypto");

const MEDICAL_RISK_RE = /(حروق|حمرا|يضرني|ألم|حساسية مفرطة|تهيج|ضيق تنفس|تنفس|تورم|burn|pain|swelling|breathing|reaction)/i;

function clean(value) {
  return String(value || "").trim();
}

function isMedicalRisk(text) {
  return MEDICAL_RISK_RE.test(clean(text));
}

function directSafeMedicalReply() {
  return "فهمتك، والسلامة هي الأهم. ما نقدرش نشخّص الحالة ولا نعطي علاج هنا. خاص التواصل بسرعة مع صيدلي أو طبيب جلدية لتقييم الأعراض. وإذا كانت صعوبة فالتنفس أو تورم قوي فالوجه أو العينين، خاص طلب مساعدة طبية مستعجلة فوراً.";
}

function validateDirectJob(job) {
  if (!job || job.channel !== "kapso_direct") return { ok: false, reason: "invalid_channel" };
  for (const key of ["messageId", "customerKey", "conversationKey", "textContent"]) {
    if (!clean(job[key])) return { ok: false, reason: `missing_${key}` };
  }
  return { ok: true };
}

function createDirectProcessor({ store, enqueueOutbound, callAdvisor, searchCatalog, searchProducts, now = () => new Date().toISOString() }) {
  if (!store || !enqueueOutbound || !callAdvisor || !searchCatalog || !searchProducts) {
    throw new Error("direct_processor_dependencies_missing");
  }

  return async function processDirectJob(job) {
    const valid = validateDirectJob(job);
    if (!valid.ok) return { ignored: true, reason: valid.reason };

    const inboundEventKey = `kapso:in:${job.messageId}`;
    const inserted = await store.insertMessage({
      event_key: inboundEventKey,
      channel: "kapso_direct",
      customer_key: job.customerKey,
      conversation_key: job.conversationKey,
      external_message_id: job.messageId,
      direction: "inbound",
      role: "user",
      content: clean(job.textContent),
      handoff_required: false,
      created_at: job.receivedAt || now()
    });
    if (!inserted) {
      const alreadyCompleted = await store.hasEvent(`kapso:out:${job.messageId}`);
      if (alreadyCompleted) return { ignored: true, reason: "duplicate_message_id" };
    }

    const history = await store.getHistory(job.conversationKey);
    // Safe observability: counts/roles only; never log customer message content.
    console.log("[KAPSO DIRECT] History loaded", JSON.stringify({
      conversationKeySuffix: String(job.conversationKey || "").slice(-8),
      rows: Array.isArray(history) ? history.length : 0,
      userRows: Array.isArray(history) ? history.filter(row => row.role === "user").length : 0,
      assistantRows: Array.isArray(history) ? history.filter(row => row.role === "assistant").length : 0,
      newestRole: Array.isArray(history) && history.length ? history[history.length - 1].role : null,
      // Diagnostic booleans only: confirm whether the recent customer-history
      // window contains the already-stated skin-type fact without logging text.
      hasKnownOilySkinFact: Array.isArray(history) && history.some(row =>
        row.role === "user" && /(?:دهني|دهنية)/i.test(String(row.content || ""))
      )
    }));
    let assistantMessage;
    let handoffRequired = false;

    if (isMedicalRisk(job.textContent)) {
      assistantMessage = directSafeMedicalReply();
      handoffRequired = true;
      await store.markHandoff(inboundEventKey, "medical_risk");
    } else {
      const [catalogResult, products] = await Promise.all([
        searchCatalog(job.textContent),
        searchProducts(job.textContent)
      ]);
      // Safe catalog observability: product titles/counts only; no customer text or secrets.
      const catalogProducts = Array.isArray(catalogResult?.products) ? catalogResult.products : [];
      const shopifyProducts = Array.isArray(products) ? products : [];
      console.log("[KAPSO DIRECT] Catalog lookup", JSON.stringify({
        catalogCount: catalogProducts.length,
        catalogTitles: catalogProducts.slice(0, 5).map(product => String(product?.title || product?.name || "").slice(0, 120)),
        shopifyCount: shopifyProducts.length,
        shopifyTitles: shopifyProducts.slice(0, 5).map(product => String(product?.title || product?.name || "").slice(0, 120))
      }));
      assistantMessage = await callAdvisor({
        userMessage: job.textContent,
        history: history.map(row => ({ role: row.role, content: row.content })),
        products: products || [],
        memory: null,
        customerName: null,
        catalogContext: catalogResult?.context || ""
      });
    }

    const outboundEventKey = `kapso:out:${job.messageId}`;
    await store.insertMessage({
      event_key: outboundEventKey,
      channel: "kapso_direct",
      customer_key: job.customerKey,
      conversation_key: job.conversationKey,
      external_message_id: null,
      direction: "outbound",
      role: "assistant",
      content: clean(assistantMessage),
      handoff_required: handoffRequired,
      created_at: now()
    });

    await enqueueOutbound({
      channel: "kapso_direct",
      eventKey: outboundEventKey,
      sourceMessageId: job.messageId,
      customerKey: job.customerKey,
      conversationKey: job.conversationKey,
      content: clean(assistantMessage),
      private: false,
      messageType: "outgoing",
      handoffRequired
    });

    return { handled: true, handoff_required: handoffRequired, outbound_event_key: outboundEventKey };
  };
}

module.exports = {
  createDirectProcessor,
  directSafeMedicalReply,
  isMedicalRisk,
  validateDirectJob
};
