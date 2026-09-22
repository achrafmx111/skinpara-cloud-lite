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

    let history = await store.getHistory(job.conversationKey);

    // Session boundary for a clearly fresh consultation. WhatsApp conversation
    // storage remains durable, but a new greeting + self-contained concern
    // should not inherit answers from an older consultation/test session.
    // Keep this conservative: ordinary follow-ups never reset context.
    const latestInbound = clean(job.textContent);
    const freshConsultationStart =
      /^(?:سلام|السلام|اهلا|أهلا|bonjour|salut|hello|hi)(?:\s|[,،.!؟:;-]|$)/i.test(latestInbound) &&
      /(?:بشر|وجه|حبوب|شعر|روتين|عناية|peau|acn[eé]|cheveux|routine|skin|acne|hair)/i.test(latestInbound);

    if (freshConsultationStart && Array.isArray(history) && history.length > 1) {
      const currentInbound = history[history.length - 1];
      history = currentInbound ? [currentInbound] : [];
      console.log("[KAPSO DIRECT] New consultation session", JSON.stringify({
        conversationKeySuffix: String(job.conversationKey || "").slice(-8),
        reason: "fresh_greeting_with_concern"
      }));
    }

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
      // Ground product discovery in the customer's recent context, not only
      // the last short WhatsApp turn (e.g. "give me a cleanser").
      const recentCustomerContext = (history || [])
        .filter(row => row.role === "user")
        .slice(-20)
        .map(row => clean(row.content))
        .filter(Boolean)
        .join(" ");
      const catalogQuery = recentCustomerContext || clean(job.textContent);

      // Catalog RAG understands one dominant intent per query. A consultation
      // can contain both a concern (oily/acne) and a requested category
      // (cleanser), so run a second focused lookup for the requested category
      // and merge verified catalog rows. This avoids losing "cleanser" when the
      // first intent parser locks onto "oily".
      const latestText = clean(job.textContent);
      const wantsCleanser = /(?:منظف|غسول|cleanser|nettoyant|gel nettoyant)/i.test(latestText);
      const focusedCatalogQuery = wantsCleanser
        ? `cleanser nettoyant ${latestText}`
        : "";

      const [primaryCatalog, focusedCatalog, products] = await Promise.all([
        searchCatalog(catalogQuery),
        focusedCatalogQuery ? searchCatalog(focusedCatalogQuery) : Promise.resolve(null),
        searchProducts(catalogQuery)
      ]);

      const mergedCatalogProducts = [];
      const seenCatalogProducts = new Set();
      for (const result of [focusedCatalog, primaryCatalog]) {
        for (const product of (Array.isArray(result?.products) ? result.products : [])) {
          const key = String(product?.id || product?.catalog_key || product?.title || "").toLowerCase();
          if (!key || seenCatalogProducts.has(key)) continue;
          seenCatalogProducts.add(key);
          mergedCatalogProducts.push(product);
          if (mergedCatalogProducts.length >= 5) break;
        }
        if (mergedCatalogProducts.length >= 5) break;
      }

      const catalogResult = {
        ...(primaryCatalog || {}),
        ok: Boolean(primaryCatalog?.ok || focusedCatalog?.ok),
        fallback: Boolean(primaryCatalog?.fallback && (!focusedCatalog || focusedCatalog?.fallback)),
        products: mergedCatalogProducts
      };
      if (catalogResult.products.length) {
        catalogResult.context = [
          "SKINPARA CATALOG INTELLIGENCE:",
          "Use only the verified products below. Do not invent product facts.",
          ...catalogResult.products.map((product, index) => [
            `PRODUCT ${index + 1}`,
            `Title: ${clean(product?.title)}`,
            product?.brand ? `Brand: ${clean(product.brand)}` : "",
            product?.category ? `Category: ${clean(product.category)}` : "",
            product?.concern ? `Concern: ${clean(product.concern)}` : "",
            product?.skin_type ? `Suitable for: ${clean(product.skin_type)}` : "",
            product?.usage ? `Usage: ${clean(product.usage)}` : "",
            product?.ai_summary ? `Summary: ${clean(product.ai_summary)}` : "",
            product?.ai_safety ? `Safety: ${clean(product.ai_safety)}` : ""
          ].filter(Boolean).join("\n"))
        ].join("\n\n");
      } else {
        catalogResult.context = "";
      }
      // Safe catalog observability: product titles/counts only; no customer text or secrets.
      const catalogProducts = Array.isArray(catalogResult?.products) ? catalogResult.products : [];
      const shopifyProducts = Array.isArray(products) ? products : [];
      console.log("[KAPSO DIRECT] Catalog lookup", JSON.stringify({
        catalogCount: catalogProducts.length,
        catalogTitles: catalogProducts.slice(0, 5).map(product => String(product?.title || product?.name || "").slice(0, 120)),
        shopifyCount: shopifyProducts.length,
        shopifyTitles: shopifyProducts.slice(0, 5).map(product => String(product?.title || product?.name || "").slice(0, 120)),
        // Safe diagnostics only: no customer text, URLs, tokens or secrets.
        catalogEnabled: primaryCatalog?.enabled ?? null,
        catalogOk: primaryCatalog?.ok ?? null,
        catalogFallback: primaryCatalog?.fallback ?? null,
        catalogReason: primaryCatalog?.reason || null,
        catalogIntent: primaryCatalog?.intelligence?.intent || null,
        focusedCatalogEnabled: focusedCatalog?.enabled ?? null,
        focusedCatalogOk: focusedCatalog?.ok ?? null,
        focusedCatalogFallback: focusedCatalog?.fallback ?? null,
        focusedCatalogReason: focusedCatalog?.reason || null,
        focusedCatalogIntent: focusedCatalog?.intelligence?.intent || null
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
