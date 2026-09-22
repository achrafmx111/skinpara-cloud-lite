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

    // Persist consultation boundaries from durable message history. A fresh
    // greeting + concern starts a new session. Follow-ups reuse the newest
    // matching boundary, so old tests/consultations never leak back in.
    const latestInbound = clean(job.textContent);
    const isConsultationStart = value =>
      /^(?:سلام|السلام|اهلا|أهلا|bonjour|salut|hello|hi)(?:\s|[,،.!؟:;-]|$)/i.test(clean(value)) &&
      /(?:بشر|وجه|حبوب|شعر|روتين|عناية|peau|acn[eé]|cheveux|routine|skin|acne|hair)/i.test(clean(value));

    let boundary = -1;
    if (Array.isArray(history)) {
      for (let i = history.length - 1; i >= 0; i--) {
        if (history[i]?.role === "user" && isConsultationStart(history[i]?.content)) {
          boundary = i;
          break;
        }
      }
    }

    const freshConsultationStart = isConsultationStart(latestInbound);
    if (freshConsultationStart && Array.isArray(history) && history.length) {
      // The just-inserted inbound row is the newest row and therefore the
      // current boundary, regardless of older identical test messages.
      history = [history[history.length - 1]];
      console.log("[KAPSO DIRECT] New consultation session", JSON.stringify({
        conversationKeySuffix: String(job.conversationKey || "").slice(-8),
        reason: "fresh_greeting_with_concern"
      }));
    } else if (boundary >= 0 && Array.isArray(history)) {
      history = history.slice(boundary);
      console.log("[KAPSO DIRECT] Consultation session restored", JSON.stringify({
        conversationKeySuffix: String(job.conversationKey || "").slice(-8),
        rows: history.length
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

    // Durable conversational memory: keep a compact, structured view of what
    // the customer has explicitly said and which verified products were shown.
    // This complements transcript history and makes long WhatsApp conversations
    // resilient to topic/product switching without inventing profile facts.
    const memoryState = typeof store.getConversationMemory === "function"
      ? (await store.getConversationMemory(job.conversationKey) || {})
      : {};
    const currentText = clean(job.textContent);
    const updatedMemory = { ...memoryState };
    if (/(?:بشرة\s+دهنية|peau\s+grasse|oily\s+skin)/i.test(currentText)) updatedMemory.skin_type = "oily";
    if (/(?:بشرة\s+(?:جافة|ناشفة)|peau\s+s[eè]che|dry\s+skin)/i.test(currentText)) updatedMemory.skin_type = "dry";
    if (/(?:بشرة\s+حساسة|peau\s+sensible|sensitive\s+skin)/i.test(currentText)) updatedMemory.skin_type = "sensitive";
    if (/(?:حبوب|حب\s+الشباب|boutons|acn[eé]|pimples)/i.test(currentText)) updatedMemory.concern = "acne";
    if (/(?:تصبغات|بقع|taches|pigmentation|dark\s+spots)/i.test(currentText)) updatedMemory.concern = "pigmentation";
    if (/[\u0600-\u06ff]/.test(currentText)) updatedMemory.language = "ar";
    else if (/\b(?:bonjour|salut|je|veux|peau|produit|cr[eè]me)\b/i.test(currentText)) updatedMemory.language = "fr";
    else if (/\b(?:hello|hi|want|skin|product|cream)\b/i.test(currentText)) updatedMemory.language = "en";
    updatedMemory.updated_at = now();
    if (typeof store.saveConversationMemory === "function") {
      await store.saveConversationMemory(job.conversationKey, updatedMemory);
    }

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
      const wantsSunscreen = /(?:واقي\s*(?:شمسي|الشمس)|كريم\s*شمسي|sun\s*cream|suncream|sunscreen|spf|[ée]cran\s+solaire|cr[èe]me\s+solaire|protection\s+solaire)/i.test(latestText);
      const hasNamedProductShape = /(?:\b\d+\s*(?:ml|g|gr|mg)\b|[A-Za-zÀ-ÿ]{4,}\s*[–—-]\s*[A-Za-zÀ-ÿ])/i.test(latestText);
      const brandLikeRequest = /(?:عندكم|كاين|بغيت|عطيني|je\s+veux|je\s+cherche|vous\s+avez|do\s+you\s+have|i\s+want)/i.test(latestText) &&
        /[A-Za-zÀ-ÿ]{4,}/.test(latestText);
      const focusedCatalogQuery = wantsCleanser
        ? `cleanser nettoyant ${latestText}`
        : wantsSunscreen
          ? `sunscreen solaire spf ${latestText}`
          : (hasNamedProductShape || brandLikeRequest)
            ? latestText
            : "";

      const [primaryCatalog, focusedCatalog, products] = await Promise.all([
        searchCatalog(catalogQuery),
        focusedCatalogQuery ? searchCatalog(focusedCatalogQuery, { directRequest: hasNamedProductShape || brandLikeRequest }) : Promise.resolve(null),
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
        memory: updatedMemory,
        customerName: null,
        catalogContext: catalogResult?.context || ""
      });

      // Structured product selection: when the advisor mentions verified catalog
      // products, capture only exact catalog rows. Downstream WhatsApp rendering
      // can use these rows instead of trusting free-form model product facts.
      const mentionedCatalogProducts = catalogProducts.filter(product => {
        const title = clean(product?.title || product?.name);
        return title && clean(assistantMessage).includes(title);
      }).slice(0, 2);
      if (mentionedCatalogProducts.length) {
        updatedMemory.recent_products = mentionedCatalogProducts.map(product => ({
          id: clean(product?.id || product?.catalog_key),
          title: clean(product?.title || product?.name),
          brand: clean(product?.brand),
          category: clean(product?.category),
          usage: clean(product?.usage),
          ai_summary: clean(product?.ai_summary),
          image_url: clean(product?.image_url || product?.image),
          price: product?.price ?? null,
          variant_id: clean(product?.variant_id || product?.shopify_variant_id),
          product_id: clean(product?.product_id || product?.shopify_product_id)
        }));
        updatedMemory.updated_at = now();
        if (typeof store.saveConversationMemory === "function") {
          await store.saveConversationMemory(job.conversationKey, updatedMemory);
        }
        job.verifiedProductSelections = mentionedCatalogProducts.map(product => ({
          id: clean(product?.id || product?.catalog_key),
          title: clean(product?.title || product?.name),
          brand: clean(product?.brand),
          category: clean(product?.category),
          usage: clean(product?.usage),
          ai_summary: clean(product?.ai_summary),
          image_url: clean(product?.image_url || product?.image),
          price: product?.price ?? null,
          variant_id: clean(product?.variant_id || product?.shopify_variant_id),
          product_id: clean(product?.product_id || product?.shopify_product_id)
        }));
      }
    }

    // Direct WhatsApp purchase flow (TEST-SAFE). Recover the most recently
    // verified product card from durable assistant history so button clicks do
    // not depend on the model remembering product state.
    // Resolve natural references to previously verified products across a long
    // conversation ("الثاني", "الأول", "celui-là", "the second one").
    // Only memory-backed verified products are eligible; ambiguity falls back
    // to normal conversation instead of guessing.
    const rememberedProducts = Array.isArray(updatedMemory.recent_products)
      ? updatedMemory.recent_products.filter(p => p && p.title).slice(-3)
      : [];
    const hydrateRememberedProduct = remembered => {
      if (!remembered) return null;
      const exact = (typeof catalogProducts !== "undefined" ? catalogProducts : []).find(product =>
        clean(product?.title || product?.name) === clean(remembered.title)
      );
      return exact || remembered;
    };
    const ordinalText = clean(job.textContent).toLowerCase();
    const ordinalIndex =
      /(?:الثاني|تاني|2(?:nd)?|deuxi[eè]me|second)/i.test(ordinalText) ? 1 :
      /(?:الأول|الاول|1(?:st)?|premier|first)/i.test(ordinalText) ? 0 :
      /(?:الثالث|3(?:rd)?|troisi[eè]me|third)/i.test(ordinalText) ? 2 :
      -1;
    const rememberedOrdinalProduct = ordinalIndex >= 0 && rememberedProducts[ordinalIndex]
      ? hydrateRememberedProduct(rememberedProducts[ordinalIndex])
      : null;
    if (rememberedOrdinalProduct && /(?:بغيت|ناخد|نختار|عطيني|هذا|هاد|celui|prendre|choisis|want|take|choose|الأول|الاول|الثاني|تاني|الثالث|first|second|third|premier|deuxi[eè]me|troisi[eè]me)/i.test(ordinalText)) {
      const title = clean(rememberedOrdinalProduct.title);
      assistantMessage = /[\u0600-\u06ff]/.test(clean(job.textContent))
        ? `أكيد، قصدك **${title}**. نكملو عليه.`
        : /\b(?:je|celui|premier|deuxi[eè]me|troisi[eè]me)\b/i.test(ordinalText)
          ? `D’accord, tu parles de **${title}**. On continue avec celui-ci.`
          : `Got it — you mean **${title}**. We can continue with that one.`;
      job.verifiedProductSelections = [rememberedOrdinalProduct];
    }

    const buttonIntent = clean(job.textContent).toLowerCase();
    const recentAssistantText = (history || []).filter(row => row.role === "assistant").slice(-8).map(row => clean(row.content)).join(" ");
    const recentCatalogProduct = (Array.isArray(job.verifiedProductSelections) ? job.verifiedProductSelections[0] : null)
      || hydrateRememberedProduct(rememberedProducts[0])
      || (typeof catalogProducts !== "undefined" ? catalogProducts : []).find(product => {
        const title = clean(product?.title || product?.name);
        return title && recentAssistantText.includes(title);
      });

    // Navigation/purchase-intent signals never create a live order. The direct
    // channel collects quantity and a delivery location, then stops at a
    // pending-stock handoff until availability is explicitly confirmed.
    const purchaseStateKey = `skinpara:direct-purchase:${job.conversationKey}`;
    const getPurchaseState = async () => {
      if (typeof store.getPurchaseState === "function") return await store.getPurchaseState(job.conversationKey);
      return null;
    };
    const savePurchaseState = async state => {
      if (typeof store.savePurchaseState === "function") return await store.savePurchaseState(job.conversationKey, state);
      return state;
    };
    const purchaseState = await getPurchaseState();
    const quantityMatch = clean(job.textContent).match(/^\s*([1-9]|1\d|20)\s*(?:حبة|وحدة|x)?\s*$/i);

    if (buttonIntent === "skinpara_more_info" || buttonIntent === "voir plus") {
      assistantMessage = recentCatalogProduct?.usage || recentCatalogProduct?.ai_summary
        ? `أكيد. ${clean(recentCatalogProduct.usage || recentCatalogProduct.ai_summary)}`
        : "أكيد. المعلومات الموثقة الإضافية على هاد المنتج ما متوفراش دابا، ونقدر نرجعو للاختيار بلا ما نخمن.";
    } else if (buttonIntent === "skinpara_back_selection" || buttonIntent === "retour") {
      await savePurchaseState(null);
      assistantMessage = "أكيد، نرجعو للاختيار. قول ليا واش بغيتي نشوفو منتج آخر ولا نكملو خطوة أخرى فالروتين.";
    } else if (buttonIntent === "skinpara_buy_now" || buttonIntent === "acheter maintenant") {
      if (!recentCatalogProduct) {
        assistantMessage = "مزيان. قبل ما نكملو الطلب، اختار المنتج من اللائحة باش نأكدوه بلا غلط.";
      } else {
        await savePurchaseState({
          step: "quantity",
          product: {
            id: clean(recentCatalogProduct.id || recentCatalogProduct.catalog_key),
            title: clean(recentCatalogProduct.title || recentCatalogProduct.name),
            variant_id: clean(recentCatalogProduct.variant_id || recentCatalogProduct.shopify_variant_id),
            product_id: clean(recentCatalogProduct.product_id || recentCatalogProduct.shopify_product_id),
            price: recentCatalogProduct.price ?? null
          },
          quantity: null,
          city: null,
          address: null,
          order_mode: "test",
          updated_at: now()
        });
        assistantMessage = `مزيان، أكدنا المنتج: **${clean(recentCatalogProduct.title || recentCatalogProduct.name)}**. شحال من وحدة بغيتي؟ (من 1 حتى 20)`;
      }
    } else if (purchaseState?.step === "quantity" && quantityMatch) {
      const quantity = Number(quantityMatch[1]);
      await savePurchaseState({ ...purchaseState, step: "city", quantity, updated_at: now() });
      assistantMessage = `تمام، الكمية: **${quantity}**. فاش مدينة غادي يكون التوصيل؟`;
    } else if (purchaseState?.step === "city" && clean(job.textContent).length >= 2) {
      const city = clean(job.textContent).slice(0, 100);
      await savePurchaseState({ ...purchaseState, step: "address", city, updated_at: now() });
      assistantMessage = "مزيان. عطيني العنوان أو الحي اللي غادي يكون فيه التوصيل.";
    } else if (purchaseState?.step === "address" && clean(job.textContent).length >= 3) {
      const nextState = { ...purchaseState, step: "pending_stock", address: clean(job.textContent).slice(0, 250), updated_at: now() };
      await savePurchaseState(nextState);
      const priceLine = nextState.product?.price != null && String(nextState.product.price).trim()
        ? `\nالثمن الموثق للوحدة: ${String(nextState.product.price).trim()}`
        : "";
      assistantMessage = `شكراً. ملخص الطلب التجريبي:\n**${nextState.product.title}**\nالكمية: **${nextState.quantity}**\nالمدينة: **${nextState.city}**\nالعنوان: **${nextState.address}**${priceLine}\n\nدابا الطلب باقي **فانتظار تأكيد التوفر**، وما تدار حتى طلب حقيقي.`;
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
      handoffRequired,
      verifiedProducts: Array.isArray(job.verifiedProductSelections) ? job.verifiedProductSelections : []
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
