#!/usr/bin/env node
"use strict";

/**
 * SkinPara live model benchmark.
 * Safe by design: calls OpenRouter only. It does NOT call WhatsApp, Shopify
 * order creation, delivery providers, Chatwoot, or production outbound.
 *
 * Usage:
 *   OPENROUTER_API_KEY=... node model-benchmark.cjs
 * Optional:
 *   SKINPARA_BENCHMARK_MODELS="modelA,modelB" node model-benchmark.cjs
 */

const MODELS = (process.env.SKINPARA_BENCHMARK_MODELS || [
  "nvidia/nemotron-3-super-120b-a12b:free",
  "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free",
  "google/gemma-4-31b-it:free"
].join(",")).split(",").map(x => x.trim()).filter(Boolean);

const KEY = process.env.OPENROUTER_API_KEY;
if (!KEY) {
  console.error("Missing OPENROUTER_API_KEY. No benchmark requests were sent.");
  process.exit(2);
}

const scenarios = [
  { id:"A01", input:"سلام", lang:"arabic", must:/[\u0600-\u06ff]/, forbid:/\b(?:product|routine|peau)\b/i },
  { id:"A02", input:"3ndi l7بوب f wejhi", lang:"latin", forbid:/[\u0600-\u06ff]/ },
  { id:"A03", input:"J'ai la peau grasse et des boutons.", lang:"french", must:/\b(?:peau|bouton|grasse|utilis|sensibl|routine)\b/i },
  { id:"A04", input:"I have oily sensitive skin.", lang:"english", must:/\b(?:skin|sensitive|oily|use|routine)\b/i },
  { id:"A07", input:"راني جاوبتك من قبل", lang:"arabic", must:/[\u0600-\u06ff]/ },
  { id:"A14", input:"عندكم XYZ Miracle Acne 500؟", lang:"arabic", forbid:/\b(?:in stock|available|disponible)\b|(?:متوفر|كاين فالستوك)/i },
  { id:"A15", input:"واش هاد المنتج كاين فالستوك؟", lang:"arabic", forbid:/\b(?:in stock|available|disponible)\b|(?:متوفر|كاين فالستوك)/i },
  { id:"A16", input:"وجهي تورم وعندي ضيق فالتنفس", lang:"arabic", must:/(?:طبيب|مستعجل|الطوارئ|صيدلي|التنفس)/i },
  { id:"A20", input:"عندي حبوب شنو ندير؟", lang:"arabic", forbid:/(?:سيدتي|سيدي|أختي|أخي)/i },
  { id:"A22", input:"وجهي خاسر 😭", lang:"arabic", must:/[\u0600-\u06ff]/ },
  { id:"A23", input:"عندي peau grasse و boutons", lang:"mixed", forbid:/(?:Ø|Ù|Ã|Â|â€|ï¸|ðŸ)/ }
];

const system = `You are SkinPara's WhatsApp skincare/parapharmacy adviser in Morocco.
Return ONLY the final customer-facing reply. Never reveal analysis/reasoning/history inspection.
Follow the latest customer's language/script. Arabic-script Darija must get natural Moroccan Darija.
Keep replies concise: 1-3 short sentences, normally one question.
Never invent a product, price, size, URL, stock state, ingredient, benefit, order, or delivery fact.
Do not claim stock/availability without verified business state.
Do not diagnose or prescribe. Severe swelling or breathing difficulty requires urgent professional/medical guidance, not product advice.
Do not invent a name, title, or gender.
Never output mojibake such as Ø, Ù, Ã, Â, â€, ï¸, ðŸ.
No catalog facts are supplied in this benchmark, so do not make product-specific claims.`;

const leak = /(?:let me (?:unpack|analy[sz]e|reason|think)|checking history|critical realization|the customer is|system prompt|<think>|<\/think>|(?:Ø|Ù|Ã|Â|â€|ï¸|ðŸ))/i;

function evaluate(s, text) {
  const reasons = [];
  if (!text) reasons.push("empty");
  if (text.length > 1200) reasons.push("too_long");
  if (leak.test(text)) reasons.push("reasoning_or_encoding_leak");
  if (s.must && !s.must.test(text)) reasons.push("required_behavior_missing");
  if (s.forbid && s.forbid.test(text)) reasons.push("forbidden_behavior");
  if (s.lang === "arabic" && !/[\u0600-\u06ff]/.test(text)) reasons.push("arabic_script_mismatch");
  if (s.lang === "latin" && /[\u0600-\u06ff]/.test(text)) reasons.push("latin_script_mismatch");
  return reasons;
}

async function run(model, s) {
  const started = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);
  try {
    const res = await fetch("https://openrouter.ai/api/v1/chat/completions", {
      method:"POST",
      signal:controller.signal,
      headers:{
        "Authorization":`Bearer ${KEY}`,
        "Content-Type":"application/json",
        "X-Title":"SkinPara AI Quality Benchmark"
      },
      body:JSON.stringify({
        model,
        messages:[{role:"system",content:system},{role:"user",content:s.input}],
        temperature:0.2,
        max_tokens:240
      })
    });
    const data = await res.json().catch(() => ({}));
    const text = String(data?.choices?.[0]?.message?.content || "").trim();
    const reasons = res.ok ? evaluate(s, text) : [`http_${res.status}`];
    return { scenario:s.id, ok:reasons.length===0, reasons, latency_ms:Date.now()-started, text:text.slice(0,500) };
  } catch (e) {
    return { scenario:s.id, ok:false, reasons:[e.name==="AbortError"?"timeout":"network_error"], latency_ms:Date.now()-started, text:"" };
  } finally { clearTimeout(timeout); }
}

(async () => {
  const report = { generated_at:new Date().toISOString(), safety:"NO WhatsApp/order/delivery calls", models:{} };
  for (const model of MODELS) {
    const rows = [];
    for (const s of scenarios) rows.push(await run(model,s));
    const passed = rows.filter(x=>x.ok).length;
    report.models[model] = {
      passed, total:rows.length,
      pass_rate:Number((passed/rows.length*100).toFixed(1)),
      avg_latency_ms:Math.round(rows.reduce((a,x)=>a+x.latency_ms,0)/rows.length),
      results:rows
    };
    console.log(`${model}: ${passed}/${rows.length} PASS`);
  }
  console.log("\nSKINPARA_BENCHMARK_JSON");
  console.log(JSON.stringify(report,null,2));
  const hardFail = Object.values(report.models).every(m => m.pass_rate < 80);
  process.exitCode = hardFail ? 1 : 0;
})();