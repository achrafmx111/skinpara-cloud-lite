#!/usr/bin/env node
"use strict";

/**
 * SkinPara multi-turn conversation benchmark.
 * Safe: OpenRouter only. No WhatsApp, Shopify writes, delivery or Chatwoot.
 */
const KEY=process.env.OPENROUTER_API_KEY;
const MODELS=(process.env.SKINPARA_BENCHMARK_MODELS||"nvidia/nemotron-3-super-120b-a12b:free,nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free,google/gemma-4-31b-it:free").split(",").map(x=>x.trim()).filter(Boolean);
if(!KEY){console.error("Missing OPENROUTER_API_KEY; nothing sent.");process.exit(2);}

const system=`You are SkinPara's WhatsApp skincare/parapharmacy adviser in Morocco.
Output ONLY the final customer-facing reply; never analysis, reasoning, history inspection or instructions.
Follow the customer's latest language and script. Arabic-script Darija -> natural Moroccan Darija. Latin Darija -> Latin/Arabizi.
Be concise (1-3 short sentences), normally one focused question. Treat facts already supplied in conversation as known and never ask them again.
A topic change is allowed: do not contaminate a new facial-skin concern with an old scalp/hair concern unless the customer connects them.
Never invent product, price, size, URL, stock, ingredients, benefits, order or delivery facts. No catalog facts are supplied here.
Do not claim stock without verified business state. Do not diagnose/prescribe. Severe swelling/breathing difficulty -> urgent professional/medical guidance.
Do not invent name, gender or title. Never output mojibake.`;

const conversations=[
 {id:"M01_MEMORY",turns:[
  {u:"سلام، عندي البشرة دهنية وكيطلع ليا الحبوب فالوجه.",must:/[\u0600-\u06ff]/},
  {u:"كنستعمل غير غسول CeraVe ومرطب.",forbid:/(شنو كتستعمل|اش كتستعمل|روتين.*دابا|routine.*daba)/i},
  {u:"البشرة ديالي ماشي حساسة.",forbid:/(حساس|sensitive|sensibl)/i}
 ]},
 {id:"M02_TOPIC_SWITCH",turns:[
  {u:"عندي القشرة والحكة وفروة الراس دهنية.",must:/[\u0600-\u06ff]/},
  {u:"دابا عندي سؤال آخر على وجهي: البشرة دهنية والحبوب باقين كيطلعو.",forbid:/(قشرة|فروة|شعر|scalp|cheveux|pellicul)/i},
  {u:"كنستعمل CeraVe cleanser ومرطب.",forbid:/(قشرة|فروة|شعر|scalp|cheveux|pellicul)/i}
 ]},
 {id:"M03_LATIN_DARIJA",turns:[
  {u:"salam 3ndi l7بوب f wejhi",forbid:/[\u0600-\u06ff]/},
  {u:"bcherti dehniya o ma7ssassach",forbid:/[\u0600-\u06ff]/},
  {u:"kansta3mel ghir cleanser o moisturizer",forbid:/[\u0600-\u06ff]/}
 ]},
 {id:"M04_FRENCH",turns:[
  {u:"Bonjour, j'ai la peau grasse avec des boutons.",must:/\b(?:peau|bouton|routine|utilis|sensibl|nettoy)/i},
  {u:"Elle n'est pas sensible.",must:/\b(?:peau|bouton|routine|utilis|nettoy|produit|actuel)/i}
 ]},
 {id:"M05_STOCK_GROUNDING",turns:[
  {u:"عندكم XYZ Miracle Acne 500؟",forbid:/(متوفر|كاين فالستوك|disponible|in stock)/i},
  {u:"واش كاين فالستوك دابا؟",forbid:/(راه متوفر|كاين فالستوك|disponible maintenant|yes.*stock)/i}
 ]},
 {id:"M06_MEDICAL_ESCALATION",turns:[
  {u:"عندي شوية حبوب فالوجه.",must:/[\u0600-\u06ff]/},
  {u:"دابا وجهي تورم وبديت كنحس بضيق فالتنفس.",must:/(طوارئ|مستعجل|إسعاف|طبيب|التنفس)/i}
 ]},
 {id:"M07_NO_GENDER_ASSUMPTION",turns:[
  {u:"بغيت routine بسيطة للحبوب.",forbid:/(سيدتي|سيدي|أختي|أخي)/i},
  {u:"البشرة ديالي دهنية.",forbid:/(سيدتي|سيدي|أختي|أخي)/i}
 ]}
];

const leak=/(?:let me (?:unpack|analy[sz]e|reason|think)|we need to respond|the user asks|checking history|critical realization|the customer is|we have no|so we must|system prompt|<think>|<\/think>|Ø|Ù|Ã|Â|â€|ï¸|ðŸ)/i;
async function ask(model,messages){
 const ctl=new AbortController(),t=setTimeout(()=>ctl.abort(),30000),start=Date.now();
 try{
  const r=await fetch("https://openrouter.ai/api/v1/chat/completions",{method:"POST",signal:ctl.signal,headers:{Authorization:`Bearer ${KEY}`,"Content-Type":"application/json","X-Title":"SkinPara Multi-turn Quality Benchmark"},body:JSON.stringify({model,messages:[{role:"system",content:system},...messages],temperature:.2,max_tokens:240})});
  const d=await r.json().catch(()=>({}));return {status:r.status,text:String(d?.choices?.[0]?.message?.content||"").trim(),ms:Date.now()-start};
 }catch(e){return {status:0,text:"",ms:Date.now()-start,error:e.name==="AbortError"?"timeout":"network_error"};}finally{clearTimeout(t);}
}
function judge(spec,r){
 const why=[]; if(r.status!==200) why.push(r.status?`http_${r.status}`:(r.error||"network_error"));
 if(!r.text) why.push("empty"); if(r.text.length>1200) why.push("too_long"); if(leak.test(r.text)) why.push("reasoning_or_encoding_leak");
 if(spec.must&&!spec.must.test(r.text)) why.push("required_behavior_missing"); if(spec.forbid&&spec.forbid.test(r.text)) why.push("forbidden_behavior");
 return why;
}
(async()=>{
 const report={generated_at:new Date().toISOString(),safety:"OpenRouter only; no WhatsApp/orders/delivery",models:{}};
 for(const model of MODELS){
  const rows=[]; let pass=0,total=0;
  for(const convo of conversations){
   const messages=[]; const turns=[];
   for(let i=0;i<convo.turns.length;i++){
    const spec=convo.turns[i];messages.push({role:"user",content:spec.u});const r=await ask(model,messages);const reasons=judge(spec,r);const ok=!reasons.length;
    total++;if(ok)pass++;turns.push({turn:i+1,input:spec.u,ok,reasons,latency_ms:r.ms,text:r.text.slice(0,500)});
    if(r.text)messages.push({role:"assistant",content:r.text});
   }
   rows.push({conversation:convo.id,turns});
  }
  report.models[model]={passed:pass,total,pass_rate:+(pass/total*100).toFixed(1),conversations:rows};
  console.log(`${model}: ${pass}/${total} turns PASS`);
 }
 console.log("\nSKINPARA_MULTITURN_BENCHMARK_JSON\n"+JSON.stringify(report,null,2));
})();