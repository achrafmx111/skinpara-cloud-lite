const test = require("node:test");
const assert = require("node:assert/strict");
const { createDirectProcessor, isMedicalRisk } = require("../direct-channel.cjs");

function harness({ memory = {}, purchase = null, advisor = "تمام" } = {}) {
  const messages = [];
  const outbound = [];
  let savedMemory = structuredClone(memory);
  let purchaseState = purchase;
  const store = {
    async insertMessage(row) { messages.push(row); return true; },
    async hasEvent() { return false; },
    async getHistory() { return messages.filter(x => x.role === "user" || x.role === "assistant"); },
    async markHandoff() {},
    async getConversationMemory() { return savedMemory; },
    async saveConversationMemory(_key, state) { savedMemory = structuredClone(state); },
    async getPurchaseState() { return purchaseState; },
    async savePurchaseState(_key, state) { purchaseState = state ? structuredClone(state) : null; return purchaseState; }
  };
  const process = createDirectProcessor({
    store,
    enqueueOutbound: async event => outbound.push(event),
    callAdvisor: async () => advisor,
    searchCatalog: async () => ({ enabled:true, ok:true, fallback:false, products:[] }),
    searchProducts: async () => [],
    now: () => "2026-09-22T12:00:00.000Z"
  });
  return { process, outbound, getMemory:()=>savedMemory, getPurchase:()=>purchaseState };
}

const job = text => ({
  channel:"kapso_direct", messageId:"m-"+Math.random(), customerKey:"c1",
  conversationKey:"conv1", textContent:text, receivedAt:"2026-09-22T12:00:00.000Z"
});

test("persists explicit consultation facts without inventing profile data", async () => {
  const h = harness();
  await h.process(job("عندي بشرة دهنية وحساسة وفيها حبوب"));
  const m = h.getMemory();
  assert.equal(m.skin_type, "sensitive");
  assert.equal(m.concern, "acne");
  assert.equal(m.language, "ar");
  assert.equal(m.gender, undefined);
});

test("resolves second remembered verified product", async () => {
  const h = harness({memory:{recent_products:[
    {id:"1",title:"PRODUCT ONE",image_url:"https://example.com/1.jpg"},
    {id:"2",title:"PRODUCT TWO",image_url:"https://example.com/2.jpg"}
  ]}});
  await h.process(job("رجع ليا للثاني"));
  const out = h.outbound.at(-1);
  assert.match(out.content,/PRODUCT TWO/);
  assert.equal(out.verifiedProducts[0].title,"PRODUCT TWO");
});

test("buy action recovers remembered product and stays test-safe", async () => {
  const h = harness({memory:{recent_products:[
    {id:"2",title:"VERIFIED PRODUCT",image_url:"https://example.com/p.jpg"}
  ]}});
  await h.process(job("skinpara_buy_now"));
  assert.equal(h.getPurchase().step,"quantity");
  assert.equal(h.getPurchase().order_mode,"test");
  assert.equal(h.getPurchase().product.title,"VERIFIED PRODUCT");
  assert.match(h.outbound.at(-1).content,/شحال من وحدة/);
});

test("purchase collection stops at pending_stock", async () => {
  const h = harness({purchase:{step:"quantity",product:{title:"VERIFIED PRODUCT"},order_mode:"test"}});
  await h.process(job("2"));
  assert.equal(h.getPurchase().step,"city");
  await h.process(job("الرباط"));
  assert.equal(h.getPurchase().step,"address");
  await h.process(job("أكدال"));
  assert.equal(h.getPurchase().step,"pending_stock");
  assert.equal(h.getPurchase().order_mode,"test");
  assert.match(h.outbound.at(-1).content,/ما تدار حتى طلب حقيقي/);
});

test("medical red flags are detected", () => {
  assert.equal(isMedicalRisk("عندي تورم قوي وضيق تنفس"), true);
});
