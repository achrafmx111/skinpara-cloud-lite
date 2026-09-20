const { safeFetch } = require('./safe-fetch.cjs');
const CATALOG_URL =
  String(
    process.env.SKINPARA_CATALOG_URL ||
    "http://skinpara-catalog-service:8792"
  ).replace(/\/+$/, "");


const RAG_ENABLED =
  String(
    process.env.SKINPARA_CATALOG_RAG_ENABLED ||
    "false"
  ).toLowerCase() ===
  "true";


const TIMEOUT_MS =
  Math.max(
    500,
    Number(
      process.env.SKINPARA_CATALOG_TIMEOUT_MS ||
      1800
    )
  );


const MAX_PRODUCTS =
  Math.min(
    5,
    Math.max(
      1,
      Number(
        process.env.SKINPARA_CATALOG_MAX_PRODUCTS ||
        5
      )
    )
  );


function withTimeout(
  ms
) {

  const controller =
    new AbortController();


  const timer =
    setTimeout(
      () =>
        controller.abort(),
      ms
    );


  return {
    controller,
    timer
  };
}


async function fetchJson(
  url
) {

  const {
    controller,
    timer
  } =
    withTimeout(
      TIMEOUT_MS
    );


  try {

      const response = await safeFetch(
          url,
        {
          signal:
            controller.signal,

          headers:{
            Accept:
              "application/json"
          }
        }
      );


    const text =
      await response.text();


    if (!response.ok) {

      throw new Error(
        `catalog_http_${response.status}`
      );
    }


    return text
      ? JSON.parse(text)
      : null;

  }
  finally {

    clearTimeout(
      timer
    );
  }
}


async function catalogHealth() {

  if (!RAG_ENABLED) {

    return {
      enabled:
        false,

      ok:
        false,

      reason:
        "disabled"
    };
  }


  try {

    const data =
      await fetchJson(
        CATALOG_URL +
        "/health"
      );


    return {
      enabled:
        true,

      ok:
        Boolean(
          data?.ok
        ),

      service:
        data?.service ||
        null
    };

  }
  catch(error) {

    return {
      enabled:
        true,

      ok:
        false,

      reason:
        error?.name ===
        "AbortError"
          ? "timeout"
          : error.message
    };
  }
}


function safeText(
  value
) {

  return String(
    value ??
    ""
  ).trim();
}


function isDirectProductRequest(
  text
) {

  const q =
    safeText(
      text
    ).toLowerCase();


  if (!q) {
    return false;
  }


  const directSignals = [

    "bghit",
    "baghi",
    "je veux",
    "je cherche",
    "i want",
    "i need",
    "Ø¹Ø·ÙŠÙ†ÙŠ",
    "Ø¨ØºÙŠØª",
    "Ø¨ØºÙŠØª Ù†Ø´Ø±ÙŠ",
    "prix",
    "price",
    "Ø«Ù…Ù†"
  ];


  return directSignals.some(
    signal =>
      q.includes(
        signal
      )
  );
}




function cleanDirectProductQuery(
  value
) {

  let q =
    safeText(
      value
    );


  const prefixes = [

    /^bghit\s+/i,
    /^baghi\s+/i,
    /^bghina\s+/i,
    /^ Ø¨ØºÙŠØª\s*/i,

    /^je\s+veux\s+/i,
    /^je\s+cherche\s+/i,
    /^je\s+voudrais\s+/i,

    /^i\s+want\s+/i,
    /^i\s+need\s+/i,
    /^looking\s+for\s+/i,

    /^prix\s+(de|du|des)?\s*/i,
    /^price\s+(of)?\s*/i
  ];


  for (
    const pattern
    of prefixes
  ) {

    q =
      q.replace(
        pattern,
        ""
      );
  }


  return q.trim();
}


async function searchCatalog(
  query,
  options={}
) {

  if (!RAG_ENABLED) {

    return {
      enabled:
        false,

      ok:
        false,

      products:
        [],

      fallback:
        true,

      reason:
        "disabled"
    };
  }


  const originalQuery =
    safeText(
      query
    );


  const directRequest =
    options.directRequest ??
    isDirectProductRequest(
      originalQuery
    );


  const q =
    directRequest
      ? cleanDirectProductQuery(
          originalQuery
        )
      : originalQuery;


  if (!q) {

    return {
      enabled:
        true,

      ok:
        true,

      products:
        [],

      fallback:
        false,

      reason:
        "empty_query"
    };
  }





  const params =
    new URLSearchParams();


  params.set(
    "q",
    q
  );


  params.set(
    "limit",
    String(
      MAX_PRODUCTS
    )
  );


  // Proactive recommendation:
  // only products explicitly marked YES.
  //
  // Direct product request:
  // LIMITED remains sellable according to SkinPara rules.
  if (!directRequest) {

    params.set(
      "recommend_only",
      "true"
    );
  }


  try {

    const result =
      await fetchJson(
        CATALOG_URL +
        "/catalog/search?" +
        params.toString()
      );


    const products =
      Array.isArray(
        result?.products
      )
        ? result.products
        : [];


    return {
      enabled:
        true,

      ok:
        true,

      fallback:
        false,

      direct_request:
        directRequest,

      intelligence:
        result?.intelligence ||
        null,

      products
    };

  }
  catch(error) {

    // IMPORTANT:
    // Failure must never break normal AI replies.
    return {
      enabled:
        true,

      ok:
        false,

      fallback:
        true,

      products:
        [],

      reason:
        error?.name ===
        "AbortError"
          ? "timeout"
          : error.message
    };
  }
}


function buildCatalogContext(
  searchResult
) {

  if (
    !searchResult?.ok ||
    !Array.isArray(
      searchResult.products
    ) ||
    searchResult.products.length ===
    0
  ) {

    return "";
  }


  const lines =
    [];


  lines.push(
    "SKINPARA CATALOG INTELLIGENCE:"
  );


  lines.push(
    "Use only the factual catalog data below for recommendation, suitability, usage and safety."
  );


  lines.push(
    "Do not reveal internal supplier, availability-verification or operational workflow information to the customer."
  );


  lines.push(
    "Do not invent ingredients, benefits, medical claims, stock or price."
  );


  lines.push(
    "Price, Shopify variant and final order data must come from Shopify, not this catalog."
  );


  lines.push("");


  searchResult.products
    .slice(
      0,
      MAX_PRODUCTS
    )
    .forEach(
      (
        product,
        index
      ) => {

        lines.push(
          `PRODUCT ${index + 1}`
        );


        lines.push(
          `Title: ${safeText(product.title)}`
        );


        if (
          safeText(
            product.brand
          )
        ) {

          lines.push(
            `Brand: ${safeText(product.brand)}`
          );
        }


        if (
          safeText(
            product.category
          )
        ) {

          lines.push(
            `Category: ${safeText(product.category)}`
          );
        }


        if (
          safeText(
            product.concern
          )
        ) {

          lines.push(
            `Concern: ${safeText(product.concern)}`
          );
        }


        if (
          safeText(
            product.skin_type
          )
        ) {

          lines.push(
            `Suitable for: ${safeText(product.skin_type)}`
          );
        }


        if (
          safeText(
            product.usage
          )
        ) {

          lines.push(
            `Usage: ${safeText(product.usage)}`
          );
        }


        if (
          safeText(
            product.ai_summary
          )
        ) {

          lines.push(
            `Summary: ${safeText(product.ai_summary)}`
          );
        }


        if (
          safeText(
            product.ai_safety
          )
        ) {

          lines.push(
            `Safety: ${safeText(product.ai_safety)}`
          );
        }


        lines.push(
          `Can recommend: ${safeText(product.can_recommend)}`
        );


        lines.push(
          `Can sell: ${safeText(product.can_sell)}`
        );


        lines.push("");
      }
    );


  return lines.join(
    "\n"
  );
}


module.exports = {

  RAG_ENABLED,

  catalogHealth,

  searchCatalog,

  buildCatalogContext,

  isDirectProductRequest
};





