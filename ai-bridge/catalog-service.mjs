import http from "node:http";
import { safeFetch as fetch } from './safe-fetch.js';


// Embedded catalog listener must use its own loopback port.
// Northflank injects PORT=8787 for the public AI Bridge; reusing that
// value here would collide with server.js in the same container.
const PORT =
  Number(
    process.env.SKINPARA_CATALOG_PORT ||
    8792
  );


const SUPABASE_URL =
  String(
    process.env.SUPABASE_URL ||
    ""
  ).replace(/\/+$/, "");


const SUPABASE_KEY =
  String(
    process.env.SUPABASE_SERVICE_ROLE_KEY ||
    ""
  );


function sendJson(
  res,
  status,
  data
) {

  res.writeHead(
    status,
    {
      "Content-Type":
        "application/json; charset=utf-8",

      "Cache-Control":
        "no-store"
    }
  );


  res.end(
    JSON.stringify(
      data
    )
  );
}


async function db(
  path
) {

  const response =
    await fetch(
      SUPABASE_URL +
      "/rest/v1" +
      path,
      {
        headers:{
          apikey:
            SUPABASE_KEY,

          Authorization:
            `Bearer ${SUPABASE_KEY}`
        }
      }
    );


  const text =
    await response.text();


  if (!response.ok) {

    throw new Error(
      `Supabase ${response.status}: Redacted`
    );
  }


  return text
    ? JSON.parse(text)
    : null;
}


function clean(
  value
) {

  return String(
    value || ""
  ).trim();
}


function safeSearchTerm(
  value
) {

  return clean(value)
    .replace(/[%*,()]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}




// ============================================================
// QUERY INTELLIGENCE V1.2
// ============================================================

function normalizeIntentText(
  value
) {

  return String(
    value || ""
  )
    .toLowerCase()
    .normalize("NFD")
    .replace(
      /[\u0300-\u036f]/g,
      ""
    )
    .replace(
      /[^a-z0-9\u0600-\u06ff]+/g,
      " "
    )
    .replace(
      /\s+/g,
      " "
    )
    .trim();
}


function understandQuery(
  original
) {

  const input =
    normalizeIntentText(
      original
    );


  const intents = [

    {
      canonical:
        "oily",

      phrases:[
        "oily skin",
        "oily",
        "peau grasse",
        "grasse",
        "bchra dhenia",
        "bachra dhenia",
        "بشرة دهنية",
        "البشرة الدهنية"
      ],

      search:[
        "oily",
        "combination skin"
      ]
    },


    {
      canonical:
        "acne",

      phrases:[
        "acne",
        "pimples",
        "blemishes",
        "boutons",
        "anti acne",
        "l7بوب",
        "l7bob",
        "الحبوب",
        "حب الشباب"
      ],

      search:[
        "acne",
        "blemishes"
      ]
    },


    {
      canonical:
        "hair loss",

      phrases:[
        "hair loss",
        "hair fall",
        "chute cheveux",
        "chute de cheveux",
        "anti chute",
        "tas9ot cha3r",
        "tsaqot cha3r",
        "تساقط الشعر"
      ],

      search:[
        "hair loss",
        "anti chute",
        "density"
      ]
    },


    {
      canonical:
        "pigmentation",

      phrases:[
        "pigmentation",
        "dark spots",
        "spots",
        "hyperpigmentation",
        "taches",
        "taches brunes",
        "taches noires",
        "lbo9a3",
        "بقع",
        "تصبغات"
      ],

      search:[
        "pigmentation",
        "dark spots"
      ]
    },


    {
      canonical:
        "sensitive",

      phrases:[
        "sensitive skin",
        "sensitive",
        "peau sensible",
        "bchra hassasa",
        "bachra hassasa",
        "بشرة حساسة",
        "البشرة الحساسة"
      ],

      search:[
        "sensitive"
      ]
    },


    {
      canonical:
        "dry",

      phrases:[
        "dry skin",
        "dry",
        "peau seche",
        "peau sèche",
        "bchra nashfa",
        "bachra nashfa",
        "بشرة جافة",
        "البشرة الجافة"
      ],

      search:[
        "dry"
      ]
    },


    {
      canonical:
        "hydration",

      phrases:[
        "hydration",
        "hydrate",
        "hydrating",
        "moisturizer",
        "hydratant",
        "hydratation",
        "tarطيب",
        "ترطيب"
      ],

      search:[
        "hydration",
        "moisturizer",
        "dry"
      ]
    },


    {
      canonical:
        "sunscreen",

      phrases:[
        "sunscreen",
        "sun cream",
        "suncream",
        "spf",
        "ecran solaire",
        "écran solaire",
        "creme solaire",
        "crème solaire",
        "protection solaire",
        "واقي شمسي",
        "واقي الشمس",
        "كريم شمسي",
        "ضد الشمس"
      ],

      search:[
        "sunscreen",
        "solaire",
        "spf",
        "sun"
      ]
    },


    {
      canonical:
        "cleanser",

      phrases:[
        "cleanser",
        "nettoyant",
        "gel nettoyant",
        "face wash",
        "غسول",
        "منظف"
      ],

      search:[
        "nettoyant",
        "cleanser",
        "gel nettoyant"
      ]
    },


    {
      canonical:
        "redness",

      phrases:[
        "redness",
        "rougeur",
        "rougeurs",
        "ihmirar",
        "احمرار"
      ],

      search:[
        "redness",
        "sensitive"
      ]
    }
  ];


  for (
    const intent
    of intents
  ) {

    for (
      const phrase
      of intent.phrases
    ) {

      if (
        input.includes(
          normalizeIntentText(
            phrase
          )
        )
      ) {

        return {
          original:
            original,

          normalized:
            input,

          intent:
            intent.canonical,

          search_terms:
            intent.search
        };
      }
    }
  }


  const tokens =
    input
      .split(" ")
      .filter(
        token =>
          token.length >= 3
      )
      .slice(
        0,
        4
      );


  return {

    original:
      original,

    normalized:
      input,

    intent:
      null,

    search_terms:
      tokens.length
        ? tokens
        : [input]
  };
}


async function searchCatalog(
  params
) {

  const originalQ =
    safeSearchTerm(
      params.get("q")
    );


  const understood =
    understandQuery(
      originalQ
    );


  const searchTerms =
    understood
      .search_terms
      .filter(Boolean)
      .slice(
        0,
        4
      );


  const brand =
    clean(
      params.get("brand")
    );


  const category =
    clean(
      params.get("category")
    );


  const concern =
    clean(
      params.get("concern")
    );


  const skinType =
    clean(
      params.get("skin_type")
    );


  const recommendOnly =
    clean(
      params.get(
        "recommend_only"
      )
    ) === "true";


  const supplierMatchedOnly =
    clean(
      params.get(
        "supplier_matched"
      )
    ) === "true";


  const limit =
    Math.min(
      20,
      Math.max(
        1,
        Number(
          params.get("limit") ||
          10
        )
      )
    );


  const candidateLimit =
    originalQ
      ? 300
      : Math.max(
          limit,
          50
        );


  const filters = [

    "select=id,catalog_key,title,brand,category,concern,skin_type,usage,ai_summary,search_keywords,upsell,alternative,can_sell,can_recommend,ai_recommendation_level,ai_safety,stock_check_rule,ai_order_rule,reorder_days,human_review,supplier_status,supplier_matched,image_url,shopify_handle,shopify_product_id,shopify_variant_id"

  ];


  filters.push(
    "can_sell=eq.YES"
  );


  if (
    originalQ &&
    searchTerms.length
  ) {

    const clauses =
      [];


    for (
      const term
      of searchTerms
    ) {

      const encoded =
        encodeURIComponent(
          term
        );


      clauses.push(
        "title.ilike.*" +
        encoded +
        "*"
      );

      clauses.push(
        "concern.ilike.*" +
        encoded +
        "*"
      );

      clauses.push(
        "skin_type.ilike.*" +
        encoded +
        "*"
      );

      clauses.push(
        "category.ilike.*" +
        encoded +
        "*"
      );

      clauses.push(
        "search_keywords.ilike.*" +
        encoded +
        "*"
      );

      clauses.push(
        "ai_summary.ilike.*" +
        encoded +
        "*"
      );
    }


    filters.push(
      "or=(" +
      clauses.join(",") +
      ")"
    );
  }


  if (
    brand
  ) {

    filters.push(
      "brand=ilike.*" +
      encodeURIComponent(
        brand
      ) +
      "*"
    );
  }


  if (
    category
  ) {

    filters.push(
      "category=ilike.*" +
      encodeURIComponent(
        category
      ) +
      "*"
    );
  }


  if (
    concern
  ) {

    filters.push(
      "concern=ilike.*" +
      encodeURIComponent(
        concern
      ) +
      "*"
    );
  }


  if (
    skinType
  ) {

    filters.push(
      "skin_type=ilike.*" +
      encodeURIComponent(
        skinType
      ) +
      "*"
    );
  }


  if (
    recommendOnly
  ) {

    filters.push(
      "can_recommend=eq.YES"
    );
  }


  if (
    supplierMatchedOnly
  ) {

    filters.push(
      "supplier_matched=eq.true"
    );
  }


  filters.push(
    "limit=" +
    candidateLimit
  );


  let rows =
    await db(
      "/skinpara_catalog?" +
      filters.join("&")
    );


  function norm(
    value
  ) {

    return normalizeIntentText(
      value
    );
  }


  function containsAny(
    value,
    terms
  ) {

    const target =
      norm(
        value
      );


    return terms.some(
      term =>
        target.includes(
          norm(
            term
          )
        )
    );
  }


  function score(
    product
  ) {

    if (
      !originalQ
    ) {

      return product
        .supplier_matched
          ? 5
          : 0;
    }


    let score =
      0;


    // ========================================================
    // SKINPARA DOMAIN GUARD V1.3
    // ========================================================

    const productCategory =
      norm(
        product.category
      );


    const productTitleForDomain =
      norm(
        product.title
      );


    const productKeywordsForDomain =
      norm(
        product.search_keywords
      );


    const domainBlob =
      [
        productTitleForDomain,
        productCategory,
        productKeywordsForDomain
      ].join(" ");


    const faceIntents =
      new Set([
        "oily",
        "acne",
        "pigmentation",
        "sensitive",
        "dry",
        "hydration",
        "redness",
        "sunscreen",
        "cleanser"
      ]);


    if (
      faceIntents.has(
        understood.intent
      )
    ) {

      // Face skincare should dominate for skin questions.
      if (
        productCategory ===
        "face"
      ) {

        score += 450;
      }


      // Hair / scalp products are not relevant to skin questions.
      if (
        productCategory ===
        "hair" ||
        domainBlob.includes(
          "scalp"
        ) ||
        domainBlob.includes(
          "cheveux"
        ) ||
        domainBlob.includes(
          "shampoing"
        ) ||
        domainBlob.includes(
          "shampoo"
        )
      ) {

        score -= 500;
      }


      // Dental / oral products must never surface for facial skin intent.
      if (
        domainBlob.includes(
          "dentifrice"
        ) ||
        domainBlob.includes(
          "dents"
        ) ||
        domainBlob.includes(
          "toothpaste"
        ) ||
        domainBlob.includes(
          "bain de bouche"
        ) ||
        domainBlob.includes(
          "mouthwash"
        ) ||
        domainBlob.includes(
          "oral care"
        )
      ) {

        score -= 900;
      }
    }


    if (
      understood.intent ===
      "hair loss"
    ) {

      if (
        productCategory ===
        "hair"
      ) {

        score += 500;
      }


      if (
        productCategory ===
        "face" ||
        productCategory ===
        "body"
      ) {

        score -= 400;
      }
    }


    const title =
      norm(
        product.title
      );


    const original =
      norm(
        originalQ
      );


    // Direct product request has highest priority.
    if (
      title === original
    ) {

      score += 1000;

    }
    else if (
      title.startsWith(
        original
      )
    ) {

      score += 600;

    }
    else if (
      title.includes(
        original
      )
    ) {

      score += 350;
    }


    // Concern is the most important recommendation signal.
    if (
      containsAny(
        product.concern,
        searchTerms
      )
    ) {

      score += 300;
    }


    // Then skin type.
    if (
      containsAny(
        product.skin_type,
        searchTerms
      )
    ) {

      score += 250;
    }


    // Then product title.
    if (
      containsAny(
        product.title,
        searchTerms
      )
    ) {

      score += 180;
    }


    if (
      containsAny(
        product.category,
        searchTerms
      )
    ) {

      score += 120;
    }


    if (
      containsAny(
        product.search_keywords,
        searchTerms
      )
    ) {

      score += 90;
    }


    if (
      containsAny(
        product.ai_summary,
        searchTerms
      )
    ) {

      score += 40;
    }


    if (
      product.can_recommend ===
      "YES"
    ) {

      score += 20;
    }


    if (
      product.supplier_matched
    ) {

      score += 5;
    }


    return score;
  }


  rows =
    rows
      .map(
        product => ({
          ...product,

          _score:
            score(
              product
            )
        })
      )
      .sort(
        (a,b) => {

          if (
            b._score !==
            a._score
          ) {

            return (
              b._score -
              a._score
            );
          }


          return String(
            a.title || ""
          ).localeCompare(
            String(
              b.title || ""
            )
          );
        }
      )
      .slice(
        0,
        limit
      );


  const products =
    rows.map(
      ({
        _score,
        ...product
      }) => product
    );


  return {

    ok:
      true,

    query:
      originalQ,

    intelligence:{

      intent:
        understood.intent,

      normalized:
        understood.normalized,

      search_terms:
        searchTerms
    },

    count:
      products.length,

    products
  };
}


async function stats() {

  async function count(
    query
  ) {

    const response =
      await fetch(
        SUPABASE_URL +
        "/rest/v1/skinpara_catalog?" +
        query,
        {
          method:
            "HEAD",

          headers:{
            apikey:
              SUPABASE_KEY,

            Authorization:
              `Bearer ${SUPABASE_KEY}`,

            Prefer:
              "count=exact"
          }
        }
      );


    if (!response.ok) {

      throw new Error(
        "Stats query failed"
      );
    }


    const range =
      response.headers.get(
        "content-range"
      ) || "0/0";


    return Number(
      range.split("/")[1] ||
      0
    );
  }


  const [
    total,
    sellable,
    recommendable,
    limited,
    matched
  ] =
    await Promise.all([

      count(
        "select=id"
      ),

      count(
        "select=id&can_sell=eq.YES"
      ),

      count(
        "select=id&can_recommend=eq.YES"
      ),

      count(
        "select=id&can_recommend=eq.LIMITED"
      ),

      count(
        "select=id&supplier_matched=eq.true"
      )

    ]);


  return {
    ok:
      true,

    total,

    can_sell:
      sellable,

    can_recommend_yes:
      recommendable,

    can_recommend_limited:
      limited,

    supplier_matched:
      matched,

    supplier_pending:
      Math.max(
        0,
        total -
        matched
      )
  };
}


async function handler(
  req,
  res
) {

  const url =
    new URL(
      req.url,
      `http://${req.headers.host}`
    );


  try {


    if (
      req.method === "GET" &&
      url.pathname === "/health"
    ) {

      return sendJson(
        res,
        200,
        {
          ok:
            true,

          service:
            "skinpara-catalog-service"
        }
      );
    }


    if (
      req.method === "GET" &&
      url.pathname === "/catalog/stats"
    ) {

      return sendJson(
        res,
        200,
        await stats()
      );
    }


    if (
      req.method === "GET" &&
      url.pathname === "/catalog/search"
    ) {

      return sendJson(
        res,
        200,
        await searchCatalog(
          url.searchParams
        )
      );
    }


    if (
      req.method === "GET" &&
      url.pathname.startsWith(
        "/catalog/product/"
      )
    ) {

      const id =
        Number(
          url.pathname
            .split("/")
            .pop()
        );


      if (!id) {

        return sendJson(
          res,
          400,
          {
            ok:
              false,

            error:
              "invalid_product_id"
          }
        );
      }


      const rows =
        await db(
          `/skinpara_catalog?id=eq.${id}&select=*`
        );


      return sendJson(
        res,
        rows.length
          ? 200
          : 404,
        rows.length
          ? {
              ok:
                true,

              product:
                rows[0]
            }
          : {
              ok:
                false,

              error:
                "product_not_found"
            }
      );
    }


    return sendJson(
      res,
      404,
      {
        ok:
          false,

        error:
          "not_found"
      }
    );


  } catch(error) {

    console.error(
      error
    );


    return sendJson(
      res,
      500,
      {
        ok:
          false,

        error:
          error.message
      }
    );
  }
}


http
  .createServer(
    handler
  )
  .listen(
    PORT,
    "0.0.0.0",
    () => {

      console.log(
        JSON.stringify({
          event:
            "catalog_service_started",

          port:
            PORT
        })
      );
    }
  );
