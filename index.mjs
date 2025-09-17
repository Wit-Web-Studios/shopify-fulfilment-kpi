import fetch from "node-fetch";

// ---------- CONFIG ----------
const API_VERSION = "2025-07";
const WINDOW_DAYS = 90;
const NAMESPACE  = "kpi";
const KEY        = "fulfillment_median_hours_90d";
// ----------------------------

const SHOP_DOMAIN = process.env.SHOP_DOMAIN;   // e.g. rivetdirect.myshopify.com
const ADMIN_TOKEN = process.env.ADMIN_TOKEN;   // Admin API access token
if (!SHOP_DOMAIN || !ADMIN_TOKEN) {
  console.error("Missing SHOP_DOMAIN or ADMIN_TOKEN.");
  process.exit(1);
}
const API = `https://${SHOP_DOMAIN}/admin/api/${API_VERSION}/graphql.json`;

const gql = async (query, variables) => {
  const resp = await fetch(API, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Shopify-Access-Token": ADMIN_TOKEN },
    body: JSON.stringify({ query, variables })
  });
  const json = await resp.json();
  if (!resp.ok || json.errors) throw new Error(`GraphQL error: ${resp.status} ${JSON.stringify(json.errors || json)}`);
  return json.data;
};

const qShopId = `query { shop { id } }`;
const qOrders = `
  query OrdersSince($first:Int!, $cursor:String, $query:String!) {
    orders(first:$first, after:$cursor, query:$query, sortKey:CREATED_AT, reverse:true) {
      edges {
        cursor
        node {
          processedAt
          fulfillments(first:10) { edges { node { createdAt } } }
        }
      }
      pageInfo { hasNextPage }
    }
  }
`;
const mSet = `
  mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $metafields) {
      metafields { namespace key type value }
      userErrors { field message }
    }
  }
`;

const median = (arr) => {
  if (!arr.length) return null;
  const s = arr.slice().sort((a,b)=>a-b);
  const m = Math.floor(s.length/2);
  return s.length % 2 ? s[m] : (s[m-1] + s[m]) / 2;
};

async function getShopId() {
  const data = await gql(qShopId, {});
  return data.shop.id;
}

async function computeStats() {
  const sinceISO = new Date(Date.now() - WINDOW_DAYS*24*60*60*1000).toISOString();
  const queryStr = `processed_at:>=${sinceISO}`;
  let cursor = null, hasNext = true;
  const diffs = [];
  let ordersCount = 0, fulfilledCount = 0;

  while (hasNext) {
    const data = await gql(qOrders, { first: 250, cursor, query: queryStr });
    const edges = data.orders.edges || [];
    hasNext = data.orders.pageInfo?.hasNextPage || false;
    cursor = edges.at(-1)?.cursor || null;

    for (const { node } of edges) {
      ordersCount++;
      const orderAt = new Date(node.processedAt);
      const fTimes = (node.fulfillments?.edges || []).map(e => new Date(e.node.createdAt).getTime());
      if (fTimes.length) {
        const firstFul = new Date(Math.min(...fTimes));
        const hours = (firstFul - orderAt) / 36e5;
        if (hours >= 0 && isFinite(hours)) { diffs.push(hours); fulfilledCount++; }
      }
    }
    const oldest = edges.at(-1)?.node?.processedAt;
    if (oldest && new Date(oldest) < new Date(sinceISO)) hasNext = false;
  }

  const avg = diffs.length ? diffs.reduce((a,b)=>a+b,0)/diffs.length : null;
  return {
    windowDays: WINDOW_DAYS,
    ordersCount,
    fulfilledCount,
    averageHours: avg==null ? null : +avg.toFixed(1),
    medianHours: diffs.length ? +median(diffs).toFixed(1) : null
  };
}

async function writeMetafield(shopId, value) {
  const res = await gql(mSet, {
    metafields: [{
      ownerId: shopId,
      namespace: NAMESPACE,
      key: KEY,
      type: "number_decimal",
      value: String(value)
    }]
  });
  const errs = res.metafieldsSet.userErrors || [];
  if (errs.length) throw new Error("metafieldsSet errors: " + JSON.stringify(errs));
  return res.metafieldsSet.metafields[0];
}

(async () => {
  try {
    const shopId = await getShopId();
    const stats = await computeStats();
    if (stats.medianHours == null) {
      console.log("No fulfilled orders in window; nothing to write.", stats);
      return;
    }
    const mf = await writeMetafield(shopId, stats.medianHours);
    console.log("Updated metafield:", mf);
    console.log("Stats:", stats);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
