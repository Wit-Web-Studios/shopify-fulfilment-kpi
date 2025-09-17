import fetch from "node-fetch";

// ---------- CONFIG ----------
const API_VERSION = "2025-07";
const WINDOWS = [30, 60, 90];        // days to compute
const NAMESPACE = "kpi";
// ----------------------------

const SHOP_DOMAIN = process.env.SHOP_DOMAIN;   // e.g. rivetdirect.myshopify.com
const ADMIN_TOKEN = process.env.ADMIN_TOKEN;   // Admin API access token

if (!SHOP_DOMAIN || !ADMIN_TOKEN) {
  console.error("Missing SHOP_DOMAIN or ADMIN_TOKEN.");
  process.exit(1);
}

const API = `https://${SHOP_DOMAIN}/admin/api/${API_VERSION}/graphql.json`;

async function gql(query, variables) {
  const resp = await fetch(API, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": ADMIN_TOKEN
    },
    body: JSON.stringify({ query, variables })
  });
  const json = await resp.json();
  if (!resp.ok || json.errors) throw new Error(`GraphQL error: ${JSON.stringify(json.errors || json)}`);
  return json.data;
}

const qShopId = `query { shop { id } }`;

const qOrders = `
  query OrdersSince($first:Int!, $cursor:String, $query:String!) {
    orders(first:$first, after:$cursor, query:$query, sortKey:CREATED_AT, reverse:true) {
      edges {
        cursor
        node {
          createdAt
          fulfillments { createdAt }
        }
      }
      pageInfo { hasNextPage }
    }
  }
`;

const mSetMetafields = `
  mutation metafieldsSet($metafields:[MetafieldsSetInput!]!) {
    metafieldsSet(metafields:$metafields) {
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

async function computeMedian(windowDays) {
  const sinceISO = new Date(Date.now() - windowDays*24*60*60*1000).toISOString();
  const queryStr = `processed_at:>=${sinceISO}`;
  let cursor = null, hasNext = true;
  const diffs = [];

  while (hasNext) {
    const data = await gql(qOrders, { first: 250, cursor, query: queryStr });
    const edges = data.orders.edges || [];
    hasNext = data.orders.pageInfo?.hasNextPage || false;
    cursor = edges.at(-1)?.cursor || null;

    for (const { node } of edges) {
      const orderAt = new Date(node.createdAt);
      const fTimes = (node.fulfillments || []).map(f => new Date(f.createdAt).getTime());
      if (fTimes.length) {
        const firstFul = new Date(Math.min(...fTimes));
        const hours = (firstFul - orderAt) / 36e5;
        if (hours >= 0 && isFinite(hours)) diffs.push(hours);
      }
    }
    const oldest = edges.at(-1)?.node?.createdAt;
    if (oldest && new Date(oldest) < new Date(sinceISO)) hasNext = false;
  }

  return diffs.length ? +median(diffs).toFixed(2) : null;
}

async function writeMetafield(shopId, key, value) {
  const res = await gql(mSetMetafields, {
    metafields: [{
      ownerId: shopId,
      namespace: NAMESPACE,
      key,
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
    for (const days of WINDOWS) {
      const medianVal = await computeMedian(days);
      if (medianVal == null) {
        console.log(`No data for ${days}d window`);
        continue;
      }
      const key = `fulfillment_median_hours_${days}d`;
      const mf = await writeMetafield(shopId, key, medianVal);
      console.log(`Updated ${days}d median →`, mf.value);
    }
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
