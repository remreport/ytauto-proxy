// One-off: runs the discover fetchers locally for a given niche and
// prints what came back. Used to eyeball topic quality before wiring
// into the Cloud Function + UI.
//
// Usage:
//   node scripts/test-discover.js "finance news"
//
// NEWSAPI_KEY is read from .env if present. Reddit + HN need no key.

const fs = require('fs');
const path = require('path');

// Tiny .env loader so this works without dotenv as a dep.
const envPath = path.join(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const m = /^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)$/.exec(line);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2];
  }
}

const {fetchAllTrending} = require('../functions/lib/discover');

const niche = process.argv[2] || 'finance news';

(async () => {
  console.log(`Fetching trending topics for: "${niche}"\n`);
  const t0 = Date.now();
  const result = await fetchAllTrending(niche);
  const elapsed = ((Date.now() - t0) / 1000).toFixed(1);

  console.log(
    `Done in ${elapsed}s — ${result.counts.reddit} Reddit + ${result.counts.hn} HN + ${result.counts.news} News + ${result.counts.trends} Trends\n`,
  );

  for (const source of ['reddit', 'hn', 'news', 'trends']) {
    const items = result[source];
    if (!items.length) {
      console.log(`── ${source.toUpperCase()} ── (empty)\n`);
      continue;
    }
    console.log(`── ${source.toUpperCase()} (${items.length}) ──`);
    for (const t of items.slice(0, 8)) {
      const age = ageString(t.createdAt);
      const eng = t.score
        ? `${t.score}↑${t.comments ? ' ' + t.comments + '💬' : ''}`
        : t.extra?.sourceName || '';
      const tag = t.extra?.subreddit ? `r/${t.extra.subreddit}` : '';
      console.log(`  • [${age}] ${t.title.slice(0, 90)}`);
      if (tag || eng) console.log(`      ${[tag, eng].filter(Boolean).join('  ·  ')}`);
    }
    console.log();
  }
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

function ageString(ts) {
  const diff = Date.now() - ts;
  const h = Math.floor(diff / 3.6e6);
  if (h < 1) return `${Math.floor(diff / 60000)}m`;
  if (h < 24) return `${h}h`;
  return `${Math.floor(h / 24)}d`;
}
