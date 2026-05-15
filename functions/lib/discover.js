// Trending-topic fetchers for the Discover stage. Pure functions —
// nothing here knows about Firestore or auth. Both the test script and
// the eventual Cloud Function consume from this module.
//
// Each fetcher returns an array of normalised topics:
//   {
//     source:    'reddit' | 'hn' | 'news',
//     title:     string,
//     url:       string (canonical link to the post/article),
//     score:     number (upvotes/points/popularity proxy; 0 if N/A),
//     comments:  number (engagement signal; 0 if N/A),
//     summary:   string (truncated to ~300 chars),
//     createdAt: number (unix ms),
//     extra:     {sourceName?, subreddit?, author?, image?}
//   }
//
// Fetchers throw on hard failure; callers should Promise.all().catch
// per source so one outage doesn't blank the whole feed.

const REDDIT_USER_AGENT = 'rem-report/1.0 (https://flourishing-squirrel-92d50e.netlify.app)';

// Curated subreddit lists per niche bucket. Targeted fetches from
// these subreddits return way less off-topic noise than Reddit's
// global search (which keyword-matches across r/relationships,
// r/AmItheAsshole, etc. when a finance term appears once in a post).
//
// Each bucket lists subreddits ordered by signal/noise ratio — earlier
// entries are higher quality. To add a new bucket: add subs here and
// matching keywords to NICHE_KEYWORDS below.
const NICHE_SUBREDDITS = {
  finance: ['personalfinance', 'investing', 'stocks', 'wallstreetbets', 'Economics', 'financialindependence', 'Bogleheads', 'StockMarket'],
  crypto: ['CryptoCurrency', 'Bitcoin', 'ethereum', 'CryptoMarkets', 'CryptoTechnology'],
  tech: ['technology', 'programming', 'webdev', 'sysadmin', 'devops', 'cscareerquestions'],
  ai: ['artificial', 'MachineLearning', 'OpenAI', 'ChatGPT', 'LocalLLaMA', 'singularity', 'ArtificialInteligence'],
  business: ['Entrepreneur', 'smallbusiness', 'startups', 'business', 'sales'],
  news: ['worldnews', 'news', 'UpliftingNews'],
  science: ['science', 'space', 'physics', 'askscience', 'EverythingScience'],
  health: ['Health', 'nutrition', 'fitness', 'loseit', 'medicine', 'wellness'],
  politics: ['politics', 'PoliticalDiscussion', 'NeutralPolitics', 'geopolitics'],
  gaming: ['Games', 'gaming', 'pcgaming', 'truegaming'],
  history: ['history', 'AskHistorians', 'todayilearned'],
  entertainment: ['movies', 'television', 'Music', 'popculturechat'],
};

const NICHE_KEYWORDS = {
  finance: ['finance', 'invest', 'stock', 'market', 'money', 'wall street', 'fed', 'interest rate', 'economy', 'inflation', 'savings', 'budget', 'retirement', 'mortgage', 'tax', 'wealth', 'dollar', 'recession'],
  crypto: ['crypto', 'bitcoin', 'ethereum', 'blockchain', 'nft', 'defi', 'web3'],
  tech: ['tech', 'software', 'coding', 'programming', 'developer', 'startup'],
  ai: ['ai', 'artificial intelligence', 'machine learning', 'llm', 'chatgpt', 'openai', 'gemini', 'claude', 'anthropic'],
  business: ['business', 'entrepreneur', 'sales', 'marketing'],
  news: ['news', 'current events', 'world', 'breaking'],
  science: ['science', 'physics', 'space', 'astronomy', 'biology', 'chemistry'],
  health: ['health', 'nutrition', 'fitness', 'medicine', 'wellness', 'diet', 'workout'],
  politics: ['politics', 'government', 'election', 'congress', 'senate'],
  gaming: ['gaming', 'video game', 'esports', 'gamer'],
  history: ['history', 'historical', 'ancient', 'medieval'],
  entertainment: ['movie', 'film', 'tv show', 'celebrity', 'music', 'pop culture'],
};

function classifyNicheToBuckets(niche) {
  const n = (niche || '').toLowerCase();
  if (!n) return [];
  const buckets = [];
  for (const [bucket, keywords] of Object.entries(NICHE_KEYWORDS)) {
    if (keywords.some((k) => n.includes(k))) buckets.push(bucket);
  }
  return buckets;
}

function getSubredditsForNiche(niche, channelOverride) {
  // Channel override wins — lets users curate their own list per channel
  // when the default niche-classifier picks badly.
  if (Array.isArray(channelOverride) && channelOverride.length) {
    return channelOverride.slice(0, 8);
  }
  const buckets = classifyNicheToBuckets(niche);
  const seen = new Set();
  const out = [];
  for (const b of buckets) {
    for (const s of NICHE_SUBREDDITS[b] || []) {
      if (!seen.has(s)) { seen.add(s); out.push(s); }
    }
  }
  return out.slice(0, 8);
}

// Fetch top posts from a single subreddit. Reddit's /r/X/top.json with
// t=day returns the highest-engagement posts of the last 24 hours —
// curated by the subreddit's audience, which is the right signal for
// niche relevance.
async function fetchSubredditTop(subreddit, {limit = 5, timeWindow = 'day'} = {}) {
  const url = `https://www.reddit.com/r/${encodeURIComponent(subreddit)}/top.json?t=${timeWindow}&limit=${limit}`;
  const resp = await fetch(url, {headers: {'User-Agent': REDDIT_USER_AGENT}});
  if (!resp.ok) {
    throw new Error(`Reddit r/${subreddit} ${resp.status}: ${(await resp.text()).slice(0, 120)}`);
  }
  const data = await resp.json();
  return (data.data?.children || []).map((c) => c.data).map((p) => ({
    source: 'reddit',
    title: p.title || '',
    url: p.permalink ? `https://reddit.com${p.permalink}` : (p.url || ''),
    score: p.score || 0,
    comments: p.num_comments || 0,
    summary: (p.selftext || '').slice(0, 300),
    createdAt: (p.created_utc || 0) * 1000,
    extra: {subreddit: p.subreddit || subreddit},
  }));
}

// Curated-subreddit Reddit fetch. Selects subreddits per niche, fetches
// top-of-day from each in parallel, merges + sorts by score. Falls back
// to the legacy broad search if no curated subreddits matched (e.g. for
// an unusual niche like "dog grooming"). The fallback path is what
// originally pulled in off-topic noise; the new curated path eliminates
// most of it.
async function fetchReddit(niche, {limit = 10, timeWindow = 'day', channelSubreddits, perSubLimit = 5} = {}) {
  const buckets = classifyNicheToBuckets(niche);
  const subreddits = getSubredditsForNiche(niche, channelSubreddits);
  console.log(`[reddit] niche="${niche}" → buckets=[${buckets.join(',')}] → subreddits=[${subreddits.join(',')}]${Array.isArray(channelSubreddits) && channelSubreddits.length ? ' (channel override)' : ''}`);

  if (!subreddits.length) {
    // Fallback: no curated match → broad search across all of Reddit.
    // This is the pre-fix behaviour, kept for niches we haven't bucketed.
    // ⚠ This is the path that surfaced r/Epstein etc. for "finance news"
    // when the broader-fetch was hit by accident. Log loudly so we know.
    console.warn(`[reddit] ⚠ FALLBACK: no curated subreddits for niche "${niche}" — using broad search (likely returns off-topic noise)`);
    const url = `https://www.reddit.com/search.json?q=${encodeURIComponent(niche)}&sort=top&t=${timeWindow}&limit=${limit}`;
    const resp = await fetch(url, {headers: {'User-Agent': REDDIT_USER_AGENT}});
    if (!resp.ok) throw new Error(`Reddit ${resp.status}: ${(await resp.text()).slice(0, 120)}`);
    const data = await resp.json();
    return (data.data?.children || []).map((c) => c.data).map((p) => ({
      source: 'reddit',
      title: p.title || '',
      url: p.permalink ? `https://reddit.com${p.permalink}` : (p.url || ''),
      score: p.score || 0,
      comments: p.num_comments || 0,
      summary: (p.selftext || '').slice(0, 300),
      createdAt: (p.created_utc || 0) * 1000,
      extra: {subreddit: p.subreddit || '', viaFallback: true},
    }));
  }

  // Curated path. One outage in a single subreddit shouldn't blank the
  // rest, so we per-sub catch errors and merge what succeeded.
  const perSub = await Promise.all(
    subreddits.map((sub) =>
      fetchSubredditTop(sub, {limit: perSubLimit, timeWindow}).catch((e) => {
        console.warn(`[reddit] r/${sub} failed: ${e.message}`);
        return [];
      }),
    ),
  );

  // Merge, dedupe by URL, sort by score, cap at limit.
  const seen = new Set();
  const merged = [];
  for (const posts of perSub) {
    for (const p of posts) {
      if (seen.has(p.url)) continue;
      seen.add(p.url);
      merged.push(p);
    }
  }
  merged.sort((a, b) => (b.score || 0) - (a.score || 0));
  const final = merged.slice(0, limit);
  console.log(`[reddit] returned ${final.length} posts from ${subreddits.length} subreddits — top subreddits: ${[...new Set(final.slice(0, 5).map((p) => p.extra?.subreddit))].join(', ')}`);
  return final;
}

async function fetchHN(niche, {limit = 10, minPoints = 10, daysBack = 90} = {}) {
  // Algolia HN Search API — free, no key.
  // Default ordering is by relevance — we layer on a recency filter
  // (created_at_i > N days ago) and a points floor so what comes back
  // is "popular AND recent", not "loosely relevant from 2017".
  const recentTs = Math.floor((Date.now() - daysBack * 86400 * 1000) / 1000);
  const filters = [`points%3E${minPoints}`, `created_at_i%3E${recentTs}`].join('%2C');
  const url = `https://hn.algolia.com/api/v1/search?query=${encodeURIComponent(niche)}&tags=story&numericFilters=${filters}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`HN ${resp.status}`);
  const data = await resp.json();
  return (data.hits || []).slice(0, limit).map((h) => ({
    source: 'hn',
    title: h.title || '',
    url: h.url || `https://news.ycombinator.com/item?id=${h.objectID}`,
    score: h.points || 0,
    comments: h.num_comments || 0,
    summary: (h.story_text || '').slice(0, 300),
    createdAt: (h.created_at_i || 0) * 1000,
    extra: {author: h.author || ''},
  }));
}

async function fetchNewsAPI(niche, {limit = 10, apiKey} = {}) {
  const key = apiKey || process.env.NEWSAPI_KEY;
  if (!key) throw new Error('NEWSAPI_KEY not provided');
  const url = `https://newsapi.org/v2/everything?q=${encodeURIComponent(niche)}&sortBy=popularity&language=en&pageSize=${limit}&apiKey=${key}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`NewsAPI ${resp.status}: ${(await resp.text()).slice(0, 120)}`);
  const data = await resp.json();
  if (data.status !== 'ok') {
    throw new Error(`NewsAPI returned status=${data.status}: ${data.message || ''}`);
  }
  return (data.articles || []).map((a) => ({
    source: 'news',
    title: a.title || '',
    url: a.url || '',
    score: 0, // NewsAPI doesn't expose a popularity score numerically
    comments: 0,
    summary: (a.description || '').slice(0, 300),
    createdAt: a.publishedAt ? new Date(a.publishedAt).getTime() : Date.now(),
    extra: {sourceName: a.source?.name || '', image: a.urlToImage || ''},
  }));
}

// Shared JSON-array extractor for LLM responses that won't reliably
// emit raw JSON despite explicit instructions. Handles:
//   - markdown code fences (```json ... ```, ``` ... ```, even mid-text)
//   - prose before or after the array
//   - language tags inside fences (json, javascript, js)
//   - trailing tokens after a complete array (Grok's "...] 2 fick så")
//   - citation footers Gemini appends like "[{...}] [1] source: ..."
// If the response was cut off before any closing ] reached us this
// throws — bump max-tokens at call site if you see that.
// Recovers a partial JSON array when the response was cut off mid-stream
// (Gemini's MAX_TOKENS, network truncation, etc.). Walks the cleaned
// content forward, tracking [/{ depth while respecting strings, and
// returns the slice up to the last complete object back at array depth.
// Returns null if no complete object was found, the parse fails, or
// input was unparseable. Never throws — caller decides what to do.
function repairTruncatedJsonArray(text) {
  if (typeof text !== 'string') return null;
  const cleaned = text
    .replace(/```(?:json|javascript|js)?\s*/gi, '')
    .replace(/```/g, '')
    .trim();
  const firstBracket = cleaned.indexOf('[');
  if (firstBracket === -1) return null;

  let depth = 0;
  let inString = false;
  let escape = false;
  // Position of the last `}` that returned us to depth 1 (i.e. inside
  // the outer array, between objects). Slicing up to here + appending ']'
  // gives a syntactically complete partial array.
  let lastObjectEnd = -1;
  for (let i = firstBracket; i < cleaned.length; i++) {
    const c = cleaned[i];
    if (escape) { escape = false; continue; }
    if (c === '\\' && inString) { escape = true; continue; }
    if (c === '"') inString = !inString;
    if (inString) continue;
    if (c === '[' || c === '{') depth++;
    else if (c === ']' || c === '}') {
      depth--;
      if (c === '}' && depth === 1) lastObjectEnd = i;
    }
  }
  if (lastObjectEnd === -1) return null;
  try {
    const repaired = cleaned.slice(firstBracket, lastObjectEnd + 1) + ']';
    const parsed = JSON.parse(repaired);
    return Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function extractJsonArray(text) {
  if (typeof text !== 'string' || !text.trim()) {
    throw new Error('extractJsonArray: empty input');
  }
  // Strip every fence opener (with optional language tag) and every
  // fence closer, anywhere in the string. Greedy — handles unclosed
  // fences (truncation case) too.
  const cleaned = text
    .replace(/```(?:json|javascript|js)?\s*/gi, '')
    .replace(/```/g, '')
    .trim();

  const firstBracket = cleaned.indexOf('[');
  if (firstBracket === -1) {
    throw new Error(`No JSON array found in response: ${text.slice(0, 200)}`);
  }

  // Bracket-balanced scan: walk forward from firstBracket tracking
  // [/] depth while respecting strings (so `[1]` inside a string
  // doesn't change depth). The first balanced ] is the array's true
  // close, regardless of trailing prose, citation footers, etc.
  let depth = 0;
  let inString = false;
  let escape = false;
  for (let i = firstBracket; i < cleaned.length; i++) {
    const c = cleaned[i];
    if (escape) { escape = false; continue; }
    if (c === '\\') { escape = true; continue; }
    if (c === '"') { inString = !inString; continue; }
    if (inString) continue;
    if (c === '[') depth++;
    else if (c === ']') {
      depth--;
      if (depth === 0) {
        return JSON.parse(cleaned.slice(firstBracket, i + 1));
      }
    }
  }
  // Reached end of string without depth returning to zero — response
  // was truncated mid-array.
  throw new Error(`Unbalanced array in response (likely truncated): ${text.slice(0, 200)}`);
}

// xAI Grok — Twitter/X-side signal via the Agent Tools API. The legacy
// /v1/chat/completions + search_parameters path was deprecated in
// May 2026 ("Grok 410: Live search is deprecated. Please switch to the
// Agent Tools API"). This version hits the new /v1/responses endpoint
// with built-in x_search + web_search tools, which xAI runs server-side
// and folds into the final assistant message.
//
// Premium source: manual-refresh only (cost ~$0.01-0.05/call), NOT in
// the every-30-min auto-refresh cron. Called from refreshPremiumSources.
//
// Response shape (per docs): {output: [{type:'message', content: [{type:'output_text', text: '...'}]}], ...}
// The output array may contain tool-call items before the final
// message — we always grab the last item of type 'message' with an
// output_text content block.
async function fetchGrok(niche, {limit = 5, apiKey, model = 'grok-4.3'} = {}) {
  if (!apiKey) throw new Error('GROK_API_KEY not provided');

  const prompt = `Find top ${limit} trending Twitter/X posts in the last 24 hours related to: "${niche}".
Focus on financial, technology, business, or scientific content with high engagement (likes, retweets, replies).
Use the x_search and web_search tools to ground your answer in real, current posts from the last 24h.

Return ONLY a JSON array — no prose, no markdown code fences, no commentary before or after.
Schema: [
  {
    "topic": string,           // 5-15 word headline summary of the post's subject
    "summary": string,         // 1-2 sentence summary of what the post says
    "engagement_signal": "high" | "medium" | "low",
    "posted_at": string,       // ISO 8601 datetime
    "url": string              // x.com link to the post if available
  }
]`;

  console.log(`[grok] POST /v1/responses model=${model} niche="${niche}" tools=[x_search,web_search]`);
  const resp = await fetch('https://api.x.ai/v1/responses', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model,
      stream: false,
      input: [{role: 'user', content: prompt}],
      tools: [{type: 'x_search'}, {type: 'web_search'}],
    }),
  });

  if (!resp.ok) {
    const errText = (await resp.text()).slice(0, 400);
    console.error(`[grok] ${resp.status} from /v1/responses (model=${model}): ${errText}`);
    throw new Error(`Grok ${resp.status} (model=${model}): ${errText}`);
  }
  const data = await resp.json();
  console.log(`[grok] response status="${data.status || 'unknown'}" output.length=${(data.output || []).length}`);

  // Find the assistant message in the output array. Tool calls land
  // earlier in the array; the final user-facing text is in the last
  // message-typed item with an output_text content block.
  const messages = (data.output || []).filter((o) =>
    o.type === 'message' || o.role === 'assistant',
  );
  let content = '';
  for (let i = messages.length - 1; i >= 0; i--) {
    const textBlock = (messages[i].content || []).find((c) => c.type === 'output_text');
    if (textBlock?.text) { content = textBlock.text; break; }
  }
  if (!content) {
    throw new Error(`Grok response had no output_text. Output shape: ${JSON.stringify((data.output || []).map((o) => o.type)).slice(0, 200)}`);
  }

  const items = extractJsonArray(content);
  if (!Array.isArray(items)) throw new Error('Grok response was JSON but not an array');

  // engagement_signal → numeric score so this feed sorts alongside vote
  // counts from Reddit/HN. Heuristic only — Claude does the final pick.
  const signalScore = {high: 1000, medium: 400, low: 100};

  return items.slice(0, limit).map((item) => ({
    source: 'grok',
    title: (item.topic || '').toString().slice(0, 200),
    url: (item.url || '').toString(),
    score: signalScore[(item.engagement_signal || '').toLowerCase()] || 0,
    comments: 0,
    summary: (item.summary || '').toString().slice(0, 300),
    createdAt: item.posted_at ? (new Date(item.posted_at).getTime() || Date.now()) : Date.now(),
    extra: {engagementSignal: item.engagement_signal || 'unknown'},
  }));
}

// Google Gemini — YouTube-side signal via Gemini's Google Search
// grounding. Premium source: manual-refresh only, NOT in the
// every-30-min auto-refresh cron. Called from refreshPremiumSources.
//
// Gemini 2.5 Flash with googleSearch tool returns grounded answers
// to "what are the top YouTube videos in {niche}". This is a soft
// signal (Gemini synthesises a likely list from search results, not
// a direct YouTube API query) but cheap and complementary to the
// other sources.
// Public entry — tries gemini-2.5-flash first (faster, cheaper), falls
// back to gemini-2.5-pro on 503/UNAVAILABLE (flash gets overloaded
// regularly during peak hours). Other errors propagate as-is.
async function fetchGemini(niche, opts = {}) {
  const primary = opts.model || 'gemini-2.5-flash';
  try {
    return await fetchGeminiOnce(niche, {...opts, model: primary});
  } catch (e) {
    const msg = e.message || '';
    const is503 = /\b503\b|UNAVAILABLE|overloaded|temporarily unavailable/i.test(msg);
    if (is503 && primary !== 'gemini-2.5-pro') {
      console.warn(`[gemini] ${primary} unavailable (${msg.slice(0, 80)}) — retrying with gemini-2.5-pro`);
      return await fetchGeminiOnce(niche, {...opts, model: 'gemini-2.5-pro'});
    }
    throw e;
  }
}

async function fetchGeminiOnce(niche, {limit = 7, apiKey, model = 'gemini-2.5-flash'} = {}) {
  if (!apiKey) throw new Error('GEMINI_API_KEY not provided');

  // Tight schema after observing truncation even at 8192 tokens. Each
  // item is now ≤80 chars per field and we drop `why_trending` entirely
  // (it doubled the byte cost without much picker value — Claude only
  // needs the topic + view count + channel to rank). Default limit
  // dropped 10→7 for the same reason.
  const prompt = `What are the most-viewed YouTube videos uploaded in the last 7 days about: "${niche}"?
Focus on videos with high engagement (>100k views ideal, mid-tier acceptable). Use Google Search to find real, current YouTube data.

CRITICAL — output format:
- Return ONLY a JSON array, nothing else.
- DO NOT wrap in code fences (no \`\`\`json or \`\`\` markers).
- DO NOT add prose, citations, or commentary before or after the array.
- DO NOT prefix with "Here is..." or any explanatory text.
- The very first character of your response MUST be [ and the last ].
- Keep every string field UNDER 80 CHARACTERS. Truncate aggressively.

Schema: [
  {
    "topic": string,            // ≤80 chars: headline summary
    "channel_name": string,     // ≤40 chars: YouTube channel name
    "view_estimate": number,    // integer view count
    "upload_date": string,      // ISO 8601 datetime
    "url": string               // youtube.com link
  }
]
Limit to top ${limit}.`;

  const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;
  console.log(`[gemini] POST generateContent model=${model} niche="${niche}" limit=${limit} googleSearch=on`);
  const resp = await fetch(url, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      contents: [{parts: [{text: prompt}]}],
      // Google Search grounding — gives Gemini access to current web
      // results, which is the only way to get fresh YouTube data
      // without hitting the YT Data API directly.
      tools: [{googleSearch: {}}],
      generationConfig: {
        temperature: 0.3,
        // 8192 was still occasionally truncating mid-array — Rem hit
        // "Unbalanced array" parse errors on busy niches. Bumped to
        // 16384 (well within gemini-2.5-flash's 65536 ceiling). The
        // tight per-field caps in the prompt keep average response
        // bytes low; the larger ceiling is just slack for rare verbose
        // turns. Cost impact is negligible since output tokens billed
        // per used-token, not per ceiling.
        maxOutputTokens: 16384,
      },
    }),
  });
  if (!resp.ok) {
    const errText = (await resp.text()).slice(0, 400);
    console.error(`[gemini] ${resp.status}: ${errText}`);
    throw new Error(`Gemini ${resp.status}: ${errText}`);
  }
  const data = await resp.json();
  const finishReason = data.candidates?.[0]?.finishReason || 'unknown';
  const content = data.candidates?.[0]?.content?.parts?.[0]?.text || '';
  console.log(`[gemini] finishReason=${finishReason} content.length=${content.length} (first 80 chars: ${content.slice(0, 80).replace(/\n/g, ' ')})`);
  if (!content) {
    // Empty-text responses happen on safety-filter trips, search-grounding
    // failures, and unexplained `finishReason: STOP` events. Used to throw,
    // which made the cron's wrapper store an error and the trending cache
    // lose its gemini field — Rem reported the discovery stage hanging
    // because the picker had no gemini data and the UI showed a red ❌.
    // Return empty array instead. The picker has 3 other sources to draw
    // from and a single gemini blip shouldn't block the pipeline.
    console.warn(`[gemini] no text returned (finishReason=${finishReason}) — returning empty array, picker will use other sources`);
    return [];
  }
  if (finishReason === 'MAX_TOKENS') {
    console.warn(`[gemini] hit MAX_TOKENS — output likely truncated mid-array. Will attempt recovery via bracket-balanced parse.`);
  }

  let items = null;
  try {
    items = extractJsonArray(content);
  } catch (parseErr) {
    // Strict parse failed — usually means the response was truncated
    // mid-object (MAX_TOKENS) or had trailing prose. Attempt repair by
    // slicing up to the last complete object back at array depth, then
    // closing the array. If even that fails, return empty rather than
    // throwing so a single Gemini blip doesn't block the entire picker.
    const msg = parseErr.message || '';
    console.warn(`[gemini] extractJsonArray failed (${msg.slice(0, 120)}) — attempting truncation repair`);
    const repaired = repairTruncatedJsonArray(content);
    if (Array.isArray(repaired) && repaired.length) {
      console.warn(`[gemini] truncation repair recovered ${repaired.length} item(s) — proceeding with partial data`);
      items = repaired;
    } else {
      console.warn(`[gemini] truncation repair returned nothing usable — returning empty array, picker will use other sources`);
      return [];
    }
  }
  if (!Array.isArray(items)) {
    console.warn(`[gemini] parsed content was not a JSON array (first 80 chars: ${content.slice(0, 80)}) — returning empty array`);
    return [];
  }
  console.log(`[gemini] parsed ${items.length} items from response`);

  return items.slice(0, limit).map((item) => ({
    source: 'gemini',
    title: (item.topic || '').toString().slice(0, 120),
    url: (item.url || '').toString(),
    score: parseInt(item.view_estimate, 10) || 0,
    comments: 0,
    summary: (item.topic || '').toString().slice(0, 200),
    createdAt: item.upload_date
      ? (new Date(item.upload_date).getTime() || Date.now())
      : Date.now(),
    extra: {
      channelName: (item.channel_name || '').toString().slice(0, 60),
      // whyTrending kept as a passthrough alias to topic for backwards
      // compatibility with the App.jsx Discover UI which displays it.
      whyTrending: (item.topic || '').toString().slice(0, 120),
    },
  }));
}

// Google Trends — unofficial public dailytrends JSON. The endpoint
// returns a list of "what's trending today" globally for the given geo,
// not niche-filtered, so we do a basic word-overlap filter against the
// niche before returning. Claude ranking in step 6 does the deeper work.
async function fetchGoogleTrends(niche, {limit = 10, geo = 'US'} = {}) {
  const url = `https://trends.google.com/trends/api/dailytrends?hl=en-US&tz=-300&geo=${geo}&ns=15`;
  const resp = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; rem-report/1.0)',
      Accept: 'application/json',
    },
  });
  if (!resp.ok) throw new Error(`Trends ${resp.status}`);
  const text = await resp.text();
  // Google prefixes the JSON with )]}', — strip before parsing.
  const cleaned = text.replace(/^\)\]\}',?\s*/, '');
  const data = JSON.parse(cleaned);
  const days = data.default?.trendingSearchesDays || [];
  const all = days.flatMap((d) => d.trendingSearches || []);

  // Niche-relevance filter: keep items where niche keywords appear in
  // the search query OR any associated article title. Falls back to the
  // raw top-N if nothing matched (Claude will filter later anyway).
  const nicheWords = niche
    .toLowerCase()
    .split(/\s+/)
    .filter((w) => w.length > 2);
  const haystackOf = (s) =>
    `${s.title?.query || ''} ${(s.articles || []).map((a) => a.title || '').join(' ')}`.toLowerCase();
  const matched = all.filter((s) =>
    nicheWords.some((w) => haystackOf(s).includes(w)),
  );
  const items = (matched.length ? matched : all).slice(0, limit);

  return items.map((s) => ({
    source: 'trends',
    title: s.title?.query || '',
    url: `https://trends.google.com/trends/explore?q=${encodeURIComponent(s.title?.query || '')}`,
    score: parseInt(String(s.formattedTraffic || '0').replace(/[^\d]/g, ''), 10) || 0,
    comments: 0,
    summary: (s.articles || [])
      .slice(0, 2)
      .map((a) => a.title || '')
      .join(' · ')
      .slice(0, 300),
    createdAt: Date.now(),
    extra: {traffic: s.formattedTraffic || ''},
  }));
}

// Run free fetchers in parallel; one source failing doesn't blank the rest.
// Reddit + Trends were removed 2026-05-11 — Reddit's anonymous JSON API
// blocks Cloud Function egress IPs (sustained 403/CORS), Trends'
// dailytrends endpoint returned 0 results consistently for our niches.
// The fetchReddit / fetchSubredditTop / fetchGoogleTrends functions
// remain exported in case external use is needed, but the orchestrator
// no longer calls them.
//
// `channelSubreddits` opt + `redditSubredditsUsed` field kept on the
// return shape for backwards compat with existing channels' Firestore
// docs and the Discover UI's expectations.
async function fetchAllTrending(niche, {newsApiKey, channelSubreddits} = {}) {
  const errors = {};
  const wrap = (key, p) => p.catch((e) => {
    const msg = e.message || 'unknown error';
    errors[key] = msg;
    console.warn(`[discover/${key}] failed: ${msg}`);
    return [];
  });
  const [hn, news] = await Promise.all([
    wrap('hn', fetchHN(niche)),
    wrap('news', fetchNewsAPI(niche, {apiKey: newsApiKey})),
  ]);
  console.log(`[discover] fetchAllTrending(${niche}) counts: h${hn.length}/n${news.length} errors: ${JSON.stringify(errors)}`);
  return {
    niche,
    fetchedAt: Date.now(),
    reddit: [],
    hn,
    news,
    trends: [],
    counts: {
      reddit: 0,
      hn: hn.length,
      news: news.length,
      trends: 0,
    },
    errors,
    redditSubredditsUsed: [],
  };
}

module.exports = {
  fetchReddit,
  fetchSubredditTop,
  fetchHN,
  fetchNewsAPI,
  fetchGoogleTrends,
  fetchGrok,
  fetchGemini,
  extractJsonArray,
  fetchAllTrending,
  classifyNicheToBuckets,
  getSubredditsForNiche,
  NICHE_SUBREDDITS,
  NICHE_KEYWORDS,
};
