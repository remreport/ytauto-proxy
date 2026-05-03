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

async function fetchReddit(niche, {limit = 10, timeWindow = 'day'} = {}) {
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
    extra: {subreddit: p.subreddit || ''},
  }));
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

// Run all fetchers in parallel; one source failing doesn't blank the rest.
async function fetchAllTrending(niche, {newsApiKey} = {}) {
  const [reddit, hn, news] = await Promise.all([
    fetchReddit(niche).catch((e) => {
      console.warn('Reddit failed:', e.message);
      return [];
    }),
    fetchHN(niche).catch((e) => {
      console.warn('HN failed:', e.message);
      return [];
    }),
    fetchNewsAPI(niche, {apiKey: newsApiKey}).catch((e) => {
      console.warn('NewsAPI failed:', e.message);
      return [];
    }),
  ]);
  return {
    niche,
    fetchedAt: Date.now(),
    reddit,
    hn,
    news,
    counts: {reddit: reddit.length, hn: hn.length, news: news.length},
  };
}

module.exports = {fetchReddit, fetchHN, fetchNewsAPI, fetchAllTrending};
