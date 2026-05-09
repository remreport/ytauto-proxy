// YT Automation proxy — handles Anthropic, Pikzels, and YouTube uploads
const http = require('http');
const https = require('https');
const { URL } = require('url');
const { google } = require('googleapis');
const admin = require('firebase-admin');

const PORT = process.env.PORT || 3001;

// ── Firebase Admin init ─────────────────────────────────────────
let firebaseReady = false;
try {
  const svcRaw = process.env.FIREBASE_SERVICE_ACCOUNT;
  if (svcRaw) {
    const svc = JSON.parse(svcRaw);
    admin.initializeApp({ credential: admin.credential.cert(svc) });
    firebaseReady = true;
    console.log('✓ Firebase Admin initialised');
  } else {
    console.warn('⚠ FIREBASE_SERVICE_ACCOUNT env var missing — YouTube endpoints will fail');
  }
} catch (e) {
  console.error('⚠ Firebase Admin init failed:', e.message);
}
const db = firebaseReady ? admin.firestore() : null;

// ── Google OAuth client factory ─────────────────────────────────
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI;
const APP_URL = process.env.APP_URL || 'https://flourishing-squirrel-92d50e.netlify.app';

function makeOAuthClient() {
  return new google.auth.OAuth2(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REDIRECT_URI);
}

const YT_SCOPES = [
  'https://www.googleapis.com/auth/youtube.upload',
  'https://www.googleapis.com/auth/youtube.readonly',
  // Read-only analytics scope. Required for channels/{cid}/analytics
  // pulls (channel summary, retention curves, traffic sources, etc).
  // Channels connected before this scope existed need to re-OAuth —
  // the dashboard will surface a "re-connect" affordance based on the
  // youtubeAnalyticsConnected flag set on callback below.
  'https://www.googleapis.com/auth/yt-analytics.readonly',
];

// ── Tiny helpers ────────────────────────────────────────────────
function sendJSON(res, status, obj) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
}
function sendText(res, status, text) {
  res.writeHead(status, { 'Content-Type': 'text/plain' });
  res.end(text);
}
function sendHTML(res, status, html) {
  res.writeHead(status, { 'Content-Type': 'text/html' });
  res.end(html);
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', c => { data += c; });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}
function downloadToBuffer(url) {
  return new Promise((resolve, reject) => {
    https.get(url, response => {
      if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
        return downloadToBuffer(response.headers.location).then(resolve).catch(reject);
      }
      if (response.statusCode !== 200) {
        return reject(new Error('Download failed with status ' + response.statusCode));
      }
      const chunks = [];
      response.on('data', c => chunks.push(c));
      response.on('end', () => resolve(Buffer.concat(chunks)));
      response.on('error', reject);
    }).on('error', reject);
  });
}

// ── Request handler ─────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
  const pathname = parsedUrl.pathname;

  // Health check
  if (req.method === 'GET' && pathname === '/') {
    return sendText(res, 200, 'YT Automation proxy is running');
  }

  // ── YouTube: get auth URL ─────────────────────────────────────
  // GET /youtube/auth-url?channelId=XXX
  if (req.method === 'GET' && pathname === '/youtube/auth-url') {
    const channelId = parsedUrl.searchParams.get('channelId');
    if (!channelId) return sendJSON(res, 400, { error: 'channelId required' });
    if (!GOOGLE_CLIENT_ID) return sendJSON(res, 500, { error: 'Google OAuth not configured on proxy' });
    const oauth = makeOAuthClient();
    const url = oauth.generateAuthUrl({
      access_type: 'offline',
      prompt: 'consent',
      scope: YT_SCOPES,
      state: channelId,
    });
    return sendJSON(res, 200, { url });
  }

  // ── YouTube: OAuth callback ───────────────────────────────────
  // Google redirects here after user logs in
  // GET /youtube/callback?code=XXX&state=channelId
  if (req.method === 'GET' && pathname === '/youtube/callback') {
    const code = parsedUrl.searchParams.get('code');
    const channelId = parsedUrl.searchParams.get('state');
    const error = parsedUrl.searchParams.get('error');
    if (error) {
      return sendHTML(res, 400, `<html><body style="font-family:sans-serif;background:#1a1a1a;color:#fff;padding:2rem;text-align:center"><h2>Authorisation cancelled</h2><p>${error}</p><p><a href="${APP_URL}" style="color:#ED6F32">← Back to app</a></p></body></html>`);
    }
    if (!code || !channelId) return sendText(res, 400, 'Missing code or state');
    if (!db) return sendText(res, 500, 'Firebase not configured on proxy');
    try {
      const oauth = makeOAuthClient();
      const { tokens } = await oauth.getToken(code);
      // Fetch the user's YouTube channel info (so we know which YT channel is connected)
      oauth.setCredentials(tokens);
      const yt = google.youtube({ version: 'v3', auth: oauth });
      let ytChannelInfo = null;
      try {
        const channelResp = await yt.channels.list({ part: ['snippet', 'contentDetails'], mine: true });
        const item = (channelResp.data.items || [])[0];
        if (item) {
          ytChannelInfo = {
            id: item.id,
            title: item.snippet?.title || '',
            thumbnail: item.snippet?.thumbnails?.default?.url || '',
          };
        }
      } catch (e) {
        console.error('Could not fetch YT channel info:', e.message);
      }
      // Inspect granted scopes — Google may grant a subset if the user
      // unchecks scopes on the consent screen. youtubeAnalyticsConnected
      // gates the dashboard + analytics refresh; lets us surface a
      // "re-connect for analytics" prompt without confusing UX when
      // upload-only auth is fine for the publish step.
      const grantedScopes = (tokens.scope || '').split(/\s+/);
      const youtubeAnalyticsConnected = grantedScopes.includes(
        'https://www.googleapis.com/auth/yt-analytics.readonly',
      );
      // Save tokens to Firestore on the channel doc
      await db.collection('channels').doc(channelId).update({
        youtubeConnected: true,
        youtubeAnalyticsConnected,
        youtubeGrantedScopes: grantedScopes,
        youtubeRefreshToken: tokens.refresh_token || null,
        youtubeAccessToken: tokens.access_token || null,
        youtubeTokenExpiry: tokens.expiry_date || null,
        youtubeChannel: ytChannelInfo,
        youtubeConnectedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      return sendHTML(res, 200, `<html><body style="font-family:sans-serif;background:#1a1a1a;color:#fff;padding:2rem;text-align:center"><h2 style="color:#2ecc71">✓ YouTube connected!</h2><p>Channel: <strong>${ytChannelInfo?.title || 'Unknown'}</strong></p><p>You can close this tab and return to the app.</p><p><a href="${APP_URL}" style="color:#ED6F32">← Back to app</a></p><script>setTimeout(()=>{window.location.href='${APP_URL}';},2500);</script></body></html>`);
    } catch (e) {
      console.error('OAuth callback error:', e);
      return sendHTML(res, 500, `<html><body style="font-family:sans-serif;background:#1a1a1a;color:#fff;padding:2rem;text-align:center"><h2 style="color:#e74c3c">Something went wrong</h2><p>${e.message}</p><p><a href="${APP_URL}" style="color:#ED6F32">← Back to app</a></p></body></html>`);
    }
  }

  // ── YouTube: disconnect ───────────────────────────────────────
  // POST /youtube/disconnect  body: { channelId }
  if (req.method === 'POST' && pathname === '/youtube/disconnect') {
    if (!db) return sendJSON(res, 500, { error: 'Firebase not configured' });
    try {
      const body = await readBody(req);
      const { channelId } = JSON.parse(body);
      if (!channelId) return sendJSON(res, 400, { error: 'channelId required' });
      await db.collection('channels').doc(channelId).update({
        youtubeConnected: false,
        youtubeAnalyticsConnected: false,
        youtubeRefreshToken: admin.firestore.FieldValue.delete(),
        youtubeAccessToken: admin.firestore.FieldValue.delete(),
        youtubeTokenExpiry: admin.firestore.FieldValue.delete(),
        youtubeGrantedScopes: admin.firestore.FieldValue.delete(),
        youtubeChannel: admin.firestore.FieldValue.delete(),
      });
      return sendJSON(res, 200, { ok: true });
    } catch (e) {
      return sendJSON(res, 500, { error: e.message });
    }
  }

  // ── YouTube: analytics refresh ────────────────────────────────
  // POST /youtube/analytics/refresh  body: { channelId }
  // Pulls 90-day analytics for ONE channel, stores at
  // channels/{cid}/analytics. Cache TTL is 24h — front-end can decide
  // when to call this; this endpoint always re-fetches.
  if (req.method === 'POST' && pathname === '/youtube/analytics/refresh') {
    if (!db) return sendJSON(res, 500, { error: 'Firebase not configured' });
    try {
      const body = await readBody(req);
      const { channelId } = JSON.parse(body);
      if (!channelId) return sendJSON(res, 400, { error: 'channelId required' });

      const snap = await db.collection('channels').doc(channelId).get();
      if (!snap.exists) return sendJSON(res, 404, { error: 'Channel not found' });
      const data = snap.data();
      if (!data.youtubeRefreshToken) return sendJSON(res, 400, { error: 'YouTube not connected for this channel' });
      if (data.youtubeAnalyticsConnected === false) {
        return sendJSON(res, 403, { error: 'Analytics scope not granted — re-connect this channel and approve the analytics scope' });
      }

      const oauth = makeOAuthClient();
      oauth.setCredentials({
        refresh_token: data.youtubeRefreshToken,
        access_token: data.youtubeAccessToken || undefined,
        expiry_date: data.youtubeTokenExpiry || undefined,
      });
      const analytics = google.youtubeAnalytics({ version: 'v2', auth: oauth });
      const yt = google.youtube({ version: 'v3', auth: oauth });

      // 90-day window — most recent full day to avoid partial-day data.
      const today = new Date();
      const endDate = new Date(today.getTime() - 24 * 60 * 60 * 1000);
      const startDate = new Date(endDate.getTime() - 90 * 24 * 60 * 60 * 1000);
      const fmt = (d) => d.toISOString().slice(0, 10);

      // 1) Channel-level summary (daily granularity for trend chart)
      let channelSummary = null;
      try {
        const r = await analytics.reports.query({
          ids: 'channel==MINE',
          startDate: fmt(startDate),
          endDate: fmt(endDate),
          metrics: 'views,estimatedMinutesWatched,averageViewDuration,subscribersGained,likes,comments,shares',
          dimensions: 'day',
        });
        const rows = r.data.rows || [];
        const headers = (r.data.columnHeaders || []).map((h) => h.name);
        const sumIdx = (m) => headers.indexOf(m);
        const totals = {
          totalViews: 0,
          totalWatchTimeMinutes: 0,
          totalSubscribersGained: 0,
          totalLikes: 0,
          totalComments: 0,
          totalShares: 0,
        };
        for (const row of rows) {
          totals.totalViews += row[sumIdx('views')] || 0;
          totals.totalWatchTimeMinutes += row[sumIdx('estimatedMinutesWatched')] || 0;
          totals.totalSubscribersGained += row[sumIdx('subscribersGained')] || 0;
          totals.totalLikes += row[sumIdx('likes')] || 0;
          totals.totalComments += row[sumIdx('comments')] || 0;
          totals.totalShares += row[sumIdx('shares')] || 0;
        }
        const avgViewDur = rows.length
          ? rows.reduce((s, r) => s + (r[sumIdx('averageViewDuration')] || 0), 0) / rows.length
          : 0;
        channelSummary = {
          ...totals,
          avgViewDuration: Math.round(avgViewDur),
          dailyTrend: rows.map((row) => ({
            day: row[sumIdx('day')],
            views: row[sumIdx('views')] || 0,
            watchMinutes: row[sumIdx('estimatedMinutesWatched')] || 0,
            subsGained: row[sumIdx('subscribersGained')] || 0,
          })),
        };
      } catch (e) {
        console.warn('channelSummary failed:', e.message);
      }

      // 2) Top 50 videos by views
      let topVideos = [];
      try {
        const r = await analytics.reports.query({
          ids: 'channel==MINE',
          startDate: fmt(startDate),
          endDate: fmt(endDate),
          metrics: 'views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,subscribersGained',
          dimensions: 'video',
          sort: '-views',
          maxResults: 50,
        });
        const rows = r.data.rows || [];
        const headers = (r.data.columnHeaders || []).map((h) => h.name);
        const idx = (m) => headers.indexOf(m);
        // Resolve video titles in batches of 50
        const videoIds = rows.map((row) => row[idx('video')]).filter(Boolean);
        const titleMap = {};
        if (videoIds.length) {
          const vr = await yt.videos.list({ part: ['snippet'], id: videoIds.slice(0, 50) });
          for (const v of (vr.data.items || [])) {
            titleMap[v.id] = {
              title: v.snippet?.title || '',
              publishedAt: v.snippet?.publishedAt || null,
              thumbnail: v.snippet?.thumbnails?.medium?.url || '',
            };
          }
        }
        topVideos = rows.map((row) => {
          const vid = row[idx('video')];
          return {
            videoId: vid,
            title: titleMap[vid]?.title || '',
            publishedAt: titleMap[vid]?.publishedAt || null,
            thumbnail: titleMap[vid]?.thumbnail || '',
            views: row[idx('views')] || 0,
            watchMinutes: row[idx('estimatedMinutesWatched')] || 0,
            avgViewDuration: row[idx('averageViewDuration')] || 0,
            avgViewPercentage: row[idx('averageViewPercentage')] || 0,
            subsGained: row[idx('subscribersGained')] || 0,
          };
        });
      } catch (e) {
        console.warn('topVideos failed:', e.message);
      }

      // 3) Retention curves for top 20 videos (1 API call per video — costly)
      const retentionCurves = {};
      const top20 = topVideos.slice(0, 20);
      for (const v of top20) {
        try {
          const r = await analytics.reports.query({
            ids: 'channel==MINE',
            startDate: fmt(startDate),
            endDate: fmt(endDate),
            metrics: 'audienceWatchRatio,relativeRetentionPerformance',
            dimensions: 'elapsedVideoTimeRatio',
            filters: `video==${v.videoId}`,
          });
          const rows = r.data.rows || [];
          const curve = rows.map((row) => ({
            x: row[0], // elapsedVideoTimeRatio 0.0-1.0
            retention: row[1], // audienceWatchRatio
            relative: row[2], // relativeRetentionPerformance
          }));
          // Find first significant drop-off
          let dropoffX = null;
          for (let i = 1; i < curve.length; i++) {
            if ((curve[i].retention || 0) < 0.5 && dropoffX === null) {
              dropoffX = curve[i].x;
              break;
            }
          }
          retentionCurves[v.videoId] = {
            title: v.title,
            published: v.publishedAt,
            curve,
            dropoffX,
          };
        } catch (e) {
          console.warn(`retention for ${v.videoId} failed:`, e.message);
        }
      }

      // 4) Demographics (age + gender)
      let demographics = null;
      try {
        const r = await analytics.reports.query({
          ids: 'channel==MINE',
          startDate: fmt(startDate),
          endDate: fmt(endDate),
          metrics: 'viewerPercentage',
          dimensions: 'ageGroup,gender',
        });
        const rows = r.data.rows || [];
        const ageGroups = {};
        const gender = {};
        for (const row of rows) {
          const [age, g, pct] = row;
          ageGroups[age] = (ageGroups[age] || 0) + (pct || 0);
          gender[g] = (gender[g] || 0) + (pct || 0);
        }
        demographics = { ageGroups, gender };
      } catch (e) {
        console.warn('demographics failed:', e.message);
      }

      // 5) Top search terms driving channel views
      let searchTerms = [];
      try {
        const r = await analytics.reports.query({
          ids: 'channel==MINE',
          startDate: fmt(startDate),
          endDate: fmt(endDate),
          metrics: 'views',
          dimensions: 'insightTrafficSourceDetail',
          filters: 'insightTrafficSourceType==YT_SEARCH',
          maxResults: 50,
          sort: '-views',
        });
        const rows = r.data.rows || [];
        const total = rows.reduce((s, r) => s + (r[1] || 0), 0);
        searchTerms = rows.map((row) => ({
          term: row[0],
          views: row[1] || 0,
          percentage: total ? Math.round(((row[1] || 0) / total) * 100 * 10) / 10 : 0,
        }));
      } catch (e) {
        console.warn('searchTerms failed:', e.message);
      }

      // Programmatic insights — non-LLM patterns extracted from the
      // raw data. Phase 3C's full Claude pass should run separately
      // (next session) and replace this with richer recommendations.
      const insights = {
        topPerformingTitlePatterns: topVideos.slice(0, 10).map((v) => v.title).filter(Boolean),
        retentionDropoffPattern: (() => {
          const dropoffs = Object.values(retentionCurves)
            .map((c) => c.dropoffX)
            .filter((x) => typeof x === 'number');
          if (!dropoffs.length) return null;
          const avg = dropoffs.reduce((s, x) => s + x, 0) / dropoffs.length;
          return {
            typicalDropoffPercent: Math.round(avg * 100),
            sampleSize: dropoffs.length,
            severity: avg < 0.3 ? 'severe' : avg < 0.5 ? 'moderate' : 'mild',
          };
        })(),
        audienceProfile: (() => {
          if (!demographics) return null;
          const ages = Object.entries(demographics.ageGroups).sort((a, b) => b[1] - a[1]);
          const genders = Object.entries(demographics.gender).sort((a, b) => b[1] - a[1]);
          if (!ages.length) return null;
          return `${ages[0][0]} ${genders[0]?.[0] || ''} primary (${Math.round(ages[0][1])}% / ${Math.round(genders[0]?.[1] || 0)}%)`.trim();
        })(),
        trendingTopics: searchTerms.slice(0, 10).map((t) => t.term),
      };

      const payload = {
        refreshedAt: admin.firestore.FieldValue.serverTimestamp(),
        daysCovered: 90,
        channelSummary,
        topVideos,
        retentionCurves,
        demographics,
        searchTerms,
        insights,
      };

      // Store under a single doc — cap on retention curves keeps doc <1MB.
      await db.collection('channels').doc(channelId).collection('analytics').doc('summary').set(payload);
      console.log(`Analytics refresh complete for ${channelId}: ${topVideos.length} videos, ${Object.keys(retentionCurves).length} curves`);
      return sendJSON(res, 200, {
        ok: true,
        topVideosCount: topVideos.length,
        retentionCurvesCount: Object.keys(retentionCurves).length,
      });
    } catch (e) {
      console.error('analytics refresh error:', e);
      return sendJSON(res, 500, { error: e.message });
    }
  }

  // ── YouTube: upload video ─────────────────────────────────────
  // POST /youtube/upload
  // body: { channelId, title, description, videoUrl, thumbnailUrl, tags, privacyStatus }
  if (req.method === 'POST' && pathname === '/youtube/upload') {
    if (!db) return sendJSON(res, 500, { error: 'Firebase not configured' });
    try {
      const body = await readBody(req);
      const p = JSON.parse(body);
      const { channelId, title, description, videoUrl, thumbnailUrl, tags, privacyStatus } = p;
      if (!channelId || !videoUrl) return sendJSON(res, 400, { error: 'channelId and videoUrl required' });

      // 1) Load tokens from Firestore
      const snap = await db.collection('channels').doc(channelId).get();
      if (!snap.exists) return sendJSON(res, 404, { error: 'Channel not found' });
      const data = snap.data();
      if (!data.youtubeRefreshToken) return sendJSON(res, 400, { error: 'YouTube not connected for this channel' });

      // 2) Set up OAuth client with refresh token
      const oauth = makeOAuthClient();
      oauth.setCredentials({
        refresh_token: data.youtubeRefreshToken,
        access_token: data.youtubeAccessToken || undefined,
        expiry_date: data.youtubeTokenExpiry || undefined,
      });
      // Refresh access token if needed (the library does this automatically on first call)

      const yt = google.youtube({ version: 'v3', auth: oauth });

      // 3) Download the video from Firebase Storage into memory
      console.log('Downloading video from storage:', videoUrl);
      const videoBuffer = await downloadToBuffer(videoUrl);
      console.log('Video downloaded:', videoBuffer.length, 'bytes');

      // 4) Upload to YouTube as a draft (private by default)
      const { Readable } = require('stream');
      const videoStream = Readable.from(videoBuffer);

      const uploadResp = await yt.videos.insert({
        part: ['snippet', 'status'],
        requestBody: {
          snippet: {
            title: (title || 'Untitled').slice(0, 100),
            description: description || '',
            tags: Array.isArray(tags) ? tags.slice(0, 15) : [],
            categoryId: '22', // "People & Blogs" — safe default
          },
          status: {
            privacyStatus: privacyStatus || 'private', // private = draft; can also be 'unlisted' or 'public'
            selfDeclaredMadeForKids: false,
          },
        },
        media: { body: videoStream },
      });

      const videoId = uploadResp.data.id;
      console.log('YouTube upload complete. videoId:', videoId);

      // 5) Upload thumbnail if provided
      if (thumbnailUrl) {
        try {
          const thumbBuffer = await downloadToBuffer(thumbnailUrl);
          const thumbStream = Readable.from(thumbBuffer);
          await yt.thumbnails.set({
            videoId,
            media: { body: thumbStream, mimeType: 'image/jpeg' },
          });
          console.log('Thumbnail uploaded');
        } catch (thumbErr) {
          console.error('Thumbnail upload failed (non-fatal):', thumbErr.message);
        }
      }

      // 6) Save fresh tokens back (in case they were refreshed)
      const fresh = oauth.credentials;
      if (fresh.access_token) {
        await db.collection('channels').doc(channelId).update({
          youtubeAccessToken: fresh.access_token,
          youtubeTokenExpiry: fresh.expiry_date || null,
        });
      }

      return sendJSON(res, 200, {
        ok: true,
        videoId,
        watchUrl: `https://youtube.com/watch?v=${videoId}`,
        studioUrl: `https://studio.youtube.com/video/${videoId}/edit`,
      });
    } catch (e) {
      console.error('YouTube upload error:', e);
      return sendJSON(res, 500, { error: e.message || 'Upload failed' });
    }
  }

  // ── Everything else requires POST with body ───────────────────
  if (req.method !== 'POST') {
    return sendText(res, 404, 'Not found');
  }

  const body = await readBody(req);
  let parsed;
  try { parsed = JSON.parse(body); }
  catch { return sendText(res, 400, 'Bad JSON'); }

  const apiKey = parsed._apiKey || '';
  delete parsed._apiKey;

  // ── Pikzels endpoint ────────────────────────────────────────
  if (pathname === '/pikzels') {
    console.log('Calling Pikzels API...');
    const payload = JSON.stringify(parsed);
    const options = {
      hostname: 'api.pikzels.com',
      path: '/v2/thumbnail/text',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload),
        'X-Api-Key': apiKey,
      }
    };
    const apiReq = https.request(options, apiRes => {
      let data = '';
      apiRes.on('data', chunk => { data += chunk; });
      apiRes.on('end', () => {
        console.log('Pikzels status:', apiRes.statusCode);
        res.writeHead(apiRes.statusCode, { 'Content-Type': 'application/json' });
        res.end(data);
      });
    });
    apiReq.on('error', err => { res.writeHead(500); res.end(JSON.stringify({ error: err.message })); });
    apiReq.write(payload);
    apiReq.end();
    return;
  }

  // ── Default: Anthropic ──────────────────────────────────────
  console.log('Calling Anthropic API...');
  const payload = JSON.stringify(parsed);
  const options = {
    hostname: 'api.anthropic.com',
    path: '/v1/messages',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(payload),
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
    }
  };
  const apiReq = https.request(options, apiRes => {
    let data = '';
    apiRes.on('data', chunk => { data += chunk; });
    apiRes.on('end', () => {
      console.log('Anthropic status:', apiRes.statusCode);
      res.writeHead(apiRes.statusCode, { 'Content-Type': 'application/json' });
      res.end(data);
    });
  });
  apiReq.on('error', err => { res.writeHead(500); res.end(JSON.stringify({ error: { message: err.message } })); });
  apiReq.write(payload);
  apiReq.end();
});

server.listen(PORT, () => console.log('✓ Proxy running on port ' + PORT));
