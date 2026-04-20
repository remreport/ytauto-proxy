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
      // Save tokens to Firestore on the channel doc
      await db.collection('channels').doc(channelId).update({
        youtubeConnected: true,
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
        youtubeRefreshToken: admin.firestore.FieldValue.delete(),
        youtubeAccessToken: admin.firestore.FieldValue.delete(),
        youtubeTokenExpiry: admin.firestore.FieldValue.delete(),
        youtubeChannel: admin.firestore.FieldValue.delete(),
      });
      return sendJSON(res, 200, { ok: true });
    } catch (e) {
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
