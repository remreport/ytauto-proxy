// YT Automation proxy — handles Anthropic, Pikzels, and YouTube uploads
const http = require('http');
const https = require('https');
const { URL } = require('url');
const path = require('path');
const fs = require('fs');
const fsp = require('fs/promises');
const os = require('os');
const { spawn } = require('child_process');
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
  // Captions API scope. Needed to fetch caption tracks (transcripts)
  // from the user's own videos for the forensic per-video analyzer.
  // youtube.readonly does NOT cover captions.download — youtube.force-ssl
  // is the documented scope. Channels connected before this scope was
  // added need to re-OAuth (youtubeCaptionsConnected flag below).
  'https://www.googleapis.com/auth/youtube.force-ssl',
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

// ── Forensic helpers (used by /youtube/analytics/forensic) ──────
// Convert YouTube ISO 8601 duration ("PT1H23M45S") to seconds.
function parseISO8601Duration(s) {
  const m = (s || '').match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
  if (!m) return 0;
  return (+m[1] || 0) * 3600 + (+m[2] || 0) * 60 + (+m[3] || 0);
}

// Parse SRT or VTT text into [{start, end, text}]. Tolerates both
// comma + period decimal separators, BOM, CRLF, and inline HTML
// styling tags YouTube sometimes emits in auto-captions.
function parseSRT(text) {
  if (!text || typeof text !== 'string') return [];
  const out = [];
  const t = text.replace(/^﻿/, '').replace(/\r\n/g, '\n');
  const blocks = t.split(/\n\n+/);
  for (const block of blocks) {
    if (/^WEBVTT/.test(block)) continue;
    const lines = block.split('\n').filter(Boolean);
    const tsLine = lines.find(L => L.includes('-->'));
    if (!tsLine) continue;
    const m = tsLine.match(/(\d{1,2}):(\d{2}):(\d{2})[.,](\d{1,3})\s*-->\s*(\d{1,2}):(\d{2}):(\d{2})[.,](\d{1,3})/);
    if (!m) continue;
    const start = (+m[1]) * 3600 + (+m[2]) * 60 + (+m[3]) + (+m[4]) / 1000;
    const end   = (+m[5]) * 3600 + (+m[6]) * 60 + (+m[7]) + (+m[8]) / 1000;
    const textLines = lines.slice(lines.indexOf(tsLine) + 1).join(' ').replace(/<[^>]+>/g, '').trim();
    if (textLines) out.push({ start: +start.toFixed(2), end: +end.toFixed(2), text: textLines });
  }
  return out;
}

// Identify retention events on a 100-point curve. Thresholds match
// the spec: spike = +1.5% over 1% interval, dip = -2.5% over 1%,
// cliff = -5% over 2%, sustained = continuous ≥10% region where
// retention stays above 75% of curve peak.
function analyzeRetentionCurve(curve) {
  if (!Array.isArray(curve) || curve.length < 5) return { spikes: [], dips: [], cliffs: [], sustained: [], peak: 0 };
  const sorted = curve
    .filter(p => typeof p?.x === 'number' && typeof p?.retention === 'number')
    .slice()
    .sort((a, b) => a.x - b.x);
  const peak = Math.max(...sorted.map(p => p.retention));
  const spikes = [];
  const dips = [];
  const cliffs = [];
  // 1% interval events
  for (let i = 1; i < sorted.length; i++) {
    const prev = sorted[i - 1];
    const curr = sorted[i];
    const delta = curr.retention - prev.retention;
    const dx = curr.x - prev.x;
    if (dx <= 0.012) {
      if (delta >= 0.015) spikes.push({ x: +curr.x.toFixed(4), delta: +delta.toFixed(4), retentionAtPoint: +curr.retention.toFixed(4) });
      else if (delta <= -0.025) dips.push({ x: +curr.x.toFixed(4), delta: +delta.toFixed(4), retentionAtPoint: +curr.retention.toFixed(4) });
    }
  }
  // 2% interval cliffs
  for (let i = 2; i < sorted.length; i++) {
    const prev = sorted[i - 2];
    const curr = sorted[i];
    const delta = curr.retention - prev.retention;
    const dx = curr.x - prev.x;
    if (dx <= 0.025 && delta <= -0.05) {
      cliffs.push({ x: +curr.x.toFixed(4), delta: +delta.toFixed(4), retentionAtPoint: +curr.retention.toFixed(4) });
    }
  }
  // Sustained high zones
  const threshold = peak * 0.75;
  const sustained = [];
  let zoneStart = null, zoneSum = 0, zoneCount = 0;
  for (let i = 0; i < sorted.length; i++) {
    const r = sorted[i].retention;
    if (r >= threshold) {
      if (zoneStart === null) zoneStart = sorted[i].x;
      zoneSum += r;
      zoneCount++;
    } else if (zoneStart !== null) {
      const zoneEnd = sorted[i - 1].x;
      if (zoneEnd - zoneStart >= 0.10) {
        sustained.push({ startX: +zoneStart.toFixed(4), endX: +zoneEnd.toFixed(4), avgRetention: +(zoneSum / zoneCount).toFixed(4) });
      }
      zoneStart = null; zoneSum = 0; zoneCount = 0;
    }
  }
  if (zoneStart !== null) {
    const zoneEnd = sorted[sorted.length - 1].x;
    if (zoneEnd - zoneStart >= 0.10) {
      sustained.push({ startX: +zoneStart.toFixed(4), endX: +zoneEnd.toFixed(4), avgRetention: +(zoneSum / zoneCount).toFixed(4) });
    }
  }
  return { spikes, dips, cliffs, sustained, peak: +peak.toFixed(4) };
}

// Given a transcript and a target time in seconds, return the
// 5-10 spoken words around that moment (window: t-1s..t+3s).
function spokenWordsAt(transcript, tSec) {
  const window = (transcript || []).filter(seg => seg.end >= tSec - 1 && seg.start <= tSec + 3);
  return window.map(s => s.text).join(' ').replace(/\s+/g, ' ').trim().slice(0, 240);
}

// Resolve the path to the yt-dlp binary. On Render this lives next
// to server.js (downloaded by buildCommand in render.yaml). Local
// dev: try YT_DLP_PATH env var, then PATH lookup. Returns null if
// no binary found — forensic endpoint then degrades gracefully and
// reports captionsAvailable=false instead of crashing.
function resolveYtDlpPath() {
  if (process.env.YT_DLP_PATH && fs.existsSync(process.env.YT_DLP_PATH)) {
    return process.env.YT_DLP_PATH;
  }
  const local = path.resolve(__dirname, process.platform === 'win32' ? 'yt-dlp.exe' : 'yt-dlp');
  if (fs.existsSync(local)) return local;
  // PATH fallback (dev): on Linux/macOS `yt-dlp` is enough; on Windows
  // it'd need .exe — child_process.spawn with shell:true would resolve
  // either, but we prefer explicit paths so production behavior is
  // predictable. Returning null here triggers a clear server error.
  return null;
}

// Fetch caption track for a YouTube video via yt-dlp (works without
// Content-ID verification — the YouTube Data API's captions.list
// silently returns empty for non-verified apps even on the user's
// own videos, hence the workaround). Returns parsed transcript
// [{start, end, text}]. Empty array if no captions exist OR if
// yt-dlp fails — caller should treat empty as "no captions".
//
// Strategy: --skip-download with --write-sub + --write-auto-sub so
// we get manual captions if present, auto-generated otherwise. We
// constrain --sub-lang to en/en-* and --sub-format to vtt because
// the parseSRT helper handles VTT cleanly. Output goes to a temp
// file we clean up after parsing. yt-dlp writes the actual file
// path to stdout (json line via --print after_move:filepath); we
// avoid that complexity and instead read every .vtt file in our
// per-call temp dir.
function fetchCaptionsViaYtdlp(videoId, ytDlpPath, timeoutMs = 30000) {
  return new Promise(async (resolve) => {
    if (!ytDlpPath) return resolve({ transcript: [], source: null, error: 'yt-dlp binary not found on host' });
    let workDir = null;
    let timer = null;
    let proc = null;
    try {
      workDir = await fsp.mkdtemp(path.join(os.tmpdir(), `ytforensic-${videoId}-`));
      const url = `https://www.youtube.com/watch?v=${videoId}`;
      const args = [
        '--skip-download',
        '--write-sub',
        '--write-auto-sub',
        '--sub-lang', 'en.*,en',
        '--sub-format', 'vtt',
        '--no-warnings',
        '--no-playlist',
        '--no-progress',
        '--socket-timeout', '15',
        '--retries', '2',
        '-o', path.join(workDir, '%(id)s.%(ext)s'),
        url,
      ];
      proc = spawn(ytDlpPath, args, { stdio: ['ignore', 'pipe', 'pipe'] });
      let stderr = '';
      proc.stderr.on('data', (c) => { stderr += c.toString(); });
      const finished = new Promise((r) => proc.on('close', r));
      timer = setTimeout(() => {
        try { proc.kill('SIGKILL'); } catch {}
      }, timeoutMs);
      const code = await finished;
      clearTimeout(timer);
      if (code !== 0) {
        // Non-zero often just means "no subs available" — yt-dlp
        // doesn't always exit 0 in that case. Fall through and
        // check the temp dir; empty result is the fallback.
        if (stderr) console.warn(`[yt-dlp] ${videoId} exit=${code}: ${stderr.split('\n')[0].slice(0, 160)}`);
      }
      const files = await fsp.readdir(workDir);
      const vtt = files.find(f => f.endsWith('.vtt'));
      if (!vtt) {
        return resolve({ transcript: [], source: null, error: code !== 0 ? 'yt-dlp failed and no subs file produced' : 'no captions available' });
      }
      const raw = await fsp.readFile(path.join(workDir, vtt), 'utf-8');
      const transcript = parseSRT(raw);
      // Detect manual vs auto from the filename suffix yt-dlp writes:
      // user-uploaded:    {id}.en.vtt  /  {id}.en-US.vtt
      // auto-generated:   {id}.en.vtt  too — but yt-dlp tags it via
      // language code list. We can't easily tell from filename alone.
      // Just record "yt-dlp" as source for now.
      return resolve({ transcript, source: 'yt-dlp', error: null });
    } catch (e) {
      if (timer) clearTimeout(timer);
      return resolve({ transcript: [], source: null, error: e.message });
    } finally {
      if (workDir) {
        fsp.rm(workDir, { recursive: true, force: true }).catch(() => {});
      }
    }
  });
}

const YT_DLP_PATH = resolveYtDlpPath();
console.log(YT_DLP_PATH ? `✓ yt-dlp binary at ${YT_DLP_PATH}` : '⚠ yt-dlp binary not found — forensic captions will be unavailable');

// Phase C — Claude pattern pass over forensic data. Uses the user's
// own Anthropic key (passed in the request body as _apiKey) so we
// don't burn a shared key. Server-side rather than client-side
// because the per-video data can be hundreds of KB and we already
// have it in memory after the forensic loop.
function claudePatternPass(perVideo, apiKey) {
  return new Promise((resolve, reject) => {
    if (!apiKey) return reject(new Error('Anthropic API key required for pattern pass'));
    const compact = Object.entries(perVideo)
      .filter(([, v]) => !v.error && (v.captionsAvailable || (v.bestSections || []).length))
      .slice(0, 10)
      .map(([, v]) => ({
        title: v.title,
        views: v.views,
        avgViewPercentage: v.avgViewPercentage,
        durationSec: v.durationSec,
        firstHookText: (v.transcript || []).filter(t => t.start < 30).map(t => t.text).join(' ').slice(0, 400),
        spikes: (v.spikeMoments || []).slice(0, 6).map(s => ({ x: +s.x.toFixed(2), gain: s.retentionGain, words: s.spokenWords })),
        dips: (v.dipMoments || []).slice(0, 6).map(d => ({ x: +d.x.toFixed(2), loss: d.retentionLoss, words: d.spokenWords })),
        cliffs: (v.cliffDrops || []).slice(0, 3).map(c => ({ x: +c.x.toFixed(2), severity: c.severity, words: c.spokenWords })),
        bestSections: (v.bestSections || []).slice(0, 3).map(b => ({ start: +b.startX.toFixed(2), end: +b.endX.toFixed(2), avg: b.avgRetention, snippet: b.transcriptSnippet })),
      }));
    if (compact.length === 0) return reject(new Error('No videos with usable forensic data'));
    const prompt = `Analyse these ${compact.length} top videos' retention data + transcripts. Each video has:
- firstHookText: the first 30s of the video's transcript = the hook
- spikes: where retention went UP (with the words spoken at that moment)
- dips: where retention went DOWN (with the words spoken)
- cliffs: sudden retention cliffs
- bestSections: sustained high-retention zones (with transcript snippet)

Extract STRUCTURAL PATTERNS shared across videos — patterns that explain WHY retention behaves the way it does, so we can replicate the wins and avoid the dips on new videos. Be specific (quote actual phrases from the data). If you can't find a pattern, say "insufficient data" rather than fabricating.

Output ONLY a JSON object, no prose, no fences:

{
  "hookPatterns": [{"pattern": "describe the hook structure", "example": "exact phrase from one of the firstHookText fields", "avgRetentionImpact": "+X% in first 30s or insufficient data"}],
  "spikeWordPatterns": [{"phrase": "exact phrase or pattern that recurs at spike moments", "occurrences": N, "avgSpike": 0.025, "where": "early|mid|late"}],
  "dipWordPatterns": [{"phrase": "phrase or pattern that recurs at dip moments", "occurrences": N, "avgDrop": 0.04, "advice": "concrete advice — what to do instead"}],
  "optimalPacing": {"introHookByXSec": 12, "sustainedSectionLength": "describe", "statRevealTiming": "X-Y% mark"},
  "structuralTemplate": {"intro": "0-X%: description", "setup": "X-Y%: description", "body": "Y-Z%: description", "climax": "Z-W%: description", "conclusion": "W-100%: description"},
  "audienceInsights": {"profile": "1-2 sentences", "triggers": "what activates retention for this audience", "repellents": "what kills retention"}
}

DATA (JSON):
${JSON.stringify(compact)}`;
    const payload = JSON.stringify({
      model: 'claude-sonnet-4-6',
      max_tokens: 3000,
      messages: [{ role: 'user', content: prompt }],
    });
    const options = {
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload),
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
      },
    };
    const apiReq = https.request(options, apiRes => {
      let buf = '';
      apiRes.on('data', c => { buf += c; });
      apiRes.on('end', () => {
        try {
          const j = JSON.parse(buf);
          if (j.error) return reject(new Error(j.error.message || 'Anthropic error'));
          const text = (j.content || []).find(b => b.type === 'text')?.text || '';
          const cleaned = text.replace(/```json|```/g, '').trim();
          const first = cleaned.indexOf('{');
          const last = cleaned.lastIndexOf('}');
          const sliced = first >= 0 && last > first ? cleaned.slice(first, last + 1) : cleaned;
          resolve(JSON.parse(sliced));
        } catch (e) {
          reject(new Error('Failed to parse Claude pattern response: ' + e.message));
        }
      });
    });
    apiReq.on('error', reject);
    apiReq.write(payload);
    apiReq.end();
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
      const youtubeCaptionsConnected = grantedScopes.includes(
        'https://www.googleapis.com/auth/youtube.force-ssl',
      );
      // Save tokens to Firestore on the channel doc
      await db.collection('channels').doc(channelId).update({
        youtubeConnected: true,
        youtubeAnalyticsConnected,
        youtubeCaptionsConnected,
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
        youtubeCaptionsConnected: false,
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

  // ── YouTube: forensic per-video analysis ─────────────────────
  // POST /youtube/analytics/forensic  body: { channelId, _apiKey }
  //
  // For each of TOP 10 videos, pulls retention curve + caption
  // transcript, identifies retention spikes / dips / cliffs /
  // sustained-high zones, and maps each event to the words spoken
  // at that exact moment. Then runs a Claude pass over the whole
  // structured dataset to extract reusable patterns (hook templates,
  // spike phrases, dip phrases, optimal pacing, structural template).
  // Saves to channels/{cid}/analytics/forensic.
  //
  // LIMITATIONS — be honest with the user about what is + isn't here:
  //   ✔ retention curve (100 datapoints) per video
  //   ✔ auto + manual captions per video
  //   ✗ music tracks used in past videos (not in API)
  //   ✗ specific clips / footage used (not in API)
  //   ✗ thumbnail visuals (only title text overlap is observable)
  //   ✗ WHY viewers drop off (only WHEN)
  //
  // Captions API has a separate quota bucket — be efficient. We only
  // fetch top 10 (not all 50 from the analytics summary) and cap the
  // transcript at 200 cues per video to keep the doc <1MB.
  if (req.method === 'POST' && pathname === '/youtube/analytics/forensic') {
    if (!db) return sendJSON(res, 500, { error: 'Firebase not configured' });
    try {
      const body = await readBody(req);
      const parsed = JSON.parse(body);
      const { channelId } = parsed;
      const userApiKey = parsed._apiKey || ''; // user's Anthropic key for the pattern pass
      if (!channelId) return sendJSON(res, 400, { error: 'channelId required' });

      const snap = await db.collection('channels').doc(channelId).get();
      if (!snap.exists) return sendJSON(res, 404, { error: 'Channel not found' });
      const data = snap.data();
      if (!data.youtubeRefreshToken) return sendJSON(res, 400, { error: 'YouTube not connected for this channel' });
      if (data.youtubeAnalyticsConnected === false) {
        return sendJSON(res, 403, { error: 'Analytics scope not granted — re-connect this channel' });
      }
      // NOTE: captions scope is no longer required. We previously used
      // youtube.force-ssl + yt.captions.list/download but YouTube
      // silently returns 0 results from that path for apps without
      // Content-ID verification. yt-dlp bypasses the restriction.

      // Need /analytics/refresh to have run first so we already have
      // top-videos + cached retention curves. Cheaper than re-querying.
      const summarySnap = await db.collection('channels').doc(channelId).collection('analytics').doc('summary').get();
      if (!summarySnap.exists) {
        return sendJSON(res, 400, { error: 'Run /youtube/analytics/refresh first to populate top-videos data' });
      }
      const summary = summarySnap.data();
      const top10 = (summary.topVideos || []).slice(0, 10);
      if (top10.length === 0) {
        return sendJSON(res, 400, { error: 'No top videos in analytics summary — channel may have no public views in the last 90 days' });
      }

      const oauth = makeOAuthClient();
      oauth.setCredentials({
        refresh_token: data.youtubeRefreshToken,
        access_token: data.youtubeAccessToken || undefined,
        expiry_date: data.youtubeTokenExpiry || undefined,
      });
      const yt = google.youtube({ version: 'v3', auth: oauth });
      const analytics = google.youtubeAnalytics({ version: 'v2', auth: oauth });
      const fmt = (d) => d.toISOString().slice(0, 10);

      // Resolve real durations once (single API call) so retention
      // x-ratios convert correctly to seconds for caption mapping.
      const videoIds = top10.map(v => v.videoId).filter(Boolean);
      const durMap = {};
      try {
        const vr = await yt.videos.list({ part: ['contentDetails'], id: videoIds });
        for (const item of (vr.data.items || [])) {
          durMap[item.id] = parseISO8601Duration(item.contentDetails?.duration || 'PT0S');
        }
      } catch (e) {
        console.warn('forensic: videos.list duration fetch failed:', e.message);
      }

      const perVideo = {};
      for (const v of top10) {
        const vid = v.videoId;
        try {
          // Retention curve — prefer cached from /analytics/refresh.
          let curve = summary.retentionCurves?.[vid]?.curve || null;
          if (!curve || curve.length < 5) {
            const today = new Date();
            const endDate = new Date(today.getTime() - 86400_000);
            const startDate = new Date(endDate.getTime() - 90 * 86400_000);
            const r = await analytics.reports.query({
              ids: 'channel==MINE',
              startDate: fmt(startDate),
              endDate: fmt(endDate),
              metrics: 'audienceWatchRatio',
              dimensions: 'elapsedVideoTimeRatio',
              filters: `video==${vid}`,
            });
            curve = (r.data.rows || []).map(row => ({ x: row[0], retention: row[1] }));
          }
          if (!curve || curve.length < 5) {
            perVideo[vid] = { title: v.title || '', views: v.views || 0, error: 'no retention curve' };
            continue;
          }

          // Caption fetch via yt-dlp. The YouTube Data API's
          // captions.list/download silently returns 0 results for
          // apps without YouTube Content-ID verification — even on
          // the user's own videos. yt-dlp bypasses that restriction
          // by parsing the public watch page (legitimate use; same
          // mechanism every YouTube downloader uses). 1-3s/video.
          let transcript = [];
          let captionSource = null;
          let captionError = null;
          const ytdlpResult = await fetchCaptionsViaYtdlp(vid, YT_DLP_PATH);
          transcript = ytdlpResult.transcript;
          captionSource = ytdlpResult.source;
          captionError = ytdlpResult.error;
          if (captionError) {
            console.warn(`forensic: yt-dlp captions failed for ${vid}: ${captionError}`);
          }

          // Duration fallback chain: API → last caption end → 600s.
          const durationSec = durMap[vid]
            || (transcript.length ? transcript[transcript.length - 1].end : 600);

          const events = analyzeRetentionCurve(curve);

          const spikeMoments = events.spikes.map(s => ({
            x: s.x,
            timestampSec: Math.round(s.x * durationSec),
            retentionGain: s.delta,
            retentionAtPoint: s.retentionAtPoint,
            spokenWords: spokenWordsAt(transcript, s.x * durationSec),
          }));
          const dipMoments = events.dips.map(d => ({
            x: d.x,
            timestampSec: Math.round(d.x * durationSec),
            retentionLoss: d.delta,
            retentionAtPoint: d.retentionAtPoint,
            spokenWords: spokenWordsAt(transcript, d.x * durationSec),
          }));
          const cliffDrops = events.cliffs.map(c => ({
            x: c.x,
            timestampSec: Math.round(c.x * durationSec),
            severity: c.delta,
            retentionAtPoint: c.retentionAtPoint,
            spokenWords: spokenWordsAt(transcript, c.x * durationSec),
          }));
          const bestSections = events.sustained.map(z => {
            const startSec = z.startX * durationSec;
            const endSec = z.endX * durationSec;
            const window = transcript.filter(seg => seg.end >= startSec && seg.start <= endSec);
            const snippet = window.map(s => s.text).join(' ').replace(/\s+/g, ' ').trim().slice(0, 360);
            return {
              startX: z.startX,
              endX: z.endX,
              startSec: Math.round(startSec),
              endSec: Math.round(endSec),
              avgRetention: z.avgRetention,
              transcriptSnippet: snippet,
            };
          });

          // Combined "retention mapped to transcript" stream for any
          // downstream consumer that wants events in time order.
          const retentionMappedToTranscript = [
            ...spikeMoments.map(e => ({ timestamp: e.timestampSec, retentionAtPoint: e.retentionAtPoint, spokenContext: e.spokenWords, eventType: 'spike' })),
            ...dipMoments.map(e => ({ timestamp: e.timestampSec, retentionAtPoint: e.retentionAtPoint, spokenContext: e.spokenWords, eventType: 'dip' })),
            ...cliffDrops.map(e => ({ timestamp: e.timestampSec, retentionAtPoint: e.retentionAtPoint, spokenContext: e.spokenWords, eventType: 'cliff' })),
            ...bestSections.map(b => ({ timestamp: b.startSec, retentionAtPoint: b.avgRetention, spokenContext: b.transcriptSnippet, eventType: 'sustained' })),
          ].sort((a, b) => a.timestamp - b.timestamp);

          perVideo[vid] = {
            title: v.title || '',
            views: v.views || 0,
            publishedAt: v.publishedAt || null,
            durationSec,
            avgRetention: v.avgViewPercentage || 0,
            avgViewPercentage: v.avgViewPercentage || 0,
            captionsAvailable: transcript.length > 0,
            captionsSource: captionSource,
            captionsError: captionError,
            transcript: transcript.slice(0, 200), // doc-size cap
            retentionMappedToTranscript,
            spikeMoments,
            dipMoments,
            cliffDrops,
            bestSections,
          };
        } catch (e) {
          console.warn(`forensic: per-video failed for ${vid}:`, e.message);
          perVideo[vid] = { title: v.title || '', views: v.views || 0, error: e.message };
        }
      }

      // Save raw per-video forensic data BEFORE the Claude pass — if
      // the LLM call fails the user still has the structured data
      // saved and the pattern step can be retried independently.
      await db.collection('channels').doc(channelId).collection('analytics').doc('forensic').set({
        refreshedAt: admin.firestore.FieldValue.serverTimestamp(),
        perVideo,
        videosAnalyzed: Object.keys(perVideo).length,
        videosWithCaptions: Object.values(perVideo).filter(p => p.captionsAvailable).length,
      });

      // Phase C — pattern extraction. Only runs if user passed their
      // Anthropic key. Pattern object is saved as a separate field on
      // the same doc so consumers can read either layer independently.
      let patterns = null;
      let patternsError = null;
      if (userApiKey) {
        try {
          patterns = await claudePatternPass(perVideo, userApiKey);
          await db.collection('channels').doc(channelId).collection('analytics').doc('forensic').update({
            patterns,
            patternsGeneratedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
        } catch (e) {
          patternsError = e.message;
          console.warn('forensic: claude pattern pass failed:', e.message);
        }
      }

      console.log(`Forensic complete for ${channelId}: ${Object.keys(perVideo).length} videos, ${Object.values(perVideo).filter(p => p.captionsAvailable).length} with captions, patterns=${!!patterns}`);
      return sendJSON(res, 200, {
        ok: true,
        videosAnalyzed: Object.keys(perVideo).length,
        videosWithCaptions: Object.values(perVideo).filter(p => p.captionsAvailable).length,
        patternsExtracted: !!patterns,
        patternsError,
      });
    } catch (e) {
      console.error('forensic error:', e);
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
