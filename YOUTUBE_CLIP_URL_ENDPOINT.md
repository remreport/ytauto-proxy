# Required Render proxy endpoint: `POST /youtube/clip-url`

**Status:** ⚠️ Must be added manually to the Render proxy repo before
Phase 21 Fix 3 (YouTube source) does anything useful. Without this
endpoint the Cloud Function will log `[youtube] clip-url proxy HTTP 404`
on every attempt and fall through to the next source.

## Why it exists

`functions/index.js` → `youtubeSearchTop()` queries the YouTube Data
API v3 for short CC-licensed videos matching a beat phrase. The API
returns videoIds, not direct download URLs. To get a playable mp4 the
proxy must run `yt-dlp --get-url` behind the Decodo residential proxy
(same pattern as the existing forensic-captions endpoint).

Direct extraction from inside Cloud Functions is not viable —
YouTube blocks GCP/AWS/Render datacenter IPs with a "Sign in to
confirm you're not a bot" interstitial. Decodo residential IPs
bypass that, but only the Render proxy holds the Decodo credentials.

## Contract

**Caller** — `ytauto-proxy/functions/index.js:3329` (`youtubeFetchClipUrl`)

```
POST https://ytauto-proxy.onrender.com/youtube/clip-url
Content-Type: application/json

{
  "videoId": "dQw4w9WgXcQ",
  "maxDuration": 240
}
```

**Success response** (HTTP 200):

```json
{ "url": "https://rr5---sn-...googlevideo.com/videoplayback?..." }
```

**Failure response** (any non-2xx, or 200 with `url` missing/non-string):
caller treats as "no clip available" and moves to the next videoId.
Return `{ "error": "<reason>" }` with an appropriate 4xx/5xx — the
caller logs the status code and skips.

## Implementation outline

Drop this into `server.js` alongside the other `/youtube/*` routes.
The `yt-dlp` binary and Decodo env vars are already in place from
the forensic-captions path (`fetchCaptionsViaYtdlp` in
`server.js:394`) — reuse the same `YT_DLP_PATH` and `DECODO_*`
plumbing.

```js
// POST /youtube/clip-url  body: { videoId, maxDuration? }
// Returns: { url } — a direct mp4 progressive download URL.
//
// Uses yt-dlp --get-url with format selector that prefers a single
// progressive mp4 file (no DASH split, no audio-only). The format
// expression below picks "best mp4 with both video+audio, <=720p,
// <=maxDuration seconds". Falls back to best available if nothing
// matches. Returns 404 if no playable format exists.
if (req.method === 'POST' && pathname === '/youtube/clip-url') {
  try {
    const body = await readBody(req);
    const { videoId, maxDuration } = JSON.parse(body);
    if (!videoId || typeof videoId !== 'string') {
      return sendJSON(res, 400, { error: 'videoId required' });
    }
    if (!YT_DLP_PATH) {
      return sendJSON(res, 500, { error: 'yt-dlp not available on host' });
    }
    const maxDur = Math.max(10, Math.min(600, parseInt(maxDuration, 10) || 240));
    const url = `https://www.youtube.com/watch?v=${videoId}`;

    // Format selector: progressive mp4 (audio+video in one file), <=720p,
    // duration <= maxDur. Progressive prevents needing to mux on the
    // render side. The duration filter rejects clips longer than the
    // beat slot will use.
    const fmt = `best[ext=mp4][height<=720][protocol^=http][acodec!=none][vcodec!=none][duration<=${maxDur}]/best[ext=mp4][height<=720][protocol^=http][duration<=${maxDur}]/best[ext=mp4][duration<=${maxDur}]`;

    const args = [
      '--get-url',
      '-f', fmt,
      '--no-warnings',
      '--no-playlist',
      '--no-progress',
      '--socket-timeout', '20',
      '--retries', '2',
    ];
    if (process.env.DECODO_USER && process.env.DECODO_PASS && process.env.DECODO_ENDPOINT) {
      const u = encodeURIComponent(process.env.DECODO_USER);
      const p = encodeURIComponent(process.env.DECODO_PASS);
      args.push('--proxy', `http://${u}:${p}@${process.env.DECODO_ENDPOINT}`);
    }
    args.push(url);

    const result = await new Promise((resolve) => {
      const proc = spawn(YT_DLP_PATH, args, { stdio: ['ignore', 'pipe', 'pipe'] });
      let stdout = '';
      let stderr = '';
      proc.stdout.on('data', (c) => { stdout += c.toString(); });
      proc.stderr.on('data', (c) => { stderr += c.toString(); });
      const timer = setTimeout(() => { try { proc.kill('SIGKILL'); } catch {} }, 25000);
      proc.on('close', (code) => {
        clearTimeout(timer);
        resolve({ code, stdout, stderr });
      });
    });

    if (result.code !== 0) {
      const firstErr = (result.stderr || '').split('\n')[0].slice(0, 200);
      console.warn(`[clip-url] ${videoId} exit=${result.code}: ${firstErr}`);
      return sendJSON(res, 404, { error: 'no playable format', detail: firstErr });
    }
    const directUrl = (result.stdout || '').trim().split('\n')[0];
    if (!directUrl || !/^https?:\/\//.test(directUrl)) {
      return sendJSON(res, 404, { error: 'yt-dlp returned no URL' });
    }
    return sendJSON(res, 200, { url: directUrl });
  } catch (e) {
    console.error('[clip-url] error:', e.message);
    return sendJSON(res, 500, { error: e.message || 'clip-url failed' });
  }
}
```

## Deployment

1. Paste the route above into `server.js` in the Render proxy repo
   alongside the other `/youtube/*` POST handlers (around line 1865,
   next to `/youtube/competitor-top-videos`).
2. Commit and push — Render auto-redeploys from the connected branch.
3. Verify the route is live:

   ```bash
   curl -X POST https://ytauto-proxy.onrender.com/youtube/clip-url \
     -H 'Content-Type: application/json' \
     -d '{"videoId":"dQw4w9WgXcQ","maxDuration":240}'
   ```

   Expected: `{"url":"https://rr5---sn-...googlevideo.com/..."}`.

## Cost / quota notes

- `yt-dlp --get-url` makes one Decodo request per call. At Decodo's
  `gate.decodo.com` pricing (~$0.0008/request after the included
  bandwidth), each clip-url lookup costs roughly $0.001.
- The Cloud Function caps candidates at `n=3` per beat phrase and bails
  on the first success, so a typical render does ≤3 lookups per beat.
  Worst-case ceiling for a 50-beat report: ~150 lookups ≈ $0.15.
- The YouTube Data API search itself costs 100 quota units per call
  against the 10K/day free tier — ~100 distinct search phrases per day
  (the function deduplicates by beat phrase).
