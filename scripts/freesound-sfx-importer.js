// Freesound → globalSfxTracks importer.
//
// Walks each of our 7 tags (whoosh / hit / impact / swoop / ding /
// transition / pop), queries Freesound for CC0 sound effects matching
// that tag, downloads the top hits, uploads to Firebase Storage, and
// creates globalSfxTracks docs.
//
// Two modes:
//   --test   Print metadata for top 5 hits per tag. No download, no
//            upload. Use to spot-check quality before the real run.
//   --full   Download + upload TARGET_PER_TAG per tag (default 4).
//
// Auth: needs FREESOUND_API_KEY in ytauto-proxy/.env. Get one at
// https://freesound.org/apiv2/apply/ (free, ~30s registration).
//
// Filters applied:
//   - duration: 0.2-5 seconds (typical SFX length)
//   - license: Creative Commons 0 (CC0) only
//   - sort: popularity (most-downloaded first)
//
// Usage:
//   FIREBASE_SERVICE_ACCOUNT_FILE=C:\Users\fiffa\firebase-key.json \
//     node scripts/freesound-sfx-importer.js --test
//   FIREBASE_SERVICE_ACCOUNT_FILE=... \
//     node scripts/freesound-sfx-importer.js --full

const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const {pipeline} = require('node:stream/promises');
const {Readable} = require('node:stream');
const {spawn} = require('node:child_process');

// Load .env so FREESOUND_API_KEY is available
const envPath = path.join(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const m = /^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)$/.exec(line);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2];
  }
}

const args = process.argv.slice(2);
const MODE = args.includes('--full') ? 'full' : 'test';
const TARGET_PER_TAG = MODE === 'test' ? 0 : 4; // 0 = print only

// Tag → search-query bundle. Freesound's full-text search across
// title/tags/description; multi-term boosts relevance. Quoted phrases
// keep multi-word tags from token-splitting too aggressively.
const TAG_QUERIES = {
  whoosh: 'whoosh',
  hit: 'thud',
  impact: 'impact boom',
  swoop: 'swoop',
  ding: 'bell chime',
  transition: 'riser',
  pop: 'pop sound',
};

const FREESOUND_BASE = 'https://freesound.org/apiv2';

async function freesoundSearch(query, page = 1) {
  if (!process.env.FREESOUND_API_KEY) throw new Error('FREESOUND_API_KEY not set in .env');
  const url = new URL(`${FREESOUND_BASE}/search/text/`);
  url.searchParams.set('query', query);
  url.searchParams.set('filter', 'license:"Creative Commons 0" duration:[0.2 TO 5.0]');
  url.searchParams.set('sort', 'downloads_desc');
  url.searchParams.set('fields', 'id,name,duration,download,previews,license,username,tags');
  url.searchParams.set('page_size', '15');
  url.searchParams.set('page', String(page));
  const r = await fetch(url.toString(), {
    headers: {Authorization: `Token ${process.env.FREESOUND_API_KEY}`},
  });
  if (!r.ok) throw new Error(`Freesound ${r.status}: ${(await r.text()).slice(0, 200)}`);
  const j = await r.json();
  return j.results || [];
}

// Freesound's `/sounds/{id}/download/` endpoint requires OAuth user-context
// auth, which we don't have. The `previews` field gives a CDN URL for the
// hq mp3 preview that's downloadable with just the API token. Quality is
// effectively the same for short SFX (hq preview is 128kbps mp3).
function pickPreviewUrl(result) {
  if (!result || !result.previews) return null;
  return result.previews['preview-hq-mp3']
      || result.previews['preview-lq-mp3']
      || null;
}

function lazyFirebaseInit() {
  const svcPath = process.env.FIREBASE_SERVICE_ACCOUNT_FILE;
  if (!svcPath) throw new Error('Set FIREBASE_SERVICE_ACCOUNT_FILE for --full mode');
  const admin = require(path.join(__dirname, '..', 'functions', 'node_modules', 'firebase-admin'));
  if (!admin.apps.length) {
    admin.initializeApp({
      credential: admin.credential.cert(JSON.parse(fs.readFileSync(svcPath, 'utf8'))),
      storageBucket: 'ytauto-95f91.firebasestorage.app',
    });
  }
  return {db: admin.firestore(), bucket: admin.storage().bucket(), FieldValue: admin.firestore.FieldValue};
}

function lazyFfprobePath() {
  return require(path.join(__dirname, '..', 'functions', 'node_modules', '@ffprobe-installer', 'ffprobe')).path;
}

async function probeDuration(localPath, ffprobePath) {
  return new Promise((resolve) => {
    const proc = spawn(ffprobePath, ['-v', 'error', '-show_format', '-of', 'json', localPath], {stdio: ['ignore', 'pipe', 'pipe']});
    let out = '';
    proc.stdout.on('data', (c) => { out += c.toString(); });
    proc.once('close', () => {
      try { resolve(parseFloat(JSON.parse(out).format?.duration || 0)); }
      catch { resolve(0); }
    });
    proc.once('error', () => resolve(0));
  });
}

async function downloadAndUpload(result, tag, fb, ffprobePath) {
  const previewUrl = pickPreviewUrl(result);
  if (!previewUrl) throw new Error('no preview URL');
  const tmp = path.join(os.tmpdir(), `fs-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.mp3`);
  try {
    const r = await fetch(previewUrl);
    if (!r.ok) throw new Error(`download ${r.status}`);
    await pipeline(Readable.fromWeb(r.body), fs.createWriteStream(tmp));
    const dur = await probeDuration(tmp, ffprobePath);
    const safeName = (result.name || `sfx-${result.id}`).replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 60);
    const storagePath = `globalSfx/fs-${result.id}-${safeName}.mp3`;
    const file = fb.bucket.file(storagePath);
    await pipeline(fs.createReadStream(tmp), file.createWriteStream({metadata: {contentType: 'audio/mpeg'}, resumable: false}));
    await file.makePublic();
    const publicUrl = `https://storage.googleapis.com/${fb.bucket.name}/${storagePath}`;
    const sizeBytes = fs.statSync(tmp).size;
    const doc = fb.db.collection('globalSfxTracks').doc();
    await doc.set({
      url: publicUrl,
      path: storagePath,
      name: result.name || `Freesound ${result.id}`,
      tags: [tag],            // store under the tag we found it via
      duration: dur || result.duration || 0,
      attribution: `Freesound — ${result.username || 'unknown'} (CC0)`,
      source: 'freesound',
      freesoundId: result.id,
      size: sizeBytes,
      type: 'audio/mpeg',
      uploadedAt: fb.FieldValue.serverTimestamp(),
    });
    return {ok: true, durationSec: dur, sizeBytes};
  } finally {
    try { fs.unlinkSync(tmp); } catch { /* ignore */ }
  }
}

(async () => {
  console.log(`mode: ${MODE}\n`);
  if (!process.env.FREESOUND_API_KEY) {
    console.error('FREESOUND_API_KEY missing in .env. Get one at https://freesound.org/apiv2/apply/');
    process.exit(1);
  }

  let fb = null;
  let ffprobePath = null;
  if (MODE === 'full') {
    fb = lazyFirebaseInit();
    ffprobePath = lazyFfprobePath();
  }

  let totalUploaded = 0;
  let totalSkipped = 0;

  for (const tag of Object.keys(TAG_QUERIES)) {
    console.log(`\n=== ${tag} ===`);
    const candidates = await freesoundSearch(TAG_QUERIES[tag]);
    console.log(`  found ${candidates.length} candidates (CC0, 0.2-5s)`);
    let kept = 0;
    const limit = MODE === 'test' ? 2 : TARGET_PER_TAG;
    for (let i = 0; i < candidates.length && kept < limit; i++) {
      const c = candidates[i];
      const previewUrl = pickPreviewUrl(c);
      if (!previewUrl) { continue; }
      if (MODE === 'test') {
        kept++;
        console.log(`  [${kept}] ${c.name?.slice(0, 60)}`);
        console.log(`      duration: ${c.duration?.toFixed(2)}s · user: ${c.username}`);
        console.log(`      page:    https://freesound.org/s/${c.id}/`);
        console.log(`      preview: ${previewUrl}`);
      } else {
        try {
          const res = await downloadAndUpload(c, tag, fb, ffprobePath);
          kept++;
          totalUploaded++;
          console.log(`  ✓ uploaded "${c.name?.slice(0, 50)}" (${res.durationSec.toFixed(2)}s, ${(res.sizeBytes / 1024).toFixed(0)}KB)`);
        } catch (e) {
          totalSkipped++;
          console.log(`  ✗ ${c.name?.slice(0, 50)}: ${e.message}`);
        }
      }
    }
    if (kept === 0) console.log(`  no usable candidates — try different query for '${tag}'`);
  }

  if (MODE === 'full') {
    console.log(`\n--- summary ---`);
    console.log(`uploaded: ${totalUploaded}`);
    console.log(`skipped:  ${totalSkipped}`);
  } else {
    console.log(`\n--- test mode complete. To run for real: ---`);
    console.log(`  node scripts/freesound-sfx-importer.js --full`);
  }
  process.exit(0);
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
