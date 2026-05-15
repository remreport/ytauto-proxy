// Internet Archive → globalMusicTracks importer.
//
// Two modes:
//   --test   Fetch metadata for ~5 tracks across 2-3 moods. Print URLs.
//            No downloads, no uploads. Use this to spot-check quality.
//   --full   Run for all 7 moods, ~5 tracks each. Download MP3s, probe
//            duration, upload to Firebase Storage, create globalMusicTracks
//            docs. Skips tracks shorter than MIN_DURATION_SEC or longer
//            than MAX_DURATION_SEC.
//
// Source: archive.org's advanced search + metadata APIs (both public, no
// key required). Filters by `licenseurl:(*publicdomain*)` so we never
// touch a non-PD/CC0 track.
//
// Usage:
//   FIREBASE_SERVICE_ACCOUNT_FILE=C:\Users\fiffa\firebase-key.json \
//     node scripts/internet-archive-importer.js --test
//   FIREBASE_SERVICE_ACCOUNT_FILE=... \
//     node scripts/internet-archive-importer.js --full

const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const {pipeline} = require('node:stream/promises');
const {Readable} = require('node:stream');
const {spawn} = require('node:child_process');

const args = process.argv.slice(2);
const MODE = args.includes('--full') ? 'full' : 'test';
const TARGET_PER_MOOD = MODE === 'test' ? 0 : 5;     // 0 = print only
const TEST_MOODS = ['calm', 'cinematic', 'corporate']; // 5 samples = 2-3 across these

const MIN_DURATION_SEC = 60;
const MAX_DURATION_SEC = 300;

// Search keyword bundles per mood. Tuned for instrumental / production
// music typical to YouTube background use. IA's catalogue skews older
// and classical — these terms try to find the most YouTube-style tracks
// available under public-domain.
const MOOD_QUERIES = {
  calm: '("ambient piano" OR "meditation" OR "soft instrumental" OR "peaceful")',
  energetic: '("upbeat instrumental" OR "motivational" OR "energetic instrumental")',
  dramatic: '("orchestral" OR "dramatic strings" OR "cinematic strings")',
  mysterious: '("ambient dark" OR "suspense instrumental" OR "mysterious")',
  uplifting: '("uplifting instrumental" OR "positive piano" OR "inspirational")',
  corporate: '("corporate" OR "business background" OR "clean instrumental")',
  cinematic: '("epic orchestral" OR "film score" OR "cinematic")',
};

// ── search helpers ─────────────────────────────────────────────────
async function iaSearch(moodQuery, rows = 10) {
  const q = `(collection:(audio_music) OR collection:(opensource_audio)) AND mediatype:(audio) AND licenseurl:(*publicdomain* OR *creativecommons.org/publicdomain*) AND ${moodQuery}`;
  const url = new URL('https://archive.org/advancedsearch.php');
  url.searchParams.set('q', q);
  url.searchParams.append('fl[]', 'identifier');
  url.searchParams.append('fl[]', 'title');
  url.searchParams.append('fl[]', 'creator');
  url.searchParams.append('fl[]', 'downloads');
  url.searchParams.append('fl[]', 'licenseurl');
  url.searchParams.set('rows', String(rows));
  url.searchParams.set('output', 'json');
  url.searchParams.set('sort[]', 'downloads desc');
  const r = await fetch(url.toString());
  if (!r.ok) throw new Error(`search ${r.status}: ${(await r.text()).slice(0, 200)}`);
  const j = await r.json();
  return j.response?.docs || [];
}

async function iaItemMp3(identifier) {
  const r = await fetch(`https://archive.org/metadata/${identifier}`);
  if (!r.ok) return null;
  const j = await r.json();
  const files = j.files || [];
  // Prefer the longest MP3 (avoid 30-second previews). Some items have
  // both VBR and 64kbps versions; we want VBR or the original mp3.
  const mp3s = files.filter((f) => /\.mp3$/i.test(f.name || ''));
  if (mp3s.length === 0) return null;
  // length / size used as duration proxies — IA's `length` field is
  // sometimes "MM:SS" or seconds. Parse defensively.
  const parseLen = (l) => {
    if (!l) return 0;
    if (typeof l === 'number') return l;
    if (/^\d+(\.\d+)?$/.test(l)) return parseFloat(l);
    const m = /^(\d+):(\d+)(?::(\d+))?$/.exec(l);
    if (m) return (+m[1]) * (m[3] ? 3600 : 60) + (+m[2]) * (m[3] ? 60 : 1) + (m[3] ? +m[3] : 0);
    return 0;
  };
  // Sort by parsed length descending; pick the longest.
  mp3s.sort((a, b) => parseLen(b.length) - parseLen(a.length));
  const chosen = mp3s[0];
  return {
    filename: chosen.name,
    durationSec: parseLen(chosen.length),
    sizeBytes: parseInt(chosen.size || '0', 10),
    url: `https://archive.org/download/${identifier}/${encodeURIComponent(chosen.name)}`,
  };
}

// ── full-mode helpers (only used when --full) ─────────────────────
function lazyFirebaseInit() {
  const svcPath = process.env.FIREBASE_SERVICE_ACCOUNT_FILE;
  if (!svcPath) throw new Error('Set FIREBASE_SERVICE_ACCOUNT_FILE for --full mode');
  const admin = require('firebase-admin');
  admin.initializeApp({
    credential: admin.credential.cert(JSON.parse(fs.readFileSync(svcPath, 'utf8'))),
    storageBucket: 'ytauto-95f91.firebasestorage.app',
  });
  return {
    db: admin.firestore(),
    bucket: admin.storage().bucket(),
    FieldValue: admin.firestore.FieldValue,
  };
}

function lazyFfprobePath() {
  return require(path.join(__dirname, '..', 'functions', 'node_modules', '@ffprobe-installer', 'ffprobe')).path;
}

async function probeFile(localPath, ffprobePath) {
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

async function downloadAndUpload(track, mood, fb, ffprobePath) {
  const safeName = (track.title || 'track').replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 60);
  const tmp = path.join(os.tmpdir(), `ia-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.mp3`);
  try {
    const r = await fetch(track.mp3.url);
    if (!r.ok) throw new Error(`download ${r.status}`);
    await pipeline(Readable.fromWeb(r.body), fs.createWriteStream(tmp));
    const probedDur = await probeFile(tmp, ffprobePath);
    if (probedDur && (probedDur < MIN_DURATION_SEC || probedDur > MAX_DURATION_SEC)) {
      throw new Error(`duration ${probedDur.toFixed(0)}s out of range [${MIN_DURATION_SEC}-${MAX_DURATION_SEC}]`);
    }
    const storagePath = `globalMusic/ia-${Date.now()}-${safeName}.mp3`;
    const file = fb.bucket.file(storagePath);
    await pipeline(fs.createReadStream(tmp), file.createWriteStream({metadata: {contentType: 'audio/mpeg'}, resumable: false}));
    await file.makePublic();
    const publicUrl = `https://storage.googleapis.com/${fb.bucket.name}/${storagePath}`;
    const sizeBytes = fs.statSync(tmp).size;
    const doc = fb.db.collection('globalMusicTracks').doc();
    await doc.set({
      url: publicUrl,
      path: storagePath,
      name: track.title || track.identifier,
      mood,
      duration: probedDur || track.mp3.durationSec || 0,
      attribution: `Internet Archive — ${track.creator || 'unknown'} (Public Domain / CC0)`,
      source: 'internet-archive',
      iaIdentifier: track.identifier,
      size: sizeBytes,
      type: 'audio/mpeg',
      uploadedAt: fb.FieldValue.serverTimestamp(),
    });
    return {ok: true, durationSec: probedDur || track.mp3.durationSec || 0, sizeBytes};
  } finally {
    try { fs.unlinkSync(tmp); } catch { /* ignore */ }
  }
}

// ── main ────────────────────────────────────────────────────────────
(async () => {
  console.log(`mode: ${MODE}\n`);
  const moods = MODE === 'test' ? TEST_MOODS : Object.keys(MOOD_QUERIES);
  const samplesPerMood = MODE === 'test' ? 2 : 8; // fetch 8, keep first 5 that pass

  let fb = null;
  let ffprobePath = null;
  if (MODE === 'full') {
    fb = lazyFirebaseInit();
    ffprobePath = lazyFfprobePath();
  }

  let totalUploaded = 0;
  let totalSkipped = 0;

  for (const mood of moods) {
    console.log(`\n=== ${mood} ===`);
    const candidates = await iaSearch(MOOD_QUERIES[mood], 10);
    console.log(`  found ${candidates.length} candidates`);
    let kept = 0;
    for (let i = 0; i < candidates.length && kept < samplesPerMood; i++) {
      const c = candidates[i];
      const mp3 = await iaItemMp3(c.identifier);
      if (!mp3) { continue; }
      // Length filter at metadata stage (cheap) — skip obviously off-spec
      if (mp3.durationSec && (mp3.durationSec < MIN_DURATION_SEC || mp3.durationSec > MAX_DURATION_SEC)) {
        console.log(`  skip ${c.identifier}: meta-duration ${mp3.durationSec}s out of range`);
        continue;
      }
      const track = {...c, mp3};
      if (MODE === 'test') {
        kept++;
        console.log(`  [${kept}] ${c.title}`);
        console.log(`      creator: ${c.creator || '(unknown)'}`);
        console.log(`      downloads: ${c.downloads || 0}`);
        console.log(`      duration: ${mp3.durationSec ? mp3.durationSec.toFixed(0) + 's' : 'unknown'}`);
        console.log(`      size: ${mp3.sizeBytes ? (mp3.sizeBytes/1024/1024).toFixed(1) + 'MB' : 'unknown'}`);
        console.log(`      page: https://archive.org/details/${c.identifier}`);
        console.log(`      mp3:  ${mp3.url}`);
      } else {
        try {
          const res = await downloadAndUpload(track, mood, fb, ffprobePath);
          kept++;
          totalUploaded++;
          console.log(`  ✓ uploaded "${c.title}" (${res.durationSec.toFixed(0)}s, ${(res.sizeBytes/1024/1024).toFixed(1)}MB)`);
          if (kept >= 5) break;
        } catch (e) {
          totalSkipped++;
          console.log(`  ✗ ${c.identifier}: ${e.message}`);
        }
      }
    }
    if (MODE === 'test' && kept === 0) {
      console.log(`  NO matches for "${mood}" — query may need tuning`);
    }
  }

  if (MODE === 'full') {
    console.log(`\n--- summary ---`);
    console.log(`uploaded: ${totalUploaded}`);
    console.log(`skipped:  ${totalSkipped}`);
  } else {
    console.log(`\n--- test mode complete. To run for real: ---`);
    console.log(`  node scripts/internet-archive-importer.js --full`);
  }
  process.exit(0);
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
