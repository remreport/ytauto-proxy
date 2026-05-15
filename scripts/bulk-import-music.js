// Bulk-import music tracks into globalMusicTracks. Reads a CSV with
// columns:
//
//   url,name,mood,attribution,source
//
// (One header row, one row per track. Quote any value containing
// commas. URL must be a direct download — Pixabay/FreePD give one;
// YouTube Audio Library does NOT, you'll need to download manually
// for those rows and use a local file path instead of an http URL.)
//
// For each row the script:
//   1. Downloads the audio (or reads from local path)
//   2. Probes duration via ffprobe
//   3. Uploads to Firebase Storage at globalMusic/{ts}_{safeName}.mp3
//   4. Creates a Firestore doc in globalMusicTracks/
//
// Usage:
//   FIREBASE_SERVICE_ACCOUNT_FILE=/path/to/key.json \
//     node scripts/bulk-import-music.js path/to/tracks.csv
//
// Idempotency: re-running with the same CSV creates DUPLICATE entries.
// Don't re-run after partial failure without trimming the CSV first.

const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const {pipeline} = require('node:stream/promises');
const {Readable} = require('node:stream');
const {spawn} = require('node:child_process');
// firebase-admin lives only inside functions/ — borrow it from there so
// this script doesn't need a duplicate install at the proxy root.
const admin = require(require('node:path').join(__dirname, '..', 'functions', 'node_modules', 'firebase-admin'));

const csvPath = process.argv[2];
if (!csvPath) { console.error('Usage: node scripts/bulk-import-music.js path/to/tracks.csv'); process.exit(1); }
if (!fs.existsSync(csvPath)) { console.error('CSV not found:', csvPath); process.exit(1); }

const svcPath = process.env.FIREBASE_SERVICE_ACCOUNT_FILE;
if (!svcPath) { console.error('Set FIREBASE_SERVICE_ACCOUNT_FILE'); process.exit(1); }

admin.initializeApp({
  credential: admin.credential.cert(JSON.parse(fs.readFileSync(svcPath, 'utf8'))),
  storageBucket: 'ytauto-95f91.firebasestorage.app',
});
const db = admin.firestore();
const bucket = admin.storage().bucket();
const FieldValue = admin.firestore.FieldValue;

const ffprobePath = require(path.join(__dirname, '..', 'functions', 'node_modules', '@ffprobe-installer', 'ffprobe')).path;
const ffmpegPath = require(path.join(__dirname, '..', 'functions', 'node_modules', '@ffmpeg-installer', 'ffmpeg')).path;

// Run ebur128 over a local audio file to extract integrated LUFS.
async function measureLufs(localPath) {
  return new Promise((resolve) => {
    const proc = spawn(ffmpegPath, ['-i', localPath, '-af', 'ebur128', '-f', 'null', '-'], {stdio: ['ignore', 'ignore', 'pipe']});
    let err = '';
    proc.stderr.on('data', (c) => { err += c.toString(); });
    proc.once('close', () => {
      const m = /Integrated loudness:[\s\S]*?I:\s*(-?[\d.]+)\s*LUFS/i.exec(err);
      resolve(m ? parseFloat(m[1]) : null);
    });
    proc.once('error', () => resolve(null));
  });
}

// Minimal CSV parser. Handles quoted fields with embedded commas. Not a
// full RFC 4180 implementation — good enough for our small curated lists.
function parseCsv(text) {
  const rows = [];
  let cur = [];
  let field = '';
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"' && text[i + 1] === '"') { field += '"'; i++; }
      else if (c === '"') { inQuotes = false; }
      else { field += c; }
    } else {
      if (c === '"') inQuotes = true;
      else if (c === ',') { cur.push(field); field = ''; }
      else if (c === '\n') { cur.push(field); rows.push(cur); cur = []; field = ''; }
      else if (c === '\r') { /* skip */ }
      else field += c;
    }
  }
  if (field || cur.length) { cur.push(field); rows.push(cur); }
  return rows.filter((r) => r.some((f) => f.trim()));
}

async function probeDuration(localPath) {
  return new Promise((resolve) => {
    const proc = spawn(ffprobePath, ['-v', 'error', '-show_format', '-of', 'json', localPath], {stdio: ['ignore', 'pipe', 'pipe']});
    let out = '';
    proc.stdout.on('data', (c) => { out += c.toString(); });
    proc.once('close', () => {
      try {
        const m = JSON.parse(out);
        resolve(parseFloat(m.format?.duration || 0));
      } catch { resolve(0); }
    });
    proc.once('error', () => resolve(0));
  });
}

async function fetchToTmp(srcUrl) {
  const tmpFile = path.join(os.tmpdir(), `music-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.mp3`);
  if (srcUrl.startsWith('http://') || srcUrl.startsWith('https://')) {
    const resp = await fetch(srcUrl);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    await pipeline(Readable.fromWeb(resp.body), fs.createWriteStream(tmpFile));
  } else {
    const abs = path.isAbsolute(srcUrl) ? srcUrl : path.resolve(process.cwd(), srcUrl);
    if (!fs.existsSync(abs)) throw new Error(`local file not found: ${abs}`);
    fs.copyFileSync(abs, tmpFile);
  }
  return tmpFile;
}

(async () => {
  const text = fs.readFileSync(csvPath, 'utf8');
  const rows = parseCsv(text);
  if (rows.length < 2) { console.error('CSV must have a header row + at least one data row'); process.exit(1); }
  const header = rows[0].map((h) => h.trim().toLowerCase());
  const required = ['url', 'name', 'mood', 'attribution', 'source'];
  for (const r of required) {
    if (!header.includes(r)) { console.error(`CSV missing required column: ${r}`); process.exit(1); }
  }
  const idx = (k) => header.indexOf(k);

  console.log(`Importing ${rows.length - 1} tracks from ${csvPath}\n`);
  let succeeded = 0;
  let failed = 0;
  for (let i = 1; i < rows.length; i++) {
    const row = rows[i];
    const url = row[idx('url')]?.trim();
    const name = row[idx('name')]?.trim();
    const mood = row[idx('mood')]?.trim().toLowerCase();
    const attribution = row[idx('attribution')]?.trim() || '';
    const source = row[idx('source')]?.trim().toLowerCase() || 'other';
    if (!url || !name || !mood) {
      console.warn(`row ${i}: skipping — missing url/name/mood`);
      failed++;
      continue;
    }

    let tmp;
    try {
      console.log(`(${i}/${rows.length - 1}) ${name} [${mood}]…`);
      tmp = await fetchToTmp(url);
      const duration = await probeDuration(tmp);
      const integratedLoudness = await measureLufs(tmp);
      const safeName = name.replace(/[^a-zA-Z0-9._-]/g, '_');
      const storagePath = `globalMusic/${Date.now()}_${i}_${safeName}.mp3`;
      const file = bucket.file(storagePath);
      await pipeline(fs.createReadStream(tmp), file.createWriteStream({metadata: {contentType: 'audio/mpeg'}, resumable: false}));
      await file.makePublic();
      const publicUrl = `https://storage.googleapis.com/${bucket.name}/${storagePath}`;
      const sizeBytes = fs.statSync(tmp).size;
      const doc = db.collection('globalMusicTracks').doc();
      await doc.set({
        url: publicUrl,
        path: storagePath,
        name,
        mood,
        duration,
        integratedLoudness,
        attribution,
        source,
        size: sizeBytes,
        type: 'audio/mpeg',
        uploadedAt: FieldValue.serverTimestamp(),
      });
      console.log(`  ✓ uploaded (${duration ? duration.toFixed(0) + 's' : 'unknown duration'}, ${(sizeBytes / 1024 / 1024).toFixed(1)}MB, ${integratedLoudness != null ? integratedLoudness.toFixed(1) + ' LUFS' : 'no LUFS'}) → ${doc.id}`);
      succeeded++;
    } catch (e) {
      console.warn(`  ✗ ${e.message}`);
      failed++;
    } finally {
      if (tmp) try { fs.unlinkSync(tmp); } catch { /* ignore */ }
    }
  }
  console.log(`\nDone: ${succeeded} succeeded, ${failed} failed.`);
  process.exit(failed > 0 ? 2 : 0);
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
