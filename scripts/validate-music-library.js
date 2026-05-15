// Probes every globalMusicTracks file via ffprobe + an actual decode
// pass. Anything that ffprobe can't decode cleanly gets deleted from
// both Firestore AND Storage. The Lambda render's ffprobe is the same
// binary version, so passing here = passing on Lambda.
//
// Failure mode we're catching: "Failed to find two consecutive MPEG
// audio frames" / "Invalid data found when processing input" — usually
// means the file was downloaded incompletely from Pixabay or has a
// container/codec that ffprobe doesn't recognize.
//
// Usage:
//   FIREBASE_SERVICE_ACCOUNT_FILE=C:\Users\fiffa\firebase-key.json \
//     node scripts/validate-music-library.js [--delete-broken]

const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const {pipeline} = require('node:stream/promises');
const {Readable} = require('node:stream');
const {spawn} = require('node:child_process');

const DELETE_BROKEN = process.argv.includes('--delete-broken');

const adminPath = path.join(__dirname, '..', 'functions', 'node_modules', 'firebase-admin');
const admin = require(adminPath);
const ffprobePath = require(path.join(__dirname, '..', 'functions', 'node_modules', '@ffprobe-installer', 'ffprobe')).path;
const ffmpegPath = require(path.join(__dirname, '..', 'functions', 'node_modules', '@ffmpeg-installer', 'ffmpeg')).path;

if (!process.env.FIREBASE_SERVICE_ACCOUNT_FILE) {
  console.error('Set FIREBASE_SERVICE_ACCOUNT_FILE'); process.exit(1);
}
admin.initializeApp({
  credential: admin.credential.cert(JSON.parse(fs.readFileSync(process.env.FIREBASE_SERVICE_ACCOUNT_FILE, 'utf8'))),
  storageBucket: 'ytauto-95f91.firebasestorage.app',
});
const db = admin.firestore();
const bucket = admin.storage().bucket();

async function probeFile(filePath) {
  // Two-stage: ffprobe metadata + ffmpeg null-decode pass. The
  // null-decode catches files where headers parse but frames don't —
  // that's the Lambda failure mode.
  return new Promise((resolve) => {
    const proc = spawn(ffprobePath, [
      '-v', 'error',
      '-select_streams', 'a:0',
      '-show_entries', 'stream=codec_name,duration',
      '-of', 'json',
      filePath,
    ], {stdio: ['ignore', 'pipe', 'pipe']});
    let out = '';
    let err = '';
    proc.stdout.on('data', (c) => { out += c.toString(); });
    proc.stderr.on('data', (c) => { err += c.toString(); });
    proc.once('close', (code) => {
      if (code !== 0) return resolve({ok: false, reason: `ffprobe exit ${code}: ${err.slice(0, 200) || '(empty)'}`});
      try {
        const m = JSON.parse(out);
        const stream = (m.streams || [])[0];
        if (!stream) return resolve({ok: false, reason: 'no audio stream'});
        const dur = parseFloat(stream.duration || 0);
        if (!dur || dur < 5) return resolve({ok: false, reason: `duration ${dur}s — too short`});
        // Stage 2: try a full decode to catch frame-data corruption.
        const decode = spawn(ffmpegPath, ['-v', 'error', '-i', filePath, '-f', 'null', '-'], {stdio: ['ignore', 'pipe', 'pipe']});
        let dErr = '';
        decode.stderr.on('data', (c) => { dErr += c.toString(); });
        decode.once('close', (dCode) => {
          if (dCode !== 0) return resolve({ok: false, reason: `decode exit ${dCode}: ${dErr.slice(0, 200) || '(empty)'}`});
          // ffmpeg sometimes returns exit 0 but writes errors to stderr
          if (/Invalid data|Failed to find|Header missing/i.test(dErr)) {
            return resolve({ok: false, reason: `decode warnings: ${dErr.slice(0, 200)}`});
          }
          resolve({ok: true, duration: dur, codec: stream.codec_name});
        });
      } catch (e) {
        resolve({ok: false, reason: `ffprobe parse: ${e.message}`});
      }
    });
  });
}

(async () => {
  const snap = await db.collection('globalMusicTracks').get();
  console.log(`Validating ${snap.size} tracks${DELETE_BROKEN ? ' (will delete broken)' : ' (DRY RUN — pass --delete-broken to delete)'}`);
  console.log('─'.repeat(80));

  let okCount = 0;
  let brokenCount = 0;
  const broken = [];

  for (const doc of snap.docs) {
    const d = doc.data();
    const tmp = path.join(os.tmpdir(), `validate-${doc.id}.mp3`);
    try {
      const resp = await fetch(d.url);
      if (!resp.ok) {
        broken.push({doc, reason: `download HTTP ${resp.status}`});
        brokenCount++;
        console.log(`  ✗ ${d.name.slice(0, 50).padEnd(50)} download HTTP ${resp.status}`);
        continue;
      }
      await pipeline(Readable.fromWeb(resp.body), fs.createWriteStream(tmp));
      const result = await probeFile(tmp);
      if (result.ok) {
        okCount++;
        console.log(`  ✓ ${d.name.slice(0, 50).padEnd(50)} ${result.duration.toFixed(0)}s ${result.codec}`);
      } else {
        broken.push({doc, reason: result.reason});
        brokenCount++;
        console.log(`  ✗ ${d.name.slice(0, 50).padEnd(50)} ${result.reason.slice(0, 60)}`);
      }
    } catch (e) {
      broken.push({doc, reason: e.message});
      brokenCount++;
      console.log(`  ✗ ${d.name.slice(0, 50).padEnd(50)} ${e.message}`);
    } finally {
      try { fs.unlinkSync(tmp); } catch { /* ignore */ }
    }
  }

  console.log('─'.repeat(80));
  console.log(`OK:     ${okCount}`);
  console.log(`Broken: ${brokenCount}`);

  if (brokenCount > 0 && DELETE_BROKEN) {
    console.log('\n=== Deleting broken tracks ===');
    for (const {doc, reason} of broken) {
      const d = doc.data();
      try { await bucket.file(d.path).delete(); } catch (e) { console.warn(`  warn deleting storage ${d.path}: ${e.message}`); }
      await doc.ref.delete();
      console.log(`  deleted ${d.name} (${reason.slice(0, 50)})`);
    }
  } else if (brokenCount > 0) {
    console.log('\n(Re-run with --delete-broken to remove these.)');
  }
  process.exit(brokenCount > 0 && !DELETE_BROKEN ? 2 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
