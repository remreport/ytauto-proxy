// Backfill integratedLoudness (LUFS) on existing globalMusicTracks
// docs that don't have it. Reads each track from Firebase Storage,
// runs ebur128, writes integratedLoudness back to the doc.
//
// Idempotent — safe to re-run; skips tracks that already have LUFS.
//
// Usage:
//   FIREBASE_SERVICE_ACCOUNT_FILE=C:\Users\fiffa\firebase-key.json \
//     node scripts/backfill-music-loudness.js

const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const {pipeline} = require('node:stream/promises');
const {Readable} = require('node:stream');
const {spawn} = require('node:child_process');

const adminPath = path.join(__dirname, '..', 'functions', 'node_modules', 'firebase-admin');
const admin = require(adminPath);
const ffmpegPath = require(path.join(__dirname, '..', 'functions', 'node_modules', '@ffmpeg-installer', 'ffmpeg')).path;

if (!process.env.FIREBASE_SERVICE_ACCOUNT_FILE) { console.error('Set FIREBASE_SERVICE_ACCOUNT_FILE'); process.exit(1); }
admin.initializeApp({
  credential: admin.credential.cert(JSON.parse(fs.readFileSync(process.env.FIREBASE_SERVICE_ACCOUNT_FILE, 'utf8'))),
});
const db = admin.firestore();

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

(async () => {
  const snap = await db.collection('globalMusicTracks').get();
  console.log(`${snap.size} tracks total. Backfilling missing LUFS…`);
  let updated = 0;
  let skipped = 0;
  let failed = 0;
  for (const doc of snap.docs) {
    const d = doc.data();
    if (typeof d.integratedLoudness === 'number') {
      skipped++;
      continue;
    }
    const tmp = path.join(os.tmpdir(), `lufs-${doc.id}.mp3`);
    try {
      const resp = await fetch(d.url);
      if (!resp.ok) throw new Error(`download HTTP ${resp.status}`);
      await pipeline(Readable.fromWeb(resp.body), fs.createWriteStream(tmp));
      const lufs = await measureLufs(tmp);
      if (lufs == null) throw new Error('ebur128 returned no result');
      await doc.ref.update({integratedLoudness: lufs});
      console.log(`  ✓ ${(d.name || '').slice(0, 50).padEnd(50)} ${lufs.toFixed(1)} LUFS`);
      updated++;
    } catch (e) {
      console.warn(`  ✗ ${(d.name || '').slice(0, 50).padEnd(50)} ${e.message}`);
      failed++;
    } finally {
      try { fs.unlinkSync(tmp); } catch { /* ignore */ }
    }
  }
  console.log(`\n--- summary ---`);
  console.log(`updated: ${updated}`);
  console.log(`already had LUFS: ${skipped}`);
  console.log(`failed: ${failed}`);
  process.exit(failed > 0 ? 2 : 0);
})().catch((err) => { console.error(err); process.exit(1); });
