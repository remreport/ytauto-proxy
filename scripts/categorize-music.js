// Interactive categorizer for music files in Downloads/.
// Walks every MP3 modified in the last 24h, asks for a mood per track,
// writes a CSV, and pipes that into bulk-import-music.js to populate
// globalMusicTracks. Resumes safely if interrupted (progress.json next
// to this script).
//
// Usage (PowerShell):
//   $env:FIREBASE_SERVICE_ACCOUNT_FILE = "C:\Users\fiffa\firebase-key.json"
//   node scripts\categorize-music.js
//
// Optional:
//   $env:DOWNLOADS_DIR = "C:\path\to\folder"   # override default
//   $env:HOURS_BACK = "48"                      # widen the time window

const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');
const {spawn, spawnSync} = require('node:child_process');

const DOWNLOADS = process.env.DOWNLOADS_DIR
  || path.join(process.env.USERPROFILE || require('node:os').homedir(), 'Downloads');
const HOURS_BACK = parseInt(process.env.HOURS_BACK || '24', 10);
const PROGRESS_FILE = path.join(__dirname, 'categorize-progress.json');
const CSV_OUTPUT = path.join(__dirname, 'categorize-output.csv');

const MOODS = ['calm', 'energetic', 'dramatic', 'mysterious', 'uplifting', 'corporate', 'cinematic'];

// 1. Scan for mp3s modified in window
function scan() {
  const cutoff = Date.now() - HOURS_BACK * 60 * 60 * 1000;
  if (!fs.existsSync(DOWNLOADS)) {
    console.error(`Downloads folder not found: ${DOWNLOADS}`);
    process.exit(1);
  }
  const out = [];
  for (const name of fs.readdirSync(DOWNLOADS)) {
    if (!name.toLowerCase().endsWith('.mp3')) continue;
    const full = path.join(DOWNLOADS, name);
    let stat;
    try { stat = fs.statSync(full); } catch { continue; }
    if (stat.mtimeMs < cutoff) continue;
    out.push({name, path: full, mtimeMs: stat.mtimeMs, sizeBytes: stat.size});
  }
  return out.sort((a, b) => a.mtimeMs - b.mtimeMs);
}

function loadProgress() {
  if (!fs.existsSync(PROGRESS_FILE)) return {};
  try { return JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8')); } catch { return {}; }
}

function saveProgress(p) { fs.writeFileSync(PROGRESS_FILE, JSON.stringify(p, null, 2)); }

function cleanName(filename) {
  return filename
    .replace(/\.mp3$/i, '')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .slice(0, 60);
}

function csvEscape(s) {
  s = String(s);
  return /[,"\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function previewFile(filePath) {
  // Open with the default media player. Detached + ignore stdio so the
  // player doesn't block the prompt.
  spawn('cmd', ['/c', 'start', '""', `"${filePath}"`], {
    shell: true,
    stdio: 'ignore',
    detached: true,
  }).unref();
}

(async () => {
  const files = scan();
  if (files.length === 0) {
    console.log(`No MP3s found in ${DOWNLOADS} in the last ${HOURS_BACK}h`);
    process.exit(0);
  }
  const progress = loadProgress();
  const remaining = files.filter((f) => !progress[f.name]).length;

  console.log(`Folder:     ${DOWNLOADS}`);
  console.log(`MP3s found: ${files.length} (modified within ${HOURS_BACK}h)`);
  console.log(`Already categorized: ${files.length - remaining}`);
  console.log(`Remaining:           ${remaining}\n`);
  console.log('Per file: type 1-7 for mood, 8 to skip (not music), P to play preview, Q to quit-and-save\n');

  const rl = readline.createInterface({input: process.stdin, output: process.stdout});
  const ask = (q) => new Promise((r) => rl.question(q, r));

  for (let i = 0; i < files.length; i++) {
    const f = files[i];
    if (progress[f.name]) {
      console.log(`[${i + 1}/${files.length}] ${f.name} → already ${progress[f.name]}`);
      continue;
    }
    let resolved = false;
    while (!resolved) {
      console.log(`\n[${i + 1}/${files.length}] ${f.name}`);
      console.log(`  size: ${(f.sizeBytes / 1024 / 1024).toFixed(1)} MB · modified: ${new Date(f.mtimeMs).toLocaleString()}`);
      console.log('    1) calm        2) energetic     3) dramatic    4) mysterious');
      console.log('    5) uplifting   6) corporate     7) cinematic   8) skip');
      console.log('    P) preview in default player    Q) quit & save');
      const ans = (await ask('  → ')).trim().toLowerCase();
      if (ans === 'q') {
        saveProgress(progress);
        console.log('Saved progress. Run again to resume.');
        rl.close();
        process.exit(0);
      }
      if (ans === 'p') {
        previewFile(f.path);
        console.log('  (player opened — make a choice when ready)');
        continue;
      }
      const n = parseInt(ans, 10);
      if (n >= 1 && n <= 7) {
        progress[f.name] = MOODS[n - 1];
        console.log(`  ✓ ${MOODS[n - 1]}`);
        saveProgress(progress);
        resolved = true;
      } else if (n === 8) {
        progress[f.name] = 'skip';
        console.log('  ✓ skipped');
        saveProgress(progress);
        resolved = true;
      } else {
        console.log('  invalid — try 1-8, P, or Q');
      }
    }
  }

  rl.close();
  console.log('\n=== All categorized ===');

  // Build the CSV
  const rows = [['url', 'name', 'mood', 'attribution', 'source']];
  let kept = 0;
  let skipped = 0;
  for (const f of files) {
    const mood = progress[f.name];
    if (!mood || mood === 'skip') { skipped++; continue; }
    rows.push([f.path, cleanName(f.name), mood, 'Pixabay Music — CC0 (no attribution required)', 'pixabay']);
    kept++;
  }
  fs.writeFileSync(CSV_OUTPUT, rows.map((r) => r.map(csvEscape).join(',')).join('\n'));
  console.log(`Wrote CSV: ${CSV_OUTPUT}`);
  console.log(`  kept:    ${kept}`);
  console.log(`  skipped: ${skipped}\n`);

  if (kept === 0) {
    console.log('Nothing to import — exiting.');
    process.exit(0);
  }

  // Run bulk-import-music.js
  console.log('=== Running bulk-import-music.js ===');
  const importer = path.join(__dirname, 'bulk-import-music.js');
  const ret = spawnSync('node', [importer, CSV_OUTPUT], {stdio: 'inherit', env: process.env});
  if (ret.status !== 0) {
    console.log(`\nbulk-import exited with code ${ret.status} — check output above`);
    process.exit(ret.status || 1);
  }

  // Coverage report (Firestore query)
  console.log('\n=== Mood coverage in globalMusicTracks ===');
  const adminPath = path.join(__dirname, '..', 'functions', 'node_modules', 'firebase-admin');
  const admin = require(adminPath);
  if (!admin.apps.length) {
    if (!process.env.FIREBASE_SERVICE_ACCOUNT_FILE) {
      console.log('(skipping coverage report — set FIREBASE_SERVICE_ACCOUNT_FILE to enable)');
      process.exit(0);
    }
    admin.initializeApp({
      credential: admin.credential.cert(JSON.parse(fs.readFileSync(process.env.FIREBASE_SERVICE_ACCOUNT_FILE, 'utf8'))),
    });
  }
  const snap = await admin.firestore().collection('globalMusicTracks').get();
  const byMood = Object.fromEntries(MOODS.map((m) => [m, 0]));
  for (const d of snap.docs) {
    const m = d.data().mood;
    if (m in byMood) byMood[m]++;
  }
  for (const m of MOODS) {
    const n = byMood[m];
    const mark = n >= 3 ? '✓' : n >= 1 ? '⚠' : '✗';
    const note = n >= 3 ? '' : ' (target ≥3)';
    console.log(`  ${mark} ${m.padEnd(12)} ${String(n).padStart(2)} tracks${note}`);
  }
  console.log(`\nTotal: ${snap.size} tracks across moods`);
  process.exit(0);
})().catch((e) => {
  console.error('Error:', e);
  process.exit(1);
});
