// Filename-heuristic auto-categorizer for Pixabay-style MP3s in
// Downloads/. Walks every .mp3 modified in the last 24h, matches the
// filename against per-mood keyword lists, picks a mood by priority,
// generates a CSV, and pipes that into bulk-import-music.js.
//
// Trade-off: ~80-90% accuracy vs zero clicks. Tracks miscategorized
// here can be deleted from the global library admin UI later.

const fs = require('node:fs');
const path = require('node:path');
const {spawnSync} = require('node:child_process');

const DOWNLOADS = process.env.DOWNLOADS_DIR
  || path.join(process.env.USERPROFILE || require('node:os').homedir(), 'Downloads');
const HOURS_BACK = parseInt(process.env.HOURS_BACK || '24', 10);
const CSV_OUTPUT = path.join(__dirname, 'categorize-output.csv');

// Per-mood keyword sets. Compared against lowercase filename. Hyphenated
// keywords like "epic-tension" only match when those words appear with
// any separator (we normalize separators before matching).
const MOOD_KEYWORDS = {
  calm:        ['peaceful','calm','ambient','meditation','soft','quiet','gentle','piano','relaxing','chill','soothing','lofi','sleep','zen'],
  energetic:   ['upbeat','energy','energetic','motivational','workout','action','fast','dance','electronic','pop','rock','party','drive'],
  dramatic:    ['dramatic','tension','intense','suspense','build','trailer','climax','war','battle'],
  mysterious:  ['mystery','mysterious','dark','eerie','creepy','horror','occult'],
  uplifting:   ['uplifting','hopeful','positive','inspirational','happy','joy','sunshine','victory'],
  corporate:   ['corporate','business','technology','professional','modern','clean','presentation','marketing'],
  cinematic:   ['cinematic','epic','orchestral','hero','film','score','soundtrack','hollywood','adventure'],
};

// Tie-break order — most specific first. cinematic catches "epic
// cinematic trailer"; dramatic catches "intense tension build" without
// stealing cinematic from "epic dramatic"; calm is the generic fallback.
const PRIORITY = ['cinematic', 'dramatic', 'mysterious', 'energetic', 'uplifting', 'corporate', 'calm'];
const DEFAULT_MOOD = 'cinematic';

function classifyByFilename(filename) {
  const lower = filename.toLowerCase().replace(/[_-]+/g, ' ');
  const matched = new Set();
  for (const [mood, keywords] of Object.entries(MOOD_KEYWORDS)) {
    for (const kw of keywords) {
      // word-boundary-ish: keyword surrounded by non-word OR start/end
      const re = new RegExp(`(?:^|\\s)${kw.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\$&').replace(/-/g, ' ')}(?:\\s|$)`);
      if (re.test(lower)) { matched.add(mood); break; }
    }
  }
  if (matched.size === 0) return {mood: DEFAULT_MOOD, matched: [], wasDefault: true};
  // Resolve by priority
  for (const m of PRIORITY) {
    if (matched.has(m)) return {mood: m, matched: [...matched], wasDefault: false};
  }
  return {mood: DEFAULT_MOOD, matched: [...matched], wasDefault: true};
}

function cleanName(filename) {
  let s = filename.replace(/\.mp3$/i, '');
  // Strip trailing -NNN or _NNN chains (Pixabay ID suffixes)
  s = s.replace(/(?:[-_]\d+)+$/g, '');
  // Replace separators with spaces, collapse, title-case
  return s.replace(/[_-]+/g, ' ').replace(/\s+/g, ' ').trim()
    .replace(/\b\w/g, (c) => c.toUpperCase()).slice(0, 60);
}

function csvEscape(s) {
  s = String(s);
  return /[,"\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

(async () => {
  if (!fs.existsSync(DOWNLOADS)) {
    console.error(`Downloads folder not found: ${DOWNLOADS}`);
    process.exit(1);
  }
  const cutoff = Date.now() - HOURS_BACK * 60 * 60 * 1000;
  const files = [];
  for (const name of fs.readdirSync(DOWNLOADS)) {
    if (!name.toLowerCase().endsWith('.mp3')) continue;
    const full = path.join(DOWNLOADS, name);
    let stat;
    try { stat = fs.statSync(full); } catch { continue; }
    if (stat.mtimeMs < cutoff) continue;
    files.push({name, path: full, mtimeMs: stat.mtimeMs, sizeBytes: stat.size});
  }

  if (files.length === 0) {
    console.log(`No MP3s found in ${DOWNLOADS} (last ${HOURS_BACK}h)`);
    process.exit(0);
  }

  console.log(`Folder:     ${DOWNLOADS}`);
  console.log(`MP3s found: ${files.length} (last ${HOURS_BACK}h)\n`);
  console.log('Filename → mood:');
  console.log('─'.repeat(80));

  const counts = Object.fromEntries(PRIORITY.map((m) => [m, 0]));
  const rows = [['url', 'name', 'mood', 'attribution', 'source']];
  let defaulted = 0;

  for (const f of files) {
    const {mood, matched, wasDefault} = classifyByFilename(f.name);
    const cleaned = cleanName(f.name);
    counts[mood]++;
    if (wasDefault) defaulted++;
    const flag = wasDefault ? ' [no-match→default]' : matched.length > 1 ? ` [tie:${matched.join('+')}]` : '';
    console.log(`  ${mood.padEnd(11)} ${cleaned.padEnd(50)}${flag}`);
    rows.push([f.path, cleaned, mood, 'Pixabay Music — CC0 (no attribution required)', 'pixabay']);
  }

  fs.writeFileSync(CSV_OUTPUT, rows.map((r) => r.map(csvEscape).join(',')).join('\n'));
  console.log('\n=== Mood distribution ===');
  for (const m of PRIORITY) {
    const mark = counts[m] >= 3 ? '✓' : counts[m] >= 1 ? '⚠' : '✗';
    console.log(`  ${mark} ${m.padEnd(12)} ${String(counts[m]).padStart(2)} tracks`);
  }
  if (defaulted > 0) console.log(`\n  ${defaulted}/${files.length} defaulted to '${DEFAULT_MOOD}' (no keywords matched)`);
  console.log(`\nWrote ${CSV_OUTPUT}\n`);

  console.log('=== Running bulk-import-music.js ===');
  const importer = path.join(__dirname, 'bulk-import-music.js');
  const ret = spawnSync('node', [importer, CSV_OUTPUT], {stdio: 'inherit', env: process.env});
  if (ret.status !== 0) {
    console.error(`\nbulk-import exited with code ${ret.status}`);
    process.exit(ret.status || 1);
  }

  // Coverage report from Firestore
  console.log('\n=== Final mood coverage in globalMusicTracks ===');
  if (!process.env.FIREBASE_SERVICE_ACCOUNT_FILE) {
    console.log('(skipping live count — set FIREBASE_SERVICE_ACCOUNT_FILE)');
    process.exit(0);
  }
  const adminPath = path.join(__dirname, '..', 'functions', 'node_modules', 'firebase-admin');
  const admin = require(adminPath);
  if (!admin.apps.length) {
    admin.initializeApp({
      credential: admin.credential.cert(JSON.parse(fs.readFileSync(process.env.FIREBASE_SERVICE_ACCOUNT_FILE, 'utf8'))),
    });
  }
  const snap = await admin.firestore().collection('globalMusicTracks').get();
  const live = Object.fromEntries(PRIORITY.map((m) => [m, 0]));
  for (const d of snap.docs) {
    const m = d.data().mood;
    if (m in live) live[m]++;
  }
  for (const m of PRIORITY) {
    const n = live[m];
    const mark = n >= 3 ? '✓' : n >= 1 ? '⚠' : '✗';
    console.log(`  ${mark} ${m.padEnd(12)} ${String(n).padStart(2)} tracks ${n >= 3 ? '' : '(target ≥3)'}`);
  }
  console.log(`\nTotal in library: ${snap.size}`);
  process.exit(0);
})().catch((e) => { console.error(e); process.exit(1); });
