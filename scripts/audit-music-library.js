// Dumps every music track visible to the renderer (global + per-channel),
// flagging each row's licensing state and whether it's currently caught
// by MUSIC_TITLE_BLOCKLIST in functions/index.js. Output is a flat list
// the user can scan to identify which tracks have hit Content ID claims
// so the title list can be extended.
//
// Usage:
//   FIREBASE_SERVICE_ACCOUNT_FILE=/path/to/key.json \
//     node scripts/audit-music-library.js [channelId]
//
// channelId is optional; defaults to bjn4AnQm8aToTsROItUB (the test
// channel). Pass any channel ID to audit a different channel's pool.

const fs = require('node:fs');
const path = require('node:path');
const admin = require(path.join(__dirname, '..', 'functions', 'node_modules', 'firebase-admin'));

const svcPath = process.env.FIREBASE_SERVICE_ACCOUNT_FILE;
if (!svcPath) {
  console.error('Set FIREBASE_SERVICE_ACCOUNT_FILE=/path/to/key.json');
  process.exit(1);
}

const channelId = process.argv[2] || 'bjn4AnQm8aToTsROItUB';

admin.initializeApp({
  credential: admin.credential.cert(JSON.parse(fs.readFileSync(svcPath, 'utf8'))),
  storageBucket: 'ytauto-95f91.firebasestorage.app',
});
const db = admin.firestore();

// Mirror the blocklist in functions/index.js so this report shows
// exactly which rows the renderer would currently exclude.
const MUSIC_TITLE_BLOCKLIST = new Set([
  'Inspirational Cinematic Orchestra',
]);

function isUsable(t) {
  if (!t) return {usable: false, reason: 'null doc'};
  if (t.licensingStatus === 'claimed') return {usable: false, reason: 'licensingStatus=claimed'};
  if (t.claimed === true) return {usable: false, reason: 'claimed=true (legacy)'};
  const tn = t.title || t.name;
  if (tn && MUSIC_TITLE_BLOCKLIST.has(tn)) return {usable: false, reason: 'in MUSIC_TITLE_BLOCKLIST'};
  return {usable: true, reason: ''};
}

function row(scope, doc) {
  const d = doc.data();
  const u = isUsable(d);
  return {
    scope,
    id: doc.id,
    title: d.title || d.name || '(unnamed)',
    mood: d.mood || '?',
    duration: typeof d.duration === 'number' ? `${Math.round(d.duration)}s` : '?',
    licensingStatus: d.licensingStatus || '',
    usable: u.usable ? 'YES' : `NO (${u.reason})`,
    url: (d.url || '').slice(0, 80),
  };
}

(async () => {
  const [globalSnap, channelSnap] = await Promise.all([
    db.collection('globalMusicTracks').get(),
    db.collection('channels').doc(channelId).collection('musicTracks').get(),
  ]);

  const rows = [];
  for (const doc of globalSnap.docs) rows.push(row('global', doc));
  for (const doc of channelSnap.docs) rows.push(row(`channel:${channelId}`, doc));

  // Sort: usable first then by scope+title, so the at-risk rows surface
  // at the top of the "NO" section for quick scanning.
  rows.sort((a, b) => {
    if (a.usable !== b.usable) return a.usable.startsWith('NO') ? -1 : 1;
    if (a.scope !== b.scope) return a.scope.localeCompare(b.scope);
    return a.title.localeCompare(b.title);
  });

  const totalGlobal = globalSnap.size;
  const totalChannel = channelSnap.size;
  const usable = rows.filter((r) => r.usable === 'YES').length;
  const blocked = rows.length - usable;

  console.log(`Music library audit — channel ${channelId}`);
  console.log(`Global tracks: ${totalGlobal} · Channel tracks: ${totalChannel} · Total: ${rows.length}`);
  console.log(`Usable for selection: ${usable} · Blocked / claimed: ${blocked}\n`);

  // Column-aligned text output for terminal scanning.
  const widths = {
    scope: Math.max(8, ...rows.map((r) => r.scope.length)),
    id: Math.max(20, ...rows.map((r) => r.id.length)),
    title: Math.min(60, Math.max(20, ...rows.map((r) => r.title.length))),
    mood: 12,
    duration: 8,
    usable: 30,
  };
  const pad = (s, w) => (s.length > w ? s.slice(0, w - 1) + '…' : s.padEnd(w));
  const header = `${pad('SCOPE', widths.scope)}  ${pad('ID', widths.id)}  ${pad('TITLE', widths.title)}  ${pad('MOOD', widths.mood)}  ${pad('DUR', widths.duration)}  ${pad('USABLE?', widths.usable)}`;
  console.log(header);
  console.log('-'.repeat(header.length));
  for (const r of rows) {
    console.log(`${pad(r.scope, widths.scope)}  ${pad(r.id, widths.id)}  ${pad(r.title, widths.title)}  ${pad(r.mood, widths.mood)}  ${pad(r.duration, widths.duration)}  ${pad(r.usable, widths.usable)}`);
  }

  process.exit(0);
})().catch((e) => {
  console.error('Audit failed:', e.message);
  process.exit(1);
});
