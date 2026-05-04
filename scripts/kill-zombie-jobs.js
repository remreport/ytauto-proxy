// scripts/kill-zombie-jobs.js
//
// One-shot: scans editingJobs + discoveryJobs for docs stuck in non-terminal
// status, force-marks them as failed so the UI's Retry button shows up.
// Mirrors the zombieJobWatchdog Cloud Function but runs immediately so we
// can clean up after a Cloud Function crash before the next watchdog tick.
//
// Usage:
//   FIREBASE_SERVICE_ACCOUNT_FILE=...firebase-key.json node scripts/kill-zombie-jobs.js [maxAgeMinutes]
//
// Default cutoff: 10 minutes since last updatedAt.

const fs = require('fs');
const admin = require('firebase-admin');

const maxAgeMin = parseInt(process.argv[2] || '10', 10);

const svcPath = process.env.FIREBASE_SERVICE_ACCOUNT_FILE;
if (!svcPath) {
  console.error('Set FIREBASE_SERVICE_ACCOUNT_FILE');
  process.exit(1);
}
admin.initializeApp({credential: admin.credential.cert(JSON.parse(fs.readFileSync(svcPath, 'utf8')))});
const db = admin.firestore();
db.settings({ignoreUndefinedProperties: true});
const FieldValue = admin.firestore.FieldValue;

(async () => {
  const cutoffMs = Date.now() - maxAgeMin * 60 * 1000;

  async function sweep(collection, activeStatuses) {
    const snap = await db.collection(collection)
      .where('status', 'in', activeStatuses)
      .get();
    let killed = 0;
    for (const doc of snap.docs) {
      const data = doc.data();
      const updatedMs = data.updatedAt?.toMillis ? data.updatedAt.toMillis() : 0;
      const ageMin = updatedMs ? Math.round((Date.now() - updatedMs) / 60000) : '?';
      const stuck = !updatedMs || updatedMs < cutoffMs;
      if (!stuck) continue;
      console.log(`  killing ${collection}/${doc.id}  status=${data.status}  age=${ageMin}min`);
      await doc.ref.update({
        status: 'failed',
        currentStep: 'Failed',
        error: `Job force-killed by zombie sweep (was stuck in ${data.status} for ${ageMin}min, likely a Cloud Function crash)`,
        updatedAt: FieldValue.serverTimestamp(),
      });
      killed++;
    }
    return killed;
  }

  console.log(`Sweeping jobs older than ${maxAgeMin} minutes...`);
  const e = await sweep('editingJobs', ['pending', 'researching', 'sourcing', 'captions', 'rendering']);
  const d = await sweep('discoveryJobs', ['pending', 'researching', 'proposing']);
  console.log(`Done. Killed ${e} editingJobs + ${d} discoveryJobs.`);
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
