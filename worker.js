// worker.js — background processor for editingJobs (Phase 2: fake steps)
//
// Polls Firestore for pending editing jobs and walks them through fake
// stages (sourcing → captions → rendering → done) with hardcoded delays.
// This validates the end-to-end orchestration; real Pexels/Pixabay/
// AssemblyAI/Remotion calls land in phase 3.
//
// On 'done' the worker atomically writes:
//   - the editingJobs doc (status='done', resultUrl)
//   - the project's editingFile + status.editing='awaiting_approval'
//   - a notification for the project owner
// so the existing pipeline picks up the result the same way it would
// pick up a worker-submitted edit.
//
// Run locally:
//   1. Download a Firebase service account JSON from Firebase Console →
//      Project Settings → Service accounts → "Generate new private key".
//      Save it outside the repo, e.g. C:\Users\fiffa\firebase-key.json
//   2. PowerShell:
//        $env:WORKER_ENABLED = "true"
//        $env:FIREBASE_SERVICE_ACCOUNT_FILE = "C:\Users\fiffa\firebase-key.json"
//        npm run worker
//   3. Logs print to stdout. Stop with Ctrl+C.

const fs = require('fs');
const admin = require('firebase-admin');

if (process.env.WORKER_ENABLED !== 'true') {
  console.log('Worker disabled. Set WORKER_ENABLED=true to run.');
  process.exit(0);
}

// Load service account from inline JSON (Render production) or from a file
// path (local dev — much easier to manage than a multi-line env var).
let svc;
try {
  if (process.env.FIREBASE_SERVICE_ACCOUNT_FILE) {
    svc = JSON.parse(fs.readFileSync(process.env.FIREBASE_SERVICE_ACCOUNT_FILE, 'utf8'));
  } else if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    svc = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  } else {
    console.error('FIREBASE_SERVICE_ACCOUNT or FIREBASE_SERVICE_ACCOUNT_FILE env var required.');
    process.exit(1);
  }
} catch (e) {
  console.error('Could not load service account:', e.message);
  process.exit(1);
}

admin.initializeApp({ credential: admin.credential.cert(svc) });
const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

const POLL_INTERVAL_MS = 30 * 1000;
const TEST_RESULT_URL = process.env.TEST_RESULT_URL ||
  'https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function processJob(job) {
  const jobRef = db.collection('editingJobs').doc(job.id);
  const projRef = db.collection('channels').doc(job.channelId)
    .collection('projects').doc(job.projectId);

  console.log(`[${job.id}] processing project "${job.projectTitle || job.projectId}"`);

  // Step 1: footage sourcing (fake — phase 3 will call Pexels + Pixabay + Replicate)
  await jobRef.update({
    status: 'sourcing',
    currentStep: 'Searching stock footage',
    progress: 10,
    updatedAt: FieldValue.serverTimestamp(),
  });
  await sleep(5000);

  // Step 2: caption generation (fake — phase 3 will call AssemblyAI)
  await jobRef.update({
    status: 'captions',
    currentStep: 'Generating word-level captions',
    progress: 40,
    updatedAt: FieldValue.serverTimestamp(),
  });
  await sleep(5000);

  // Step 3: render (fake — phase 4 will call Remotion Lambda)
  await jobRef.update({
    status: 'rendering',
    currentStep: 'Rendering video',
    progress: 70,
    updatedAt: FieldValue.serverTimestamp(),
  });
  await sleep(10000);

  // Step 4: done — atomic batch so the project + job + notification all land together
  const batch = db.batch();
  batch.update(jobRef, {
    status: 'done',
    currentStep: 'Complete',
    progress: 100,
    resultUrl: TEST_RESULT_URL,
    updatedAt: FieldValue.serverTimestamp(),
  });
  batch.update(projRef, {
    editingFile: {
      url: TEST_RESULT_URL,
      path: '',
      name: 'auto-edit.mp4',
      size: 0,
      type: 'video/mp4',
    },
    editingNote: 'Auto-edited by AI — review carefully',
    'status.editing': 'awaiting_approval',
  });
  if (job.ownerId) {
    const notifRef = db.collection('users').doc(job.ownerId)
      .collection('notifications').doc();
    const title = (job.projectTitle || 'project').slice(0, 30);
    batch.set(notifRef, {
      type: 'submission',
      message: `🤖 AI edit complete for "${title}" — ready to review`,
      fromName: 'AI Editor',
      link: null,
      read: false,
      ts: FieldValue.serverTimestamp(),
    });
  }
  await batch.commit();
  console.log(`[${job.id}] done`);
}

async function tick() {
  try {
    const snap = await db.collection('editingJobs')
      .where('status', '==', 'pending')
      .orderBy('createdAt', 'asc')
      .limit(1)
      .get();
    if (snap.empty) return;
    const doc = snap.docs[0];
    const job = { id: doc.id, ...doc.data() };
    try {
      await processJob(job);
    } catch (e) {
      console.error(`[${job.id}] failed:`, e);
      await db.collection('editingJobs').doc(job.id).update({
        status: 'failed',
        currentStep: 'Failed',
        error: e.message || 'Unknown error',
        updatedAt: FieldValue.serverTimestamp(),
      }).catch(err => console.error('Could not mark failed:', err));
    }
  } catch (e) {
    console.error('Poll failed:', e.message);
    if (/index/i.test(e.message || '')) {
      console.error('  → Firestore needs a composite index on editingJobs (status asc + createdAt asc).');
      console.error('  → The error message above usually contains a one-click link to create it.');
    }
  }
}

console.log('Worker started.');
console.log(`  Poll interval: ${POLL_INTERVAL_MS / 1000}s`);
console.log(`  Test result URL: ${TEST_RESULT_URL}`);

(async function loop() {
  while (true) {
    await tick();
    await sleep(POLL_INTERVAL_MS);
  }
})().catch(e => {
  console.error('Worker crashed:', e);
  process.exit(1);
});
