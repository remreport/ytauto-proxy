// scripts/seed-test-job.js
//
// Programmatically inserts an editingJobs doc for a given channel+project,
// pins it to that project as editingJobId, and tails the doc until the
// background worker either finishes or fails it. Used for end-to-end
// testing without clicking the UI button.
//
// Usage:
//   FIREBASE_SERVICE_ACCOUNT_FILE=C:\Users\fiffa\firebase-key.json \
//     node scripts/seed-test-job.js [channelName] [projectTitle]
//
// Defaults: channelName="Rem Report", projectTitle="dfg"

const fs = require('fs');
const admin = require('firebase-admin');

const channelName = process.argv[2] || 'Rem Report';
const projectTitle = process.argv[3] || 'dfg';

const svcPath = process.env.FIREBASE_SERVICE_ACCOUNT_FILE;
if (!svcPath) {
  console.error('Set FIREBASE_SERVICE_ACCOUNT_FILE env var to your Firebase key path.');
  process.exit(1);
}
const svc = JSON.parse(fs.readFileSync(svcPath, 'utf8'));
admin.initializeApp({ credential: admin.credential.cert(svc) });

const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

(async () => {
  // Find the channel
  const chSnap = await db.collection('channels').where('name', '==', channelName).get();
  if (chSnap.empty) {
    console.error(`No channel found with name="${channelName}"`);
    process.exit(1);
  }
  const channel = chSnap.docs[0];
  console.log(`Channel:  ${channel.id}  (${channel.data().name})`);
  const ownerId = channel.data().ownerId;

  // Find the project
  const projSnap = await db.collection('channels').doc(channel.id)
    .collection('projects').where('title', '==', projectTitle).get();
  if (projSnap.empty) {
    console.error(`No project found in "${channelName}" with title="${projectTitle}"`);
    process.exit(1);
  }
  const project = projSnap.docs[0];
  const proj = project.data();
  console.log(`Project:  ${project.id}  (${proj.title})`);
  console.log(`  voiceover: ${proj.voiceoverFile?.url ? 'present' : 'MISSING (fake worker proceeds anyway)'}`);
  console.log(`  status.editing: ${proj.status?.editing || 'pending'}`);

  // Create the job
  const jobRef = db.collection('editingJobs').doc();
  await jobRef.set({
    channelId: channel.id,
    projectId: project.id,
    ownerId,
    projectTitle: proj.title || 'project',
    voiceoverUrl: proj.voiceoverFile?.url || '',
    scriptText: proj.script || '',
    status: 'pending',
    progress: 0,
    currentStep: 'Queued',
    error: null,
    footageUrls: [],
    captions: null,
    musicUrl: null,
    resultUrl: null,
    createdAt: FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
  });
  console.log(`\nCreated editingJob: ${jobRef.id}`);

  // Pin to project (matches what the UI does on kickOffAiEdit)
  await db.collection('channels').doc(channel.id)
    .collection('projects').doc(project.id)
    .update({ editingJobId: jobRef.id });
  console.log('Pinned editingJobId on project');

  // Tail the job until done/failed
  console.log(`\nWatching ${jobRef.id} (worker polls every 30s, may wait up to that long for first pickup)...`);
  let lastTag = '';
  const startedAt = Date.now();
  for (let i = 0; i < 90; i++) {           // 90 * 2s = 180s max
    const snap = await jobRef.get();
    const job = snap.data();
    const tag = `${job.status}/${job.progress || 0}%`;
    if (tag !== lastTag) {
      const elapsed = ((Date.now() - startedAt) / 1000).toFixed(0);
      console.log(`  [+${elapsed}s] ${tag.padEnd(18)} ${job.currentStep || ''}`);
      lastTag = tag;
    }
    if (job.status === 'done' || job.status === 'failed') {
      console.log(`\nFinal: ${job.status}`);
      if (job.resultUrl) console.log(`  resultUrl: ${job.resultUrl}`);
      if (job.error) console.log(`  error: ${job.error}`);
      if (job.footageUrls && job.footageUrls.length) {
        console.log(`  footageUrls (${job.footageUrls.length}):`);
        job.footageUrls.slice(0, 3).forEach((u, idx) => {
          const short = u.length > 100 ? u.slice(0, 97) + '...' : u;
          console.log(`    ${idx + 1}. ${short}`);
        });
        if (job.footageUrls.length > 3) console.log(`    ... and ${job.footageUrls.length - 3} more`);
      }
      if (job.captions && job.captions.length) {
        console.log(`  captions (${job.captions.length} sentences):`);
        job.captions.slice(0, 3).forEach((s, idx) => {
          const text = s.text && s.text.length > 80 ? s.text.slice(0, 77) + '...' : (s.text || '');
          const wc = (s.words || []).length;
          console.log(`    ${idx + 1}. [${s.start.toFixed(1)}s-${s.end.toFixed(1)}s · ${wc} words] ${text}`);
        });
        if (job.captions.length > 3) console.log(`    ... and ${job.captions.length - 3} more`);
      }
      process.exit(job.status === 'done' ? 0 : 2);
    }
    await new Promise((r) => setTimeout(r, 2000));
  }
  console.error('\nTimed out after 180s waiting for completion. Worker may not be running.');
  process.exit(1);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
