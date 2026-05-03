// scripts/seed-discovery-job.js
//
// End-to-end test for the discovery pipeline. Sets the channel's niche
// (if missing), creates an empty project with discoveryStatus='pending',
// inserts a discoveryJobs doc to fire the Cloud Function, then tails
// the project doc until discoveryStatus='proposing' and prints the
// pickedTopic + pickedTitle + variants.
//
// Usage:
//   FIREBASE_SERVICE_ACCOUNT_FILE=...firebase-key.json node scripts/seed-discovery-job.js [channelName] [niche]
//
// Defaults: channelName="Rem Report", niche="finance news"

const fs = require('fs');
const admin = require('firebase-admin');

const channelName = process.argv[2] || 'Rem Report';
const wantNiche = process.argv[3] || 'finance news';

const svcPath = process.env.FIREBASE_SERVICE_ACCOUNT_FILE;
if (!svcPath) {
  console.error('Set FIREBASE_SERVICE_ACCOUNT_FILE env var.');
  process.exit(1);
}
admin.initializeApp({credential: admin.credential.cert(JSON.parse(fs.readFileSync(svcPath, 'utf8')))});
const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

(async () => {
  const chSnap = await db.collection('channels').where('name', '==', channelName).get();
  if (chSnap.empty) throw new Error(`No channel "${channelName}"`);
  const channel = chSnap.docs[0];
  const ch = channel.data();
  console.log(`Channel: ${channel.id}  (${ch.name})`);

  if (ch.niche !== wantNiche) {
    await channel.ref.update({niche: wantNiche});
    console.log(`Set niche to "${wantNiche}"`);
  } else {
    console.log(`Niche already: "${ch.niche}"`);
  }

  // Create empty project
  const projRef = db.collection('channels').doc(channel.id).collection('projects').doc();
  await projRef.set({
    title: '',
    topic: '',
    status: {discover: 'in_progress', script: 'pending', voiceover: 'pending', editing: 'pending', thumbnail: 'pending', publish: 'pending'},
    created: new Date().toISOString().slice(0, 10),
    voiceoverPerson: '', editingPerson: '', thumbnailPerson: '',
    script: '', ytTitle: '', ytDesc: '', notes: '',
    discoveryStatus: 'pending',
    createdAt: FieldValue.serverTimestamp(),
  });
  console.log(`Project: ${projRef.id}  (empty — AI will fill it in)`);

  // Create discoveryJob
  const jobRef = db.collection('discoveryJobs').doc();
  await jobRef.set({
    channelId: channel.id,
    projectId: projRef.id,
    ownerId: ch.ownerId,
    niche: wantNiche,
    status: 'pending',
    progress: 0,
    currentStep: 'Queued',
    error: null,
    createdAt: FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
  });
  console.log(`Job: ${jobRef.id}\n`);

  // Tail
  console.log(`Watching project ${projRef.id} for discoveryStatus changes...\n`);
  const startedAt = Date.now();
  let lastTag = '';
  for (let i = 0; i < 90; i++) {
    const snap = await projRef.get();
    const p = snap.data();
    const tag = p.discoveryStatus || '?';
    if (tag !== lastTag) {
      const elapsed = ((Date.now() - startedAt) / 1000).toFixed(0);
      console.log(`  [+${elapsed}s] discoveryStatus=${tag}`);
      lastTag = tag;
    }
    if (tag === 'proposing') {
      console.log(`\n── PROPOSAL ──`);
      console.log(`Topic:    ${p.pickedTopic}`);
      if (p.pickedTopicSummary) console.log(`Summary:  ${p.pickedTopicSummary}`);
      if (p.pickedTopicReasoning) console.log(`Why:      ${p.pickedTopicReasoning}`);
      if (p.pickedTopicSources?.length) console.log(`Sources:  ${p.pickedTopicSources.join(', ')}`);
      console.log(`\nPicked title: ${p.pickedTitle}`);
      console.log(`\nAll ${p.titleVariants?.length || 0} variants:`);
      (p.titleVariants || []).forEach((v, idx) => {
        const star = v.title === p.pickedTitle ? '★' : ' ';
        console.log(`  ${star} [${v.ctrPrediction || '?'.padEnd(11)}] ${v.title}`);
        if (v.reasoning) console.log(`      ${v.reasoning}`);
      });
      process.exit(0);
    }
    if (tag === 'failed') {
      console.error(`\nFailed: ${p.discoveryError}`);
      process.exit(2);
    }
    await new Promise((r) => setTimeout(r, 2000));
  }
  console.error('Timed out after 180s');
  process.exit(1);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
