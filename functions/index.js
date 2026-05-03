// Firebase Cloud Function — AI editing job processor.
//
// Triggered the moment a doc lands in editingJobs/{jobId}. Same code path
// as the local worker.js but event-driven and serverless: no laptop, no
// polling. The frontend creating a job triggers this within ~1s.
//
// Secrets are stored in Google Secret Manager (set via
// `firebase functions:secrets:set NAME --data-file=-`) and bound to the
// function declaration below. Non-secret config (Lambda function name +
// serve URL, AWS region) is hardcoded since it changes rarely.

const {onDocumentCreated} = require('firebase-functions/v2/firestore');
const {defineSecret} = require('firebase-functions/params');
const {setGlobalOptions} = require('firebase-functions/v2');
const admin = require('firebase-admin');
// @remotion/lambda-client is heavy (pulls in AWS SDK) and pushes the
// module-load past Cloud Functions' 10s analysis budget. Lazy-loaded
// inside renderOnLambda below.

setGlobalOptions({region: 'us-central1'});

// Default service account works inside Cloud Functions — no key file needed.
admin.initializeApp({
  storageBucket: 'ytauto-95f91.firebasestorage.app',
});
const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

// ─── Secrets (live in Google Secret Manager) ────────────────────────
const PEXELS_API_KEY = defineSecret('PEXELS_API_KEY');
const ASSEMBLYAI_API_KEY = defineSecret('ASSEMBLYAI_API_KEY');
const REPLICATE_API_KEY = defineSecret('REPLICATE_API_KEY');
const AWS_ACCESS_KEY_ID = defineSecret('AWS_ACCESS_KEY_ID');
const AWS_SECRET_ACCESS_KEY = defineSecret('AWS_SECRET_ACCESS_KEY');

// ─── Non-secret config ──────────────────────────────────────────────
const AWS_REGION = 'us-east-1';
const REMOTION_LAMBDA_FUNCTION_NAME =
  'remotion-render-4-0-456-mem3008mb-disk10240mb-300sec';
const REMOTION_LAMBDA_SERVE_URL =
  'https://remotionlambda-useast1-1zsfacnzw9.s3.us-east-1.amazonaws.com/sites/rem-report/index.html';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── AssemblyAI ─────────────────────────────────────────────────────
async function generateCaptionsFromAssemblyAI(audioUrl) {
  if (!process.env.ASSEMBLYAI_API_KEY) {
    throw new Error('ASSEMBLYAI_API_KEY not bound to this function');
  }
  if (!audioUrl) throw new Error('No voiceover URL on job; cannot transcribe');

  const headers = {
    Authorization: process.env.ASSEMBLYAI_API_KEY,
    'Content-Type': 'application/json',
  };

  const submit = await fetch('https://api.assemblyai.com/v2/transcript', {
    method: 'POST',
    headers,
    body: JSON.stringify({
      audio_url: audioUrl,
      speech_models: ['universal-2'],
    }),
  });
  if (!submit.ok) {
    throw new Error(
      `AssemblyAI submit ${submit.status}: ${(await submit.text()).slice(0, 200)}`,
    );
  }
  const {id} = await submit.json();

  for (let i = 0; i < 120; i++) {
    await sleep(2000);
    const poll = await fetch(`https://api.assemblyai.com/v2/transcript/${id}`, {headers});
    const data = await poll.json();
    if (data.status === 'completed') break;
    if (data.status === 'error')
      throw new Error(`AssemblyAI: ${data.error || 'transcription failed'}`);
    if (i === 119) throw new Error('AssemblyAI: still transcribing after 4 minutes');
  }

  const sentResp = await fetch(
    `https://api.assemblyai.com/v2/transcript/${id}/sentences`,
    {headers},
  );
  if (!sentResp.ok) {
    throw new Error(`AssemblyAI sentences ${sentResp.status}`);
  }
  const {sentences = []} = await sentResp.json();
  return sentences.map((s) => ({
    start: s.start / 1000,
    end: s.end / 1000,
    text: s.text,
    words: (s.words || []).map((w) => ({
      text: w.text,
      start: w.start / 1000,
      end: w.end / 1000,
    })),
  }));
}

// ─── Anthropic ──────────────────────────────────────────────────────
async function callAnthropic(apiKey, prompt, maxTokens = 3000) {
  const resp = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-6',
      max_tokens: maxTokens,
      messages: [{role: 'user', content: prompt}],
    }),
  });
  if (!resp.ok) {
    throw new Error(`Anthropic ${resp.status}: ${(await resp.text()).slice(0, 200)}`);
  }
  const data = await resp.json();
  return data.content?.find((b) => b.type === 'text')?.text || '';
}

async function breakIntoBeats(captions, anthropicKey) {
  const sentences = captions.map((s, i) => ({
    index: i,
    start: s.start,
    end: s.end,
    text: s.text,
  }));

  const prompt = `You are given timed sentences from a YouTube video transcript.

Group consecutive sentences into "beats" of approximately 3 seconds. A beat must consist of one or more whole consecutive sentences — never split a sentence mid-way. Combine adjacent short sentences when needed; keep long sentences (>3.5s) as their own beat.

For each beat, output:
- start: start time of the first sentence in the beat (seconds, decimal)
- end: end time of the last sentence in the beat (seconds, decimal)
- sentence: concatenated sentence text
- keywords: 2-3 short visual keywords for stock-video search. Concrete nouns and visual concepts only — e.g. "city skyline at night", "person typing on laptop", "ocean waves crashing". Avoid abstract concepts.
- fluxPrompt: a detailed photorealistic image generation prompt describing the same scene cinematically. One vivid sentence, 16:9, no text overlays.

Output ONLY valid JSON. No preamble, no markdown fences, no commentary — just the array:

[
  {
    "start": 0.2,
    "end": 1.7,
    "sentence": "Hey, you. It's me.",
    "keywords": ["person waving at camera", "warm portrait"],
    "fluxPrompt": "Close-up portrait of a friendly person looking directly at camera with warm natural lighting, photorealistic, shallow depth of field"
  }
]

Sentences:
${JSON.stringify(sentences)}`;

  const text = await callAnthropic(anthropicKey, prompt);
  const cleaned = text.replace(/```(?:json)?\s*|\s*```/g, '').trim();
  const beats = JSON.parse(cleaned);
  if (!Array.isArray(beats)) throw new Error('Claude returned non-array');
  return beats;
}

// ─── Pexels ─────────────────────────────────────────────────────────
async function pexelsSearchSingle(query) {
  if (!process.env.PEXELS_API_KEY) throw new Error('PEXELS_API_KEY not bound');
  const url = new URL('https://api.pexels.com/videos/search');
  url.searchParams.set('query', query);
  url.searchParams.set('per_page', '5');
  url.searchParams.set('orientation', 'landscape');
  url.searchParams.set('size', 'large');

  const resp = await fetch(url, {headers: {Authorization: process.env.PEXELS_API_KEY}});
  if (!resp.ok) {
    console.warn(`Pexels ${resp.status} for "${query}"`);
    return null;
  }
  const data = await resp.json();
  const video = (data.videos || [])[0];
  if (!video) return null;
  const files = (video.video_files || []).filter((f) => f.file_type === 'video/mp4');
  if (!files.length) return null;
  files.sort(
    (a, b) => Math.abs((a.height || 0) - 1080) - Math.abs((b.height || 0) - 1080),
  );
  return files[0].link;
}

// ─── Replicate Flux ─────────────────────────────────────────────────
async function generateFluxImage(prompt) {
  if (!process.env.REPLICATE_API_KEY) throw new Error('REPLICATE_API_KEY not bound');

  const headers = {
    Authorization: `Bearer ${process.env.REPLICATE_API_KEY}`,
    'Content-Type': 'application/json',
    Prefer: 'wait',
  };

  const submit = await fetch(
    'https://api.replicate.com/v1/models/black-forest-labs/flux-1.1-pro/predictions',
    {
      method: 'POST',
      headers,
      body: JSON.stringify({
        input: {
          prompt,
          aspect_ratio: '16:9',
          output_format: 'jpg',
          output_quality: 90,
        },
      }),
    },
  );
  if (!submit.ok) {
    throw new Error(`Replicate ${submit.status}: ${(await submit.text()).slice(0, 200)}`);
  }
  let data = await submit.json();
  while (data.status === 'starting' || data.status === 'processing') {
    await sleep(2000);
    const poll = await fetch(
      `https://api.replicate.com/v1/predictions/${data.id}`,
      {headers},
    );
    data = await poll.json();
  }
  if (data.status !== 'succeeded') {
    throw new Error(`Replicate prediction ${data.status}: ${data.error || ''}`);
  }
  const out = data.output;
  if (Array.isArray(out)) return out[0];
  if (typeof out === 'string') return out;
  throw new Error('Replicate unexpected output shape');
}

// ─── Beat-aware sourcing ────────────────────────────────────────────
async function sourceBeatAwareFootage(captions, anthropicKey, jobRef, baseProgress, span) {
  await jobRef.update({
    currentStep: 'Breaking script into beats with Claude',
    progress: baseProgress,
    updatedAt: FieldValue.serverTimestamp(),
  });
  const rawBeats = await breakIntoBeats(captions, anthropicKey);
  console.log(`got ${rawBeats.length} beats from Claude`);

  const footage = [];
  for (let i = 0; i < rawBeats.length; i++) {
    const beat = rawBeats[i];
    const keywords = beat.keywords || [];
    const query = keywords.join(' ');

    await jobRef.update({
      currentStep: `Sourcing beat ${i + 1}/${rawBeats.length}: ${keywords.join(', ').slice(0, 60)}`,
      progress: baseProgress + Math.round(((i + 1) / rawBeats.length) * span),
      updatedAt: FieldValue.serverTimestamp(),
    });

    let url = null;
    let source = null;
    if (query) {
      url = await pexelsSearchSingle(query);
      if (url) source = 'pexels';
    }
    if (!url && beat.fluxPrompt) {
      console.log(`beat ${i + 1}: Pexels miss for "${query}" — Flux fallback`);
      try {
        url = await generateFluxImage(beat.fluxPrompt);
        source = 'flux';
      } catch (e) {
        console.warn(`beat ${i + 1}: Flux failed: ${e.message}`);
      }
    }

    footage.push({
      beatIndex: i,
      start: beat.start,
      end: beat.end,
      sentence: beat.sentence,
      keywords,
      fluxPrompt: beat.fluxPrompt || null,
      source,
      url,
    });
    console.log(`beat ${i + 1}: ${source || 'NO MATCH'}`);
  }
  return footage;
}

// ─── Remotion Lambda ────────────────────────────────────────────────
async function renderOnLambda(inputProps, jobRef) {
  const {renderMediaOnLambda, getRenderProgress} = require('@remotion/lambda-client');

  const {renderId, bucketName} = await renderMediaOnLambda({
    region: AWS_REGION,
    functionName: REMOTION_LAMBDA_FUNCTION_NAME,
    serveUrl: REMOTION_LAMBDA_SERVE_URL,
    composition: 'MainComp',
    inputProps,
    codec: 'h264',
    privacy: 'public',
    imageFormat: 'jpeg',
    framesPerLambda: 200,
    maxRetries: 2,
  });
  console.log(`Lambda renderId: ${renderId}`);

  let lastPct = -1;
  while (true) {
    await sleep(3000);
    const {getRenderProgress: getProgress} = require('@remotion/lambda-client');
    const progress = await getProgress({
      renderId,
      bucketName,
      functionName: REMOTION_LAMBDA_FUNCTION_NAME,
      region: AWS_REGION,
    });
    if (progress.fatalErrorEncountered) {
      throw new Error(
        `Lambda render failed: ${progress.errors?.[0]?.message || 'unknown'}`,
      );
    }
    const pct = Math.round((progress.overallProgress || 0) * 100);
    if (pct !== lastPct) {
      await jobRef.update({
        currentStep: `Rendering on Lambda — ${pct}%`,
        progress: 70 + Math.round((progress.overallProgress || 0) * 25),
        updatedAt: FieldValue.serverTimestamp(),
      });
      lastPct = pct;
    }
    if (progress.done) return progress.outputFile;
  }
}

async function uploadRenderToFirebaseStorage(s3Url, channelId, projectId) {
  const resp = await fetch(s3Url);
  if (!resp.ok) throw new Error(`Could not fetch Lambda output: HTTP ${resp.status}`);
  const buffer = Buffer.from(await resp.arrayBuffer());

  const bucket = admin.storage().bucket();
  const path = `channels/${channelId}/projects/${projectId}/editing/auto-edit-${Date.now()}.mp4`;
  const file = bucket.file(path);
  await file.save(buffer, {metadata: {contentType: 'video/mp4'}});
  await file.makePublic();
  return {
    url: `https://storage.googleapis.com/${bucket.name}/${path}`,
    path,
    size: buffer.length,
  };
}

// ─── Main job handler ───────────────────────────────────────────────
async function processJob(job) {
  const jobRef = db.collection('editingJobs').doc(job.id);
  const projRef = db.collection('channels').doc(job.channelId)
    .collection('projects').doc(job.projectId);

  const projSnap = await projRef.get();
  if (!projSnap.exists) throw new Error(`Project ${job.projectId} not found`);
  const proj = projSnap.data();
  console.log(`processing "${proj.title}"`);

  // Step 1 — captions
  await jobRef.update({
    status: 'captions',
    currentStep: 'Transcribing voiceover with AssemblyAI',
    progress: 10,
    updatedAt: FieldValue.serverTimestamp(),
  });
  const captions = await generateCaptionsFromAssemblyAI(job.voiceoverUrl);
  await jobRef.update({
    captions,
    progress: 30,
    currentStep: `Transcribed ${captions.length} sentences`,
    updatedAt: FieldValue.serverTimestamp(),
  });

  // Step 2 — sourcing
  await jobRef.update({
    status: 'sourcing',
    currentStep: 'Looking up Anthropic key',
    progress: 35,
    updatedAt: FieldValue.serverTimestamp(),
  });
  const ownerSnap = await db.collection('users').doc(job.ownerId).get();
  const anthropicKey = ownerSnap.data()?.settings?.anthropicKey;
  if (!anthropicKey) {
    throw new Error('Owner has no Anthropic key — set it under Settings → Integrations');
  }
  const footage = await sourceBeatAwareFootage(captions, anthropicKey, jobRef, 35, 25);
  const footageUrls = footage.map((f) => f.url).filter(Boolean);
  const pex = footage.filter((f) => f.source === 'pexels').length;
  const flux = footage.filter((f) => f.source === 'flux').length;
  await jobRef.update({
    footage,
    footageUrls,
    progress: 60,
    currentStep: `Sourced ${footage.length} beats — ${pex} Pexels · ${flux} Flux`,
    updatedAt: FieldValue.serverTimestamp(),
  });

  // Step 3 — Remotion Lambda render
  await jobRef.update({
    status: 'rendering',
    currentStep: 'Invoking Lambda renderer',
    progress: 70,
    updatedAt: FieldValue.serverTimestamp(),
  });
  // Lambda needs AWS creds — in Cloud Functions they come from secrets.
  // The @remotion/lambda-client picks them up from process.env.
  const s3OutputUrl = await renderOnLambda(
    {voiceoverUrl: job.voiceoverUrl, footage, captions},
    jobRef,
  );
  await jobRef.update({
    currentStep: 'Copying render to Firebase Storage',
    progress: 95,
    updatedAt: FieldValue.serverTimestamp(),
  });
  const finalFile = await uploadRenderToFirebaseStorage(
    s3OutputUrl,
    job.channelId,
    job.projectId,
  );

  // Step 4 — done (atomic batch)
  const batch = db.batch();
  batch.update(jobRef, {
    status: 'done',
    currentStep: 'Complete',
    progress: 100,
    resultUrl: finalFile.url,
    updatedAt: FieldValue.serverTimestamp(),
  });
  batch.update(projRef, {
    editingFile: {
      url: finalFile.url,
      path: finalFile.path,
      name: 'auto-edit.mp4',
      size: finalFile.size,
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
  console.log('done');
}

// ─── Trigger ────────────────────────────────────────────────────────
exports.processEditingJob = onDocumentCreated(
  {
    document: 'editingJobs/{jobId}',
    secrets: [
      PEXELS_API_KEY,
      ASSEMBLYAI_API_KEY,
      REPLICATE_API_KEY,
      AWS_ACCESS_KEY_ID,
      AWS_SECRET_ACCESS_KEY,
    ],
    // Event-triggered Cloud Functions cap at 540s. Plenty for our test
    // jobs (~150s) — if real production hits this on long videos we'll
    // switch to a fan-out: trigger queues, do work in a HTTPS function
    // (60 min cap) called by the trigger.
    timeoutSeconds: 540,
    memory: '1GiB',
    region: 'us-central1',
  },
  async (event) => {
    const jobId = event.params.jobId;
    const data = event.data?.data();
    if (!data) {
      console.warn(`[${jobId}] no data on event`);
      return;
    }
    if (data.status !== 'pending') {
      console.log(`[${jobId}] status=${data.status}, skipping`);
      return;
    }
    const job = {id: jobId, ...data};
    console.log(`[${jobId}] starting`);
    try {
      await processJob(job);
      console.log(`[${jobId}] done`);
    } catch (e) {
      console.error(`[${jobId}] failed:`, e);
      await db.collection('editingJobs').doc(jobId).update({
        status: 'failed',
        currentStep: 'Failed',
        error: e.message || 'Unknown error',
        updatedAt: FieldValue.serverTimestamp(),
      }).catch((err) => console.error('Could not mark failed:', err));
    }
  },
);
