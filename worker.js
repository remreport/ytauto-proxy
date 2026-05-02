// worker.js — background processor for editingJobs.
//
// Pipeline order (matters: sourcing reads captions for beat alignment):
//   pending → captions → sourcing → rendering → done
//
// Phase 3 progress:
//   - captions  : real (AssemblyAI)
//   - sourcing  : real, beat-aware (Claude → per-beat Pexels → Flux fallback)
//   - rendering : still fake; phase 3d swaps for Remotion Lambda
//
// On 'done' the worker atomically writes:
//   - editingJobs doc (status, resultUrl)
//   - project's editingFile + status.editing='awaiting_approval'
//   - owner notification
// Existing pipeline picks up the result identically to a worker submission.
//
// Run locally:
//   1. Download a Firebase service account JSON. Save outside the repo.
//   2. Put your API keys in .env (gitignored): PEXELS_API_KEY,
//      ASSEMBLYAI_API_KEY, REPLICATE_API_KEY.
//   3. .\start-worker.ps1   (loads .env automatically)

const fs = require('fs');
const admin = require('firebase-admin');

if (process.env.WORKER_ENABLED !== 'true') {
  console.log('Worker disabled. Set WORKER_ENABLED=true to run.');
  process.exit(0);
}

// Service account: file (local dev) or inline JSON (Render production)
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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── AssemblyAI ─────────────────────────────────────────────────────
// Submit voiceover URL, poll until complete, return sentence-level
// captions with word-level timing in seconds.
async function generateCaptionsFromAssemblyAI(audioUrl) {
  if (!process.env.ASSEMBLYAI_API_KEY) {
    throw new Error('ASSEMBLYAI_API_KEY env var missing');
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
    throw new Error(`AssemblyAI submit ${submit.status}: ${(await submit.text()).slice(0, 200)}`);
  }
  const { id } = await submit.json();

  let lastStatus = '';
  for (let i = 0; i < 120; i++) {
    await sleep(2000);
    const poll = await fetch(`https://api.assemblyai.com/v2/transcript/${id}`, { headers });
    const data = await poll.json();
    if (data.status !== lastStatus) {
      console.log(`  AssemblyAI: ${data.status}`);
      lastStatus = data.status;
    }
    if (data.status === 'completed') break;
    if (data.status === 'error') throw new Error(`AssemblyAI: ${data.error || 'transcription failed'}`);
    if (i === 119) throw new Error('AssemblyAI: still transcribing after 4 minutes');
  }

  const sentResp = await fetch(`https://api.assemblyai.com/v2/transcript/${id}/sentences`, { headers });
  if (!sentResp.ok) {
    throw new Error(`AssemblyAI sentences ${sentResp.status}: ${(await sentResp.text()).slice(0, 200)}`);
  }
  const { sentences = [] } = await sentResp.json();

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

// ─── Anthropic (direct, not via proxy) ──────────────────────────────
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
      messages: [{ role: 'user', content: prompt }],
    }),
  });
  if (!resp.ok) {
    throw new Error(`Anthropic ${resp.status}: ${(await resp.text()).slice(0, 200)}`);
  }
  const data = await resp.json();
  return data.content?.find((b) => b.type === 'text')?.text || '';
}

// Send timed sentences to Claude. Get back beats grouped to ~3 seconds,
// each annotated with stock-search keywords + a Flux fallback prompt.
// Beats are constrained to whole-sentence boundaries so timing always
// aligns with real audio.
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
  try {
    const beats = JSON.parse(cleaned);
    if (!Array.isArray(beats)) throw new Error('expected an array');
    return beats;
  } catch (e) {
    throw new Error(`Could not parse Claude beats response: ${e.message}. Raw: ${text.slice(0, 300)}`);
  }
}

// ─── Pexels (single-result per beat) ────────────────────────────────
async function pexelsSearchSingle(query) {
  if (!process.env.PEXELS_API_KEY) throw new Error('PEXELS_API_KEY env var missing');
  const url = new URL('https://api.pexels.com/videos/search');
  url.searchParams.set('query', query);
  url.searchParams.set('per_page', '5');
  url.searchParams.set('orientation', 'landscape');
  url.searchParams.set('size', 'large');

  const resp = await fetch(url, {
    headers: { Authorization: process.env.PEXELS_API_KEY },
  });
  if (!resp.ok) {
    console.warn(`  Pexels ${resp.status} for "${query}"`);
    return null;
  }
  const data = await resp.json();
  const video = (data.videos || [])[0];
  if (!video) return null;
  const files = (video.video_files || []).filter((f) => f.file_type === 'video/mp4');
  if (!files.length) return null;
  files.sort((a, b) => (b.height || 0) - (a.height || 0));
  return files[0].link;
}

// ─── Replicate Flux 1.1 Pro (16:9 photorealistic image generation) ──
async function generateFluxImage(prompt) {
  if (!process.env.REPLICATE_API_KEY) throw new Error('REPLICATE_API_KEY env var missing');

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

  // With Prefer: wait, succeeded result usually arrives in the initial
  // response. If not, fall back to polling.
  while (data.status === 'starting' || data.status === 'processing') {
    await sleep(2000);
    const poll = await fetch(`https://api.replicate.com/v1/predictions/${data.id}`, { headers });
    data = await poll.json();
  }
  if (data.status !== 'succeeded') {
    throw new Error(`Replicate prediction ${data.status}: ${data.error || ''}`);
  }
  const out = data.output;
  if (Array.isArray(out)) return out[0];
  if (typeof out === 'string') return out;
  throw new Error(`Replicate returned unexpected output: ${JSON.stringify(out).slice(0, 200)}`);
}

// ─── Beat-aware sourcing orchestrator ───────────────────────────────
// 1. Ask Claude to break the captions into ~3s beats with keywords + flux prompts
// 2. For each beat, try Pexels with the keywords; fall through to Flux on miss
// 3. Update jobRef.currentStep per beat for live progress
async function sourceBeatAwareFootage(captions, anthropicKey, jobRef, baseProgress, progressSpan) {
  await jobRef.update({
    currentStep: 'Breaking script into beats with Claude',
    progress: baseProgress,
    updatedAt: FieldValue.serverTimestamp(),
  });
  const rawBeats = await breakIntoBeats(captions, anthropicKey);
  console.log(`  got ${rawBeats.length} beats from Claude`);

  const footage = [];
  for (let i = 0; i < rawBeats.length; i++) {
    const beat = rawBeats[i];
    const keywords = beat.keywords || [];
    const query = keywords.join(' ');

    await jobRef.update({
      currentStep: `Sourcing beat ${i + 1}/${rawBeats.length}: ${keywords.join(', ').slice(0, 60)}`,
      progress: baseProgress + Math.round(((i + 1) / rawBeats.length) * progressSpan),
      updatedAt: FieldValue.serverTimestamp(),
    });

    let url = null;
    let source = null;

    if (query) {
      url = await pexelsSearchSingle(query);
      if (url) source = 'pexels';
    }

    if (!url && beat.fluxPrompt) {
      console.log(`  beat ${i + 1}: Pexels miss for "${query}" — generating with Flux`);
      try {
        url = await generateFluxImage(beat.fluxPrompt);
        source = 'flux';
      } catch (e) {
        console.warn(`  beat ${i + 1}: Flux failed: ${e.message}`);
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
    console.log(`  beat ${i + 1}: ${source || 'NO MATCH'} — ${(url || '').slice(0, 80)}`);
  }
  return footage;
}

// ─── Job processor ──────────────────────────────────────────────────
async function processJob(job) {
  const jobRef = db.collection('editingJobs').doc(job.id);
  const projRef = db.collection('channels').doc(job.channelId)
    .collection('projects').doc(job.projectId);

  const projSnap = await projRef.get();
  if (!projSnap.exists) throw new Error(`Project ${job.projectId} not found`);
  const proj = projSnap.data();
  console.log(`[${job.id}] processing "${proj.title}"`);

  // Step 1 — captions (must come before sourcing for beat alignment)
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
  console.log(`[${job.id}] transcribed ${captions.length} sentences`);

  // Step 2 — beat-aware sourcing (Claude + per-beat Pexels + Flux fallback)
  await jobRef.update({
    status: 'sourcing',
    currentStep: 'Looking up Anthropic key',
    progress: 35,
    updatedAt: FieldValue.serverTimestamp(),
  });
  const ownerSnap = await db.collection('users').doc(job.ownerId).get();
  const anthropicKey = ownerSnap.data()?.settings?.anthropicKey;
  if (!anthropicKey) {
    throw new Error('Owner has no Anthropic key — set it under Settings → Integrations in the app first');
  }

  // 35% → 60%: 25% span across N beats
  const footage = await sourceBeatAwareFootage(captions, anthropicKey, jobRef, 35, 25);
  const footageUrls = footage.map((f) => f.url).filter(Boolean);
  const pexCount = footage.filter((f) => f.source === 'pexels').length;
  const fluxCount = footage.filter((f) => f.source === 'flux').length;

  await jobRef.update({
    footage,
    footageUrls,
    progress: 60,
    currentStep: `Sourced ${footage.length} beats — ${pexCount} Pexels · ${fluxCount} Flux`,
    updatedAt: FieldValue.serverTimestamp(),
  });
  console.log(`[${job.id}] sourced ${footage.length} beats (${pexCount} Pexels, ${fluxCount} Flux)`);

  // Step 3 — render (still fake, phase 3d)
  await jobRef.update({
    status: 'rendering',
    currentStep: 'Rendering video (fake)',
    progress: 70,
    updatedAt: FieldValue.serverTimestamp(),
  });
  await sleep(10000);

  // Step 4 — done (atomic batch: job + project + notification)
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
      }).catch((err) => console.error('Could not mark failed:', err));
    }
  } catch (e) {
    console.error('Poll failed:', e.message);
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
})().catch((e) => {
  console.error('Worker crashed:', e);
  process.exit(1);
});
