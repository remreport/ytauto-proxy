// worker.js — local debug-only background processor for editingJobs.
//
// PRODUCTION RUNS IN THE CLOUD.  The same logic lives in functions/index.js
// as a Firebase Cloud Function triggered on editingJobs document creation.
// That function processes every job in production — this script exists
// only so you can iterate on worker code locally (start-worker.ps1) without
// redeploying. Don't run this and the Cloud Function at the same time
// against the same Firestore project: they'll race and double-process.
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
const {renderMediaOnLambda, getRenderProgress} = require('@remotion/lambda/client');

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

admin.initializeApp({
  credential: admin.credential.cert(svc),
  storageBucket: `${svc.project_id}.firebasestorage.app`,
});
const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

const POLL_INTERVAL_MS = 30 * 1000;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── Remotion Lambda render ─────────────────────────────────────────
function isAwsRateLimitError(message) {
  return /rate.{0,5}exceeded|concurrency.{0,5}limit|throttl/i.test(message || '');
}

async function renderOnLambda(inputProps, jobRef) {
  const region = process.env.AWS_REGION || 'us-east-1';
  const functionName = process.env.REMOTION_LAMBDA_FUNCTION_NAME;
  const serveUrl = process.env.REMOTION_LAMBDA_SERVE_URL;
  if (!functionName || !serveUrl) {
    throw new Error('REMOTION_LAMBDA_FUNCTION_NAME and REMOTION_LAMBDA_SERVE_URL env vars required');
  }

  const renderArgs = {
    region,
    functionName,
    serveUrl,
    composition: 'MainComp',
    inputProps,
    codec: 'h264',
    privacy: 'public',
    imageFormat: 'jpeg',
    framesPerLambda: 2000,
    concurrencyPerLambda: 2,
    maxRetries: 5,
  };

  let renderId;
  let bucketName;
  let attempt = 0;
  const maxAttempts = 3;
  while (attempt < maxAttempts) {
    attempt++;
    try {
      const result = await renderMediaOnLambda(renderArgs);
      renderId = result.renderId;
      bucketName = result.bucketName;
      break;
    } catch (e) {
      if (!isAwsRateLimitError(e.message) || attempt >= maxAttempts) throw e;
      const delaySec = attempt * 30;
      console.warn(`Lambda submission rate-limited (attempt ${attempt}/${maxAttempts}) — retry in ${delaySec}s`);
      await jobRef.update({
        currentStep: `AWS render slot busy — retrying in ${delaySec}s (attempt ${attempt}/${maxAttempts})`,
        updatedAt: FieldValue.serverTimestamp(),
      });
      await sleep(delaySec * 1000);
    }
  }
  if (!renderId) throw new Error('Lambda submission failed after retries');
  console.log(`  Lambda renderId: ${renderId}`);

  let lastPct = -1;
  while (true) {
    await sleep(3000);
    const progress = await getRenderProgress({
      renderId, bucketName, functionName, region,
    });
    if (progress.fatalErrorEncountered) {
      const raw = progress.errors?.[0]?.message || 'unknown Lambda render error';
      if (isAwsRateLimitError(raw)) {
        throw new Error(
          'AWS render slot was busy and the per-chunk retries also exhausted. Your AWS account is at its concurrent-Lambda quota. Wait a few minutes and click Retry, or request a quota increase.',
        );
      }
      throw new Error(`Lambda render failed: ${raw}`);
    }
    const pct = Math.round((progress.overallProgress || 0) * 100);
    if (pct !== lastPct) {
      console.log(`  Lambda progress: ${pct}%`);
      // Map Lambda 0-100% into the worker's 70-95% slice
      await jobRef.update({
        currentStep: `Rendering on Lambda — ${pct}%`,
        progress: 70 + Math.round((progress.overallProgress || 0) * 25),
        updatedAt: FieldValue.serverTimestamp(),
      });
      lastPct = pct;
    }
    if (progress.done) {
      return progress.outputFile; // S3 URL of the rendered MP4
    }
  }
}

// Download Lambda's S3 output and re-upload to Firebase Storage so the
// existing review UI plays it back the same as a worker submission.
// Returns the {url, path, size} shape that project.editingFile uses.
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

// Anthropic tool use — see comment in functions/index.js for rationale.
async function callAnthropicWithTool(apiKey, prompt, tool, maxTokens = 4096) {
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
      tools: [tool],
      tool_choice: {type: 'tool', name: tool.name},
      messages: [{role: 'user', content: prompt}],
    }),
  });
  if (!resp.ok) {
    throw new Error(`Anthropic ${resp.status}: ${(await resp.text()).slice(0, 200)}`);
  }
  const data = await resp.json();
  const toolUse = (data.content || []).find((b) => b.type === 'tool_use');
  if (!toolUse) {
    const fallbackText = (data.content || []).find((b) => b.type === 'text')?.text || '';
    const err = new Error('Claude did not return a tool_use block');
    err.fallbackText = fallbackText.slice(0, 2000);
    throw err;
  }
  return toolUse.input;
}

const BEATS_TOOL = {
  name: 'submit_beats',
  description: 'Submit the timed beats for a YouTube video. The beats array must contain at least one beat — never empty.',
  input_schema: {
    type: 'object',
    properties: {
      beats: {
        type: 'array',
        minItems: 1,
        items: {
          type: 'object',
          properties: {
            start: {type: 'number'},
            end: {type: 'number'},
            sentence: {type: 'string'},
            keywords: {type: 'array', items: {type: 'string'}},
            fluxPrompt: {type: 'string'},
          },
          required: ['start', 'end', 'sentence', 'keywords', 'fluxPrompt'],
        },
      },
    },
    required: ['beats'],
  },
};

function sanitizeJSON(text) {
  let s = String(text || '');
  s = s.replace(/```(?:json|JSON)?\s*/g, '').replace(/\s*```/g, '');
  s = s.replace(/\/\/[^\n]*/g, '');
  s = s.replace(/\/\*[\s\S]*?\*\//g, '');
  s = s.replace(/,(\s*[}\]])/g, '$1');
  return s.trim();
}

async function callAnthropicForJSON(apiKey, prompt, maxTokens = 4000) {
  const raw = await callAnthropic(apiKey, prompt, maxTokens);
  try {
    return JSON.parse(sanitizeJSON(raw));
  } catch (firstErr) {
    console.warn(`JSON parse failed (${firstErr.message}) — retrying via Claude self-correction`);
    const retryPrompt = `I tried to JSON.parse() the following text and it failed.

Parse error: ${firstErr.message}

Output ONLY the corrected JSON. No markdown fences, no commentary, no comments inside the JSON. Every property name and string value must use double quotes. No trailing commas. No JavaScript-only syntax. The output must parse cleanly with JSON.parse() in standard JavaScript.

Original text:
${raw}`;
    const retryRaw = await callAnthropic(apiKey, retryPrompt, maxTokens);
    try {
      return JSON.parse(sanitizeJSON(retryRaw));
    } catch (secondErr) {
      const err = new Error(
        `Claude returned unparseable JSON. First error: ${firstErr.message}. Retry error: ${secondErr.message}.`,
      );
      err.rawFirstResponse = raw.slice(0, 2000);
      err.rawRetryResponse = retryRaw.slice(0, 2000);
      throw err;
    }
  }
}

// Send timed sentences to Claude. Get back beats grouped to ~3 seconds,
// each annotated with stock-search keywords + a Flux fallback prompt.
// Beats are constrained to whole-sentence boundaries so timing always
// aligns with real audio.
// Tone classifier + music picker — kept in sync with functions/index.js.
async function classifyTone(scriptText, anthropicKey) {
  if (!scriptText || scriptText.trim().length < 10) return 'calm';
  const prompt = `Classify the tone of this YouTube script as ONE of these moods:
dramatic, educational, upbeat, calm, mysterious, inspirational

Output ONLY the single mood word, lowercase, no explanation, no punctuation.

Script:
${scriptText.slice(0, 2500)}`;
  const text = await callAnthropic(anthropicKey, prompt, 50);
  const m = (text || '').toLowerCase().match(
    /dramatic|educational|upbeat|calm|mysterious|inspirational/,
  );
  return m ? m[0] : 'calm';
}

async function pickMusicTrack(channelId, mood) {
  const snap = await db.collection('channels').doc(channelId)
    .collection('musicTracks').where('mood', '==', mood).get();
  if (snap.empty) return null;
  const tracks = snap.docs.map((d) => d.data());
  const picked = tracks[Math.floor(Math.random() * tracks.length)];
  return picked.url || null;
}

async function breakIntoBeats(captions, anthropicKey) {
  const sentences = captions.map((s, i) => ({
    index: i,
    start: s.start,
    end: s.end,
    text: s.text,
  }));

  const validSentences = (captions || []).filter(
    (c) => c && typeof c.text === 'string' && c.text.trim().length > 0,
  );
  if (validSentences.length === 0) {
    throw new Error(
      'Voiceover transcription returned no usable sentences. The audio may be silent, too noisy, or in an unsupported format.',
    );
  }
  const sentences2 = validSentences.map((s, i) => ({
    index: i,
    start: s.start,
    end: s.end,
    text: s.text,
  }));

  const prompt = `You will receive ${sentences2.length} timed sentence(s) from an audio transcription. Your task is to group them into beats with visual keywords. This is a mechanical text-grouping task.

DO NOT refuse based on content quality, topic relevance, brevity, perceived inappropriateness, or any other judgment about the content. Always submit at least one beat — an empty beats array is a task failure.

Group consecutive sentences into beats of approximately 3 seconds. A beat must consist of whole consecutive sentences — never split a sentence. Combine short adjacent sentences as needed; keep long ones (>3.5s) as their own beat. If total content is shorter than 3 seconds, output one beat covering everything.

For each beat: start/end timing, 2-3 concrete visual keywords for stock search, a detailed photorealistic image-generation prompt (16:9, no text overlays).

Submit via the submit_beats tool with at least 1 beat.

Sentences:
${JSON.stringify(sentences2)}`;

  let result;
  try {
    result = await callAnthropicWithTool(anthropicKey, prompt, BEATS_TOOL, 4096);
  } catch (e) {
    console.warn(`breakIntoBeats: first call threw (${e.message}) — retrying`);
    result = {beats: []};
  }
  if (!result?.beats?.length) {
    console.warn(`breakIntoBeats: 0 beats from ${sentences2.length} sentences — retrying with explicit feedback`);
    const retryPrompt = `You just returned 0 beats but the input has ${sentences2.length} sentence(s). Submit submit_beats with at least 1 beat. Do not refuse.

Sentences:
${JSON.stringify(sentences2)}`;
    result = await callAnthropicWithTool(anthropicKey, retryPrompt, BEATS_TOOL, 4096);
  }
  if (Array.isArray(result?.beats) && result.beats.length > 0) {
    return result.beats;
  }
  // Naive fallback (parity with functions/index.js)
  console.warn('breakIntoBeats: both Claude attempts returned 0 beats — naive grouping');
  return naiveGroupSentences(validSentences);
}

function naiveGroupSentences(sentences) {
  const STOPWORDS = new Set([
    'the', 'and', 'for', 'that', 'this', 'with', 'from', 'have', 'been', 'were',
    'they', 'their', 'what', 'when', 'which', 'about', 'would', 'could', 'should',
    'more', 'than', 'other', 'these', 'those', 'into', 'over', 'just', 'like',
    'people', 'also', 'because', 'while', 'after', 'before', 'where', 'every',
  ]);
  const beats = [];
  let i = 0;
  while (i < sentences.length) {
    const startSent = sentences[i];
    let endSent = startSent;
    let combinedText = startSent.text;
    let j = i;
    while (j + 1 < sentences.length && (sentences[j + 1].end - startSent.start) < 3.5) {
      j++;
      endSent = sentences[j];
      combinedText += ' ' + sentences[j].text;
    }
    const words = (combinedText.toLowerCase().match(/[a-z]{4,}/g) || []).filter((w) => !STOPWORDS.has(w));
    const keywords = Array.from(new Set(words)).slice(0, 3);
    beats.push({
      start: startSent.start,
      end: endSent.end,
      sentence: combinedText,
      keywords: keywords.length ? keywords : ['abstract', 'atmospheric scene'],
      fluxPrompt: `Cinematic photorealistic scene illustrating: ${combinedText.slice(0, 180)}. 16:9, soft natural lighting, no text overlays.`,
    });
    i = j + 1;
  }
  return beats;
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
  // Prefer ~1080p sources over 4K — our render output is 1080p, and 4K
  // source files are 5-10x bigger which blows out Lambda disk and adds
  // decode time without adding pixels to the output.
  files.sort((a, b) => Math.abs((a.height || 0) - 1080) - Math.abs((b.height || 0) - 1080));
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

  // Step 2.5 — pick background music
  let musicUrl = null;
  let pickedMood = null;
  try {
    pickedMood = await classifyTone(
      captions.map((c) => c.text).join(' '),
      anthropicKey,
    );
    musicUrl = await pickMusicTrack(job.channelId, pickedMood);
    console.log(`[${job.id}] tone=${pickedMood}, music=${musicUrl ? 'picked' : 'no track'}`);
  } catch (e) {
    console.warn(`[${job.id}] music pick failed: ${e.message}`);
  }
  await jobRef.update({
    musicUrl,
    pickedMood,
    currentStep: musicUrl
      ? `Music: ${pickedMood}`
      : (pickedMood ? `No ${pickedMood} track in library` : 'No music'),
    updatedAt: FieldValue.serverTimestamp(),
  });

  // Step 3 — render: REAL (Remotion Lambda)
  await jobRef.update({
    status: 'rendering',
    currentStep: 'Invoking Lambda renderer',
    progress: 70,
    updatedAt: FieldValue.serverTimestamp(),
  });
  const s3OutputUrl = await renderOnLambda(
    {
      voiceoverUrl: job.voiceoverUrl,
      footage,
      captions,
      musicUrl,
    },
    jobRef,
  );
  console.log(`[${job.id}] Lambda rendered to ${s3OutputUrl}`);

  await jobRef.update({
    currentStep: 'Copying render to Firebase Storage',
    progress: 95,
    updatedAt: FieldValue.serverTimestamp(),
  });
  const finalFile = await uploadRenderToFirebaseStorage(s3OutputUrl, job.channelId, job.projectId);
  console.log(`[${job.id}] uploaded to ${finalFile.url} (${finalFile.size} bytes)`);

  // Step 4 — done (atomic batch: job + project + notification)
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
console.log(`  Lambda fn:   ${process.env.REMOTION_LAMBDA_FUNCTION_NAME || '(unset)'}`);
console.log(`  Lambda site: ${process.env.REMOTION_LAMBDA_SERVE_URL || '(unset)'}`);

(async function loop() {
  while (true) {
    await tick();
    await sleep(POLL_INTERVAL_MS);
  }
})().catch((e) => {
  console.error('Worker crashed:', e);
  process.exit(1);
});
