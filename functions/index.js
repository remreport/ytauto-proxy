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
const {onSchedule} = require('firebase-functions/v2/scheduler');
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
// Globally drop undefined values from .update()/.set() payloads — without
// this, a single undefined nested deep in a payload throws and aborts the
// write, which on the catch path produces zombie jobs that never reach
// status='failed'. Has to be set before the first DB operation.
db.settings({ignoreUndefinedProperties: true});
const FieldValue = admin.firestore.FieldValue;

// ─── Secrets (live in Google Secret Manager) ────────────────────────
const PEXELS_API_KEY = defineSecret('PEXELS_API_KEY');
const ASSEMBLYAI_API_KEY = defineSecret('ASSEMBLYAI_API_KEY');
const REPLICATE_API_KEY = defineSecret('REPLICATE_API_KEY');
const AWS_ACCESS_KEY_ID = defineSecret('AWS_ACCESS_KEY_ID');
const AWS_SECRET_ACCESS_KEY = defineSecret('AWS_SECRET_ACCESS_KEY');
const NEWSAPI_KEY = defineSecret('NEWSAPI_KEY');

// ─── Non-secret config ──────────────────────────────────────────────
const AWS_REGION = 'us-east-1';
const REMOTION_LAMBDA_FUNCTION_NAME =
  'remotion-render-4-0-456-mem3008mb-disk10240mb-900sec';
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

// Best-effort cleanup before JSON.parse: strips markdown fences, line
// and block comments, trailing commas, leading/trailing whitespace.
// Anything more exotic falls through to the Claude retry path below.
// Anthropic tool use — gives Claude a JSON Schema. The API validates
// the response against the schema server-side and returns a structured
// tool_use block whose .input is already an object. No text-mode JSON
// parsing on our side, so unescaped quotes / fences / trailing commas
// in narrative content can't break the pipeline.
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

// Tool schemas — kept here so they live next to the call sites.
const BEATS_TOOL = {
  name: 'submit_beats',
  description: 'Submit the timed beats for a YouTube video. Each beat is a contiguous group of whole sentences that will be rendered with one stock clip or one Flux-generated image. The beats array must contain at least one beat — never submit an empty array.',
  input_schema: {
    type: 'object',
    properties: {
      beats: {
        type: 'array',
        description: 'Array of beats in chronological order. Beats must consist of whole sentences from the input transcript — never split a sentence mid-way. MUST contain at least one beat.',
        minItems: 1,
        items: {
          type: 'object',
          properties: {
            start: {type: 'number', description: 'Start time of the first sentence in the beat (seconds, decimal).'},
            end: {type: 'number', description: 'End time of the last sentence in the beat (seconds, decimal).'},
            sentence: {type: 'string', description: 'Concatenated sentence text for this beat.'},
            keywords: {
              type: 'array',
              items: {type: 'string'},
              description: '2-3 short concrete visual keywords for stock-video search (e.g. "city skyline at night"). Avoid abstract concepts.',
            },
            fluxPrompt: {type: 'string', description: 'A detailed photorealistic image generation prompt describing the same scene cinematically. One vivid sentence, 16:9, no text overlays.'},
          },
          required: ['start', 'end', 'sentence', 'keywords', 'fluxPrompt'],
        },
      },
    },
    required: ['beats'],
  },
};

const TOPIC_PROPOSAL_TOOL = {
  name: 'submit_topic_proposal',
  description: 'Submit the chosen YouTube topic from a list of trending candidates plus 5 ranked viral title variants.',
  input_schema: {
    type: 'object',
    properties: {
      pickedTopic: {type: 'string', description: 'Concise topic title, 6-12 words.'},
      pickedTopicSummary: {type: 'string', description: '2-3 sentence summary of what the video would cover.'},
      pickedTopicReasoning: {type: 'string', description: '1-2 sentence reasoning citing source convergence, freshness, and niche fit.'},
      pickedTopicSources: {
        type: 'array',
        items: {type: 'string', enum: ['reddit', 'hn', 'news', 'trends']},
        description: 'Which sources the picked topic appeared on.',
      },
      titleVariants: {
        type: 'array',
        description: 'Exactly 5 ranked title variants for the picked topic.',
        items: {
          type: 'object',
          properties: {
            title: {type: 'string'},
            ctrPrediction: {type: 'string', enum: ['high', 'medium-high', 'medium']},
            reasoning: {type: 'string', description: 'One short sentence explaining the title pattern and why it should hook viewers.'},
          },
          required: ['title', 'ctrPrediction', 'reasoning'],
        },
      },
      pickedTitle: {type: 'string', description: 'The title from titleVariants predicted to have highest CTR (must match one of the variants exactly).'},
    },
    required: ['pickedTopic', 'pickedTopicSummary', 'pickedTopicReasoning', 'titleVariants', 'pickedTitle'],
  },
};

// JSON helpers — kept for any future text-JSON callers, plus richer
// diagnostics on parse failure.
function extractErrorContext(text, errorMessage) {
  const posMatch = /position (\d+)/i.exec(errorMessage || '');
  if (!posMatch) return null;
  const pos = parseInt(posMatch[1], 10);
  const lineMatch = /line (\d+) column (\d+)/i.exec(errorMessage || '');
  return {
    position: pos,
    line: lineMatch ? parseInt(lineMatch[1], 10) : null,
    column: lineMatch ? parseInt(lineMatch[2], 10) : null,
    before: text.slice(Math.max(0, pos - 100), pos),
    at: text.slice(pos, pos + 1),
    after: text.slice(pos + 1, pos + 100),
  };
}

// Pull every diagnostic field off an error so the failed job doc has
// enough context for me to debug without instrumenting anything else.
// Saves full raw responses (no truncation) — Firestore strings can be
// up to 1 MiB and Claude's longest replies are ~30 KB.
function buildErrorDetail(e) {
  const detail = {
    message: e?.message || 'Unknown error',
    code: e?.code || null,
    name: e?.name || null,
  };
  if (e?.rawFirstResponse) detail.rawFirstResponse = e.rawFirstResponse;
  if (e?.rawRetryResponse) detail.rawRetryResponse = e.rawRetryResponse;
  if (e?.firstContext) detail.firstContext = e.firstContext;
  if (e?.retryContext) detail.retryContext = e.retryContext;
  if (e?.fallbackText) detail.fallbackText = e.fallbackText;
  if (e?.diagnosticInput) detail.diagnosticInput = e.diagnosticInput;
  return detail;
}

function sanitizeJSON(text) {
  let s = String(text || '');
  s = s.replace(/```(?:json|JSON)?\s*/g, '').replace(/\s*```/g, '');
  s = s.replace(/\/\/[^\n]*/g, '');
  s = s.replace(/\/\*[\s\S]*?\*\//g, '');
  s = s.replace(/,(\s*[}\]])/g, '$1');
  return s.trim();
}

// One-attempt-and-retry JSON wrapper. If the first JSON.parse fails we
// send the raw response BACK to Claude with the parse error and ask it
// to return strictly valid JSON. Only after the retry also fails do we
// surface an error — and that error carries both raw responses for
// post-mortem on the job doc.
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
      err.code = 'JSON_PARSE_FAILED';
      err.rawFirstResponse = raw;
      err.rawRetryResponse = retryRaw;
      err.firstContext = extractErrorContext(sanitizeJSON(raw), firstErr.message);
      err.retryContext = extractErrorContext(sanitizeJSON(retryRaw), secondErr.message);
      throw err;
    }
  }
}

// Classify the script's overall tone into one of six moods. Used to
// pick a background music track from the channel's library. Falls back
// to 'calm' (least-likely-to-clash) if Claude returns something weird.
// Mood enum must stay in sync with App.jsx MOODS constant.
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
  // Pre-flight: separate "audio transcribed to nothing" from "Claude refused".
  // If we don't have any usable sentence text we fail here with a message that
  // points at the audio, not at the LLM.
  const validSentences = (captions || []).filter(
    (c) => c && typeof c.text === 'string' && c.text.trim().length > 0,
  );
  if (validSentences.length === 0) {
    throw new Error(
      'Voiceover transcription returned no usable sentences. The audio may be silent, too noisy, or in an unsupported format.',
    );
  }

  const sentences = validSentences.map((s, i) => ({
    index: i,
    start: s.start,
    end: s.end,
    text: s.text,
  }));

  const prompt = `You will receive ${sentences.length} timed sentence(s) from an audio transcription. Your task is to group them into "beats" with visual keywords for stock footage. This is a mechanical text-grouping task.

DO NOT refuse this task based on content quality, topic relevance, brevity, perceived inappropriateness, or any other judgment about the content. The transcription is whatever the speaker said; your job is just to group it. Always submit at least one beat. An empty beats array is a task failure.

Group consecutive sentences into beats of approximately 3 seconds. A beat must consist of one or more whole consecutive sentences — never split a sentence mid-way. Combine adjacent short sentences as needed; keep long sentences (>3.5s) as their own beat. If the total content is shorter than 3 seconds, output a single beat covering all sentences.

For each beat, decide:
- start / end timing (seconds, from the input — start of first sentence to end of last sentence in the beat)
- 2-3 concrete visual keywords for stock-video search (e.g. "person waving at camera", "city skyline at night")
- A detailed photorealistic image-generation prompt describing the same scene cinematically (16:9, no text overlays, one vivid sentence)

Submit your beats via the submit_beats tool. The beats array MUST contain at least 1 beat.

Sentences:
${JSON.stringify(sentences)}`;

  let result;
  try {
    result = await callAnthropicWithTool(anthropicKey, prompt, BEATS_TOOL, 4096);
  } catch (e) {
    // First call may fail with "no tool_use block" if Claude refused entirely.
    // Fall through to the retry path so we get one more shot.
    console.warn(`breakIntoBeats: first call threw (${e.message}) — will retry with explicit feedback`);
    result = {beats: []};
  }

  if (!result?.beats?.length) {
    console.warn(`breakIntoBeats: 0 beats from ${sentences.length} sentences — retrying with explicit feedback`);
    const retryPrompt = `You just returned 0 beats from the submit_beats tool, but the input has ${sentences.length} sentence(s). This is a task failure.

You MUST submit submit_beats with at least 1 beat. The submit_beats input_schema requires beats to be a non-empty array (minItems: 1). Do not refuse. Do not return empty. Group the sentences below into one or more beats and submit.

Sentences:
${JSON.stringify(sentences)}`;
    try {
      result = await callAnthropicWithTool(anthropicKey, retryPrompt, BEATS_TOOL, 4096);
    } catch (e) {
      console.warn(`breakIntoBeats retry threw (${e.message}) — falling back to naive grouping`);
      result = {beats: []};
    }
  }

  if (Array.isArray(result?.beats) && result.beats.length > 0) {
    return result.beats;
  }

  // Last-resort deterministic fallback. Claude refused twice — group the
  // sentences ourselves so the pipeline can still produce a render. Output
  // quality is degraded (keywords are extracted heuristically, fluxPrompt
  // is a generic illustrate-this template), but the user gets SOMETHING
  // back instead of a stuck job. job.naiveFallbackUsed flags it.
  console.warn(`breakIntoBeats: both Claude attempts returned 0 beats — using naive grouping fallback`);
  return naiveGroupSentences(validSentences);
}

// Deterministic fallback when Claude refuses to call submit_beats.
// Groups consecutive sentences into ~3-second windows, extracts heuristic
// keywords, builds a generic fluxPrompt. Always returns at least one beat
// for any non-empty input.
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
    while (
      j + 1 < sentences.length &&
      (sentences[j + 1].end - startSent.start) < 3.5
    ) {
      j++;
      endSent = sentences[j];
      combinedText += ' ' + sentences[j].text;
    }
    const words = (combinedText.toLowerCase().match(/[a-z]{4,}/g) || [])
      .filter((w) => !STOPWORDS.has(w));
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
function isAwsRateLimitError(message) {
  return /rate.{0,5}exceeded|concurrency.{0,5}limit|throttl/i.test(message || '');
}

async function renderOnLambda(inputProps, jobRef) {
  const {renderMediaOnLambda, getRenderProgress} = require('@remotion/lambda-client');

  const renderArgs = {
    region: AWS_REGION,
    functionName: REMOTION_LAMBDA_FUNCTION_NAME,
    serveUrl: REMOTION_LAMBDA_SERVE_URL,
    composition: 'MainComp',
    inputProps,
    codec: 'h264',
    privacy: 'public',
    imageFormat: 'jpeg',
    // Higher framesPerLambda = fewer concurrent Lambdas. New AWS accounts
    // start with a 10 concurrent execution quota; 4000 frames/Lambda keeps
    // a 5-min/30fps render to ~3 Lambdas, a 10-min render to ~5. Requires
    // the 900s function timeout (a 4000-frame chunk at concurrencyPerLambda=2
    // takes well under that). Once the quota increase lands we can drop
    // this back to 1000-2000 for faster wall-clock renders.
    framesPerLambda: 4000,
    // Frames rendered concurrently inside each Lambda. Capped to the
    // number of vCPUs the function has (Remotion validates this and
    // throws otherwise). Our 3008MB memory tier maps to ~2 vCPUs, so
    // concurrencyPerLambda=2 gives roughly 2× per-Lambda speed without
    // spawning more outer Lambdas. Bump to 4 after redeploying Lambda
    // at >=5120MB (3+ vCPUs).
    concurrencyPerLambda: 2,
    // Per-Lambda invocation retries; Remotion handles transient throttles
    // at the chunk level automatically.
    maxRetries: 5,
  };

  // Outer retry: the initial spawn can be rejected by AWS even when later
  // invocations would succeed. Exponential backoff between attempts.
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
      const delaySec = attempt * 30; // 30s, 60s
      console.warn(
        `Lambda submission rate-limited (attempt ${attempt}/${maxAttempts}) — retry in ${delaySec}s`,
      );
      await jobRef.update({
        currentStep: `AWS render slot busy — retrying in ${delaySec}s (attempt ${attempt}/${maxAttempts})`,
        updatedAt: FieldValue.serverTimestamp(),
      });
      await sleep(delaySec * 1000);
    }
  }
  console.log(`Lambda renderId: ${renderId}`);

  let lastPct = -1;
  while (true) {
    await sleep(3000);
    const progress = await getRenderProgress({
      renderId,
      bucketName,
      functionName: REMOTION_LAMBDA_FUNCTION_NAME,
      region: AWS_REGION,
    });
    if (progress.fatalErrorEncountered) {
      const raw = progress.errors?.[0]?.message || 'unknown error';
      // Rephrase rate limits with actionable guidance
      if (isAwsRateLimitError(raw)) {
        throw new Error(
          `AWS render slot was busy and the per-chunk retries also exhausted. Your AWS account is at its concurrent-Lambda quota (default 10 for new accounts). Wait a few minutes and click Retry, or request a quota increase from AWS Service Quotas.`,
        );
      }
      throw new Error(`Lambda render failed: ${raw}`);
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

  // Step 2.5 — pick background music. Tone classifier reads the script;
  // we query the channel's musicTracks for matching mood and pick at
  // random. Failure is non-fatal — render proceeds without music.
  let musicUrl = null;
  let pickedMood = null;
  try {
    pickedMood = await classifyTone(
      captions.map((c) => c.text).join(' '),
      anthropicKey,
    );
    musicUrl = await pickMusicTrack(job.channelId, pickedMood);
    console.log(`tone=${pickedMood}, music=${musicUrl ? 'picked' : 'no track for this mood'}`);
  } catch (e) {
    console.warn(`Music pick failed: ${e.message} — proceeding without`);
  }
  await jobRef.update({
    musicUrl,
    pickedMood,
    currentStep: musicUrl
      ? `Music: ${pickedMood}`
      : (pickedMood ? `No ${pickedMood} track in library — silent music bed` : 'No music'),
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
    {voiceoverUrl: job.voiceoverUrl, footage, captions, musicUrl},
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

// ─── Discovery: cached trending fetch + Claude ranking ─────────────
// Cache lives on the channel doc as trendingCache: {niche, fetchedAt,
// reddit, hn, news, trends}. Fresh = same niche + <6h old.
async function getTrendingForChannel(channelId, niche) {
  const chRef = db.collection('channels').doc(channelId);
  const chSnap = await chRef.get();
  const cache = chSnap.data()?.trendingCache;
  const isFresh =
    cache && cache.niche === niche && cache.fetchedAt &&
    (Date.now() - cache.fetchedAt) < 6 * 3600 * 1000;
  if (isFresh) {
    console.log('using cached trending data, age:', Math.round((Date.now() - cache.fetchedAt) / 60000), 'min');
    return cache;
  }
  console.log('fetching fresh trending data for', niche);
  const {fetchAllTrending} = require('./lib/discover');
  const fresh = await fetchAllTrending(niche, {newsApiKey: process.env.NEWSAPI_KEY});
  await chRef.update({trendingCache: fresh});
  return fresh;
}

// Send the union of fetched topics to Claude, get back a ranked pick +
// 5 title variants + reasoning, all as JSON. The prompt heavily biases
// toward multi-source convergence and recency.
async function rankAndPickTopic(trending, niche, anthropicKey) {
  const slim = (arr) => (arr || []).slice(0, 8).map((t) => ({
    source: t.source,
    title: t.title,
    score: t.score,
    comments: t.comments,
    summary: t.summary,
    age: Math.round((Date.now() - (t.createdAt || Date.now())) / 3.6e6) + 'h',
    ...(t.extra?.subreddit ? {subreddit: t.extra.subreddit} : {}),
    ...(t.extra?.sourceName ? {publisher: t.extra.sourceName} : {}),
  }));
  const allTopics = [
    ...slim(trending.reddit),
    ...slim(trending.hn),
    ...slim(trending.news),
    ...slim(trending.trends),
  ];

  const prompt = `You are a YouTube content strategist for a faceless channel in the niche "${niche}".

I have ${allTopics.length} trending topics from Reddit, Hacker News, NewsAPI, and Google Trends. Pick the SINGLE BEST topic for a 5-minute YouTube video.

Criteria (in priority order):
1. Genuinely fits the niche "${niche}" — exclude noise that just keyword-matched (e.g. relationship drama with "finance" mentioned once)
2. Strong engagement signal across multiple sources (multi-source convergence on the same story is the strongest signal)
3. Fresh enough to feel "trending" — last 24-72h ideal, last week max
4. Has substance — could fill 5 minutes with research, not a one-line meme
5. Suitable for narration over stock footage (faceless format) — avoid topics that need a specific person on camera

Then generate 5 viral title variants for that topic, ranked by predicted CTR using these patterns (mix them):
- Curiosity gap: "The [thing] nobody is talking about"
- Numbers: "[N] reasons why X happened"
- Contrarian: "Why everyone is wrong about [X]"
- News hook: "What [news event] really means"
- Question hook: "Did [X] just signal [Y]?"

Submit your proposal via the submit_topic_proposal tool. Include exactly 5 title variants and ensure pickedTitle exactly matches one of them.

Topics:
${JSON.stringify(allTopics, null, 2)}`;

  const parsed = await callAnthropicWithTool(anthropicKey, prompt, TOPIC_PROPOSAL_TOOL, 4096);
  if (!parsed.pickedTopic || !Array.isArray(parsed.titleVariants) || parsed.titleVariants.length < 1) {
    throw new Error('submit_topic_proposal tool returned malformed proposal');
  }
  return parsed;
}

// ─── Trigger: discovery jobs ────────────────────────────────────────
exports.processDiscoveryJob = onDocumentCreated(
  {
    document: 'discoveryJobs/{jobId}',
    secrets: [NEWSAPI_KEY],
    timeoutSeconds: 300,
    memory: '512MiB',
    region: 'us-central1',
  },
  async (event) => {
    const jobId = event.params.jobId;
    const data = event.data?.data();
    if (!data) return;
    if (data.status !== 'pending') {
      console.log(`[${jobId}] status=${data.status}, skipping`);
      return;
    }
    console.log(`[${jobId}] discovery job for niche "${data.niche}"`);

    const jobRef = db.collection('discoveryJobs').doc(jobId);
    const projRef = data.projectId
      ? db.collection('channels').doc(data.channelId).collection('projects').doc(data.projectId)
      : null;

    try {
      const ownerSnap = await db.collection('users').doc(data.ownerId).get();
      const anthropicKey = ownerSnap.data()?.settings?.anthropicKey;
      if (!anthropicKey) {
        throw new Error('Owner has no Anthropic key — set it under Settings → Integrations');
      }

      // Step 1: research
      await jobRef.update({
        status: 'researching',
        currentStep: 'Pulling trending topics from Reddit, HN, NewsAPI, Trends',
        progress: 20,
        updatedAt: FieldValue.serverTimestamp(),
      });
      if (projRef) await projRef.update({discoveryStatus: 'researching'});

      const trending = await getTrendingForChannel(data.channelId, data.niche);
      const totalCount =
        (trending.reddit?.length || 0) + (trending.hn?.length || 0) +
        (trending.news?.length || 0) + (trending.trends?.length || 0);
      console.log(`[${jobId}] ${totalCount} topics fetched`);

      // Step 2: rank with Claude
      await jobRef.update({
        currentStep: `Ranking ${totalCount} topics with Claude`,
        progress: 60,
        trendingTopicData: {
          fetchedAt: trending.fetchedAt,
          counts: trending.counts,
        },
        updatedAt: FieldValue.serverTimestamp(),
      });

      const proposal = await rankAndPickTopic(trending, data.niche, anthropicKey);
      console.log(`[${jobId}] picked: ${proposal.pickedTopic}`);
      console.log(`[${jobId}] title: ${proposal.pickedTitle}`);

      // Step 3: propose — mirror to project so the pipeline UI sees it
      await jobRef.update({
        status: 'proposing',
        currentStep: 'Awaiting approval',
        progress: 100,
        pickedTopic: proposal.pickedTopic,
        pickedTopicSummary: proposal.pickedTopicSummary,
        pickedTopicReasoning: proposal.pickedTopicReasoning,
        pickedTopicSources: proposal.pickedTopicSources || [],
        titleVariants: proposal.titleVariants,
        pickedTitle: proposal.pickedTitle,
        updatedAt: FieldValue.serverTimestamp(),
      });
      if (projRef) {
        await projRef.update({
          discoveryStatus: 'proposing',
          pickedTopic: proposal.pickedTopic,
          pickedTopicSummary: proposal.pickedTopicSummary,
          pickedTopicReasoning: proposal.pickedTopicReasoning,
          pickedTopicSources: proposal.pickedTopicSources || [],
          titleVariants: proposal.titleVariants,
          pickedTitle: proposal.pickedTitle,
        });
      }

      // Notify owner the proposal is ready
      if (data.ownerId) {
        const notifRef = db.collection('users').doc(data.ownerId).collection('notifications').doc();
        await notifRef.set({
          type: 'submission',
          message: `🔍 AI proposed a topic: "${proposal.pickedTopic.slice(0, 60)}" — review`,
          fromName: 'Discover',
          link: null,
          read: false,
          ts: FieldValue.serverTimestamp(),
        });
      }
    } catch (e) {
      console.error(`[${jobId}] discovery failed:`, e);
      try {
        await jobRef.update({
          status: 'failed',
          currentStep: 'Failed',
          error: e.message || 'Unknown error',
          errorDetail: buildErrorDetail(e),
          updatedAt: FieldValue.serverTimestamp(),
        });
      } catch (richUpdateErr) {
        console.error(`[${jobId}] rich discovery error update failed — falling back to minimal`);
        await jobRef.update({
          status: 'failed',
          currentStep: 'Failed',
          error: `${e.message || 'Unknown error'}  [errorDetail save failed: ${richUpdateErr.message}]`,
          updatedAt: FieldValue.serverTimestamp(),
        }).catch(() => {});
      }
      if (projRef) {
        await projRef.update({
          discoveryStatus: 'failed',
          discoveryError: e.message || 'Unknown error',
        }).catch(() => {});
      }
    }
  },
);

// ─── Watchdog: clean up jobs that get stuck mid-flight ──────────────
// Runs every 5 minutes. Any job in a non-terminal status whose updatedAt
// is older than 10 minutes is force-marked failed. Catches the case
// where the function process itself crashed before the catch block
// could run.
exports.zombieJobWatchdog = onSchedule(
  {
    schedule: 'every 5 minutes',
    region: 'us-central1',
    timeoutSeconds: 60,
    memory: '256MiB',
  },
  async () => {
    const cutoffMs = Date.now() - 10 * 60 * 1000;

    async function sweep(collectionName, activeStatuses) {
      const snap = await db.collection(collectionName)
        .where('status', 'in', activeStatuses)
        .limit(50)
        .get();
      let killed = 0;
      for (const doc of snap.docs) {
        const data = doc.data();
        const updatedMs = data.updatedAt?.toMillis ? data.updatedAt.toMillis() : 0;
        if (!updatedMs || updatedMs > cutoffMs) continue;
        const ageMin = Math.round((Date.now() - updatedMs) / 60000);
        console.log(`[watchdog] ${collectionName}/${doc.id} stuck ${ageMin}min in ${data.status} — marking failed`);
        await doc.ref.update({
          status: 'failed',
          currentStep: 'Failed',
          error: `Job timed out — Cloud Function may have crashed before recording the failure (last status: ${data.status}, ${ageMin} minutes ago)`,
          updatedAt: FieldValue.serverTimestamp(),
        }).catch((err) => console.error(`[watchdog] could not mark ${doc.id}:`, err));
        killed++;
      }
      return killed;
    }

    const editingKilled = await sweep(
      'editingJobs',
      ['pending', 'researching', 'sourcing', 'captions', 'rendering'],
    );
    const discoveryKilled = await sweep(
      'discoveryJobs',
      ['pending', 'researching', 'proposing'],
    );
    if (editingKilled || discoveryKilled) {
      console.log(`[watchdog] swept editingJobs:${editingKilled}, discoveryJobs:${discoveryKilled}`);
    }
  },
);

// ─── Trigger: editing jobs (existing) ───────────────────────────────
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
      // Bulletproof failure update: try the rich update first (with full
      // errorDetail), and if THAT throws (Firestore rejecting some weird
      // value, doc too large, etc.) fall through to a minimal update so
      // the job NEVER stays in a non-terminal status. The watchdog cleans
      // up if even this fails.
      try {
        await db.collection('editingJobs').doc(jobId).update({
          status: 'failed',
          currentStep: 'Failed',
          error: e.message || 'Unknown error',
          errorDetail: buildErrorDetail(e),
          updatedAt: FieldValue.serverTimestamp(),
        });
      } catch (richUpdateErr) {
        console.error(`[${jobId}] rich error update failed (${richUpdateErr.message}) — falling back to minimal`);
        await db.collection('editingJobs').doc(jobId).update({
          status: 'failed',
          currentStep: 'Failed',
          error: `${e.message || 'Unknown error'}  [errorDetail save failed: ${richUpdateErr.message}]`,
          updatedAt: FieldValue.serverTimestamp(),
        }).catch((minErr) => console.error(`[${jobId}] even minimal update failed:`, minErr));
      }
    }
  },
);
