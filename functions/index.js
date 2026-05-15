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

const {onDocumentCreated, onDocumentUpdated} = require('firebase-functions/v2/firestore');
const {onSchedule} = require('firebase-functions/v2/scheduler');
const {onRequest, onCall, HttpsError} = require('firebase-functions/v2/https');
const {defineSecret} = require('firebase-functions/params');
const {setGlobalOptions} = require('firebase-functions/v2');
const admin = require('firebase-admin');
const crypto = require('node:crypto');
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
// NOTE: YouTube Data API access for the competitor scraper reuses an
// existing channel's OAuth refresh token via the Render proxy's
// /youtube/competitor-top-videos endpoint — no separate API key needed.
// NOTE: Anthropic keys are stored per-user in Firestore at
// /users/{uid}/secrets/keys.anthropicKey — NOT in Secret Manager.
// scrapeCompetitorThumbnails + analyzeOwnThumbnailFormula load the
// owner user's key via loadOwnerAnthropicKey() at runtime.
// Shared secret used by the event-triggered kicker to authenticate to
// the long-running HTTP worker. Both functions bind it; only the holder
// of the secret can invoke the worker.
const KICKER_SECRET = defineSecret('KICKER_SECRET');
// Gemini voiceover-timing analysis (used by getGeminiTimingTags) and
// Logo.dev company-logo lookup (used by enrichBeatsWithEntityImages).
// Both have graceful no-op fallbacks if missing — but the features
// degrade silently, so always bind these on processEditingJobHttp.
const GEMINI_API_KEY = defineSecret('GEMINI_API_KEY');
const LOGO_DEV_API_KEY = defineSecret('LOGO_DEV_API_KEY');

// ─── Non-secret config ──────────────────────────────────────────────
const AWS_REGION = 'us-east-1';
const REMOTION_LAMBDA_FUNCTION_NAME =
  'remotion-render-4-0-456-mem3008mb-disk10240mb-900sec';
const REMOTION_LAMBDA_SERVE_URL =
  'https://remotionlambda-useast1-1zsfacnzw9.s3.us-east-1.amazonaws.com/sites/rem-report-v6/index.html';
// Render-hosted proxy — used by the auto-forensic scheduler to call
// /youtube/analytics/refresh and /youtube/analytics/forensic on a cron.
// Same URL the frontend uses; the proxy validates per-channel auth
// against Firestore on every request.
const RENDER_PROXY_URL = 'https://ytauto-proxy.onrender.com';
// Estimate per scheduled forensic run (Decodo bandwidth + per-video
// processing). Real cost varies by video length × caption density;
// $0.30 is a conservative floor based on the first observed run. The
// scheduler records this estimate per run for the cost dashboard;
// when actual Decodo billing data is available we can swap to real
// numbers without changing the schema.
const FORENSIC_RUN_COST_ESTIMATE_USD = 0.30;

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
// Node 22's built-in fetch is backed by undici with default
// headersTimeout: 300_000 + bodyTimeout: 300_000. Dense breakIntoBeats
// tool_use responses (18k+ output tokens after the Issue 4 prompt
// tightening) routinely take 4-5 minutes — the body-bytes-flowing
// clock fires at 300s and propagates as a generic "fetch failed",
// unrelated to the function's own timeoutSeconds. Custom Agent at
// module level so the dispatcher is reused across invocations of the
// same warm instance. 600s gives comfortable headroom under the 9-min
// Gen 2 callable cap and the 60-min HTTP cap on production workers.
const {Agent: UndiciAgent} = require('undici');
const longTimeoutAgent = new UndiciAgent({
  headersTimeout: 600_000,
  bodyTimeout: 600_000,
});

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
    dispatcher: longTimeoutAgent,
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
  // Surface stop_reason. If max_tokens hit, the tool_use input is
  // partial — Claude prunes optional fields to stay under budget. That's
  // exactly the "134 beats with 0 overlays" bug: max_tokens=4096 wasn't
  // enough for a 134-beat tool call so Claude omitted every non-required
  // field. Caller can decide whether to retry with larger budget.
  if (data.stop_reason === 'max_tokens') {
    const inputBytes = JSON.stringify(toolUse.input || {}).length;
    console.warn(`[callAnthropicWithTool] STOP_REASON=max_tokens — tool=${tool.name} budget=${maxTokens} returned input size=${inputBytes} bytes. Tool output likely pruned of optional fields.`);
  }
  // Telemetry hook: also surface usage tokens when available.
  if (data.usage) {
    console.log(`[callAnthropicWithTool] ${tool.name} usage: in=${data.usage.input_tokens} out=${data.usage.output_tokens} stop=${data.stop_reason}`);
  }
  return toolUse.input;
}

// Tool schemas — kept here so they live next to the call sites.
const BEATS_TOOL = {
  name: 'submit_beats',
  description: 'Submit the timed beats for a YouTube video. Each beat is a contiguous group of whole sentences that will be rendered with one stock clip or (rarely, when stock fails) an AI-generated video clip. The beats array must contain at least one beat — never submit an empty array.',
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
            fluxPrompt: {type: 'string', description: 'A vivid photorealistic prompt for AI video generation, used only when stock footage fails or for hero moments. One cinematic sentence describing the scene with camera direction, lighting, mood. No text overlays. 16:9 framing.'},
            heroMoment: {type: 'boolean', description: 'TRUE for 3-5 standout beats per video — the intro hook, a major statistic reveal, an emotional turn, the conclusion. FALSE for every other beat. You MUST set this on every beat (true or false explicitly).'},
            shouldAnimate: {type: 'boolean', description: 'BE VERY AGGRESSIVE — for a 5-min finance video aim for 12-15 shouldAnimate=true beats (the user explicitly wants more visual life). Set TRUE for: ANY abstract economic concept (recession, inflation, growth, decline), ANY market behaviour (crash, rally, panic, FOMO), ANY regulatory action (vote, decision, ruling, policy), ANY prediction or forecast ("could", "might", "expected"), ANY metaphor / analogy, ANY transitional beat between concepts, ANY systemic phenomenon (supply chain, monetary policy). Set FALSE only for: specific named entities like Powell or Apple HQ (entityPortrait handles those), specific concrete physical objects, beats anchored to a chart/ticker (overlays handle those). When in doubt, set TRUE. More animations = better. Hard cap is 15 per video; if you flag more, the pipeline picks the highest-impact 15 (hero beats first).'},
            kenBurnsIntensity: {type: 'string', enum: ['subtle', 'medium', 'aggressive'], description: 'Visual motion intensity for this beat. Default medium. Use aggressive on hero beats and energetic moments; subtle on quiet/contemplative beats.'},
            entities: {
              type: 'array',
              description: 'REQUIRED for any beat that names a specific real person, company, or organization. The pipeline auto-fetches a Wikipedia image and injects an entityPortrait overlay — without this, the named-person beat renders with no visual reference and looks generic. For finance / news content expect 1-2 entities on at least 30% of beats: people like Jerome Powell, Cynthia Lummis, Jensen Huang, Larry Fink; companies like Nvidia, Apple, Goldman Sachs, BlackRock; organizations like the Fed, IMF, SEC, Treasury. Only skip on pure-concept beats (e.g. "inflation", "the market") with no specific entity named.',
              maxItems: 3,
              items: {
                type: 'object',
                properties: {
                  name: {type: 'string', description: 'Canonical name (e.g. "Jerome Powell", "Goldman Sachs", "Federal Reserve").'},
                  type: {type: 'string', enum: ['person', 'company', 'place', 'event'], description: 'What kind of entity this is. Drives layout (people get portrait crop, companies get logo treatment).'},
                },
                required: ['name', 'type'],
              },
            },
            musicMood: {
              type: 'string',
              enum: ['calm', 'energetic', 'dramatic', 'mysterious', 'uplifting', 'corporate', 'cinematic'],
              description: 'OPTIONAL. Set ONLY on the FIRST beat (i=0). One of the listed moods, picked to match the overall script tone. Drives background-music selection for the whole video. Leave undefined on every other beat.',
            },
            sfx: {
              type: 'array',
              description: 'OPTIONAL. Up to 2 sound effects to overlay on this beat. Use sparingly — average 1 SFX per 4-5 beats across the video. Skip on most beats.',
              maxItems: 2,
              items: {
                type: 'object',
                properties: {
                  tag: {type: 'string', enum: ['whoosh', 'transition', 'impact', 'ding'], description: 'Only these four tags are allowed. whoosh/transition for topic shifts; impact/ding for stat reveals or hero moments.'},
                  offset: {type: 'number', description: 'Seconds from beat.start. 0 = beat start.'},
                },
                required: ['tag', 'offset'],
              },
            },
            overlays: {
              type: 'array',
              description: 'On-screen overlays for this beat. AT MOST 1 per beat — no exceptions. The vast majority of beats should have ZERO overlays (just footage + voiceover + captions). Only place an overlay when the beat carries a single, undeniably overlay-worthy moment: a hero stat reveal, a one-line cited quote, a key source attribution. Two adjacent beats with overlays look chaotic; aim for ~1 overlay per 3-5 seconds of video, not 1 per beat. A 5-minute video should have 6-10 overlays, NOT 15-20. Empty overlays array is the default; opt INTO an overlay only when the beat clearly demands it.',
              maxItems: 1,
              items: {
                type: 'object',
                properties: {
                  type: {
                    type: 'string',
                    enum: [
                      'stat', 'lowerThird', 'quote',                                             // legacy types still in active use
                      'bigStat',                                                                 // headline number; auto-animates count-up when text is parseable
                      'lottie',                                                                  // Lottie animation (scaffold — needs assets registered)
                      'entityPortrait',                                                          // server-injected, do not place manually
                    ],
                    description: 'Overlay type. Pick: bigStat for headline numbers (the renderer auto-animates a 0→target count-up when the text parses as ≥$1B currency or ≥20% percent — no separate countUp type, just supply the final number string), lowerThird for source attribution, quote for cited statements, stat for small corner badges. entityPortrait is server-injected from beat.entities — do NOT place manually. lottie is scaffolded but no asset library yet — do NOT place manually until told otherwise.',
                  },
                  name: {type: 'string', maxLength: 80, description: 'entityPortrait only (server-injected): entity name label.'},
                  entityType: {type: 'string', enum: ['person', 'company', 'place', 'event'], description: 'entityPortrait only (server-injected): drives layout treatment.'},
                  text: {type: 'string', maxLength: 80, description: 'Used by: stat, lowerThird, quote, bigStat (the headline number). For bigStat use symbols ($2.4T not $2,400,000,000,000). Max 80 chars.'},
                  position: {type: 'string', enum: ['topLeft', 'topRight', 'bottomLeft', 'bottomRight', 'center', 'top', 'bottom'], description: 'For legacy types: corner anchor. For bigStat: center | top | bottom. For tickerSymbol: typically topRight. Avoid bottom-* on legacy types when captions occupy the lower-third.'},
                  startOffset: {type: 'number', description: 'Seconds from beat.start.'},
                  duration: {type: 'number', description: 'Seconds the overlay stays visible. 2-5s for bigStat, 1.5-3s for others.'},

                  // ─ bigStat fields ─
                  // bigStat is intentionally minimal: just `text`, optional
                  // `position`. The number always renders in brand orange
                  // with a brand-orange halo behind it (one visual identity
                  // for every bigStat — no color variants, no subtitle).
                  // Numeric text auto-animates a 0→target count-up at
                  // render time when it parses as ≥$1B currency or ≥20%
                  // percent. Non-numeric text renders static.

                  // ─ lottie fields ─
                  lottieFile: {type: 'string', maxLength: 40, description: 'lottie only. Logical key for a Lottie JSON registered in LOTTIE_MAP in Overlay.jsx. Currently NO keys are registered — do not place lottie overlays until the asset library has been populated.'},

                  // (webpageScreenshot fields removed — overlay type deleted.)

                  // ─ comparison fields ─
                  leftLabel: {type: 'string', maxLength: 40, description: 'comparison only. Top label on the left side (e.g. "Before", "2024").'},
                  leftValue: {type: 'string', maxLength: 30, description: 'comparison only. Big value on the left side (e.g. "$48K").'},
                  rightLabel: {type: 'string', maxLength: 40, description: 'comparison only. Top label on the right side.'},
                  rightValue: {type: 'string', maxLength: 30, description: 'comparison only. Big value on the right side.'},
                  vsLabel: {type: 'string', enum: ['vs', 'or', 'versus'], description: 'comparison only. Center separator label.'},

                  // ─ tickerSymbol fields ─
                  symbol: {type: 'string', maxLength: 10, description: 'tickerSymbol only. Stock ticker (e.g. "AAPL").'},
                  price: {type: 'string', maxLength: 16, description: 'tickerSymbol only. Current price string with currency symbol (e.g. "$182.45").'},
                  change: {type: 'string', maxLength: 12, description: 'tickerSymbol only. Change with sign + unit (e.g. "+2.34%").'},
                  direction: {type: 'string', enum: ['up', 'down'], description: 'tickerSymbol only. Up = green, down = red. Infer from the sign of `change`.'},

                  // ─ newsAlert fields ─
                  headline: {type: 'string', maxLength: 120, description: 'newsAlert only. The breaking headline text.'},
                  source: {type: 'string', maxLength: 80, description: 'quote: REQUIRED — named person/company being quoted. MUST also appear in beat.entities so the renderer can pair the quote under that entity\'s portrait. Quotes without a same-beat entityPortrait whose name matches `source` are DROPPED server-side. newsAlert: optional attribution (e.g. "Reuters", "Bloomberg").'},
                  urgency: {type: 'string', enum: ['breaking', 'alert', 'update'], description: 'newsAlert only. breaking = red, alert = orange, update = neutral.'},

                  // ─ progressBar fields ─
                  label: {type: 'string', maxLength: 40, description: 'progressBar only. Label above the bar (e.g. "Unemployment Rate").'},
                  value: {type: 'number', description: 'progressBar only. Target value the bar fills to (0-100 for percent, raw amount for currency/plain).'},
                  format: {type: 'string', enum: ['percent', 'currency', 'plain'], description: 'progressBar only. How to format the counter — percent appends %, currency prepends $.'},
                  showCounter: {type: 'boolean', description: 'progressBar only. true = show counter ticking up alongside bar fill.'},

                  // ─ miniChart fields ─
                  chartLabel: {type: 'string', maxLength: 40, description: 'miniChart only. Short label above the sparkline (e.g. "S&P 500 — last 30 days").'},
                  chartDirection: {type: 'string', enum: ['up', 'down', 'volatile', 'flat'], description: 'miniChart only. Drives synthetic data shape + colour (up=green, down=red, volatile=mixed, flat=neutral).'},
                  chartPoints: {type: 'integer', description: 'miniChart only. Number of data points to plot (8-20 typical). Defaults to 12.'},
                  chartCorner: {type: 'string', enum: ['topRight', 'topLeft', 'bottomRight'], description: 'miniChart only. Which corner. Default topRight.'},

                  // ─ circleEmphasis fields ─
                  circlePosition: {
                    type: 'object',
                    description: 'circleEmphasis only. Center + radius of the hand-drawn circle, in 1920x1080 coordinates.',
                    properties: {
                      x: {type: 'number'},
                      y: {type: 'number'},
                      radius: {type: 'number'},
                    },
                  },
                  color: {type: 'string', enum: ['red', 'yellow', 'accent'], description: 'circleEmphasis only. Stroke color.'},
                  drawSpeed: {type: 'string', enum: ['fast', 'medium', 'slow'], description: 'circleEmphasis only. How long the stroke takes to draw on (fast ≈400ms, slow ≈1s).'},
                },
                required: ['type', 'startOffset', 'duration'],
              },
            },
          },
          required: ['start', 'end', 'sentence', 'keywords', 'fluxPrompt', 'heroMoment'],
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

// Picks a music track for a render. Reads the channel's musicSettings
// to decide whether to include the global library; falls back broadly
// if no mood-matched track exists. Returns null when nothing is
// available (silent music bed is fine — better than fake-cheerful).
// Returns false for tracks the user has flagged after a YouTube
// Content ID claim. Pipeline excludes them at selection time so the
// next render doesn't reuse the offending audio.

// Phase 5A — title-based blocklist for tracks that have triggered
// Content ID claims but haven't (yet) been flagged with
// licensingStatus='claimed' on their Firestore doc. Cheaper than
// chasing the doc down via a one-off script: filter in code, the row
// stays in the DB in case we want to re-enable later. Match is exact
// against either `title` or `name` to cover both field-name styles.
const MUSIC_TITLE_BLOCKLIST = new Set([
  'Inspirational Cinematic Orchestra',
  'Sigmamusicart Epic Cinematic',
]);

// Substring keyword blocklist — case-insensitive match against the
// track title. Catches variants like "Sigmamusicart Epic Cinematic" or
// "Epic Cinematic Adventure" without requiring each title to be
// enumerated by hand. Accepted tradeoff: some clean tracks containing
// these stock phrases may also match. Royalty-free libraries reuse
// these phrases heavily, and Content ID claims have correlated with
// them, so the false-positive risk is bounded.
const MUSIC_KEYWORD_BLOCKLIST = [
  'epic cinematic',
  'cinematic orchestra',
];

function isMusicTrackUsable(t) {
  if (!t) return false;
  if (t.licensingStatus === 'claimed') return false;
  if (t.claimed === true) return false; // legacy field name
  const titleOrName = t.title || t.name;
  if (!titleOrName) return true;
  if (MUSIC_TITLE_BLOCKLIST.has(titleOrName)) {
    console.log(`[music] excluded "${titleOrName}" — matched MUSIC_TITLE_BLOCKLIST`);
    return false;
  }
  const lower = titleOrName.toLowerCase();
  for (const kw of MUSIC_KEYWORD_BLOCKLIST) {
    if (lower.includes(kw)) {
      console.log(`[music] excluded "${titleOrName}" — matched keyword "${kw}"`);
      return false;
    }
  }
  return true;
}

async function pickMusicTrack(channelId, mood) {
  const settings = await getChannelMusicSettings(channelId);
  const useGlobal = settings.useGlobalLibrary !== false;

  // Build the candidate pool. Per-channel tracks always included; global
  // included only when the toggle is on. Querying both in parallel.
  const queries = [
    db.collection('channels').doc(channelId).collection('musicTracks')
      .where('mood', '==', mood).get(),
  ];
  if (useGlobal) {
    queries.push(
      db.collection('globalMusicTracks').where('mood', '==', mood).get(),
    );
  }
  const [chSnap, globalSnap] = await Promise.all(queries);
  let pool = chSnap.docs.map((d) => d.data()).filter(isMusicTrackUsable);
  if (globalSnap) pool = pool.concat(globalSnap.docs.map((d) => d.data()).filter(isMusicTrackUsable));

  // Mood-pool empty? Fall back to ANY track (mood missing better than
  // silent for renders where the channel has only wrong-mood tracks).
  if (pool.length === 0) {
    const fallbackQueries = [
      db.collection('channels').doc(channelId).collection('musicTracks').get(),
    ];
    if (useGlobal) fallbackQueries.push(db.collection('globalMusicTracks').get());
    const [chAny, globalAny] = await Promise.all(fallbackQueries);
    pool = chAny.docs.map((d) => d.data()).filter(isMusicTrackUsable);
    if (globalAny) pool = pool.concat(globalAny.docs.map((d) => d.data()).filter(isMusicTrackUsable));
    if (pool.length) {
      console.log(`[music] no '${mood}' tracks — falling back to any-mood pool (${pool.length} tracks after claim-filter)`);
    }
  }

  if (pool.length === 0) return null;
  const picked = pool[Math.floor(Math.random() * pool.length)];
  return picked.url || null;
}

// Claude-driven music selection. Replaces blind random-pick from a
// mood pool with a single LLM judgement call against the full track
// list. Returns {url, volumeRecommendation, trackId, reasoning} or
// throws on any failure (caller falls back to pickMusicTrack).
//
// Concrete tradeoff vs old pickMusicTrack: one extra Anthropic call
// (~$0.01 with Sonnet, ~5-8s wall-clock) for selection that's
// "better than random" — bounded above by what the track metadata
// supports. With the existing Pixabay library, expect modest gains
// over random when 5+ tracks share a mood; bigger gains as the
// library grows or as tracks pick up real LUFS / description metadata.
async function selectMusicForVideo({
  channelId,
  anthropicKey,
  voiceoverDurationSec,
  topic,
  scriptText,
  tone,
  beatCount,
  heroBeatTimestamps,
}) {
  if (!anthropicKey) throw new Error('no anthropic key for music selection');
  // Pull every track the channel can use — same union pattern as the
  // old picker. Random fallback re-uses pickMusicTrack which queries
  // identically, so we're not adding extra reads.
  const settings = await getChannelMusicSettings(channelId);
  const useGlobal = settings.useGlobalLibrary !== false;
  const queries = [
    db.collection('channels').doc(channelId).collection('musicTracks').get(),
  ];
  if (useGlobal) queries.push(db.collection('globalMusicTracks').get());
  const [chSnap, globalSnap] = await Promise.all(queries);
  const tracks = [];
  for (const d of chSnap.docs) {
    const t = {id: d.id, source: 'channel', ...d.data()};
    if (isMusicTrackUsable(t)) tracks.push(t);
  }
  if (globalSnap) for (const d of globalSnap.docs) {
    const t = {id: d.id, source: 'global', ...d.data()};
    if (isMusicTrackUsable(t)) tracks.push(t);
  }
  if (tracks.length === 0) {
    // Distinguish "no tracks at all" from "all tracks claim-flagged"
    // so the user sees a meaningful error in the editing-job log.
    throw new Error('no tracks available — all candidates either missing or flagged as claimed');
  }

  // Pre-filter to tracks long enough to cover the video without heavy
  // looping (≥ 80% of voiceover length). If the resulting pool is too
  // small we widen back to the full set — Claude can still pick the
  // longest available even when nothing matches the duration target.
  const longEnough = tracks.filter((t) => (t.duration || 0) >= voiceoverDurationSec * 0.8);
  const candidatePool = longEnough.length >= 3 ? longEnough : tracks;

  const trackLines = candidatePool.map((t) => {
    const dur = t.duration ? `${Math.round(t.duration)}s` : 'unknown duration';
    return `- ID:${t.id}  mood:${t.mood||'?'}  ${dur}  name:"${(t.name||'').slice(0, 60)}"`;
  }).join('\n');

  const scriptIntro = (scriptText || '').slice(0, 500);
  const scriptOutro = (scriptText || '').slice(-200);

  const prompt = `Pick ONE background music track for this YouTube video.

VIDEO
- Topic: ${topic || '(unknown)'}
- Duration: ${Math.round(voiceoverDurationSec)}s (≈${(voiceoverDurationSec / 60).toFixed(1)} min)
- Tone: ${tone || '(unknown)'}
- Script intro: ${scriptIntro}
- Script climax: ${scriptOutro}

TRACKS (${candidatePool.length}):
${trackLines}

Pick the best fit on mood + topic. Prefer tracks with duration ≥ video length when possible.
Don't worry about volume — that's controlled separately by the render pipeline.

Output ONLY a JSON object, no prose, no fences:
{"trackId":"<id>","reasoning":"<one short sentence>"}`;

  const result = await callAnthropicForJSON(anthropicKey, prompt, 300);
  const trackId = result?.trackId;
  if (!trackId) throw new Error(`Claude returned no trackId: ${JSON.stringify(result).slice(0, 200)}`);
  const picked = candidatePool.find((t) => t.id === trackId);
  if (!picked) throw new Error(`Claude picked unknown trackId ${trackId}`);
  return {
    url: picked.url,
    trackId: picked.id,
    trackName: picked.name,
    trackMood: picked.mood,
    trackDuration: picked.duration || 0,
    reasoning: (result.reasoning || '').slice(0, 200),
  };
}

// Run ebur128 over a local audio file to extract integrated loudness
// (LUFS). Returns null on failure — never blocks an upload, just means
// the track lands without precomputed loudness metadata.
async function measureIntegratedLoudness(localPath) {
  const ffmpegPath = getFfmpegPath();
  if (!ffmpegPath) return null;
  return new Promise((resolve) => {
    const {spawn} = require('node:child_process');
    const proc = spawn(ffmpegPath, [
      '-i', localPath,
      '-af', 'ebur128',
      '-f', 'null', '-',
    ], {stdio: ['ignore', 'ignore', 'pipe']});
    let err = '';
    proc.stderr.on('data', (c) => { err += c.toString(); });
    const timer = setTimeout(() => { try { proc.kill('SIGKILL'); } catch {} resolve(null); }, 60000);
    proc.once('close', () => {
      clearTimeout(timer);
      // ebur128 prints a final summary block ending with "Integrated loudness:"
      const m = /Integrated loudness:[\s\S]*?I:\s*(-?[\d.]+)\s*LUFS/i.exec(err);
      resolve(m ? parseFloat(m[1]) : null);
    });
    proc.once('error', () => { clearTimeout(timer); resolve(null); });
  });
}

// Run astats to extract peak level (dB). Used for SFX upload.
async function measurePeakDb(localPath) {
  const ffmpegPath = getFfmpegPath();
  if (!ffmpegPath) return null;
  return new Promise((resolve) => {
    const {spawn} = require('node:child_process');
    const proc = spawn(ffmpegPath, [
      '-i', localPath,
      '-af', 'astats=metadata=1:reset=0',
      '-f', 'null', '-',
    ], {stdio: ['ignore', 'ignore', 'pipe']});
    let err = '';
    proc.stderr.on('data', (c) => { err += c.toString(); });
    const timer = setTimeout(() => { try { proc.kill('SIGKILL'); } catch {} resolve(null); }, 60000);
    proc.once('close', () => {
      clearTimeout(timer);
      // astats prints multiple "Peak level dB" — take the last (overall)
      const matches = [...err.matchAll(/Peak level dB:\s*(-?[\d.]+)/g)];
      resolve(matches.length ? parseFloat(matches[matches.length - 1][1]) : null);
    });
    proc.once('error', () => { clearTimeout(timer); resolve(null); });
  });
}

async function getChannelMusicSettings(channelId) {
  // Default volume 0.06 (6%) — constant under voiceover. No ducking.
  // Voiceover sits on top at 1.0 and music doesn't compete.
  const fallback = {useGlobalLibrary: true, preferredMoods: [], volume: 0.06};
  if (!channelId) return fallback;
  try {
    const doc = await db.collection('channels').doc(channelId).get();
    return {...fallback, ...(doc.data()?.musicSettings || {})};
  } catch (e) {
    console.warn(`[music] failed to read musicSettings: ${e.message}`);
    return fallback;
  }
}

// ─── SFX (sound effects) ───────────────────────────────────────────
// Locked tag set. New tags need code changes — keeps Claude from
// inventing tags we don't have, and the volume table from drifting.
const SFX_TAGS = ['whoosh', 'hit', 'impact', 'swoop', 'ding', 'transition', 'pop'];

// Per-tag volume in linear 0-1 (Remotion's <Audio volume>). Roughly
// translated from the dB targets agreed in the SFX spec — they sit at
// background-cue levels so they enhance, never dominate, voiceover.
const SFX_VOLUME_PRESETS = {
  impact: 0.32,      // -10 dB — for stat reveals
  hit: 0.25,         // -12 dB — for transitions
  ding: 0.20,        // -14 dB — for stat / quote reveals
  pop: 0.20,         // -14 dB — for text appears
  whoosh: 0.16,      // -16 dB — for camera moves
  swoop: 0.16,       // -16 dB — for sweeps
  transition: 0.13,  // -18 dB — background cues
};

async function getChannelSfxSettings(channelId) {
  const fallback = {useGlobalLibrary: true};
  if (!channelId) return fallback;
  try {
    const doc = await db.collection('channels').doc(channelId).get();
    return {...fallback, ...(doc.data()?.sfxSettings || {})};
  } catch (e) {
    console.warn(`[sfx] failed to read sfxSettings: ${e.message}`);
    return fallback;
  }
}

// Pick a random SFX track for a tag. Same union pattern as music:
// per-channel always included, global included when useGlobalLibrary.
// SFX tracks have a `tags` array (multi-tag) since the same file may
// fit several categories ("hit"+"impact" e.g.). Filters by
// array-contains. Returns the track's public URL or null.
async function pickSfxTrack(channelId, tag) {
  if (!SFX_TAGS.includes(tag)) {
    console.warn(`[sfx] unknown tag '${tag}' — skipping`);
    return null;
  }
  const settings = await getChannelSfxSettings(channelId);
  const useGlobal = settings.useGlobalLibrary !== false;

  const queries = [
    db.collection('channels').doc(channelId).collection('sfxTracks')
      .where('tags', 'array-contains', tag).get(),
  ];
  if (useGlobal) {
    queries.push(
      db.collection('globalSfxTracks').where('tags', 'array-contains', tag).get(),
    );
  }
  const [chSnap, globalSnap] = await Promise.all(queries);
  let pool = chSnap.docs.map((d) => d.data());
  if (globalSnap) pool = pool.concat(globalSnap.docs.map((d) => d.data()));
  if (pool.length === 0) return null;
  const picked = pool[Math.floor(Math.random() * pool.length)];
  return picked.url || null;
}

// Read this channel's analytics insights from Firestore. Returns null
// when the channel hasn't connected analytics or hasn't refreshed yet,
// in which case callers fall back to today's behaviour. Channel
// isolation: only this channel's data is consulted — never any other.
async function getChannelAnalyticsInsights(channelId) {
  if (!channelId) return null;
  try {
    const doc = await db.collection('channels').doc(channelId)
      .collection('analytics').doc('summary').get();
    return doc.exists ? (doc.data().insights || null) : null;
  } catch (e) {
    console.warn(`[analytics] failed to read insights for ${channelId}: ${e.message}`);
    return null;
  }
}

// Phase E — read this channel's forensic doc (per-video retention +
// transcript analysis + Claude-extracted patterns). Returns null if
// the forensic analyzer hasn't been run for this channel. Channel
// isolation: only ever reads forensic data for the channelId passed in.
async function getChannelForensicData(channelId) {
  if (!channelId) return null;
  try {
    const doc = await db.collection('channels').doc(channelId)
      .collection('analytics').doc('forensic').get();
    return doc.exists ? doc.data() : null;
  } catch (e) {
    console.warn(`[forensic] failed to read for ${channelId}: ${e.message}`);
    return null;
  }
}

// Wikipedia thumbnail lookup for an entity name. Returns null on
// 404 / disambiguation / no thumbnail. Public REST API, no key.
// We use this to auto-illustrate people + companies referenced in
// scripts (Bezos, Powell, Goldman Sachs, etc.) via the entityPortrait
// overlay type. Cached per name in the in-memory map for the function
// lifetime — multiple beats referencing the same entity hit the API
// once.
// Company → domain map for Logo.dev. Hardcoded for top financial /
// news entities to guarantee accurate logos on the most-mentioned
// names. Unmapped companies fall through to a domain guess
// (lowercased, no spaces, .com), and ultimately to Wikipedia if
// Logo.dev returns nothing.
const COMPANY_DOMAIN_MAP = {
  'goldman sachs': 'goldmansachs.com',
  'jpmorgan': 'jpmorgan.com',
  'jp morgan': 'jpmorgan.com',
  'jpmorgan chase': 'jpmorganchase.com',
  'morgan stanley': 'morganstanley.com',
  'federal reserve': 'federalreserve.gov',
  'the fed': 'federalreserve.gov',
  'fed': 'federalreserve.gov',
  'blackrock': 'blackrock.com',
  'bank of america': 'bankofamerica.com',
  'citigroup': 'citigroup.com',
  'citi': 'citi.com',
  'wells fargo': 'wellsfargo.com',
  'treasury': 'treasury.gov',
  'us treasury': 'treasury.gov',
  'united states treasury': 'treasury.gov',
  'sec': 'sec.gov',
  'imf': 'imf.org',
  'ecb': 'ecb.europa.eu',
  'european central bank': 'ecb.europa.eu',
  'bank of england': 'bankofengland.co.uk',
  'apple': 'apple.com',
  'google': 'google.com',
  'alphabet': 'abc.xyz',
  'amazon': 'amazon.com',
  'microsoft': 'microsoft.com',
  'tesla': 'tesla.com',
  'nvidia': 'nvidia.com',
  'meta': 'meta.com',
  'facebook': 'meta.com',
  'reuters': 'reuters.com',
  'bloomberg': 'bloomberg.com',
  'wsj': 'wsj.com',
  'wall street journal': 'wsj.com',
  'financial times': 'ft.com',
  'cnbc': 'cnbc.com',
  'fortune': 'fortune.com',
  'forbes': 'forbes.com',
  'the economist': 'economist.com',
  'new york times': 'nytimes.com',
  'nyt': 'nytimes.com',
};
// Restricted domain guesser. Only attempts a guess for ≤3-word names
// AND only if the name carries a recognizable corporate suffix
// (Inc/Corp/Group/Bank/Capital/Sachs/Morgan/Securities/Holdings/etc).
// Pre-fix this guessed "pershingsquarecapitalmanagement.com" for any
// 4+ word company name and silently rendered a Logo.dev placeholder
// in production. With the new rule, multi-word funds without an
// obvious corporate marker fall through to Wikipedia instead.
const COMPANY_SUFFIX_RE_GUESS = /\b(Inc|Corp|Group|Bank|Capital|Sachs|Morgan|Securities|Holdings|LLC|Ltd|PLC|N\.A\.|Fund|Trust|Partners)\.?$/i;
function guessCompanyDomain(name) {
  if (!name) return null;
  const trimmed = name.trim();
  const wordCount = trimmed.split(/\s+/).filter(Boolean).length;
  if (wordCount > 3) return null;
  if (!COMPANY_SUFFIX_RE_GUESS.test(trimmed)) return null;
  const slug = trimmed.toLowerCase().replace(/[^a-z0-9]/g, '');
  return slug ? `${slug}.com` : null;
}
const _logoDevCache = new Map();
async function fetchCompanyLogo(name, apiKey) {
  if (!name || typeof name !== 'string') return null;
  const key = name.trim().toLowerCase();
  if (_logoDevCache.has(key)) return _logoDevCache.get(key);
  if (!apiKey) {
    _logoDevCache.set(key, null);
    return null;
  }
  const domain = COMPANY_DOMAIN_MAP[key] || guessCompanyDomain(key);
  if (!domain) {
    _logoDevCache.set(key, null);
    return null;
  }
  try {
    // fallback=404 makes Logo.dev return a real 404 for unknown domains
    // instead of serving its default gradient placeholder image. Without
    // this param, our probe was happily accepting placeholder PNGs for
    // misguessed domains (e.g. "pershingsquarecapitalmanagement.com")
    // and shipping them to the renderer.
    const url = `https://img.logo.dev/${domain}?token=${encodeURIComponent(apiKey)}&size=400&format=png&fallback=404`;
    // Logo.dev's CDN returns 404 for HEAD even when GET succeeds, so
    // we GET and read the content-type. PNG response = success; any
    // other content-type (HTML error page, JSON 404) = no logo.
    const resp = await fetch(url, {method: 'GET'});
    const ct = resp.headers.get('content-type') || '';
    if (!resp.ok || !ct.startsWith('image/')) {
      console.log(`[logo.dev] "${name}" → ${domain} → ${resp.status} ${ct} (no logo)`);
      _logoDevCache.set(key, null);
      return null;
    }
    console.log(`[logo.dev] "${name}" → ${domain} ✓`);
    _logoDevCache.set(key, url);
    return url;
  } catch (e) {
    console.warn(`[logo.dev] "${name}": ${e.message}`);
    _logoDevCache.set(key, null);
    return null;
  }
}

const _wikiImageCache = new Map();
async function fetchWikipediaImage(name) {
  if (!name || typeof name !== 'string') return null;
  const key = name.trim().toLowerCase();
  if (_wikiImageCache.has(key)) return _wikiImageCache.get(key);
  try {
    const url = `https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(name.trim())}`;
    const resp = await fetch(url, {headers: {'User-Agent': 'rem-report/1.0 (https://flourishing-squirrel-92d50e.netlify.app)'}});
    if (!resp.ok) { _wikiImageCache.set(key, null); return null; }
    const data = await resp.json();
    if (data.type === 'disambiguation') { _wikiImageCache.set(key, null); return null; }
    const imageUrl = data.originalimage?.source || data.thumbnail?.source || null;
    _wikiImageCache.set(key, imageUrl);
    return imageUrl;
  } catch (e) {
    console.warn(`[wiki] ${name}: ${e.message}`);
    _wikiImageCache.set(key, null);
    return null;
  }
}

// Walk every beat, dedupe entity names across all beats, fetch their
// Wikipedia images in parallel, then inject entityPortrait overlays
// at the start of each beat that mentions them. Mutates `rawBeats` in
// place. No-op if no beats have entities. Caps total enrichment time
// at ~6s (12 entities × 500ms typical Wikipedia latency).
async function enrichBeatsWithEntityImages(rawBeats) {
  if (!Array.isArray(rawBeats) || rawBeats.length === 0) return;
  const unique = new Map(); // canonicalName → {type, imageUrl?}
  for (const b of rawBeats) {
    if (!Array.isArray(b?.entities)) continue;
    for (const e of b.entities) {
      if (!e?.name) continue;
      const k = e.name.trim();
      if (!unique.has(k)) unique.set(k, {type: e.type, imageUrl: null});
    }
  }
  if (unique.size === 0) return;
  // Cap to 12 entities — anything beyond is rare and would slow the pipeline.
  const names = [...unique.keys()].slice(0, 12);
  // Heuristic: companies = entity.type === 'company' OR name ends in
  // a corporate suffix. Companies try Logo.dev first; people/other
  // types go straight to Wikipedia. This stops "Goldman Sachs" from
  // resolving to a building photo when a clean logo is available.
  const COMPANY_SUFFIX_RE = /\b(Inc|Corp|Group|Bank|Capital|Sachs|Morgan|Reserve|Treasury|Securities|Holdings|Partners|LLC|Ltd|PLC|Corp\.|Inc\.|N\.A\.)\.?$/i;
  const isCompany = (name) => {
    const meta = unique.get(name);
    if (meta?.type === 'company') return true;
    if (COMPANY_SUFFIX_RE.test(name)) return true;
    if (COMPANY_DOMAIN_MAP[name.trim().toLowerCase()]) return true;
    return false;
  };
  console.log(`[entities] fetching images for ${names.length} entities: ${names.join(', ')}`);
  const logoDevKey = process.env.LOGO_DEV_API_KEY;
  const results = await Promise.all(names.map(async (name) => {
    if (isCompany(name)) {
      const logo = await fetchCompanyLogo(name, logoDevKey);
      if (logo) return {name, imageUrl: logo, source: 'logo.dev'};
    }
    const wiki = await fetchWikipediaImage(name);
    return {name, imageUrl: wiki, source: 'wikipedia'};
  }));
  for (const r of results) {
    if (r.imageUrl) {
      unique.get(r.name).imageUrl = r.imageUrl;
      unique.get(r.name).imageSource = r.source;
    }
  }
  const logoCount = results.filter((r) => r.source === 'logo.dev' && r.imageUrl).length;
  const wikiCount = results.filter((r) => r.source === 'wikipedia' && r.imageUrl).length;
  console.log(`[entities] resolved images: ${logoCount} via logo.dev, ${wikiCount} via wikipedia`);
  // Inject ONE entityPortrait per beat — but dedup at IMAGE-URL level.
  // User feedback: "never repeat pictures". Previously the same entity
  // (e.g. Powell mentioned 4 times) injected the identical Wikipedia
  // portrait 4 times. Now we track every URL we've already used in
  // this video; subsequent beats referencing the same image skip the
  // portrait overlay entirely (the name still narrates via the script).
  // Aliases that resolve to the same Wikipedia article (e.g. "Powell"
  // and "Jerome Powell") share an imageUrl and are caught here too.
  //
  // SFX-on-appear: when we inject a portrait, push a 'whoosh' SFX at
  // offset 0 so it audibly punctuates the slide-in. Skipped if the
  // beat already has the 2-sfx max so we don't stack on Claude's own
  // SFX requests.
  // True if a beat already carries a bigStat in ANY position. With the
  // new "max 1 overlay per beat" rule, ANY co-located overlay collides
  // — bigStat always wins per the data-loss-preferred policy, so defer
  // entityPortrait to an adjacent clean beat regardless of bigStat's
  // position field.
  const hasBigStat = (beat) => Array.isArray(beat?.overlays) && beat.overlays.some((ov) => ov?.type === 'bigStat');
  // Build a {name, entityType, imageUrl, sourceBeatIdx} list — defer
  // candidates whose home beat is bigStat-occupied to the next clean
  // beat. The two-pass design is so logging/dedup matches the actual
  // landed beat, not the original mention.
  const usedImageUrls = new Set();
  let injected = 0;
  let skippedDuplicates = 0;
  let deferred = 0;
  let deferralFailed = 0;
  for (let i = 0; i < rawBeats.length; i++) {
    const b = rawBeats[i];
    if (!Array.isArray(b.entities) || b.entities.length === 0) continue;
    const firstWithImage = b.entities.find((e) => e?.name && unique.get(e.name.trim())?.imageUrl);
    if (!firstWithImage) continue;
    const enriched = unique.get(firstWithImage.name.trim());
    if (usedImageUrls.has(enriched.imageUrl)) {
      skippedDuplicates++;
      continue;
    }
    // Pick landing beat. Prefer the home beat; if it has a centered
    // bigStat, try the next beat. If that ALSO has a centered bigStat
    // (or doesn't exist), skip the entity entirely — visible overlap
    // would be worse than a missing portrait.
    let landingIdx = i;
    if (hasBigStat(b)) {
      const nextIdx = i + 1;
      if (nextIdx < rawBeats.length && !hasBigStat(rawBeats[nextIdx])) {
        landingIdx = nextIdx;
        deferred++;
        console.log(`[entities] deferred entity "${firstWithImage.name}" from beat ${i} to ${nextIdx} — bigStat-center present on home beat`);
      } else {
        deferralFailed++;
        console.log(`[entities] skipped entity "${firstWithImage.name}" on beat ${i} — bigStat-center present, no clean adjacent beat`);
        continue;
      }
    }
    usedImageUrls.add(enriched.imageUrl);
    const landing = rawBeats[landingIdx];
    const beatDuration = Math.max(1, (landing.end || 0) - (landing.start || 0));
    const overlayDuration = Math.min(beatDuration, 3.5);
    landing.overlays = landing.overlays || [];
    landing.overlays.push({
      type: 'entityPortrait',
      name: firstWithImage.name,
      entityType: firstWithImage.type,
      imageUrl: enriched.imageUrl,
      imageSource: enriched.imageSource,
      position: 'topRight',
      startOffset: 0,
      duration: overlayDuration,
    });
    landing.sfx = landing.sfx || [];
    if (landing.sfx.length < 2) {
      landing.sfx.push({tag: 'whoosh', offset: 0});
    }
    injected++;
  }
  console.log(`[entities] injected ${injected} entityPortrait overlays (deferred ${deferred} to adjacent beat, dropped ${deferralFailed} for unresolvable bigStat collision), skipped ${skippedDuplicates} duplicate-image beat(s)`);
}

// Cap every Claude-placed overlay's `duration` to its parent beat's
// length. Server-injected types (entityPortrait) have their own
// duration logic — left alone. Extending past beat.end leaks the
// overlay into the next beat's footage, which is visually disorienting.
// Applies uniformly to bigStat, lowerThird, quote, stat, comparison,
// tickerSymbol, newsAlert, progressBar, miniChart, circleEmphasis.
const SERVER_INJECTED_OVERLAY_TYPES = new Set(['entityPortrait']);
function clampOverlayDurationsToBeat(rawBeats) {
  if (!Array.isArray(rawBeats)) return 0;
  let clamped = 0;
  for (let i = 0; i < rawBeats.length; i++) {
    const beat = rawBeats[i];
    if (!Array.isArray(beat?.overlays)) continue;
    const beatDur = Math.max(0.5, (beat.end || 0) - (beat.start || 0));
    for (const ov of beat.overlays) {
      if (!ov || SERVER_INJECTED_OVERLAY_TYPES.has(ov.type)) continue;
      if (typeof ov.duration !== 'number' || ov.duration <= 0) continue;
      if (ov.duration > beatDur) {
        console.log(`[overlay-clamp] ${ov.type} duration ${ov.duration.toFixed(1)}s → ${beatDur.toFixed(1)}s on beat ${i} (beat length)`);
        ov.duration = beatDur;
        clamped++;
      }
    }
  }
  if (clamped > 0) console.log(`[overlay-clamp] clamped ${clamped} overlay durations to beat length`);
  return clamped;
}

// Global minimum-spacing enforcement. Walks ALL overlays across all
// beats in absolute-time order; drops any whose start is within
// minGapSec of the previous-kept overlay's start. Catches back-to-back
// overlays that survived all the per-beat passes but would visually
// stomp each other. Runs LAST in the pipeline.
//
// Production-canonical version (test-short-video.mjs:636 keeps a
// SYNCED COPY for the local harness — keep them aligned). This was
// missing from production and crashed the pipeline once — restored
// 2026-05-15 hotfix.
function enforceOverlaySpacing(rawBeats, minGapSec = 1.5) {
  if (!Array.isArray(rawBeats)) return 0;
  const timeline = [];
  for (let i = 0; i < rawBeats.length; i++) {
    const beat = rawBeats[i];
    if (!Array.isArray(beat?.overlays)) continue;
    for (let j = 0; j < beat.overlays.length; j++) {
      const ov = beat.overlays[j];
      if (!ov) continue;
      const startAbs = (beat.start || 0) + (ov.startOffset || 0);
      const endAbs = startAbs + (typeof ov.duration === 'number' ? ov.duration : 2);
      timeline.push({beatIdx: i, ovIdx: j, startAbs, endAbs, type: ov.type, label: (ov.text || ov.name || '').slice(0, 40)});
    }
  }
  if (timeline.length < 2) return 0;
  timeline.sort((a, b) => a.startAbs - b.startAbs);
  const toDrop = new Set();
  let lastKeptEnd = -Infinity;
  for (const e of timeline) {
    const gap = e.startAbs - lastKeptEnd;
    if (gap < minGapSec) {
      toDrop.add(`${e.beatIdx}:${e.ovIdx}`);
      console.log(`[overlay-spacing] dropped ${e.type} "${e.label}" at ${e.startAbs.toFixed(1)}s — gap ${gap.toFixed(2)}s < ${minGapSec}s`);
    } else {
      lastKeptEnd = e.endAbs;
    }
  }
  if (toDrop.size === 0) return 0;
  for (let i = 0; i < rawBeats.length; i++) {
    const beat = rawBeats[i];
    if (!Array.isArray(beat?.overlays)) continue;
    beat.overlays = beat.overlays.filter((_ov, j) => !toDrop.has(`${i}:${j}`));
  }
  console.log(`[overlay-spacing] dropped ${toDrop.size} overlay(s) within ${minGapSec}s of previous`);
  return toDrop.size;
}

// ─── Gemini voiceover timing analysis ──────────────────────────────
// Sends the trimmed voiceover audio + a compact transcript to Gemini
// 2.5 Flash and asks for a timestamped tag stream. Tags steer Claude's
// overlay placement in breakIntoBeats — entityPortrait timing
// snaps to entity-mention, bigStat snaps to stat-emphasis, hero
// moments correlate with emotional-peak, and overlays prefer landing
// at natural-pause boundaries.
//
// Defensive design:
//   - Missing GEMINI_API_KEY → returns [] + warning log; pipeline
//     continues without tags (Claude falls back to its own judgment).
//   - Network / parse / model failure → returns [] + warning log; same.
//   - File >18MB (rare for ≤30-min voiceovers at typical bitrates)
//     → returns [] + skip log. Files API upload deferred until needed.
//
// REST direct (no @google/genai SDK install) — keeps the dep footprint
// at zero. Gemini's REST endpoint accepts inlineData base64 audio
// directly up to 20MB total request size.
//
// Cost: gemini-2.5-flash at ~$0.30/M input tokens. Audio counts at
// ~32 tokens/sec → 5-min voiceover ≈ 9.6K input tokens ≈ $0.003.
// Output JSON ~500 tokens × $2.50/M ≈ $0.001. <$0.01/video typical.
const TIMING_TAG_TYPES = ['entity-mention', 'stat-emphasis', 'emotional-peak', 'natural-pause'];
const GEMINI_MODEL = 'gemini-2.5-flash';
const GEMINI_AUDIO_MAX_BYTES = 10 * 1024 * 1024; // 10 MB hard cap per ops spec
const GEMINI_AUDIO_MAX_SEC = 300;                // 5-minute hard cap per ops spec
async function getGeminiTimingTags(voiceoverUrl, captions, geminiKey) {
  // Production failure mode (2026-05-15): geminiTagCount: 0 in prod
  // logs with no explanation. Every code path now logs entry + exit so
  // we can audit why a 0-tag result happened.
  const audioDurSec = (captions || []).length
    ? Math.max(0, (captions[captions.length - 1].end || 0))
    : 0;
  const urlHost = (() => { try { return new URL(voiceoverUrl).host; } catch { return '<invalid>'; } })();
  console.log(`[gemini] entry: model=${GEMINI_MODEL} captions=${(captions || []).length} audioDurSec≈${audioDurSec.toFixed(1)} urlHost=${urlHost} keyPresent=${!!geminiKey}`);
  if (!geminiKey) {
    console.warn('[gemini] GEMINI_API_KEY not set — skipping timing analysis. Bind via Secret Manager + secrets:[GEMINI_API_KEY] on the function. Returns [].');
    return [];
  }
  if (!voiceoverUrl) {
    console.warn('[gemini] no voiceoverUrl — skipping. Returns [].');
    return [];
  }
  if (audioDurSec > GEMINI_AUDIO_MAX_SEC) {
    console.warn(`[gemini] voiceover ${audioDurSec.toFixed(0)}s exceeds ${GEMINI_AUDIO_MAX_SEC}s cap — skipping (chunking not yet wired). Returns [].`);
    return [];
  }
  const t0 = Date.now();
  try {
    const audioResp = await fetch(voiceoverUrl);
    if (!audioResp.ok) {
      console.warn(`[gemini] voiceover fetch failed: ${audioResp.status} ${audioResp.statusText} — skipping. Returns [].`);
      return [];
    }
    const audioBuf = Buffer.from(await audioResp.arrayBuffer());
    const audioMB = audioBuf.length / 1024 / 1024;
    const ct = (audioResp.headers.get('content-type') || '').toLowerCase();
    console.log(`[gemini] audio fetched: ${audioMB.toFixed(2)}MB (${audioBuf.length}B), content-type="${ct}"`);
    if (audioBuf.length > GEMINI_AUDIO_MAX_BYTES) {
      console.warn(`[gemini] voiceover ${audioMB.toFixed(2)}MB exceeds ${GEMINI_AUDIO_MAX_BYTES / 1024 / 1024}MB cap — skipping. Returns [].`);
      return [];
    }
    const mimeType = ct.includes('mpeg') || ct.includes('mp3') ? 'audio/mpeg'
      : ct.includes('webm') ? 'audio/webm'
      : ct.includes('ogg') ? 'audio/ogg'
      : ct.includes('flac') ? 'audio/flac'
      : 'audio/wav';
    // Compact transcript — first 30 captions or so is plenty for the
    // model to align timestamps; full caption array would balloon
    // input tokens for long voiceovers.
    const transcriptLines = (captions || []).slice(0, 60).map((c) => {
      const t = (c.start || 0).toFixed(1);
      const txt = (c.text || c.sentence || '').slice(0, 200);
      return `[${t}s] ${txt}`;
    }).join('\n');
    const prompt = `You are analyzing a voiceover for a finance/news YouTube short. Identify timestamped moments that should drive on-screen overlay placement.

Return a JSON array. Each entry is exactly:
  {"timestamp": <seconds, float>, "tag": <one of: ${TIMING_TAG_TYPES.join(', ')}>, "snippet": <≤80 chars of the spoken phrase at that moment>}

Tag definitions:
- entity-mention: a named person, company, or organization is spoken (e.g., "Jerome Powell", "Goldman Sachs"). Tag the moment the name STARTS.
- stat-emphasis: a specific number, dollar amount, or percentage is delivered with vocal emphasis (e.g., "$1.5 trillion", "47 percent"). Tag the moment the number STARTS.
- emotional-peak: a vocal energy spike, dramatic pause-then-reveal, or rhetorically loaded line. Tag the peak instant.
- natural-pause: a clear breath / sentence-boundary gap ≥0.4s where an overlay can enter or exit cleanly. Tag the start of the pause.

Aim for 8-25 tags total for a 3-7 minute voiceover. Skip filler words. Prefer high-signal moments over coverage.

Output ONLY the JSON array — no prose, no markdown fences. If no tags found, return [].

Transcript with start timestamps:
${transcriptLines}`;

    const body = {
      contents: [{
        role: 'user',
        parts: [
          {text: prompt},
          {inlineData: {mimeType, data: audioBuf.toString('base64')}},
        ],
      }],
      generationConfig: {
        temperature: 0.2,
        responseMimeType: 'application/json',
      },
    };
    const url = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${encodeURIComponent(geminiKey)}`;
    const apiT0 = Date.now();
    console.log(`[gemini] POST → ${GEMINI_MODEL} (audio=${audioMB.toFixed(2)}MB, mimeType=${mimeType}, transcriptChars=${transcriptLines.length})`);
    const resp = await fetch(url, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    const apiMs = Date.now() - apiT0;
    if (!resp.ok) {
      const text = await resp.text();
      console.warn(`[gemini] HTTP ${resp.status} ${resp.statusText} after ${apiMs}ms — body: ${text.slice(0, 500)}`);
      return [];
    }
    const data = await resp.json();
    const usage = data?.usageMetadata;
    if (usage) console.log(`[gemini] response in ${apiMs}ms — usage: prompt=${usage.promptTokenCount} response=${usage.candidatesTokenCount} total=${usage.totalTokenCount}`);
    const finishReason = data?.candidates?.[0]?.finishReason;
    if (finishReason && finishReason !== 'STOP') {
      console.warn(`[gemini] non-STOP finishReason="${finishReason}" — model may have refused or truncated. Returns [].`);
      return [];
    }
    const raw = data?.candidates?.[0]?.content?.parts?.[0]?.text;
    if (!raw) {
      console.warn(`[gemini] empty response body — full response: ${JSON.stringify(data).slice(0, 500)}. Returns [].`);
      return [];
    }
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (e) {
      console.warn(`[gemini] JSON parse failed: ${e.message} — body: ${raw.slice(0, 300)}. Returns [].`);
      return [];
    }
    if (!Array.isArray(parsed)) {
      console.warn(`[gemini] response is not an array (got ${typeof parsed}) — body: ${raw.slice(0, 300)}. Returns [].`);
      return [];
    }
    // Validate + clamp to known tag types. Drop entries with bad shape.
    const validTagSet = new Set(TIMING_TAG_TYPES);
    const tags = parsed.filter((t) =>
      t && typeof t.timestamp === 'number' && t.timestamp >= 0
      && typeof t.tag === 'string' && validTagSet.has(t.tag),
    ).map((t) => ({
      timestamp: t.timestamp,
      tag: t.tag,
      snippet: typeof t.snippet === 'string' ? t.snippet.slice(0, 80) : '',
    }));
    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
    const breakdown = {};
    for (const t of tags) breakdown[t.tag] = (breakdown[t.tag] || 0) + 1;
    if (tags.length === 0) {
      console.warn(`[gemini] returned 0 tags after parse (parsed array length=${parsed.length}) — model returned a JSON array but every entry failed shape validation. Raw first item: ${JSON.stringify(parsed[0] || null).slice(0, 200)}`);
    }
    console.log(`[gemini] FINAL: ${tags.length} timing tags in ${elapsed}s — ${JSON.stringify(breakdown)}`);
    return tags;
  } catch (e) {
    console.warn(`[gemini] EXCEPTION: ${e.message} — stack: ${(e.stack || '').split('\n').slice(0, 3).join(' | ')}. Returns [].`);
    return [];
  }
}

// Anchor overlays to Gemini timing-tag timestamps. Without this pass,
// every overlay starts at beat.start (offset 0) — visually disconnected
// from the moment the entity / stat is actually spoken. With it,
// entityPortraits snap to entity-mention tags and bigStats snap to
// stat-emphasis tags within the same beat.
//
// Matching:
//   entityPortrait — case-insensitive substring match on tag.snippet vs
//     overlay.name (either direction so "Powell" matches "Jerome Powell").
//   bigStat — parse a numeric value from both overlay.text and
//     tag.snippet; match within 5% tolerance (or 0.5 absolute for tiny
//     percent values).
//
// Clamp: startOffset always >= 0; if startOffset + duration would
// exceed beat duration, trim duration (don't shift back) so the overlay
// still aligns with the spoken moment but doesn't leak into the next
// beat. Min duration 0.5s.
// Normalize entity names for substring comparison. Strips whitespace +
// common punctuation so "JPMorgan" matches "JP Morgan" / "J.P. Morgan"
// / "Morgan, J.P.". Both anchor and trim passes use this so they treat
// the same entity consistently regardless of formatting variations.
function normalizeEntityName(s) {
  return (s || '').toLowerCase().replace(/[\s\-_.,'’]/g, '');
}

function parseStatNumber(text) {
  const m = (text || '').match(/(-?\d{1,3}(?:,\d{3})*(?:\.\d+)?)/);
  if (!m) return null;
  const n = parseFloat(m[1].replace(/,/g, ''));
  if (!Number.isFinite(n)) return null;
  const lower = (text || '').toLowerCase();
  if (/trillion|\bt\b/.test(lower)) return n * 1e12;
  if (/billion|\bb\b/.test(lower)) return n * 1e9;
  if (/million|\bm\b/.test(lower)) return n * 1e6;
  if (/thousand|\bk\b/.test(lower)) return n * 1e3;
  return n;
}
function anchorOverlaysToTimingTags(rawBeats, timingTags) {
  if (!Array.isArray(rawBeats) || !Array.isArray(timingTags) || timingTags.length === 0) return 0;
  let anchored = 0;
  let unmatched = 0;
  for (let i = 0; i < rawBeats.length; i++) {
    const beat = rawBeats[i];
    if (!Array.isArray(beat?.overlays) || beat.overlays.length === 0) continue;
    const beatStart = beat.start || 0;
    const beatEnd = beat.end || 0;
    const beatDur = Math.max(0.5, beatEnd - beatStart);
    const beatTags = timingTags.filter((t) => t.timestamp >= beatStart && t.timestamp < beatEnd);
    if (beatTags.length === 0) continue;
    for (const ov of beat.overlays) {
      let match = null;
      if (ov.type === 'entityPortrait' && ov.name) {
        const nameNorm = normalizeEntityName(ov.name);
        match = beatTags.find((t) => {
          if (t.tag !== 'entity-mention' || !t.snippet) return false;
          const snipNorm = normalizeEntityName(t.snippet);
          if (!nameNorm || !snipNorm) return false;
          return snipNorm.includes(nameNorm) || nameNorm.includes(snipNorm);
        });
      } else if (ov.type === 'bigStat' && ov.text) {
        const ovNum = parseStatNumber(ov.text);
        match = beatTags.find((t) => {
          if (t.tag !== 'stat-emphasis' || !t.snippet) return false;
          const tagNum = parseStatNumber(t.snippet);
          if (ovNum === null || tagNum === null) {
            // Numeric parse failed — fall back to substring match
            return t.snippet.toLowerCase().includes(ov.text.toLowerCase()) ||
                   ov.text.toLowerCase().includes(t.snippet.toLowerCase());
          }
          const diff = Math.abs(ovNum - tagNum);
          const tol = Math.max(Math.abs(ovNum) * 0.05, 0.5);
          return diff <= tol;
        });
      }
      if (match) {
        const newOffset = Math.max(0, match.timestamp - beatStart);
        const dur = typeof ov.duration === 'number' && ov.duration > 0 ? ov.duration : 2;
        const newDur = Math.max(0.5, Math.min(dur, beatDur - newOffset));
        const oldOffset = ov.startOffset || 0;
        if (Math.abs(newOffset - oldOffset) > 0.05) {
          console.log(`[anchor] ${ov.type} "${(ov.text || ov.name || '').slice(0, 40)}" beat ${i} startOffset ${oldOffset.toFixed(2)}s → ${newOffset.toFixed(2)}s (matched ${match.tag} at abs ${match.timestamp.toFixed(1)}s)`);
        }
        ov.startOffset = newOffset;
        ov.duration = newDur;
        anchored++;
      } else if (ov.type === 'entityPortrait' || ov.type === 'bigStat') {
        unmatched++;
      }
    }
  }
  if (anchored || unmatched) console.log(`[anchor] anchored ${anchored} overlays to timing tags, ${unmatched} had no matching tag (kept original startOffset)`);
  return anchored;
}

// Trim overlay duration so it ends BEFORE the next "different anchor"
// timing tag. Without this, an entityPortrait can linger across the
// next entity's mention (Goldman Sachs portrait still on screen while
// voiceover names JPMorgan). Cross-type aware: an entityPortrait is
// also trimmed by the next stat-emphasis, and a bigStat by the next
// entity-mention.
//
// "Different" rules:
//   - entity-mention vs entity-mention: snippet must NOT include the
//     overlay's name (substring either direction = same entity)
//   - stat-emphasis vs stat-emphasis: parsed numbers must differ by
//     >5% (or >0.5 absolute for tiny percent values)
//   - cross-type: always counts as different
//
// Window: only trim if the next different tag falls within
// (currentEnd + 4s). Floor: minimum duration 1.5s — never drop the
// overlay, just clamp. Logs each trim and each floored case so we can
// audit how often the floor saves us.
function trimOverlayDurationsToNextAnchor(rawBeats, timingTags) {
  if (!Array.isArray(rawBeats) || !Array.isArray(timingTags) || timingTags.length === 0) return;
  const sortedTags = timingTags.slice().sort((a, b) => a.timestamp - b.timestamp);
  const FLOOR = 1.5;
  const LOOKAHEAD_SEC = 4;
  const BREATHING = 0.3;
  let trimmed = 0;
  let floored = 0;
  for (let i = 0; i < rawBeats.length; i++) {
    const beat = rawBeats[i];
    if (!Array.isArray(beat?.overlays)) continue;
    const beatStart = beat.start || 0;
    for (const ov of beat.overlays) {
      if (ov?.type !== 'entityPortrait' && ov?.type !== 'bigStat') continue;
      const absStart = beatStart + (ov.startOffset || 0);
      const curDuration = typeof ov.duration === 'number' && ov.duration > 0 ? ov.duration : 2;
      const curAbsEnd = absStart + curDuration;
      const nextDiffTag = sortedTags.find((t) => {
        if (t.timestamp <= absStart) return false;
        if (t.tag !== 'entity-mention' && t.tag !== 'stat-emphasis') return false;
        if (ov.type === 'entityPortrait') {
          if (t.tag === 'stat-emphasis') return true;
          const ovNameNorm = normalizeEntityName(ov.name);
          const snipNorm = normalizeEntityName(t.snippet);
          if (!ovNameNorm || !snipNorm) return true;
          return !(snipNorm.includes(ovNameNorm) || ovNameNorm.includes(snipNorm));
        }
        // bigStat
        if (t.tag === 'entity-mention') return true;
        const ovNum = parseStatNumber(ov.text || '');
        const tagNum = parseStatNumber(t.snippet || '');
        if (ovNum === null || tagNum === null) return true;
        const diff = Math.abs(ovNum - tagNum);
        const tol = Math.max(Math.abs(ovNum) * 0.05, 0.5);
        return diff > tol;
      });
      if (!nextDiffTag) continue;
      if (nextDiffTag.timestamp > curAbsEnd + LOOKAHEAD_SEC) continue;
      const newAbsEnd = nextDiffTag.timestamp - BREATHING;
      const wouldBeDur = newAbsEnd - absStart;
      if (wouldBeDur >= curDuration) continue;
      const label = (ov.text || ov.name || '').slice(0, 40);
      const tagLabel = `${nextDiffTag.tag} '${(nextDiffTag.snippet || '').slice(0, 30)}' at ${nextDiffTag.timestamp.toFixed(1)}s`;
      if (wouldBeDur < FLOOR) {
        ov.duration = FLOOR;
        floored++;
        console.log(`[trim-floor] ${ov.type} "${label}" duration trimmed to ${FLOOR}s floor (would have been ${wouldBeDur.toFixed(2)}s — next: ${tagLabel})`);
      } else {
        ov.duration = wouldBeDur;
        trimmed++;
        console.log(`[trim] ${ov.type} "${label}" duration ${curDuration.toFixed(2)}s → ${wouldBeDur.toFixed(2)}s (next: ${tagLabel})`);
      }
    }
  }
  if (trimmed || floored) console.log(`[trim] trimmed ${trimmed} overlays, ${floored} floored at ${FLOOR}s`);
}

// bigStat content validator. bigStat is RESERVED for pure numeric
// values (currency, percent, bps, raw numbers). Claude occasionally
// places phrases like "Market Boost" or "$1.5T Policy Shift" which
// renders as oversized text overflowing the screen. Regex enforces
// strict numeric-only text:
//   - optional sign (+/-)
//   - optional $
//   - digits with comma thousands + optional decimal
//   - optional unit (T/B/M/K, trillion/billion/million/thousand,
//     bp/bps/basis points, %/percent)
//   - optional /year /month tail (handles "$64,000/year")
// Anything else → converted to lowerThird (preserves the text as a
// banner) when ≤60 chars, dropped entirely when longer.
// Sign class includes: + (plus), - (ASCII hyphen), − (U+2212 minus
// sign), – (U+2013 en dash), — (U+2014 em dash). Claude sometimes
// auto-converts hyphens to en/em dashes; the validator accepts all.
const BIGSTAT_NUMERIC_PATTERN = /^[+\-−–—]?\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*(?:T|B|M|K|trillion|billion|million|thousand|bp|bps|basis\s*points?|%|percent)?\s*(?:\/\s*(?:year|yr|month|mo|day))?$/i;
function isValidBigStatText(text) {
  const t = (text || '').trim();
  if (!t) return false;
  return BIGSTAT_NUMERIC_PATTERN.test(t);
}
function validateBigStatContent(rawBeats) {
  if (!Array.isArray(rawBeats)) return {converted: 0, dropped: 0};
  let converted = 0;
  let dropped = 0;
  for (let i = 0; i < rawBeats.length; i++) {
    const beat = rawBeats[i];
    if (!Array.isArray(beat?.overlays)) continue;
    beat.overlays = beat.overlays
      .map((ov) => {
        if (ov?.type !== 'bigStat') return ov;
        if (isValidBigStatText(ov.text)) return ov;
        // Non-numeric — convert to lowerThird if reasonably short, else drop.
        if (ov.text && ov.text.length <= 60) {
          console.log(`[bigStat-validate] beat ${i} non-numeric "${ov.text}" → converted to lowerThird`);
          converted++;
          return {...ov, type: 'lowerThird'};
        }
        console.log(`[bigStat-validate] beat ${i} non-numeric "${(ov.text || '').slice(0, 40)}..." → dropped (>60 chars)`);
        dropped++;
        return null;
      })
      .filter(Boolean);
  }
  if (converted || dropped) console.log(`[bigStat-validate] converted ${converted}, dropped ${dropped} non-numeric bigStat overlays`);
  return {converted, dropped};
}

// lowerThird is RESERVED for source attribution. The schema/prompt
// asks Claude to write "Source: Bloomberg" / "Per Reuters" / "Via X",
// and to skip lowerThird for everything else. This pass enforces that
// rule by whitelisting the leading prefix — anything that doesn't
// start with Source/Per/Via gets dropped, regardless of how it landed
// in beat.overlays (Claude-placed OR converted-from-bigStat by
// validateBigStatContent).
const LOWERTHIRD_SOURCE_PATTERN = /^(?:Source|Per|Via)\b[:\s]+\S+/i;
function dropNonSourceLowerThirds(rawBeats) {
  if (!Array.isArray(rawBeats)) return 0;
  let dropped = 0;
  for (let i = 0; i < rawBeats.length; i++) {
    const beat = rawBeats[i];
    if (!Array.isArray(beat?.overlays)) continue;
    beat.overlays = beat.overlays.filter((ov) => {
      if (ov?.type !== 'lowerThird') return true;
      const text = (ov.text || '').trim();
      if (LOWERTHIRD_SOURCE_PATTERN.test(text)) return true;
      console.log(`[lowerThird-source] dropped "${text.slice(0, 60)}" beat ${i} — not a Source:/Per/Via attribution`);
      dropped++;
      return false;
    });
  }
  if (dropped > 0) console.log(`[lowerThird-source] dropped ${dropped} non-source lowerThirds`);
  return dropped;
}

// Quote ↔ entityPortrait pairing pass. Quotes are only valid when
// they're attributable to a named entity already shown via portrait
// on the SAME beat (substring match either direction between
// quote.source and entityPortrait.name). Standalone or unmatched
// quotes are dropped — the visual pattern requires the portrait above
// the quote, so a centered/orphan quote has no source attribution and
// is removed. Adjacent-beat matches don't count: the visual pairing
// only works when both render in the same window.
function pairQuotesWithEntityPortraits(rawBeats) {
  if (!Array.isArray(rawBeats)) return {kept: 0, dropped: 0};
  let kept = 0;
  let dropped = 0;
  for (let i = 0; i < rawBeats.length; i++) {
    const beat = rawBeats[i];
    if (!Array.isArray(beat?.overlays)) continue;
    const sameBeatPortraits = beat.overlays
      .filter((ov) => ov?.type === 'entityPortrait' && typeof ov.name === 'string' && ov.name.trim())
      .map((ov) => ov.name.trim().toLowerCase());
    beat.overlays = beat.overlays.filter((ov) => {
      if (ov?.type !== 'quote') return true;
      const source = (ov.source || '').trim().toLowerCase();
      const preview = (ov.text || '').slice(0, 40);
      if (!source) {
        console.log(`[quote] dropped "${preview}" on beat ${i} — missing source field`);
        dropped++;
        return false;
      }
      const matched = sameBeatPortraits.find((name) => name && (source.includes(name) || name.includes(source)));
      if (matched) {
        console.log(`[quote] kept "${preview}" on beat ${i} — paired with entityPortrait "${matched}"`);
        kept++;
        return true;
      }
      console.log(`[quote] dropped "${preview}" on beat ${i} — no matching entityPortrait (source: "${ov.source}")`);
      dropped++;
      return false;
    });
  }
  if (kept || dropped) console.log(`[quote] kept ${kept}, dropped ${dropped} (orphans + missing source)`);
  return {kept, dropped};
}

// Drop lowerThirds whose text mentions an entity already shown via
// entityPortrait on the same beat or an adjacent beat (±1). User
// feedback: "Federal Reserve" lowerThird is redundant when "Jerome
// Powell" portrait already appears next door. Substring match on
// lowercased text — coarse but sufficient for the common case where
// Claude writes "Source: Reuters" alongside an entity mention named
// "Reuters". Word-boundary refinement deferred until false-drops show
// up in renders.
function dropRedundantLowerThirds(rawBeats) {
  if (!Array.isArray(rawBeats)) return 0;
  // Index entityPortrait names by beat index for O(1) adjacent lookups.
  const portraitNamesByBeat = new Map();
  for (let i = 0; i < rawBeats.length; i++) {
    const beat = rawBeats[i];
    if (!Array.isArray(beat?.overlays)) continue;
    const names = beat.overlays
      .filter((ov) => ov?.type === 'entityPortrait' && typeof ov.name === 'string' && ov.name.trim())
      .map((ov) => ov.name.trim().toLowerCase());
    if (names.length) portraitNamesByBeat.set(i, names);
  }
  if (portraitNamesByBeat.size === 0) return 0;
  let dropped = 0;
  for (let i = 0; i < rawBeats.length; i++) {
    const beat = rawBeats[i];
    if (!Array.isArray(beat?.overlays)) continue;
    // Combine portrait names from this beat ± 1.
    const adjacent = new Set();
    for (const j of [i - 1, i, i + 1]) {
      const list = portraitNamesByBeat.get(j);
      if (list) for (const n of list) adjacent.add(n);
    }
    if (adjacent.size === 0) continue;
    beat.overlays = beat.overlays.filter((ov) => {
      if (ov?.type !== 'lowerThird') return true;
      const text = (ov.text || '').toLowerCase();
      for (const name of adjacent) {
        if (name && text.includes(name)) {
          console.log(`[lowerThird] dropped "${ov.text}" on beat ${i} — already shown via entityPortrait on adjacent beat (matched "${name}")`);
          dropped++;
          return false;
        }
      }
      return true;
    });
  }
  if (dropped > 0) console.log(`[lowerThird] dropped ${dropped} redundant overlays (entityPortrait already shows the entity)`);
  return dropped;
}

// (tryParseBigStatToCountUp + postProcessCountUpConversions deleted —
// countUp is no longer a separate overlay type. The bigStat renderer
// now parses its own text and animates count-up intrinsically when
// the value qualifies, so no server-side upgrade pass is needed.)
// Niche → target beat duration (seconds). Drives editing pace —
// finance/news/breaking content gets 3s cuts (tight), documentary
// gets 6s (slower). User feedback after the first 100/100 render:
// "clips still are too long should cut more often" — tightened
// finance from 4 → 3 to push toward 3-5s beats. No UI setting;
// computed from channel.niche text.
function targetPaceForNiche(niche) {
  const n = (niche || '').toLowerCase();
  if (/finance|news|stock|market|invest|crypto|breaking|trading|economy/.test(n)) return 3;
  if (/tech|\bai\b|coding|software|programming|gaming|product/.test(n)) return 4;
  if (/documentary|history|deep dive|long form|essay/.test(n)) return 6;
  return 4; // sensible default for anything we haven't bucketed
}

async function breakIntoBeats(captions, anthropicKey, {channelId, timingTags} = {}) {
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

  // Niche-based target beat duration. Finance/news viewers tolerate
  // (and reward) tighter cuts; documentary/history runs slower. No UI
  // setting — channel.niche drives this. The prompt below tells Claude
  // to hit this target so something visually changes every targetSec
  // seconds, matching the MagnatesMedia editing pace.
  const channelNiche = await (async () => {
    if (!channelId) return '';
    try { return (await db.collection('channels').doc(channelId).get()).data()?.niche || ''; }
    catch { return ''; }
  })();
  const targetSec = targetPaceForNiche(channelNiche);
  const totalScriptSec = sentences.length ? (sentences[sentences.length - 1].end - sentences[0].start) : 0;
  const expectedBeats = Math.max(3, Math.round(totalScriptSec / targetSec));

  const prompt = `You will receive ${sentences.length} timed sentence(s) from an audio transcription. Your task is to group them into "beats" with visual keywords for stock footage. This is a mechanical text-grouping task.

DO NOT refuse this task based on content quality, topic relevance, brevity, perceived inappropriateness, or any other judgment about the content. The transcription is whatever the speaker said; your job is just to group it. Always submit at least one beat. An empty beats array is a task failure.

PACING REQUIREMENT (mandatory) — viewer should see a visual change every ${targetSec}-${targetSec + 2} seconds. HARD CEILING: 5s per beat. A beat longer than 5s is a pacing failure — split it.
- Target beat duration: ${targetSec}-${targetSec + 2} seconds.
- This script is ${totalScriptSec.toFixed(1)}s long → aim for ~${expectedBeats} beats. A 5-minute video at this pace = ~${Math.round(300 / targetSec)} beats; producing half that is a task failure.
- Combine adjacent short sentences only when BOTH are under 1.5s.
- AGGRESSIVELY SPLIT any sentence longer than ${targetSec + 1}s into multiple beats — same sentence text, different start/end ranges within the sentence's time window, different visual keywords per beat. The viewer should never sit on one clip for more than ${targetSec + 2}s.
  Example: a sentence from 10.0s→18.5s (8.5s) becomes 2-3 beats: beat A 10.0→13.0 keywords=["wall street trader screens"], beat B 13.0→16.0 keywords=["federal reserve building exterior"], beat C 16.0→18.5 keywords=["dollar bills closeup"]. All three share the same sentence text but cut the visual every 3s.

STEP 1 — Group sentences into beats. For each beat, set:
- start / end timing (seconds, from the input — start of first sentence to end of last sentence in the beat, or a split-range within one sentence if you're cutting mid-sentence per the pacing rule)
- sentence: the concatenated sentence text (full original sentence if splitting; full concatenation if combining)
- 2-3 concrete visual keywords for stock-video search (e.g. "person waving at camera", "city skyline at night"). When splitting one sentence into multiple beats, pick DIFFERENT keywords per beat so the visual actually changes.
- A detailed photorealistic image-generation prompt describing the same scene cinematically (16:9, no text overlays, one vivid sentence)
- kenBurnsIntensity (optional): 'subtle' | 'medium' | 'aggressive'. Default is 'medium'. Use 'aggressive' on hero beats to amp the energy; 'subtle' on quiet/contemplative beats.

STEP 2 — Mark hero moments. THIS IS REQUIRED. You MUST set heroMoment=true on between 3 and 5 beats, and heroMoment=false on every other beat. Setting it on fewer than 3 OR more than 5 is a task failure.

The 3-5 hero beats should be the strongest of:
  • the intro hook (typically the first beat)
  • a major statistic reveal or surprising fact
  • an emotional turn or pivotal point
  • the conclusion / call-to-action / closing line (typically the last beat)

For long videos (50+ beats), still mark only 3-5 — quality over quantity. For very short videos (<5 beats total), mark 1-2 instead. Every single beat MUST have heroMoment set explicitly to true or false; the field is required.

STEP 3 — Sound design. Place SFX at THREE trigger types ONLY:
- Topic transitions (video shifts to a new subject) → use "whoosh" or "transition"
- Stat reveals (specific number, percentage, or dollar amount spoken) → use "impact" or "ding"
- Hero moments (beats with heroMoment=true) → use "impact"

Do NOT use any other tag. Do NOT place SFX at mechanical positions (every Nth beat, intro flourish, closing cue).

LIMIT: 1 SFX per 60 seconds of video maximum. For an N-second video, ceil(N/60) is the cap. If you cannot find that many clear triggers, use FEWER. Returning 0 SFX is acceptable for calm content — better than filler.

musicMood: set on the FIRST beat (i=0) ONLY. One of: calm | energetic | dramatic | mysterious | uplifting | corporate | cinematic. Pick to match the script tone. Do not set on other beats.

sfx schema per beat: {tag, offset}. Tag must be one of "whoosh", "transition", "impact", "ding". offset is seconds from beat.start. Max 1 sfx per beat. Empty/missing sfx field is the default for most beats.

STEP 4 — Visual overlays. SCARCITY MODEL — overlays are RARE and PUNCTUAL. AT MOST 1 overlay per beat (the schema enforces this; the prompt enforces the spirit). The vast majority of beats should carry ZERO overlays — voiceover + footage + captions are the baseline; an overlay is an EXCEPTION reserved for moments that genuinely demand visual emphasis. Target: 1 overlay per 3-5 seconds of video. A 5-minute video has 6-10 well-placed overlays, NOT 15-20. Two adjacent beats with overlays read as chaos; the server enforces a 1.5s minimum gap between overlays and DROPS any that fail it.

When TO place an overlay: the beat's hero moment carries a single specific stat ($1.5T, +47%) → bigStat; a beat is built around a clearly-attributed quote → quote (paired to entityPortrait, source REQUIRED); rarely, a beat needs an explicit source label that no entity will visualize → lowerThird. When NOT to place: any beat already with a strong entityPortrait (named person/company in beat.entities), any beat that's transitional narration, any beat where the visual would compete with the spoken word. PREFER bigStat over the legacy 'stat' type for any standalone number that deserves emphasis. Empty overlays array is the right answer for most beats.

OVERLAY TYPES — pick the type that matches the trigger, then fill the type-specific fields.

- 'bigStat' — HEADLINE NUMBER. Use ONLY when the beat's hero moment is a pure numeric value: currency, percentage, basis points, or a raw large number — NEVER a phrase or descriptive label.
  ✅ VALID text examples: "$1.5T", "$500M", "$64,000", "$2.4 trillion", "4%", "+187%", "-25%", "9.1%", "25 basis points", "25bp", "$64,000/year"
  ❌ INVALID — DO NOT USE for bigStat (the server will convert these to lowerThird or drop them):
     "Market Boost", "$1.5 Trillion Market Boost", "Federal Reserve Decision", "Policy Shift", "Big Move", "Q3 Recovery", "Trillion Dollar Boost"
  The number renders in brand orange with a brand-orange halo, and auto-animates a 0→target count-up when it parses as currency (≥$1B) or percent (≥20%). DO NOT add a subtitle, color, or any other styling — bigStat is JUST the numeric value, no surrounding words.
  REQUIRED: text (numeric only — see examples above; use symbols: $2.4T not $2,400,000,000,000)
  OPTIONAL: position ('center' default | 'top' | 'bottom')
  duration: 2.5-5s. Will be capped to the beat's length on the server, so don't exceed beat.end - beat.start.

- 'lowerThird' — SOURCE ATTRIBUTION ONLY. RARE — only when the voiceover EXPLICITLY cites a publication/news source ("according to Bloomberg", "WSJ reports", "per Reuters", "data from the Financial Times", "Forbes analysis shows"). Hard cap: 1-2 lowerThirds across the entire video.
  ✅ VALID text examples: "Source: Bloomberg", "Per Reuters", "Source: WSJ", "Per Financial Times", "Via CNBC", "Source: NYT"
  ❌ INVALID — DO NOT USE lowerThird for: speaker job titles ("Federal Reserve Chair"), entity already shown via portrait ("Goldman Sachs" if Goldman Sachs is in beat.entities), generic context, project names, anything other than a literal news-source citation. The server enforces the prefix whitelist (Source:/Per/Via …) and DROPS anything else.
  REQUIRED: text. Duration: 2-4s.

- 'quote' — CITED QUOTE. Use ONLY when paraphrasing a statement attributable to a named person or organization. Renders as an italic card directly UNDER the entityPortrait of that source — never standalone, never centered. The \`source\` field MUST match a name in beat.entities (so the entityPortrait gets injected and the quote pairs to it). Quotes without a matching same-beat entityPortrait are dropped server-side.
  REQUIRED: text, source (the named entity). DO NOT supply position — the quote always anchors under the portrait. Duration: 3-5s.

- 'stat' — SMALL CORNER BADGE. Use only if bigStat would be too dominant.
  REQUIRED: text. Position 'topRight'/'topLeft'. Duration: 2-3s.

NOTE on entity portraits: do NOT place 'entityPortrait' overlays manually. Instead, list named people / companies on beat.entities — the pipeline server-injects a Wikipedia portrait overlay in the top-right corner automatically.

OVERLAY PLACEMENT GUIDANCE:
- A typical 5-min finance video gets 6-10 well-placed overlays — NOT one per beat. Most beats carry ZERO overlays.
- Place overlays only on the few beats that genuinely demand visual emphasis: the hero stat reveal, the headline quote, a critical source attribution. Skip every transitional, narrative, or already-portrait-served beat.
- For named real people/companies, ALWAYS prefer adding them to beat.entities (server fetches a Wikipedia portrait) over inventing an overlay. A portrait IS the visual treatment — do NOT add a redundant lowerThird for an entity already in beat.entities.
- Server-side enforcement you should anticipate: 1 overlay per beat hard cap, 1.5s minimum gap between overlays anywhere in the video, lowerThird text mentioning an adjacent-beat entity is dropped, and any overlay colliding with bigStat is dropped (bigStat wins).

GENERAL RULES:
- Max 1 overlay per beat (the maxItems cap). Most beats should have zero.
- Use symbols ($2.4T, 47%, 100K) not full numbers in bigStat/stat/comparison.
- One concept per overlay. Don't cram.
- PREFER bigStat over the legacy 'stat' type for any standalone number.

TIMING:
- startOffset: seconds from beat.start. 0 = at beat start.
- duration: see per-type guidance above. Bigger overlays (bigStat, comparison) need 3-5s; ticker/circle can be shorter.

STEP 5 — Submit via the submit_beats tool. The beats array MUST contain at least 1 beat.
${(Array.isArray(timingTags) && timingTags.length > 0) ? `
VOICEOVER TIMING ANALYSIS (from Gemini audio analysis of the actual voiceover delivery):
These are timestamped placement opportunities derived from the speaker's vocal cues. Treat them as STRONG hints — when a beat overlaps one of these timestamps, snap the corresponding overlay's startOffset to this moment so the visual hits exactly when the voice does.
- entity-mention → align entityPortrait timing (server-injects from beat.entities; if you see this tag, make sure the entity is in beat.entities so the portrait gets injected at the right beat).
- stat-emphasis → bigStat startOffset should land here. The vocal emphasis = the visual reveal.
- emotional-peak → strong candidate for heroMoment=true on the containing beat.
- natural-pause → safe boundary for overlay enter/exit; avoid mid-sentence overlay swaps.

Tags:
${timingTags.map((t) => `- ${t.timestamp.toFixed(1)}s [${t.tag}]${t.snippet ? `: "${t.snippet.replace(/"/g, '\\"')}"` : ''}`).join('\n')}
` : ''}
Sentences:
${JSON.stringify(sentences)}${await (async () => {
  // Append per-channel retention guidance if this channel has connected
  // YouTube analytics and refreshed at least once. No-op for channels
  // without analytics — defaults to today's behaviour.
  const [insights, forensic] = await Promise.all([
    getChannelAnalyticsInsights(channelId),
    getChannelForensicData(channelId),
  ]);
  if (!insights && !forensic) return '';
  let guidance = '\n\nCHANNEL-SPECIFIC GUIDANCE (from this channel\'s past 90-day analytics — use this to optimise for THIS audience):';
  if (insights?.audienceProfile) {
    guidance += `\n- Primary audience: ${insights.audienceProfile}.`;
  }
  if (insights?.retentionDropoffPattern && typeof insights.retentionDropoffPattern.typicalDropoffPercent === 'number') {
    const x = insights.retentionDropoffPattern.typicalDropoffPercent;
    guidance += `\n- This channel's audience typically drops off around the ${x}% mark of past videos. Counter this: at the beat covering the ${x}% timestamp of THIS video, prefer heroMoment=true; consider an SFX impact + a stat overlay there. Strong hook in the first 30s is critical.`;
  }
  if (Array.isArray(insights?.trendingTopics) && insights.trendingTopics.length) {
    guidance += `\n- Trending search terms for this channel: ${insights.trendingTopics.slice(0, 5).join(', ')}. Use related visual keywords in beats where the script naturally touches these.`;
  }

  // Phase E: forensic-derived percentage-mapped retention guidance.
  // This is the high-resolution layer — actual spike/dip/cliff
  // moments and the structural template extracted by Claude from
  // this channel's top 10 videos' captions + retention curves.
  if (forensic?.patterns) {
    const fp = forensic.patterns;
    guidance += '\n\nFORENSIC RETENTION DATA (from this channel\'s top 10 videos\' transcripts + retention curves — these are PROVEN patterns for this audience, not generic advice):';
    if (fp.structuralTemplate) {
      const t = fp.structuralTemplate;
      const lines = ['intro','setup','body','climax','conclusion']
        .filter(k => t[k])
        .map(k => `${k} (${t[k]})`)
        .join(' / ');
      if (lines) guidance += `\n- Structural template proven for this channel: ${lines}. Place heroMoment=true beats at the climax window.`;
    }
    if (fp.optimalPacing) {
      const p = fp.optimalPacing;
      const bits = [];
      if (p.introHookByXSec) bits.push(`hook payoff by ${p.introHookByXSec}s`);
      if (p.sustainedSectionLength) bits.push(`sustained sections: ${p.sustainedSectionLength}`);
      if (p.statRevealTiming) bits.push(`stat reveals best at ${p.statRevealTiming} mark — drop a stat overlay there`);
      if (bits.length) guidance += `\n- Pacing: ${bits.join('; ')}.`;
    }
    if (Array.isArray(fp.spikeWordPatterns) && fp.spikeWordPatterns.length) {
      const ex = fp.spikeWordPatterns.slice(0, 4)
        .map(s => `"${s.phrase}"${s.where ? ` (${s.where})` : ''}`)
        .join(', ');
      guidance += `\n- Spike phrases that lifted retention historically: ${ex}. When a beat\'s sentence lands one of these phrases or a structural cousin, mark heroMoment=true and add a "transition" or "impact" SFX.`;
    }
    if (Array.isArray(fp.dipWordPatterns) && fp.dipWordPatterns.length) {
      const ex = fp.dipWordPatterns.slice(0, 3)
        .map(d => `"${d.phrase}"`)
        .join(', ');
      guidance += `\n- Dip phrases that historically lost retention: ${ex}. If a beat\'s sentence resembles these, do NOT mark hero — instead bridge with a strong visual + concise keywords.`;
    }
    if (fp.audienceInsights?.triggers || fp.audienceInsights?.repellents) {
      const ai = fp.audienceInsights;
      if (ai.triggers) guidance += `\n- Retention triggers for this audience: ${ai.triggers}.`;
      if (ai.repellents) guidance += `\n- Retention repellents (avoid): ${ai.repellents}.`;
    }
  }

  // Per-video forensic raw data — surface the most-actionable bits
  // (the dropoff timestamps and best sections) even when patterns
  // didn't run successfully. Capped to a few examples to keep prompt size sane.
  if (forensic?.perVideo) {
    const videos = Object.values(forensic.perVideo).filter(v => !v.error);
    const allCliffs = videos.flatMap(v => (v.cliffDrops || []).map(c => c.x)).filter(x => typeof x === 'number');
    if (allCliffs.length >= 3) {
      const avgCliff = Math.round((allCliffs.reduce((s, x) => s + x, 0) / allCliffs.length) * 100);
      guidance += `\n- HISTORICAL CLIFF DROPS cluster around the ${avgCliff}% mark across ${videos.length} top videos. Defend this position in THIS video: heroMoment=true at the ~${avgCliff}% beat, SFX impact, stat overlay if any number is spoken there.`;
    }
  }

  return guidance;
})()}`;

  let result;
  try {
    result = await callAnthropicWithTool(anthropicKey, prompt, BEATS_TOOL, 32768);
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
      result = await callAnthropicWithTool(anthropicKey, retryPrompt, BEATS_TOOL, 32768);
    } catch (e) {
      console.warn(`breakIntoBeats retry threw (${e.message}) — falling back to naive grouping`);
      result = {beats: []};
    }
  }

  if (Array.isArray(result?.beats) && result.beats.length > 0) {
    // Visual-density gate. Claude sometimes returns sparse beats
    // despite explicit minimums in the prompt. We do up to 2 regens
    // with concrete deficit feedback ("you returned X overlays / Y
    // entities / Z animations vs minimums A/B/C — regenerate denser").
    // Keep whichever attempt scored highest on a composite metric.
    const totalSec = totalScriptSec || (sentences.length
      ? sentences[sentences.length - 1].end - sentences[0].start
      : 0);
    const totalMin = Math.max(0.5, totalSec / 60);

    const measure = (beats) => {
      const overlays = countOverlays(beats);
      let entities = 0;
      let animations = 0;
      for (const b of (beats || [])) {
        if (Array.isArray(b?.entities)) entities += b.entities.length;
        if (b?.shouldAnimate === true) animations++;
      }
      return {overlays, entities, animations};
    };
    const targets = {
      // Conservative floors aligned with the "6-12 overlays per 5-min,
      // quality over quantity" guidance. Earlier 3/min targets pushed
      // Claude past the schema and resulted in 0 overlays returned
      // (Claude failing entire field rather than partially complying).
      overlays: Math.max(4, Math.floor(totalMin * 1.5)),     // 1.5/min, min 4
      entities: Math.max(3, Math.floor(totalMin * 1.2)),      // 1.2/min, min 3
      animations: Math.max(3, Math.floor(totalMin * 1.5)),    // 1.5/min, min 3
    };
    const score = (m) =>
      Math.min(1, m.overlays / targets.overlays) +
      Math.min(1, m.entities / targets.entities) +
      Math.min(1, m.animations / targets.animations);

    let bestBeats = result.beats;
    let bestMeasure = measure(bestBeats);
    let bestScore = score(bestMeasure);
    console.log(`breakIntoBeats: initial measure overlays=${bestMeasure.overlays}/${targets.overlays}, entities=${bestMeasure.entities}/${targets.entities}, animations=${bestMeasure.animations}/${targets.animations} → score ${bestScore.toFixed(2)}/3.00`);

    const deficitLines = (m) => {
      const lines = [];
      if (m.overlays < targets.overlays) lines.push(`overlays: ${m.overlays}/${targets.overlays}`);
      if (m.entities < targets.entities) lines.push(`entities: ${m.entities}/${targets.entities}`);
      if (m.animations < targets.animations) lines.push(`shouldAnimate beats: ${m.animations}/${targets.animations}`);
      return lines;
    };
    const passed = bestScore >= 2.7; // 90% of full score across 3 dimensions

    if (!passed) {
      for (let attempt = 1; attempt <= 2; attempt++) {
        const deficits = deficitLines(bestMeasure);
        if (!deficits.length) break;
        console.warn(`breakIntoBeats: density attempt ${attempt}/2 — deficits: ${deficits.join('; ')}`);
        const regenPrompt = `${prompt}

PREVIOUS RESPONSE INSUFFICIENT — visual density below floors for a ${totalSec.toFixed(0)}s video:
${deficits.map((d) => `  • ${d}`).join('\n')}

Regenerate the SAME ${totalSec.toFixed(0)}s of beats with MORE visual density on every beat. Walk the STEP 4 rules. Every specific number → bigStat overlay. Every named source → lowerThird overlay. Every quoted statement → quote overlay. Every named person/company/org → entry on beat.entities (the server fetches a Wikipedia portrait). Every abstract concept → shouldAnimate=TRUE. Don't add new sentences — increase the visual annotation on the existing beats.`;
        let regenBeats = null;
        try {
          const regen = await callAnthropicWithTool(anthropicKey, regenPrompt, BEATS_TOOL, 32768);
          if (Array.isArray(regen?.beats) && regen.beats.length > 0) regenBeats = regen.beats;
        } catch (e) {
          console.warn(`breakIntoBeats: density regen ${attempt} threw (${e.message})`);
          break;
        }
        if (!regenBeats) break;
        const regenMeasure = measure(regenBeats);
        const regenScore = score(regenMeasure);
        console.log(`breakIntoBeats: regen ${attempt} measure overlays=${regenMeasure.overlays}, entities=${regenMeasure.entities}, animations=${regenMeasure.animations} → score ${regenScore.toFixed(2)}`);
        if (regenScore > bestScore) {
          bestBeats = regenBeats;
          bestMeasure = regenMeasure;
          bestScore = regenScore;
        }
        if (bestScore >= 2.7) break;
      }
    }
    console.log(`breakIntoBeats: final ${bestMeasure.overlays} overlays, ${bestMeasure.entities} entities, ${bestMeasure.animations} animations across ${bestBeats.length} beats (score ${bestScore.toFixed(2)})`);
    return bestBeats;
  }

  // Last-resort deterministic fallback. Claude refused twice — group the
  // sentences ourselves so the pipeline can still produce a render. Output
  // quality is degraded (keywords are extracted heuristically, fluxPrompt
  // is a generic illustrate-this template), but the user gets SOMETHING
  // back instead of a stuck job. job.naiveFallbackUsed flags it.
  console.warn(`breakIntoBeats: both Claude attempts returned 0 beats — using naive grouping fallback`);
  return naiveGroupSentences(validSentences);
}

// Per-duration minimum overlay floor. Returns 0 for very short videos
// (no point forcing overlays on a 30s clip).
function overlayMinimumForDuration(totalSec) {
  if (totalSec >= 7 * 60) return 8;
  if (totalSec >= 4 * 60) return 4;
  if (totalSec >= 1 * 60) return Math.max(1, Math.floor(totalSec / 60));
  return 0;
}

function countOverlays(beats) {
  let n = 0;
  for (const b of (beats || [])) {
    if (Array.isArray(b?.overlays)) n += b.overlays.length;
  }
  return n;
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
// Returns up to N candidate URLs (top distinct videos for the query).
// We try each through validatePexelsVideo so a corrupted top result
// doesn't blow up the render — fall through to next, then to Flux.
async function pexelsSearchTop(query, n = 3) {
  if (!process.env.PEXELS_API_KEY) throw new Error('PEXELS_API_KEY not bound');
  const url = new URL('https://api.pexels.com/videos/search');
  url.searchParams.set('query', query);
  url.searchParams.set('per_page', '10');
  url.searchParams.set('orientation', 'landscape');
  url.searchParams.set('size', 'large');

  const resp = await fetch(url, {headers: {Authorization: process.env.PEXELS_API_KEY}});
  if (!resp.ok) {
    console.warn(`Pexels ${resp.status} for "${query}"`);
    return [];
  }
  const data = await resp.json();
  const out = [];
  for (const video of data.videos || []) {
    const files = (video.video_files || []).filter((f) => f.file_type === 'video/mp4');
    if (!files.length) continue;
    files.sort((a, b) => Math.abs((a.height || 0) - 1080) - Math.abs((b.height || 0) - 1080));
    out.push(files[0].link);
    if (out.length >= n) break;
  }
  return out;
}

// Pixabay videos. Free CC0 commercial-use library. Same shape as
// Pexels — returns up to N direct mp4 URLs sorted by relevance.
// PIXABAY_API_KEY must be in env (register at pixabay.com/api/docs/).
async function pixabaySearchTop(query, n = 3) {
  if (!process.env.PIXABAY_API_KEY) return [];
  const url = new URL('https://pixabay.com/api/videos/');
  url.searchParams.set('key', process.env.PIXABAY_API_KEY);
  url.searchParams.set('q', query);
  url.searchParams.set('per_page', '10');
  url.searchParams.set('safesearch', 'true');
  const resp = await fetch(url);
  if (!resp.ok) {
    console.warn(`Pixabay ${resp.status} for "${query}"`);
    return [];
  }
  const data = await resp.json();
  const out = [];
  for (const hit of (data.hits || [])) {
    const videos = hit.videos || {};
    // Prefer 1080p (large) → 720p (medium) → 360p (small) — match the
    // resolution tier we want for our composition.
    const file = videos.large?.url || videos.medium?.url || videos.small?.url;
    if (file) out.push(file);
    if (out.length >= n) break;
  }
  return out;
}

// Wikimedia Commons. No API key. Searches the public commons for
// video files matching the query. Quality varies wildly — most
// hits are stills/animations/educational clips, not stock-footage
// quality. Useful as a tail-end source when other libraries miss.
async function wikimediaSearchTop(query, n = 3) {
  // Two-step: search for matching files in namespace 6 (File:) →
  // pull imageinfo (url + mime) for each → keep video MIME types.
  const url = new URL('https://commons.wikimedia.org/w/api.php');
  url.searchParams.set('action', 'query');
  url.searchParams.set('format', 'json');
  url.searchParams.set('generator', 'search');
  url.searchParams.set('gsrsearch', query + ' filemime:video');
  url.searchParams.set('gsrnamespace', '6');
  url.searchParams.set('gsrlimit', '15');
  url.searchParams.set('prop', 'imageinfo');
  url.searchParams.set('iiprop', 'url|mime|size');
  url.searchParams.set('origin', '*');
  const resp = await fetch(url);
  if (!resp.ok) {
    console.warn(`Wikimedia ${resp.status} for "${query}"`);
    return [];
  }
  const data = await resp.json();
  const pages = Object.values(data.query?.pages || {});
  const out = [];
  for (const p of pages) {
    const info = (p.imageinfo || [])[0];
    if (!info || !info.mime || !info.url) continue;
    if (!/^video\//i.test(info.mime)) continue;
    // Wikimedia hosts .webm and .ogv heavily — Lambda Chromium can
    // decode webm; ogv is hit-or-miss. Prefer mp4 when available, accept
    // webm. Skip ogv.
    if (/ogg|ogv/i.test(info.url)) continue;
    out.push(info.url);
    if (out.length >= n) break;
  }
  return out;
}

// Internet Archive movies collection, public-domain filtered. No API
// key. Quality is variable — older PD footage, often grainy. Default-off
// in the channel toggle list because most channels won't want this.
async function archiveSearchTop(query, n = 3) {
  const q = `${query} AND mediatype:(movies) AND licenseurl:(*publicdomain*)`;
  const url = new URL('https://archive.org/advancedsearch.php');
  url.searchParams.set('q', q);
  url.searchParams.append('fl[]', 'identifier');
  url.searchParams.append('fl[]', 'title');
  url.searchParams.set('rows', '10');
  url.searchParams.set('output', 'json');
  url.searchParams.set('sort[]', 'downloads desc');
  const resp = await fetch(url);
  if (!resp.ok) {
    console.warn(`InternetArchive ${resp.status} for "${query}"`);
    return [];
  }
  const data = await resp.json();
  const docs = (data.response?.docs || []).slice(0, n);
  const out = [];
  // Resolve each identifier → file list via /metadata/. Pick first mp4.
  for (const d of docs) {
    try {
      const m = await fetch(`https://archive.org/metadata/${d.identifier}`);
      if (!m.ok) continue;
      const meta = await m.json();
      const files = meta.files || [];
      const mp4 = files.find((f) => /\.mp4$/i.test(f.name || ''));
      if (mp4) {
        out.push(`https://archive.org/download/${d.identifier}/${encodeURIComponent(mp4.name)}`);
      }
    } catch { /* skip */ }
    if (out.length >= n) break;
  }
  return out;
}

// Channel-aware footage source chain. Tries enabled sources in order
// until N validated candidates land. Returns {urls, sourceCounts}.
//
// Default sources (when channel has no footageSources field):
//   pexels: true, pixabay: true, wikimedia: true, archive: false.
//
// Each source's candidates run through validatePexelsVideo (the HEAD +
// ftyp check works on any mp4 regardless of CDN). First-N-validated
// wins; sources later in the chain only run when earlier ones return
// empty for this query.
async function searchAllFootageSources(channelId, query, n = 3) {
  // Read channel's footageSources toggles from settings doc, default
  // to the broad set if missing. Same pattern as musicSettings.
  const fallback = {pexels: true, pixabay: true, wikimedia: true, archive: false};
  let cfg = fallback;
  if (channelId) {
    try {
      const ch = await db.collection('channels').doc(channelId).get();
      cfg = {...fallback, ...(ch.data()?.footageSources || {})};
    } catch {/* keep fallback */}
  }

  const order = [
    cfg.pexels && {name: 'pexels', fn: pexelsSearchTop},
    cfg.pixabay && {name: 'pixabay', fn: pixabaySearchTop},
    cfg.wikimedia && {name: 'wikimedia', fn: wikimediaSearchTop},
    cfg.archive && {name: 'archive', fn: archiveSearchTop},
  ].filter(Boolean);

  const sourceCounts = {pexels: 0, pixabay: 0, wikimedia: 0, archive: 0};
  for (const src of order) {
    let urls;
    try { urls = await src.fn(query, n); }
    catch (e) { console.warn(`[footage] ${src.name} threw for "${query}": ${e.message}`); continue; }
    if (!urls || urls.length === 0) continue;
    sourceCounts[src.name] = urls.length;
    return {urls, source: src.name, sourceCounts};
  }
  return {urls: [], source: null, sourceCounts};
}

// Validate a Pexels mp4 URL before committing it to render. Catches
// the ~5% of files that are missing frames, truncated, or 404s, which
// blow up Remotion mid-render with "No frame found at position X".
//   - HEAD: 2xx, content-type video/*, content-length sane
//   - Range GET (first 1 MB): MP4 starts with 'ftyp' box at offset 4
async function validatePexelsVideo(url) {
  try {
    const head = await fetch(url, {method: 'HEAD'});
    if (!head.ok) return {ok: false, reason: `HEAD ${head.status}`};
    const ct = head.headers.get('content-type') || '';
    if (!/^video\//i.test(ct)) return {ok: false, reason: `bad content-type: ${ct}`};
    const len = parseInt(head.headers.get('content-length') || '0', 10);
    if (!len || len < 100_000 || len > 500_000_000) {
      return {ok: false, reason: `bad content-length: ${len}`};
    }
    const ranged = await fetch(url, {headers: {Range: 'bytes=0-1048575'}});
    if (!ranged.ok && ranged.status !== 206) return {ok: false, reason: `Range ${ranged.status}`};
    const buf = Buffer.from(await ranged.arrayBuffer());
    if (buf.length < 12) return {ok: false, reason: 'range body too small'};
    const magic = buf.toString('ascii', 4, 8);
    if (magic !== 'ftyp') return {ok: false, reason: `no ftyp box (got "${magic}")`};
    return {ok: true, length: len};
  } catch (e) {
    return {ok: false, reason: e.message};
  }
}

// ─── Universal source cache to Firebase Storage ─────────────────────
// Lambda fetches every render asset from this cache instead of the
// original source. Firebase Storage is fast, never rate-limited, and
// stays available even if Pexels deletes a video or a Replicate URL
// expires. Cache key = sha1(sourceUrl) so the same external file maps
// to one canonical cache entry across all renders.
//
// Streaming: piping fetch.body straight into the storage write stream
// keeps memory flat regardless of file size. The previous Buffer.from
// (await resp.arrayBuffer()) approach OOM'd the 1GB function on long
// scripts where 60+ × 30MB videos were buffered concurrently.
async function cacheRemoteToFirebase(sourceUrl, {prefix = 'cache/footage', defaultExt = 'mp4'} = {}) {
  const hash = crypto.createHash('sha1').update(sourceUrl).digest('hex');
  const extMatch = sourceUrl.match(/\.([a-z0-9]{2,4})(?:\?|$)/i);
  const ext = (extMatch ? extMatch[1].toLowerCase() : defaultExt);
  const path = `${prefix}/${hash}.${ext}`;
  const bucket = admin.storage().bucket();
  const file = bucket.file(path);

  const [exists] = await file.exists();
  if (exists) {
    return {url: `https://storage.googleapis.com/${bucket.name}/${path}`, hit: true};
  }

  const resp = await fetch(sourceUrl);
  if (!resp.ok) throw new Error(`Cache fetch ${resp.status} for ${sourceUrl.slice(0, 80)}`);

  const {pipeline} = require('node:stream/promises');
  const {Readable} = require('node:stream');
  await pipeline(
    Readable.fromWeb(resp.body),
    file.createWriteStream({
      metadata: {contentType: resp.headers.get('content-type') || 'application/octet-stream'},
      resumable: false, // single-shot upload, faster for our file sizes
    }),
  );
  await file.makePublic();
  return {url: `https://storage.googleapis.com/${bucket.name}/${path}`, hit: false};
}

// ─── Corrupt-cache eviction + blocklist ─────────────────────────────
// When Lambda fails on a Firebase-cached source, that file's bytes are
// permanently broken. Every retry re-uses the same hashed cache entry
// → same failure forever. We delete the bad file and remember the
// upstream Pexels URL on the channel doc so future caches skip it.
function extractStoragePathFromUrl(url) {
  if (!url) return null;
  const bucket = admin.storage().bucket();
  const prefix = `https://storage.googleapis.com/${bucket.name}/`;
  if (url.startsWith(prefix)) return url.slice(prefix.length);
  return null;
}

async function deleteCachedFile(url) {
  const path = extractStoragePathFromUrl(url);
  if (!path) return false;
  try {
    await admin.storage().bucket().file(path).delete();
    console.log(`[cache] deleted corrupt file: ${path}`);
    return true;
  } catch (e) {
    console.warn(`[cache] failed to delete ${path}: ${e.message}`);
    return false;
  }
}

async function getChannelBlocklist(channelId) {
  if (!channelId) return new Set();
  try {
    const doc = await db.collection('channels').doc(channelId).get();
    return new Set(doc.data()?.corruptCacheBlocklist || []);
  } catch (e) {
    console.warn(`[cache] failed to read blocklist for ${channelId}: ${e.message}`);
    return new Set();
  }
}

async function addToCacheBlocklist(channelId, sourceUrl) {
  if (!channelId || !sourceUrl) return;
  try {
    await db.collection('channels').doc(channelId).set(
      {corruptCacheBlocklist: FieldValue.arrayUnion(sourceUrl)},
      {merge: true},
    );
    console.log(`[cache] blocklisted upstream URL: ${sourceUrl.slice(0, 100)}`);
  } catch (e) {
    console.warn(`[cache] failed to update blocklist: ${e.message}`);
  }
}

// ─── ffprobe pre-flight validation ──────────────────────────────────
// Lambda Chromium can't recover from a corrupt source file: the whole
// render aborts with delayRender / fatalErrorEncountered. Each retry
// after that costs another full Lambda render (~5 min wall on a
// 12-min video). To avoid the retry tax, we shell out to ffprobe on
// every cached video URL BEFORE invoking Lambda — any beat whose
// header doesn't parse, has no video stream, or reports zero duration
// gets swapped to a Flux fallback up front.
//
// The binary is supplied by @ffprobe-installer/ffprobe (per-platform
// optional deps; Cloud Build picks linux-x64 on deploy). If the
// binary fails to load for any reason we resolve {skipped:true} and
// fall through — the existing mid-render swap loop is still wired up
// as a last-line backstop.
let _ffprobePath = null;
let _ffprobeVersionLogged = false;
function getFfprobePath() {
  if (_ffprobePath !== null) return _ffprobePath;
  try {
    _ffprobePath = require('@ffprobe-installer/ffprobe').path;
    console.log(`ffprobe binary path: ${_ffprobePath}`);
  } catch (e) {
    console.warn(`ffprobe binary unavailable: ${e.message} — preflight will be skipped`);
    _ffprobePath = false;
  }
  return _ffprobePath;
}

function selfTestFfprobeOnce() {
  if (_ffprobeVersionLogged) return;
  _ffprobeVersionLogged = true;
  const p = getFfprobePath();
  if (!p) return;
  const {spawn} = require('node:child_process');
  const proc = spawn(p, ['-version'], {stdio: ['ignore', 'pipe', 'pipe']});
  let out = '';
  let err = '';
  proc.stdout.on('data', (c) => { out += c.toString(); });
  proc.stderr.on('data', (c) => { err += c.toString(); });
  proc.once('close', (code, signal) => {
    console.log(`[ffprobe self-test] exit=${code} signal=${signal} stdout=${out.split('\n')[0] || '(empty)'} stderr=${(err || '').slice(0, 200) || '(empty)'}`);
  });
  proc.once('error', (e) => {
    console.warn(`[ffprobe self-test] spawn error: ${e.message}`);
  });
}

// The static ffprobe binary (johnvansickle build) segfaults on https://
// URLs because its TLS layer is incomplete. Workaround: fetch the bytes
// in node — which has working TLS — and pipe them into ffprobe's stdin.
// ffprobe sees a non-seekable pipe, but parses streams + duration from
// container metadata as it reads, so for mp4/mov/webm this Just Works.
function probeVideoUrl(url, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const ffprobePath = getFfprobePath();
    if (!ffprobePath) return resolve({skipped: true, reason: 'no-binary'});

    const {spawn} = require('node:child_process');
    const proc = spawn(ffprobePath, [
      '-v', 'error',
      '-show_streams',
      '-show_format',
      '-of', 'json',
      'pipe:0',
    ], {stdio: ['pipe', 'pipe', 'pipe']});

    let out = '';
    let err = '';
    proc.stdout.on('data', (c) => { out += c.toString(); });
    proc.stderr.on('data', (c) => { err += c.toString(); });

    let settled = false;
    const finalize = (fn, payload) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { proc.stdin.destroy(); } catch { /* ignore */ }
      try { proc.kill('SIGKILL'); } catch { /* ignore */ }
      fn(payload);
    };

    const timer = setTimeout(() => {
      finalize(reject, new Error(`ffprobe timeout after ${timeoutMs}ms`));
    }, timeoutMs);

    proc.once('error', (e) => {
      finalize(reject, new Error(`ffprobe spawn: ${e.message}`));
    });
    proc.once('close', (code, signal) => {
      if (settled) return;
      // Killed by signal usually means we destroyed stdin after ffprobe
      // already had what it needed but kept reading. If we got valid JSON
      // on stdout, treat it as success regardless of signal.
      if (out) {
        try {
          const meta = JSON.parse(out);
          const videoStream = (meta.streams || []).find((s) => s.codec_type === 'video');
          if (!videoStream) return finalize(reject, new Error('no video stream'));
          const duration = parseFloat(meta.format?.duration || videoStream.duration || 0);
          if (!duration || duration <= 0) return finalize(reject, new Error('zero duration'));
          return finalize(resolve, {
            duration,
            codec: videoStream.codec_name,
            width: videoStream.width,
            height: videoStream.height,
          });
        } catch { /* fall through */ }
      }
      if (code === null) {
        return finalize(resolve, {skipped: true, reason: `signal=${signal}`});
      }
      if (code !== 0) return finalize(reject, new Error(`ffprobe exit ${code}: ${(err || '').slice(0, 200) || '(empty stderr)'}`));
      finalize(reject, new Error(`ffprobe produced no output`));
    });

    // Stream the URL bytes into ffprobe's stdin. We may write more than
    // ffprobe needs — when that happens ffprobe closes stdin early and
    // EPIPE comes back here; that's not an error, swallow it.
    (async () => {
      try {
        const resp = await fetch(url);
        if (!resp.ok) {
          return finalize(reject, new Error(`HTTP ${resp.status} fetching ${url.slice(0, 80)}`));
        }
        const {pipeline} = require('node:stream/promises');
        const {Readable} = require('node:stream');
        await pipeline(Readable.fromWeb(resp.body), proc.stdin)
          .catch((e) => {
            // EPIPE / ERR_STREAM_PREMATURE_CLOSE / ECANCELED all mean
            // ffprobe got what it needed and closed stdin early.
            if (!/EPIPE|premature close|ECANCELED/i.test(e.message)) {
              console.warn(`[ffprobe] stream piping warning: ${e.message}`);
            }
          });
      } catch (e) {
        finalize(reject, new Error(`stream-to-ffprobe: ${e.message}`));
      }
    })();
  });
}

// Deep validation: downloads the file to /tmp, runs ffprobe metadata
// AND frame-decode checks at 5 positions (20/40/60/80/95% of duration).
// Catches the "headers look fine but mid-file frame data is corrupt"
// case that produces "No frame found at position X" errors on Lambda
// renders. The previous one-frame-at-50% probe missed real corruption
// at other positions — e.g. a Pexels clip with bad bytes around 92%
// passed probe, cached, then crashed seven retries in a row. Five
// probe positions across the clip's timeline catch this without
// materially increasing probe time (input-side seek on a local file
// is ms-level; 5 frames typically <500ms total, 40s worst-case).
//
// Backwards-compat: legacy `decodeAtFraction` opt still accepted; if
// passed, validation reverts to the single-frame check at that point.
const DECODE_PROBE_FRACTIONS = [0.20, 0.40, 0.60, 0.80, 0.95];
async function deepValidateBeat(url, opts = {}) {
  const {decodeAtFraction, decodeAtFractions} = opts;
  const fractions = Array.isArray(decodeAtFractions) && decodeAtFractions.length
    ? decodeAtFractions
    : (typeof decodeAtFraction === 'number' ? [decodeAtFraction] : DECODE_PROBE_FRACTIONS);
  const fs = require('node:fs');
  const path = require('node:path');
  const os = require('node:os');
  const ffprobePath = getFfprobePath();
  if (!ffprobePath) return {skipped: true, reason: 'no-ffprobe'};

  const tmpFile = path.join(os.tmpdir(), `probe-${crypto.randomBytes(6).toString('hex')}.mp4`);
  try {
    await downloadUrlToFile(url, tmpFile);

    // Stage A: ffprobe metadata on the local file (no TLS in play).
    const meta = await new Promise((resolve, reject) => {
      const {spawn} = require('node:child_process');
      const proc = spawn(ffprobePath, [
        '-v', 'error',
        '-show_streams',
        '-show_format',
        '-of', 'json',
        tmpFile,
      ], {stdio: ['ignore', 'pipe', 'pipe']});
      let out = '';
      let err = '';
      proc.stdout.on('data', (c) => { out += c.toString(); });
      proc.stderr.on('data', (c) => { err += c.toString(); });
      const timer = setTimeout(() => {
        proc.kill('SIGKILL');
        reject(new Error('ffprobe timeout'));
      }, 15000);
      proc.once('close', (code) => {
        clearTimeout(timer);
        if (code !== 0) return reject(new Error(`ffprobe exit ${code}: ${(err || '').slice(0, 200)}`));
        try {
          const m = JSON.parse(out);
          const vid = (m.streams || []).find((s) => s.codec_type === 'video');
          if (!vid) return reject(new Error('no video stream'));
          const dur = parseFloat(m.format?.duration || vid.duration || 0);
          if (!dur || dur <= 0) return reject(new Error('zero duration'));
          resolve({duration: dur, codec: vid.codec_name, width: vid.width, height: vid.height});
        } catch (e) {
          reject(new Error(`ffprobe parse: ${e.message}`));
        }
      });
      proc.once('error', (e) => {
        clearTimeout(timer);
        reject(new Error(`ffprobe spawn: ${e.message}`));
      });
    });

    // Stage B: decode one frame at each probe position. Fast on a
    // seekable local file (input-side seek). First failure short-
    // circuits the rest — if frame data is bad at any position the
    // source is corrupt for our purposes. We skip decode probing for
    // very short clips (<2s) where the positions would cluster onto
    // the same frame anyway.
    const ffmpegPath = getFfmpegPath();
    const probedFractions = [];
    if (ffmpegPath && meta.duration > 2.0) {
      for (const frac of fractions) {
        const seekTo = Math.max(0, meta.duration * frac);
        try {
          await runFfmpeg([
            '-v', 'error',
            '-ss', String(seekTo),
            '-i', tmpFile,
            '-frames:v', '1',
            '-f', 'null',
            '-',
          ], {label: `decode-check@${Math.round(frac * 100)}%`, timeoutMs: 8000});
          probedFractions.push(frac);
        } catch (e) {
          throw new Error(`frame decode failed at ${Math.round(frac * 100)}% (t=${seekTo.toFixed(1)}s): ${e.message.slice(0, 120)}`);
        }
      }
    }

    return {...meta, probedFractions};
  } finally {
    try { fs.unlinkSync(tmpFile); } catch { /* ignore */ }
  }
}

// Two-phase preflight to avoid the Replicate-rate-limit trap that
// killed earlier runs (free-tier "burst 1" → each Flux call queues
// 12s, Promise.all batches stalled, function timed out).
//
// Phase 1: probe every non-Flux beat. Collect failures, never swap.
// Threshold check: if >20% of probes fail, we don't trust the probe
// and abort preflight entirely — let the mid-render swap loop handle
// any genuine corruption. (The probe is a fresh-bytes check; a high
// failure rate almost always means the probe itself is being too
// strict, since sourcing already passed HEAD + ftyp.)
//
// Phase 2 (only if under threshold): for each failed beat, try the
// stored Pexels alternates first — these are fast (one fetch + one
// probe each, no external API) and don't touch Replicate. Only fall
// back to Flux when every alternate fails.
async function preflightValidateFootage(footage, jobRef, {channelId} = {}) {
  selfTestFfprobeOnce();
  const VALIDATE_BATCH = 5;
  const ABORT_FAIL_RATE = 0.20;
  const t0 = Date.now();

  // Phase 1: probe-only. No swaps, no external API calls. On failure
  // for a Firebase-cached beat we eagerly evict the cached file +
  // blocklist its upstream URL — the file's bytes are permanently
  // bad and would corrupt every retry otherwise.
  const failures = [];
  let probedCount = 0;
  let evictedCount = 0;
  for (let i = 0; i < footage.length; i += VALIDATE_BATCH) {
    const end = Math.min(i + VALIDATE_BATCH, footage.length);
    await jobRef.update({
      currentStep: `Pre-flight validating clips — ${end}/${footage.length}`,
      updatedAt: FieldValue.serverTimestamp(),
    });
    await Promise.all(footage.slice(i, end).map(async (beat, batchIdx) => {
      const idx = i + batchIdx;
      if (!beat || !beat.url || beat.source === 'flux') return;
      probedCount++;
      try {
        const r = await deepValidateBeat(beat.url);
        if (r && r.skipped) return;
      } catch (probeErr) {
        failures.push({idx, reason: probeErr.message.slice(0, 160)});
        console.warn(`[preflight] beat ${idx + 1} probe failed: ${probeErr.message.slice(0, 120)}`);
        // If beat.url is one of OUR cache files, the bytes there are
        // proven bad — delete + remember the upstream URL so future
        // caches skip it. Best-effort, never throws.
        if (extractStoragePathFromUrl(beat.url)) {
          if (await deleteCachedFile(beat.url)) evictedCount++;
        }
        if (channelId && beat.originalUrl && beat.originalUrl !== beat.url) {
          await addToCacheBlocklist(channelId, beat.originalUrl);
        }
      }
    }));
  }
  const probeMs = Date.now() - t0;

  // Threshold abort: if too many INFRASTRUCTURE probes failed (network
  // / ffprobe spawn / TLS), the probe itself is the problem and we
  // bail. Frame-decode failures are explicitly excluded — those are
  // real corruption that we must fix, not noise. Without this split,
  // the new 5-position probe (which catches more real corruption than
  // the old 1-position check) would trip the abort threshold on the
  // exact incidents it's meant to repair.
  const isFrameDecodeFailure = (reason) => /^frame decode failed/.test(reason || '');
  const infraFailures = failures.filter((f) => !isFrameDecodeFailure(f.reason));
  const failRate = probedCount ? infraFailures.length / probedCount : 0;
  if (failRate > ABORT_FAIL_RATE) {
    console.warn(`[preflight] ${infraFailures.length}/${probedCount} (${(failRate * 100).toFixed(0)}%) failed probe — over ${(ABORT_FAIL_RATE * 100)}% threshold, aborting preflight; mid-render swap loop is the backstop`);
    await jobRef.update({
      'timings.validationMs': probeMs,
      'timings.preflightProbed': probedCount,
      'timings.preflightInvalid': failures.length,
      'timings.preflightInfraFailures': infraFailures.length,
      'timings.preflightEvicted': evictedCount,
      'timings.preflightSwapped': 0,
      'timings.preflightAborted': true,
    }).catch(() => {});
    return;
  }

  // Phase 2: repair each failure. Try Pexels alternates first (cheap,
  // no rate limit). Only call Flux if every alternate fails.
  let pexelsSwaps = 0;
  // fluxSwaps stays at 0 — Flux fallback removed; field kept for back-
  // compat with the UI's stats rendering.
  const fluxSwaps = 0;
  let unfixed = 0;
  for (let i = 0; i < failures.length; i += VALIDATE_BATCH) {
    const batch = failures.slice(i, i + VALIDATE_BATCH);
    await jobRef.update({
      currentStep: `Pre-flight repair — ${i + batch.length}/${failures.length} (Pexels alternate first, Flux as last resort)`,
      updatedAt: FieldValue.serverTimestamp(),
    });
    await Promise.all(batch.map(async ({idx, reason}) => {
      const beat = footage[idx];
      const alternates = beat.pexelsAlternates || [];
      // Try each Pexels alternate sequentially. Probe → cache → swap.
      for (const altUrl of alternates) {
        try {
          const r = await probeVideoUrl(altUrl, 30000);
          if (r && r.skipped) continue; // un-probable doesn't help us
        } catch { continue; }
        let cachedUrl = altUrl;
        try {
          const c = await cacheRemoteToFirebase(altUrl, {prefix: 'cache/footage', defaultExt: 'mp4'});
          cachedUrl = c.url;
        } catch { /* fall through with raw URL */ }
        footage[idx] = {
          ...beat,
          previousUrl: beat.url,
          url: cachedUrl,
          originalUrl: altUrl,
          replacedReason: `pre-flight alt-Pexels (was: ${reason})`,
          pexelsAlternates: alternates.filter((a) => a !== altUrl),
        };
        pexelsSwaps++;
        console.log(`[preflight] beat ${idx + 1} swapped to alternate Pexels`);
        return;
      }
      // No working Pexels alternate — drop the beat. Gap-fill will
      // stretch the previous beat to cover. Flux fallback removed
      // (AI imagery is off-brand for finance/news content).
      footage[idx] = {
        ...beat,
        url: null,
        source: null,
        previousUrl: beat.url,
        replacedReason: `pre-flight: alts exhausted, beat skipped (was: ${reason})`,
      };
      unfixed++;
      console.warn(`[preflight] beat ${idx + 1} skipped — no working Pexels source`);
    }));
  }

  const validationMs = Date.now() - t0;
  await jobRef.update({
    footage,
    'timings.validationMs': validationMs,
    'timings.preflightProbed': probedCount,
    'timings.preflightInvalid': failures.length,
    'timings.preflightEvicted': evictedCount,
    'timings.preflightSwapped': pexelsSwaps + fluxSwaps,
    'timings.preflightPexelsSwaps': pexelsSwaps,
    'timings.preflightFluxSwaps': fluxSwaps,
    'timings.preflightUnfixed': unfixed,
  }).catch(() => {});
  console.log(`pre-flight: probed ${probedCount}, ${failures.length} invalid, ${evictedCount} evicted, ${pexelsSwaps} alt-Pexels + ${fluxSwaps} Flux swapped, ${unfixed} unfixed (took ${(validationMs / 1000).toFixed(1)}s)`);
}

// ─── ffmpeg (companion to ffprobe) ──────────────────────────────────
// Used for two things:
//   1. Audio clipping — slice the voiceover + music into per-segment
//      files so each segment Lambda render gets its own short audio.
//   2. MP4 concatenation — stitch the segment renders into one final
//      output via the concat demuxer (-c copy, no re-encode).
//
// Same TLS caveat as ffprobe — johnvansickle's static binary segfaults
// on https URLs. We always operate on local /tmp files; bytes get
// fetched into /tmp via node fetch (which has working TLS).
let _ffmpegPath = null;
function getFfmpegPath() {
  if (_ffmpegPath !== null) return _ffmpegPath;
  try {
    _ffmpegPath = require('@ffmpeg-installer/ffmpeg').path;
    console.log(`ffmpeg binary path: ${_ffmpegPath}`);
  } catch (e) {
    console.warn(`ffmpeg binary unavailable: ${e.message}`);
    _ffmpegPath = false;
  }
  return _ffmpegPath;
}

async function runFfmpeg(args, {timeoutMs = 300000, label = 'ffmpeg'} = {}) {
  const ffmpegPath = getFfmpegPath();
  if (!ffmpegPath) throw new Error('ffmpeg binary not available');
  const {spawn} = require('node:child_process');
  return new Promise((resolve, reject) => {
    const proc = spawn(ffmpegPath, args, {stdio: ['ignore', 'pipe', 'pipe']});
    let stderr = '';
    proc.stderr.on('data', (c) => { stderr += c.toString(); });
    const timer = setTimeout(() => {
      proc.kill('SIGKILL');
      reject(new Error(`${label} timeout after ${timeoutMs}ms`));
    }, timeoutMs);
    proc.once('close', (code, signal) => {
      clearTimeout(timer);
      if (code === 0) return resolve();
      reject(new Error(`${label} exit ${code} signal=${signal}: ${stderr.slice(-400) || '(empty stderr)'}`));
    });
    proc.once('error', (e) => {
      clearTimeout(timer);
      reject(new Error(`${label} spawn: ${e.message}`));
    });
  });
}

// Stream a remote URL into a local /tmp file. Uses node fetch (TLS OK).
async function downloadUrlToFile(url, localPath) {
  const fs = require('node:fs');
  const {pipeline} = require('node:stream/promises');
  const {Readable} = require('node:stream');
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`HTTP ${resp.status} fetching ${url.slice(0, 80)}`);
  await pipeline(Readable.fromWeb(resp.body), fs.createWriteStream(localPath));
}

// Upload a local file to Firebase Storage (streamed) and return the
// public URL. Mirrors uploadRenderToFirebaseStorage but for /tmp files.
async function uploadLocalFileToFirebase(localPath, storagePath, contentType, {jobRef = null, label = 'Uploading'} = {}) {
  const fs = require('node:fs');
  const {pipeline} = require('node:stream/promises');
  const {Transform} = require('node:stream');
  const stat = fs.statSync(localPath);
  const totalBytes = stat.size;
  const totalMB = (totalBytes / 1024 / 1024).toFixed(1);

  const bucket = admin.storage().bucket();
  const file = bucket.file(storagePath);

  // No `resumable: false` — single-shot PUTs hang on multi-hundred-MB
  // files. The Cloud Storage SDK defaults to resumable for >5MB which
  // is what we want for final renders.
  const writeStream = file.createWriteStream({
    metadata: {contentType},
  });

  let bytesWritten = 0;
  let lastProgressUpdate = 0;
  const meter = new Transform({
    transform(chunk, _enc, cb) {
      bytesWritten += chunk.length;
      const now = Date.now();
      if (jobRef && now - lastProgressUpdate > 5000) {
        const mb = (bytesWritten / 1024 / 1024).toFixed(1);
        const pct = totalBytes ? Math.round((bytesWritten / totalBytes) * 100) : 0;
        jobRef.update({
          currentStep: `${label} — ${mb} / ${totalMB} MB (${pct}%)`,
          updatedAt: FieldValue.serverTimestamp(),
        }).catch(() => {});
        lastProgressUpdate = now;
      }
      cb(null, chunk);
    },
  });

  // Hard timeout — if a 200MB upload doesn't finish in 10 min something
  // is wrong, fail loud rather than letting the function hit its 60-min
  // ceiling.
  const UPLOAD_TIMEOUT_MS = 10 * 60 * 1000;
  let timer;
  try {
    await Promise.race([
      pipeline(fs.createReadStream(localPath), meter, writeStream),
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`Upload hung after ${UPLOAD_TIMEOUT_MS / 1000}s for ${totalMB}MB file → ${storagePath}`)),
          UPLOAD_TIMEOUT_MS,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }

  await file.makePublic();
  console.log(`uploaded ${totalMB}MB → ${storagePath}`);
  return `https://storage.googleapis.com/${bucket.name}/${storagePath}`;
}

// ─── Long-video splitting ───────────────────────────────────────────
// Lambda's main coordinator function has a 900s ceiling. A 12-min
// 30fps composition runs ~12 chunks; if any one is slow, the main
// function wallclocks before the last chunk lands. We split anything
// past 8 min into ~5-min pieces, render in parallel, then concat the
// MP4s with -c copy (no re-encode, ~30s for typical sizes).
const SEGMENT_THRESHOLD_SEC = 8 * 60;
const SEGMENT_TARGET_SEC = 5 * 60;

function splitIntoSegments(durationSec) {
  if (durationSec <= SEGMENT_THRESHOLD_SEC) {
    return [{index: 0, start: 0, end: durationSec}];
  }
  const numSegs = Math.max(2, Math.ceil(durationSec / SEGMENT_TARGET_SEC));
  const segLen = durationSec / numSegs;
  const segments = [];
  for (let i = 0; i < numSegs; i++) {
    segments.push({
      index: i,
      start: i * segLen,
      end: i === numSegs - 1 ? durationSec : (i + 1) * segLen,
    });
  }
  return segments;
}

// Clip a captions array to a [segStart, segEnd] window and remap
// timestamps so the segment starts at 0. A caption straddling the
// boundary stays with the segment that contains its start.
function sliceCaptionsForSegment(captions, segStart, segEnd) {
  const out = [];
  for (const c of captions) {
    if (c.start >= segEnd || c.end <= segStart) continue;
    if (c.start < segStart) continue; // caption belongs to prior segment
    out.push({
      ...c,
      start: c.start - segStart,
      end: Math.min(c.end, segEnd) - segStart,
      words: (c.words || []).map((w) => ({
        ...w,
        start: w.start - segStart,
        end: Math.min(w.end, segEnd) - segStart,
      })),
    });
  }
  return out;
}

// Stretch each visible beat's `end` forward to the start of the next
// visible beat so footage is on screen continuously — no black flashes
// during inter-sentence pauses. Last visible beat extends to the
// composition's total duration. Beats without a `url` (skipped sources)
// are kept in the array (preserve indexing for the swap loop) but are
// skipped both as the "current" beat being stretched and as the "next"
// neighbour driving the stretch — they're invisible to MainComp anyway.
// Original `end` is preserved on `originalEnd` for diagnostics.
function stretchFootageToFillGaps(footage, totalDuration) {
  const result = footage.slice();
  for (let i = 0; i < result.length; i++) {
    const cur = result[i];
    if (!cur || !cur.url || cur.start == null) continue;
    let nextStart = totalDuration;
    for (let j = i + 1; j < result.length; j++) {
      const nxt = result[j];
      if (nxt && nxt.url && nxt.start != null) {
        nextStart = nxt.start;
        break;
      }
    }
    if ((cur.end || 0) < nextStart) {
      result[i] = {...cur, originalEnd: cur.end, end: nextStart};
    }
  }
  return result;
}

// Same shape for footage. Beats are kept by start-time ownership.
function sliceFootageForSegment(footage, segStart, segEnd) {
  const out = [];
  for (const b of footage) {
    if (!b || b.start == null) continue;
    if (b.start >= segEnd || b.end <= segStart) continue;
    if (b.start < segStart) continue;
    out.push({
      ...b,
      start: b.start - segStart,
      end: Math.min(b.end, segEnd) - segStart,
    });
  }
  return out;
}

// Clip an audio file to a time window, upload to Firebase Storage,
// return the public URL.
//
// Two combined fixes vs the original `-c copy` approach:
//
//   1. `-stream_loop -1` so the source loops infinitely. Background
//      music tracks are typically 1-3 min; voiceovers are often longer.
//      Without looping, segments whose `segStart` falls past the music
//      duration get empty output files — Lambda can't ffprobe an empty
//      mp3 and fails the render. Loop = source plays continuously, we
//      always have audio at any seek point.
//
//   2. Re-encode to 128kbps CBR mp3 (libmp3lame). VBR sources cut with
//      `-c copy` produce frame-misaligned output that Lambda's stricter
//      ffprobe rejects. Re-encoding produces clean frames every time.
//
// Plus a post-clip ffprobe validation: if the output we just made
// can't be probed by our own ffprobe, throw immediately rather than
// uploading a broken file and discovering the problem 5+ min later.
async function clipAudioToFirebase(localInputPath, segStart, segDur, storagePath, contentType = 'audio/mpeg') {
  const fs = require('node:fs');
  const path = require('node:path');
  const os = require('node:os');
  const tmpOut = path.join(os.tmpdir(), `seg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.mp3`);
  // -ss AFTER -i (output-side seek) is required when -stream_loop is
  // active — input-side seek doesn't compose cleanly with the loop.
  await runFfmpeg([
    '-y',
    '-stream_loop', '-1',
    '-i', localInputPath,
    '-ss', String(segStart),
    '-t', String(segDur),
    '-c:a', 'libmp3lame',
    '-b:a', '128k',
    '-ac', '2',
    tmpOut,
  ], {label: `ffmpeg-clip-${storagePath.split('/').pop()}`, timeoutMs: 180000});

  // Validate the output before uploading. Catches:
  //  - 0-byte / truncated files (ffmpeg succeeded but produced nothing)
  //  - Files our ffprobe can't decode (Lambda's would be even stricter)
  try {
    const stat = fs.statSync(tmpOut);
    if (stat.size < 1024) {
      throw new Error(`clip output is ${stat.size} bytes — too small to be valid mp3`);
    }
    await new Promise((resolve, reject) => {
      const ffprobePath = getFfprobePath();
      if (!ffprobePath) return resolve(); // can't probe → trust ffmpeg
      const {spawn} = require('node:child_process');
      const proc = spawn(ffprobePath, [
        '-v', 'error',
        '-select_streams', 'a:0',
        '-show_entries', 'stream=codec_name,duration',
        '-of', 'json',
        tmpOut,
      ], {stdio: ['ignore', 'pipe', 'pipe']});
      let out = '';
      let err = '';
      proc.stdout.on('data', (c) => { out += c.toString(); });
      proc.stderr.on('data', (c) => { err += c.toString(); });
      proc.once('close', (code) => {
        if (code !== 0) return reject(new Error(`clip output failed local ffprobe (exit ${code}): ${err.slice(0, 200)}`));
        try {
          const m = JSON.parse(out);
          const dur = parseFloat((m.streams || [])[0]?.duration || 0);
          if (!dur || dur < segDur * 0.9) {
            return reject(new Error(`clip output duration ${dur}s is less than expected ${segDur}s (>10% short)`));
          }
          resolve();
        } catch (e) { reject(new Error(`ffprobe parse: ${e.message}`)); }
      });
      proc.once('error', (e) => reject(new Error(`ffprobe spawn: ${e.message}`)));
    });
  } catch (validationErr) {
    try { fs.unlinkSync(tmpOut); } catch { /* ignore */ }
    throw new Error(`clipAudioToFirebase validation failed for ${storagePath}: ${validationErr.message}`);
  }

  try {
    return await uploadLocalFileToFirebase(tmpOut, storagePath, contentType);
  } finally {
    try { fs.unlinkSync(tmpOut); } catch { /* ignore */ }
  }
}

// ─── Dead-air trim ──────────────────────────────────────────────────
// AssemblyAI gives us word-level timestamps. Long inter-word gaps are
// dead air on the voiceover that hurts retention. We detect any gap
// past `gapThreshold` seconds, splice the voiceover with ffmpeg to
// keep only the spoken regions, and remap the caption timestamps so
// everything downstream (beat extraction, render) operates on the
// trimmed timeline.
function buildKeepRegions(captions, {gapThreshold = 0.6, padding = 0.1} = {}) {
  const words = [];
  for (const c of captions) for (const w of (c.words || [])) words.push(w);
  if (words.length < 2) return [];
  const regions = [];
  let curStart = Math.max(0, words[0].start - padding);
  let curEnd = words[0].end + padding;
  for (let i = 1; i < words.length; i++) {
    const w = words[i];
    const gap = w.start - words[i - 1].end;
    if (gap > gapThreshold) {
      regions.push({start: curStart, end: curEnd});
      curStart = Math.max(0, w.start - padding);
    }
    curEnd = w.end + padding;
  }
  regions.push({start: curStart, end: curEnd});
  return regions;
}

function remapCaptionsByKeepRegions(captions, regions) {
  // For any time t on the original timeline, return the corresponding
  // time on the trimmed timeline. Times that fall inside a removed gap
  // snap to the next region's start.
  function remap(t) {
    let acc = 0;
    for (const r of regions) {
      if (t >= r.end) acc += r.end - r.start;
      else if (t >= r.start) return acc + (t - r.start);
      else return acc;
    }
    return acc;
  }
  return captions.map((c) => ({
    ...c,
    start: remap(c.start),
    end: remap(c.end),
    words: (c.words || []).map((w) => ({
      ...w,
      start: remap(w.start),
      end: remap(w.end),
    })),
  }));
}

async function trimVoiceoverAudio(inPath, regions, outPath) {
  // ffmpeg filter_complex: atrim each region, asetpts to reset PTS,
  // then concat them all into one continuous stream. Single ffmpeg
  // call regardless of region count.
  const trims = regions.map((r, i) =>
    `[0:a]atrim=start=${r.start.toFixed(3)}:end=${r.end.toFixed(3)},asetpts=PTS-STARTPTS[a${i}]`
  ).join(';');
  const concatLabels = regions.map((_, i) => `[a${i}]`).join('');
  const filter = `${trims};${concatLabels}concat=n=${regions.length}:v=0:a=1[out]`;
  await runFfmpeg([
    '-y', '-i', inPath,
    '-filter_complex', filter,
    '-map', '[out]',
    outPath,
  ], {label: 'trim-deadair', timeoutMs: 180000});
}

// Driver: download the voiceover, find dead-air gaps, ffmpeg-splice
// out everything but spoken regions, upload trimmed audio, remap
// captions. Returns the trimmed URL + captions or the originals if
// trimming was skipped (too little to gain or zero gaps).
async function trimDeadAirAndReupload(voiceoverUrl, captions, jobRef) {
  const regions = buildKeepRegions(captions);
  if (regions.length < 2) {
    return {voiceoverUrl, captions, removedSec: 0};
  }
  const totalKept = regions.reduce((s, r) => s + (r.end - r.start), 0);
  const firstStart = regions[0].start;
  const lastEnd = regions[regions.length - 1].end;
  const removedSec = (lastEnd - firstStart) - totalKept;
  if (removedSec < 1.0) {
    console.log(`dead-air trim: only ${removedSec.toFixed(1)}s detected — skipping`);
    return {voiceoverUrl, captions, removedSec: 0};
  }

  await jobRef.update({
    currentStep: `Trimming ${removedSec.toFixed(1)}s of dead air from voiceover`,
    progress: 25,
    updatedAt: FieldValue.serverTimestamp(),
  });

  const fs = require('node:fs');
  const path = require('node:path');
  const os = require('node:os');
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'trim-'));
  try {
    const inPath = path.join(tmpDir, 'in.mp3');
    const outPath = path.join(tmpDir, 'out.mp3');
    await downloadUrlToFile(voiceoverUrl, inPath);
    await trimVoiceoverAudio(inPath, regions, outPath);
    const trimmedPath = `cache/voiceovers/trimmed-${jobRef.id}-${Date.now()}.mp3`;
    const trimmedUrl = await uploadLocalFileToFirebase(outPath, trimmedPath, 'audio/mpeg');
    const trimmedCaptions = remapCaptionsByKeepRegions(captions, regions);
    console.log(`dead-air trim: removed ${removedSec.toFixed(1)}s across ${regions.length - 1} gaps`);
    return {voiceoverUrl: trimmedUrl, captions: trimmedCaptions, removedSec};
  } finally {
    try { fs.rmSync(tmpDir, {recursive: true, force: true}); } catch { /* ignore */ }
  }
}

// Run YouTube-targeting loudness normalisation on an mp4. Single-pass
// loudnorm (faster than 2-pass, slightly less accurate but well within
// YouTube's tolerance band). Audio is re-encoded to AAC; video stream
// copied. Returns path to a new file alongside the input.
//
// Targets: I=-14 LUFS (YouTube standard), TP=-1.5 dBTP (true-peak
// ceiling), LRA=11 LU (loudness range — moderate dynamic range).
async function applyLoudnorm(inputPath, jobRef = null) {
  const fs = require('node:fs');
  const path = require('node:path');
  const stat = fs.statSync(inputPath);
  const sizeMb = (stat.size / 1024 / 1024).toFixed(1);
  if (jobRef) {
    await jobRef.update({
      currentStep: `Normalising audio loudness — ${sizeMb}MB`,
      updatedAt: FieldValue.serverTimestamp(),
    }).catch(() => {});
  }
  const ext = path.extname(inputPath) || '.mp4';
  const outPath = inputPath.replace(new RegExp(`${ext.replace('.', '\\.')}$`), '') + `.norm${ext}`;
  await runFfmpeg([
    '-y',
    '-i', inputPath,
    '-c:v', 'copy',
    '-c:a', 'aac',
    '-b:a', '192k',
    '-af', 'loudnorm=I=-14:TP=-1.5:LRA=11',
    outPath,
  ], {label: 'loudnorm', timeoutMs: 5 * 60 * 1000});
  return outPath;
}

// Download every segment's S3 mp4 to /tmp, write a concat-demuxer
// list file, run ffmpeg with -c copy, return path of the joined mp4.
async function concatSegmentMp4s(s3Urls, jobRef) {
  const fs = require('node:fs');
  const path = require('node:path');
  const os = require('node:os');
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'concat-'));
  try {
    await jobRef.update({
      currentStep: `Concatenating ${s3Urls.length} segments`,
      progress: 92,
      updatedAt: FieldValue.serverTimestamp(),
    });
    const localFiles = [];
    for (let i = 0; i < s3Urls.length; i++) {
      const local = path.join(tmpDir, `seg-${i}.mp4`);
      await downloadUrlToFile(s3Urls[i], local);
      localFiles.push(local);
    }
    const listPath = path.join(tmpDir, 'list.txt');
    fs.writeFileSync(listPath, localFiles.map((f) => `file '${f.replace(/'/g, "'\\''")}'`).join('\n'));
    const outPath = path.join(tmpDir, 'final.mp4');
    await runFfmpeg([
      '-y',
      '-f', 'concat',
      '-safe', '0',
      '-i', listPath,
      '-c', 'copy',
      outPath,
    ], {label: 'ffmpeg-concat', timeoutMs: 180000});
    return {outPath, tmpDir};
  } catch (e) {
    try { fs.rmSync(tmpDir, {recursive: true, force: true}); } catch { /* ignore */ }
    throw e;
  }
}

// ─── Replicate Flux ─────────────────────────────────────────────────
// Free-tier Replicate enforces "6 req/min, burst 1". When several
// preflight swaps fire in parallel they hit 429 immediately. Retry
// with a backoff that respects the server's "resets in ~Xs" hint.
async function generateFluxImage(prompt) {
  if (!process.env.REPLICATE_API_KEY) throw new Error('REPLICATE_API_KEY not bound');

  const headers = {
    Authorization: `Bearer ${process.env.REPLICATE_API_KEY}`,
    'Content-Type': 'application/json',
    Prefer: 'wait',
  };

  let submit;
  let attempt = 0;
  const maxAttempts = 6;
  while (true) {
    attempt++;
    submit = await fetch(
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
    if (submit.ok) break;
    const body = await submit.text();
    if (submit.status === 429 && attempt < maxAttempts) {
      // Try to honour the "resets in ~Xs" hint, default 12s.
      const m = /resets in[^0-9]*([0-9]+)s/i.exec(body);
      const waitSec = m ? Math.min(60, parseInt(m[1], 10) + 2) : 12 * attempt;
      console.warn(`Replicate 429 (attempt ${attempt}/${maxAttempts}) — waiting ${waitSec}s`);
      await sleep(waitSec * 1000);
      continue;
    }
    throw new Error(`Replicate ${submit.status}: ${body.slice(0, 200)}`);
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

// ─── AI Video (Replicate) ───────────────────────────────────────────
// Two models exposed in the channel UI: a quality default (Kling) and a
// cost-conscious option (Hailuo). Wan was tested but quality wasn't
// competitive enough to keep in the lineup. Add models here only after
// running a real test prediction and confirming output is on-brand.
//
// Cost is computed as `costPerSecond × output.video_output_duration_seconds`
// (taken from Replicate's prediction.metrics response). Replicate doesn't
// expose per-prediction cost in the API, so we estimate from documented
// rates × actual output duration. Reconcile with the billing dashboard
// monthly to detect drift.
//
// Hard ceilings (NEVER bypassed regardless of channel config):
//   MAX_AI_CLIPS_PER_RENDER   ─ catches misconfigured channels with $9999 budgets
//   ABSOLUTE_DOLLAR_CEILING   ─ catches misconfigured prices in the table itself
const MAX_AI_CLIPS_PER_RENDER = 10;
const ABSOLUTE_DOLLAR_CEILING_PER_RENDER = 10;

const AI_VIDEO_MODELS = {
  'kwaivgi/kling-v2.5-turbo-pro': {
    label: 'Kling 2.5 Turbo Pro',
    tier: 'premium',
    costPerSecond: 0.07,            // $0.35 per 5s clip (verified test)
    expectedDurationSec: 5,
    estimatedGenTimeSec: 130,
    inputBuilder: (prompt) => ({prompt, duration: 5, aspect_ratio: '16:9'}),
  },
  'minimax/hailuo-02': {
    label: 'Hailuo 02',
    tier: 'budget',
    costPerSecond: 0.045,           // $0.27 per 6s clip (verified test)
    expectedDurationSec: 6,
    estimatedGenTimeSec: 80,
    inputBuilder: (prompt) => ({
      prompt, duration: 6, resolution: '768p', prompt_optimizer: true,
    }),
  },
};
const DEFAULT_AI_VIDEO_MODEL = 'kwaivgi/kling-v2.5-turbo-pro';

function estimateAiVideoClipCostUsd(modelSlug) {
  const m = AI_VIDEO_MODELS[modelSlug];
  if (!m) return 0;
  return m.costPerSecond * m.expectedDurationSec;
}

// Read a channel's AI Video config with safe defaults. Missing config
// = feature disabled (status quo for every existing channel).
async function getChannelAiVideoConfig(channelId) {
  const fallback = {
    enabled: false,
    model: DEFAULT_AI_VIDEO_MODEL,
    mode: 'fallback-only',
    perRenderBudgetUsd: 5,
    monthlyBudgetUsd: 50,
    monthlyUsdSpent: 0,
  };
  if (!channelId) return fallback;
  try {
    const doc = await db.collection('channels').doc(channelId).get();
    const cfg = doc.data()?.aiVideo || {};
    return {...fallback, ...cfg};
  } catch (e) {
    console.warn(`[ai-video] failed to read channel config: ${e.message}`);
    return fallback;
  }
}

// Atomically increment a channel's monthly AI-video spend. Resets at
// the first of each month — we compare the stored monthlyResetAt month
// against today; if older, reset to 0 before adding.
async function incrementChannelMonthlySpend(channelId, addUsd) {
  if (!channelId || !addUsd) return;
  try {
    const ref = db.collection('channels').doc(channelId);
    await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      const cfg = snap.data()?.aiVideo || {};
      const now = new Date();
      const thisMonthKey = `${now.getUTCFullYear()}-${(now.getUTCMonth() + 1).toString().padStart(2, '0')}`;
      const lastResetKey = cfg.monthlyResetMonth || null;
      const baseSpent = (lastResetKey === thisMonthKey) ? (cfg.monthlyUsdSpent || 0) : 0;
      tx.set(ref, {
        aiVideo: {
          ...cfg,
          monthlyUsdSpent: baseSpent + addUsd,
          monthlyResetMonth: thisMonthKey,
        },
      }, {merge: true});
    });
  } catch (e) {
    console.warn(`[ai-video] failed to update monthly spend: ${e.message}`);
  }
}

// Single-clip generator. Returns {url, costUsd, durationSec, predictTimeSec}
// or throws. Callers should treat throws as "skip this beat" — never
// allow an AI failure to crash the whole render.
async function generateAiVideoClip(modelSlug, prompt) {
  const model = AI_VIDEO_MODELS[modelSlug];
  if (!model) throw new Error(`Unknown AI video model: ${modelSlug}`);
  if (!process.env.REPLICATE_API_KEY) throw new Error('REPLICATE_API_KEY not bound');

  const headers = {
    Authorization: `Bearer ${process.env.REPLICATE_API_KEY}`,
    'Content-Type': 'application/json',
  };
  const body = JSON.stringify({input: model.inputBuilder(prompt)});

  // Submit with 429 backoff (same pattern as Flux fallback used to use).
  let submit;
  let attempt = 0;
  const maxAttempts = 6;
  while (true) {
    attempt++;
    submit = await fetch(`https://api.replicate.com/v1/models/${modelSlug}/predictions`, {
      method: 'POST',
      headers,
      body,
    });
    if (submit.ok) break;
    const text = await submit.text();
    if (submit.status === 429 && attempt < maxAttempts) {
      const m = /resets in[^0-9]*([0-9]+)s/i.exec(text);
      const waitSec = m ? Math.min(60, parseInt(m[1], 10) + 2) : 12 * attempt;
      console.warn(`[ai-video] Replicate 429 (attempt ${attempt}/${maxAttempts}) — waiting ${waitSec}s`);
      await sleep(waitSec * 1000);
      continue;
    }
    if (submit.status === 402) {
      throw new Error('Replicate insufficient credit — check billing dashboard');
    }
    throw new Error(`Replicate ${submit.status}: ${text.slice(0, 200)}`);
  }

  const id = (await submit.json()).id;
  // Poll. Each model takes 60-180s; 4s polling interval keeps overhead low.
  const t0 = Date.now();
  const POLL_TIMEOUT_MS = 5 * 60 * 1000;
  let data;
  while (true) {
    if (Date.now() - t0 > POLL_TIMEOUT_MS) {
      throw new Error(`Replicate prediction ${id} timed out after 5 min`);
    }
    await sleep(4000);
    const poll = await fetch(`https://api.replicate.com/v1/predictions/${id}`, {headers});
    data = await poll.json();
    if (data.status === 'succeeded' || data.status === 'failed' || data.status === 'canceled') break;
  }
  if (data.status !== 'succeeded') {
    throw new Error(`Replicate ${data.status}: ${(data.error || '').toString().slice(0, 200)}`);
  }
  const url = Array.isArray(data.output) ? data.output[0] : data.output;
  if (!url) throw new Error('Replicate succeeded but no output URL');
  const durationSec = data.metrics?.video_output_duration_seconds || model.expectedDurationSec;
  const costUsd = +(model.costPerSecond * durationSec).toFixed(4);
  return {
    url,
    costUsd,
    durationSec,
    predictTimeSec: data.metrics?.predict_time || null,
    modelSlug,
  };
}

// Drive AI generation across many beats with budget enforcement +
// parallel batches. Mutates the `footage` array in place: successful
// generations get cached and the beat's `url` + `source` are updated.
// Beats that exhaust the budget OR fail generation stay url=null
// (gap-fill will cover them at render time).
async function runAiVideoGenerationPass(footage, beatIndices, channelId, cfg, jobRef, {maxClipsOverride, label = 'ai-video'} = {}) {
  if (!beatIndices.length) return {clipsGenerated: 0, costUsd: 0, skippedDueBudget: 0};

  // Default cap is MAX_AI_CLIPS_PER_RENDER. The emergency AI-only
  // fallback (FIX 4 in renderWithSwapLoop) passes a higher override
  // so it can replace every beat in a render — budget enforcement
  // below still keeps total spend under ABSOLUTE_DOLLAR_CEILING_PER_RENDER.
  const effectiveClipCap = Math.max(1, maxClipsOverride || MAX_AI_CLIPS_PER_RENDER);
  const cap = Math.min(effectiveClipCap, beatIndices.length);
  const queue = beatIndices.slice(0, cap);
  const overflowFromCap = beatIndices.length - cap;
  if (overflowFromCap > 0) {
    console.warn(`[${label}] capped at ${effectiveClipCap} (override=${maxClipsOverride ?? 'none'}, default=${MAX_AI_CLIPS_PER_RENDER}) — ${overflowFromCap} additional beats will be skipped`);
  }

  const perClipEstimate = estimateAiVideoClipCostUsd(cfg.model);
  const renderBudget = Math.min(cfg.perRenderBudgetUsd || 0, ABSOLUTE_DOLLAR_CEILING_PER_RENDER);
  const monthlyRemaining = Math.max(0, (cfg.monthlyBudgetUsd || 0) - (cfg.monthlyUsdSpent || 0));
  const effectiveBudget = Math.min(renderBudget, monthlyRemaining);
  console.log(`[ai-video] queue=${queue.length} model=${cfg.model} per-clip≈$${perClipEstimate.toFixed(2)} render-budget=$${renderBudget} monthly-remaining=$${monthlyRemaining.toFixed(2)} effective=$${effectiveBudget.toFixed(2)}`);

  let spentUsd = 0;
  let clipsGenerated = 0;
  let skippedDueBudget = 0;
  const BATCH = 5;
  const t0 = Date.now();

  for (let i = 0; i < queue.length; i += BATCH) {
    const batch = queue.slice(i, i + BATCH);
    // Pre-flight per-batch budget check. If even one more clip would push
    // us past the budget, skip the rest of the queue. Predictable cap.
    if (spentUsd + perClipEstimate > effectiveBudget) {
      skippedDueBudget += queue.length - i;
      console.warn(`[ai-video] budget exhausted at $${spentUsd.toFixed(2)} of $${effectiveBudget.toFixed(2)} — skipping remaining ${skippedDueBudget} beats`);
      break;
    }

    await jobRef.update({
      currentStep: `Generating AI video clips — ${i + 1}-${i + batch.length}/${queue.length} (${cfg.model})`,
      updatedAt: FieldValue.serverTimestamp(),
    });

    const results = await Promise.all(batch.map(async (idx) => {
      const beat = footage[idx];
      const prompt = beat?.fluxPrompt || beat?.sentence || (beat?.keywords || []).join(', ');
      if (!prompt) return {idx, error: 'no prompt available'};
      try {
        const gen = await generateAiVideoClip(cfg.model, prompt);
        // Cache to Firebase Storage so Lambda fetches from a stable URL
        // and so the universal cache covers it on retries.
        let stableUrl = gen.url;
        try {
          const c = await cacheRemoteToFirebase(gen.url, {prefix: 'cache/ai-video', defaultExt: 'mp4'});
          stableUrl = c.url;
        } catch (cacheErr) {
          console.warn(`[ai-video] beat ${idx + 1}: cache failed (${cacheErr.message}) — using raw Replicate URL`);
        }
        return {idx, gen: {...gen, url: stableUrl}};
      } catch (e) {
        return {idx, error: e.message};
      }
    }));

    for (const r of results) {
      if (r.error) {
        console.warn(`[ai-video] beat ${r.idx + 1}: ${r.error}`);
        continue;
      }
      const beat = footage[r.idx];
      footage[r.idx] = {
        ...beat,
        url: r.gen.url,
        source: 'ai-video',
        originalUrl: r.gen.url,
        aiVideo: {
          model: r.gen.modelSlug,
          costUsd: r.gen.costUsd,
          durationSec: r.gen.durationSec,
          predictTimeSec: r.gen.predictTimeSec,
          promptUsed: (beat.fluxPrompt || '').slice(0, 200),
        },
      };
      spentUsd += r.gen.costUsd;
      clipsGenerated++;
    }
  }

  const elapsedMs = Date.now() - t0;
  if (clipsGenerated && channelId) {
    await incrementChannelMonthlySpend(channelId, spentUsd);
  }
  console.log(`[ai-video] done: ${clipsGenerated} clips, $${spentUsd.toFixed(2)} spent, ${skippedDueBudget} skipped (budget), in ${(elapsedMs / 1000).toFixed(1)}s`);

  return {clipsGenerated, costUsd: spentUsd, skippedDueBudget, elapsedMs};
}

// ─── Beat-aware sourcing ────────────────────────────────────────────
async function sourceBeatAwareFootage(captions, anthropicKey, jobRef, baseProgress, span, {channelId, aiVideoOverride, timingTags} = {}) {
  await jobRef.update({
    currentStep: 'Breaking script into beats with Claude',
    progress: baseProgress,
    updatedAt: FieldValue.serverTimestamp(),
  });
  const beatsT0 = Date.now();
  const rawBeats = await breakIntoBeats(captions, anthropicKey, {channelId, timingTags});
  // Enrich beats with Wikipedia portraits for named entities. Inserts
  // entityPortrait overlays where lookups succeeded. Silent on failure.
  await enrichBeatsWithEntityImages(rawBeats).catch((e) =>
    console.warn(`[entities] enrich threw: ${e.message}`),
  );
  // (countUp upgrade pass removed — bigStat now animates count-up
  // intrinsically when its text parses as currency ≥$1B or percent
  // ≥20%. No separate type, no post-process.)
  // Order matters:
  // 1. validate bigStat text — non-numeric → lowerThird BEFORE the
  //    lowerThird-redundancy / source-only passes consider it
  // 2. anchor every overlay (including converted lowerThirds and the
  //    server-injected entityPortraits) to its matching Gemini tag
  //    timestamp so visuals land when the words are spoken
  // 3. pair-or-drop quotes (uses entityPortrait names)
  // 4. drop redundant lowerThirds (same portrait names)
  // 5. drop non-source lowerThirds (Source:/Per/Via whitelist)
  // 6. clamp durations to beat lengths
  // 7. enforce 1.5s minimum spacing between overlays globally
  //    — runs LAST so spacing uses the post-anchor + post-clamp times
  validateBigStatContent(rawBeats);
  anchorOverlaysToTimingTags(rawBeats, timingTags);
  trimOverlayDurationsToNextAnchor(rawBeats, timingTags);
  pairQuotesWithEntityPortraits(rawBeats);
  dropRedundantLowerThirds(rawBeats);
  dropNonSourceLowerThirds(rawBeats);
  clampOverlayDurationsToBeat(rawBeats);
  enforceOverlaySpacing(rawBeats, 1.5);
  const beatsExtractMs = Date.now() - beatsT0;
  const claudeHeroCount = rawBeats.filter((b) => b && b.heroMoment).length;

  // Overlay-count diagnostics. If Claude is consistently returning 0
  // overlays, the prompt isn't firing them — surface counts in the job
  // doc + logs so we can tell from outside the function call.
  const overlayTotals = {total: 0, byType: {}};
  for (const b of rawBeats) {
    if (!Array.isArray(b?.overlays)) continue;
    for (const ov of b.overlays) {
      overlayTotals.total++;
      overlayTotals.byType[ov.type] = (overlayTotals.byType[ov.type] || 0) + 1;
    }
  }

  await jobRef.update({
    'timings.beatsExtractMs': beatsExtractMs,
    'timings.beatsCount': rawBeats.length,
    'timings.overlaysPlanned': overlayTotals.total,
    'timings.overlaysByType': overlayTotals.byType,
  }).catch(() => {});
  console.log(`got ${rawBeats.length} beats from Claude (${(beatsExtractMs / 1000).toFixed(1)}s) — ${claudeHeroCount} marked heroMoment, ${overlayTotals.total} overlays planned (${JSON.stringify(overlayTotals.byType)})`);

  // Heuristic hero-moment fallback. If the channel is in hero-only mode
  // and Claude returned <3 hero beats (we observed Claude marking zero
  // on long videos despite explicit prompt instruction), force-mark
  // first / longest-middle / last so AI generation has something to
  // target. This guarantees hero-only mode never silently produces zero
  // AI clips.
  const aiCfg = await getChannelAiVideoConfig(channelId);
  // Per-job override: frontend's pre-render cost modal sets this to
  // 'disabled' on the editingJob doc when the user clicks "Skip AI".
  // One-time only — channel config is untouched.
  //
  // Re-read from Firestore at decision time (not just trusting the
  // in-memory `aiVideoOverride` param). The HTTP worker reads the job
  // doc once at entry; if the override gets set after that read (e.g.
  // an admin patch mid-flight, or a frontend edit after submit), the
  // in-memory copy would be stale. Firestore re-read = source of truth.
  let effectiveOverride = aiVideoOverride;
  try {
    const jobSnap = await jobRef.get();
    const liveOverride = jobSnap.data()?.aiVideoOverride;
    if (liveOverride && liveOverride !== effectiveOverride) {
      console.log(`[ai-video] override re-read: param=${aiVideoOverride} → live=${liveOverride}`);
      effectiveOverride = liveOverride;
    }
  } catch (e) {
    console.warn(`[ai-video] failed to re-read override: ${e.message} — using in-memory ${aiVideoOverride}`);
  }
  if (effectiveOverride === 'disabled') {
    aiCfg.enabled = false;
    console.log('[ai-video] override=disabled — skipping AI for this run only');
  }
  const heroOnlyActive = aiCfg.enabled && aiCfg.mode === 'hero-only';
  if (heroOnlyActive && claudeHeroCount < 3) {
    console.warn(`[hero-fallback] mode=hero-only but Claude returned ${claudeHeroCount} hero beats — applying heuristic markers`);
    if (rawBeats.length === 1) {
      rawBeats[0].heroMoment = true;
    } else if (rawBeats.length === 2) {
      rawBeats[0].heroMoment = true;
      rawBeats[1].heroMoment = true;
    } else {
      // first + last + longest beat in the middle third
      rawBeats[0].heroMoment = true;
      rawBeats[rawBeats.length - 1].heroMoment = true;
      const third = Math.floor(rawBeats.length / 3);
      const start = Math.max(1, third);
      const end = Math.min(rawBeats.length - 1, rawBeats.length - third);
      let longestIdx = start;
      let longestDur = 0;
      for (let i = start; i < end; i++) {
        const dur = (rawBeats[i].end || 0) - (rawBeats[i].start || 0);
        if (dur > longestDur) { longestDur = dur; longestIdx = i; }
      }
      rawBeats[longestIdx].heroMoment = true;
    }
    const finalHeroCount = rawBeats.filter((b) => b && b.heroMoment).length;
    console.warn(`[hero-fallback] now ${finalHeroCount} hero beats: indices ${rawBeats.map((b, i) => b.heroMoment ? i : -1).filter((i) => i >= 0).join(', ')}`);
  }

  // No SFX heuristic fallback. We trust Claude's strict-trigger
  // placement per the rewritten STEP 3 prompt — fewer well-placed SFX
  // beats more "filler at mechanical positions" SFX. If Claude returns
  // zero, the render is silent on the SFX layer and that's acceptable.
  const claudeSfxCount = rawBeats.reduce((s, b) => s + ((b?.sfx?.length) || 0), 0);
  console.log(`[sfx] Claude placed ${claudeSfxCount} sfx across ${rawBeats.length} beats (no heuristic fallback)`);

  // Pull the channel's known-bad upstream URLs once so we can skip any
  // Pexels candidate that's been blocklisted on a prior render.
  const blocklist = await getChannelBlocklist(channelId);
  if (blocklist.size) console.log(`channel ${channelId}: ${blocklist.size} blocklisted upstream URLs`);

  const perBeatT0 = Date.now();
  const footage = [];
  // Per-video stock URL dedup. The previous pipeline picked the top
  // validated Pexels candidate per beat with no awareness of prior
  // beats — for a 122-beat finance video where many beats search
  // similar keywords ("wall street", "trader screens", "stock chart"),
  // Pexels often returns the same #1 clip multiple times. User
  // feedback: "never use the same picture". This set tracks every
  // URL we've already assigned this render; the candidate loop below
  // skips matches before validation, and a forced-repeat fallback
  // accepts a known URL only when every other option is exhausted.
  const usedStockUrls = new Set();
  let stockDedupSkips = 0;
  let stockDedupForcedRepeats = 0;
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
    let pexelsAlternates = [];
    const validationFailures = [];
    if (query) {
      // Multi-source chain: Pexels → Pixabay → Wikimedia → Internet
      // Archive (channel-toggleable). First source returning candidates
      // becomes the source for the beat. Validation pattern unchanged
      // — first candidate that passes HEAD+ftyp wins.
      const {urls: rawCandidates, source: pickedSource} = await searchAllFootageSources(channelId, query, 3);
      const candidates = rawCandidates.filter((c) => !blocklist.has(c));
      if (candidates.length < rawCandidates.length) {
        console.log(`beat ${i + 1}: skipped ${rawCandidates.length - candidates.length} blocklisted candidate(s)`);
      }
      let dedupSkipsThisBeat = 0;
      for (let ci = 0; ci < candidates.length; ci++) {
        const cand = candidates[ci];
        if (usedStockUrls.has(cand)) {
          dedupSkipsThisBeat++;
          stockDedupSkips++;
          continue;
        }
        const v = await validatePexelsVideo(cand);
        if (v.ok) {
          url = cand;
          source = pickedSource;
          usedStockUrls.add(cand);
          // Alternates are saved for the swap loop / preflight repair.
          // Field name kept as `pexelsAlternates` for backward compat —
          // they're alternates regardless of which source they came from.
          // Filter out URLs already used so later swaps don't reintroduce
          // a duplicate.
          pexelsAlternates = candidates.slice(ci + 1).filter((c) => !usedStockUrls.has(c));
          break;
        }
        validationFailures.push({url: cand, reason: v.reason});
        console.warn(`beat ${i + 1}: ${pickedSource} candidate failed validation (${v.reason})`);
      }
      // Forced-repeat fallback. If we couldn't pick a fresh URL because
      // every candidate was already used (not because they failed
      // validation), it's better to repeat a clip than leave the beat
      // empty. Validation still runs so the chosen repeat isn't broken.
      if (!url && dedupSkipsThisBeat > 0 && dedupSkipsThisBeat === candidates.length) {
        for (let ci = 0; ci < candidates.length; ci++) {
          const cand = candidates[ci];
          const v = await validatePexelsVideo(cand);
          if (v.ok) {
            url = cand;
            source = pickedSource;
            pexelsAlternates = candidates.slice(ci + 1);
            stockDedupForcedRepeats++;
            console.log(`beat ${i + 1}: dedup forced repeat (all candidates already used this render)`);
            break;
          }
          validationFailures.push({url: cand, reason: v.reason});
        }
      }
    }
    if (!url) {
      console.log(`beat ${i + 1}: no validated stock match across enabled sources — skipping (gap-fill or AI video may cover)`);
    }

    footage.push({
      beatIndex: i,
      start: beat.start,
      end: beat.end,
      sentence: beat.sentence,
      keywords,
      fluxPrompt: beat.fluxPrompt || null,
      heroMoment: !!beat.heroMoment,
      source,
      url,
      pexelsAlternates,
      // Pass-through fields for sound design — resolved into URLs later
      // after the loop so we can batch the Firestore reads.
      ...(beat.sfx && Array.isArray(beat.sfx) && beat.sfx.length ? {sfxRequested: beat.sfx} : {}),
      ...(beat.overlays && Array.isArray(beat.overlays) && beat.overlays.length ? {overlays: beat.overlays} : {}),
      ...(i === 0 && beat.musicMood ? {musicMoodFromClaude: beat.musicMood} : {}),
      ...(validationFailures.length ? {pexelsValidationFailures: validationFailures} : {}),
    });
    console.log(`beat ${i + 1}: ${source || 'NO MATCH'}${beat.heroMoment ? ' [HERO]' : ''}${beat.sfx?.length ? ` [+${beat.sfx.length} sfx]` : ''}`);
  }

  const perBeatSourcingMs = Date.now() - perBeatT0;
  // Tally where each beat's footage came from for transparency.
  const footageBySource = {pexels: 0, pixabay: 0, wikimedia: 0, archive: 0, 'ai-video': 0, none: 0};
  for (const b of footage) {
    if (!b) continue;
    const k = b.url ? (b.source || 'none') : 'none';
    footageBySource[k] = (footageBySource[k] || 0) + 1;
  }
  await jobRef.update({
    'timings.perBeatSourcingMs': perBeatSourcingMs,
    'timings.footageBySource': footageBySource,
  }).catch(() => {});
  console.log(`  per-beat sourcing total: ${(perBeatSourcingMs / 1000).toFixed(1)}s for ${footage.length} beats — by source: ${JSON.stringify(footageBySource)}`);

  // AI video generation pass. Runs between Pexels sourcing and caching
  // so the rest of the pipeline (preflight, render, gap-fill) treats
  // AI clips identically to Pexels clips. Channel config gates whether
  // this fires at all; budget enforcement is in-memory during the pass.
  const aiVideoT0 = Date.now();
  // aiCfg already loaded at top of function (with aiVideoOverride applied).
  let aiVideoStats = {clipsGenerated: 0, costUsd: 0, skippedDueBudget: 0};
  if (aiCfg.enabled && aiCfg.mode !== 'disabled') {
    let candidates = [];
    if (aiCfg.mode === 'fallback-only') {
      // Beats where every Pexels candidate failed validation.
      candidates = footage
        .map((b, idx) => (b && !b.url ? idx : -1))
        .filter((idx) => idx >= 0);
    } else if (aiCfg.mode === 'hero-only') {
      // Beats Claude flagged as hero moments — replace stock with AI
      // even if Pexels found something, since hero beats benefit from
      // bespoke generation.
      candidates = footage
        .map((b, idx) => (b && b.heroMoment ? idx : -1))
        .filter((idx) => idx >= 0);
    }
    // Abstract-concept beats (Claude's shouldAnimate=true) ALWAYS get
    // AI generation regardless of mode — they're the ones stock can't
    // illustrate well (recession, AI takeover, market psychology).
    // Cap is per-video for cost predictability; channel-level monthly
    // budget cap (discoveryConfig.monthlyBudgetUsd) gates the absolute
    // ceiling via the existing AI-video budget enforcement in
    // runAiVideoGenerationPass. Bumped 8→15 — for finance videos the
    // user wants Kling on every abstract economic concept, not just
    // hero beats. Cost: ~15 × $0.25 = ~$3.75/video.
    const SHOULD_ANIMATE_MAX = 15;
    // Smart prioritization when Claude flags more than the cap allows:
    // hero beats first (highest user impact), then beats whose
    // fluxPrompt mentions abstract concept keywords, then chronological.
    const ABSTRACT_RE = /\b(recession|inflation|crash|panic|euphoria|dystopia|takeover|collapse|policy|supply chain|crisis|boom|bust)\b/i;
    const animateCandidates = footage
      .map((b, idx) => (b && b.shouldAnimate === true ? {idx, b} : null))
      .filter(Boolean)
      .sort((a, b) => {
        const aScore = (a.b.heroMoment ? 2 : 0) + (ABSTRACT_RE.test(a.b.fluxPrompt || '') ? 1 : 0);
        const bScore = (b.b.heroMoment ? 2 : 0) + (ABSTRACT_RE.test(b.b.fluxPrompt || '') ? 1 : 0);
        if (bScore !== aScore) return bScore - aScore;
        return a.idx - b.idx; // stable chronological
      });
    const animateBeats = animateCandidates.slice(0, SHOULD_ANIMATE_MAX).map((x) => x.idx);
    if (animateBeats.length) {
      // Merge with mode-driven candidates, dedupe by index.
      const seen = new Set(candidates);
      for (const i of animateBeats) {
        if (!seen.has(i)) { seen.add(i); candidates.push(i); }
      }
      console.log(`[ai-video] shouldAnimate=true on ${animateBeats.length} beat(s) — added to AI generation queue`);
    }
    if (candidates.length) {
      console.log(`[ai-video] mode=${aiCfg.mode} flagged ${candidates.length} beat(s) for generation`);
      aiVideoStats = await runAiVideoGenerationPass(footage, candidates, channelId, aiCfg, jobRef);
    } else {
      console.log(`[ai-video] mode=${aiCfg.mode} — no candidate beats, skipping`);
    }
  } else {
    console.log(`[ai-video] disabled for channel (enabled=${aiCfg.enabled}, mode=${aiCfg.mode})`);
  }
  await jobRef.update({
    'timings.aiVideoMs': Date.now() - aiVideoT0,
    'timings.aiVideoBeatsCount': aiVideoStats.clipsGenerated,
    'timings.aiVideoCostUsd': aiVideoStats.costUsd,
    'timings.aiVideoSkippedDueBudget': aiVideoStats.skippedDueBudget,
    'timings.aiVideoModel': aiCfg.enabled ? aiCfg.model : null,
  }).catch(() => {});

  // Recompute footageBySource AFTER the AI-video pass. The earlier snapshot
  // (line ~2976) captured pre-AI counts so ai-video always showed 0 even
  // when Kling clips replaced Pexels beats — diagnostic confusion.
  const footageBySourceFinal = {pexels: 0, pixabay: 0, wikimedia: 0, archive: 0, 'ai-video': 0, none: 0};
  for (const b of footage) {
    if (!b) continue;
    const k = b.url ? (b.source || 'none') : 'none';
    footageBySourceFinal[k] = (footageBySourceFinal[k] || 0) + 1;
  }
  await jobRef.update({
    'timings.footageBySource': footageBySourceFinal,
  }).catch(() => {});
  console.log(`  footage final breakdown (post-AI): ${JSON.stringify(footageBySourceFinal)}`);

  // Cache every external asset to Firebase Storage. Lambda fetches from
  // here instead of Pexels/Replicate — fast, reliable, no rate limits.
  //
  // Batched: 5 concurrent uploads at a time to cap bandwidth + memory.
  // Promise.all over all 60+ beats at once OOM'd the function on long
  // scripts. Streaming (resp.body → createWriteStream) keeps each
  // upload's memory footprint flat regardless of file size.
  const CACHE_BATCH = 5;
  const cached = new Array(footage.length);
  let cacheHits = 0;
  let cacheMisses = 0;
  let cacheErrors = 0;
  const cachingT0 = Date.now();
  for (let i = 0; i < footage.length; i += CACHE_BATCH) {
    const end = Math.min(i + CACHE_BATCH, footage.length);
    await jobRef.update({
      currentStep: `Caching clips to Firebase Storage — ${end}/${footage.length}`,
      updatedAt: FieldValue.serverTimestamp(),
    });
    await Promise.all(footage.slice(i, end).map(async (beat, batchIdx) => {
      const idx = i + batchIdx;
      if (!beat || !beat.url) {
        cached[idx] = beat;
        return;
      }
      // AI-video beats are already cached during generation. Skip the
      // re-cache to avoid downloading our own bucket file back to /tmp
      // and re-uploading it. cacheHits stays unchanged for these.
      if (beat.source === 'ai-video' && extractStoragePathFromUrl(beat.url)) {
        cached[idx] = beat;
        return;
      }
      try {
        const result = await cacheRemoteToFirebase(beat.url, {
          prefix: beat.source === 'flux' ? 'cache/flux' : 'cache/footage',
          defaultExt: beat.source === 'flux' ? 'jpg' : 'mp4',
        });
        if (result.hit) cacheHits++; else cacheMisses++;
        cached[idx] = {...beat, originalUrl: beat.url, url: result.url};
      } catch (e) {
        cacheErrors++;
        console.warn(`beat ${idx + 1}: caching failed (${e.message}) — Lambda will hit ${beat.source} directly`);
        cached[idx] = {...beat, cachingError: e.message};
      }
    }));
  }
  const cachingMs = Date.now() - cachingT0;
  await jobRef.update({
    'timings.cachingMs': cachingMs,
    'timings.cacheHits': cacheHits,
    'timings.cacheMisses': cacheMisses,
    'timings.cacheErrors': cacheErrors,
  }).catch(() => {});
  console.log(`cached ${cacheHits + cacheMisses}/${footage.length} beats: ${cacheHits} hits + ${cacheMisses} downloads + ${cacheErrors} errors (took ${(cachingMs / 1000).toFixed(1)}s)`);
  return cached;
}

// ─── Remotion Lambda ────────────────────────────────────────────────
function isAwsRateLimitError(message) {
  return /rate.{0,5}exceeded|concurrency.{0,5}limit|throttl/i.test(message || '');
}

// Pull every plausible source URL out of a Lambda error message. Covers
// every Remotion failure shape we've seen so far where a single beat's
// asset is the culprit:
//   "original source = https://…"               (compositor frame errors)
//   "Fetching … &src=URL_ENCODED …"             (delayRender timeouts)
//   bare "https://…/file.mp4" anywhere in text  (catch-all)
// Returns a deduped array. Empty array means "no actionable URL" — the
// caller treats those as non-recoverable.
function extractFailedUrlsFromError(message) {
  if (!message) return [];
  const urls = new Set();

  const reOriginal = /original source\s*=\s*(https?:\/\/[^)\s'"]+)/gi;
  let m;
  while ((m = reOriginal.exec(message)) !== null) urls.add(m[1]);

  const reSrcEnc = /[?&]src=(https?(?:%3A%2F%2F|:\/\/)[^&'"\s)]+)/gi;
  while ((m = reSrcEnc.exec(message)) !== null) {
    let url = m[1];
    try { if (url.includes('%')) url = decodeURIComponent(url); } catch {}
    urls.add(url);
  }

  const reAnyMedia = /https?:\/\/[^\s'"<>)]+\.(?:mp4|jpg|jpeg|png|webp|mov|avi|mkv)/gi;
  while ((m = reAnyMedia.exec(message)) !== null) urls.add(m[0]);

  return Array.from(urls);
}

async function renderOnLambda(inputProps, jobRef, {progressLabel = 'Rendering on Lambda'} = {}) {
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
    // 400 frames/Lambda. Was 1000 — fine for sparse compositions but
    // the recent prompt fixes pushed per-video overlay count to 50+,
    // plus entity portraits and AI animations, making each frame ~2-3×
    // heavier. A 1000-frame chunk now risks the 900s AWS Lambda max
    // (the orchestrator timed out at 899999ms waiting for slow chunks).
    // Smaller chunks → each finishes well under budget → orchestrator
    // returns sooner. For a 17,910-frame (10-min) video this means
    // ~45 chunks vs 18. AWS default concurrent-Lambda quota is 1000 so
    // headroom is enormous; cost is unchanged (Lambda billed by total
    // duration, not invocation count).
    framesPerLambda: 400,
    concurrencyPerLambda: 2,
    maxRetries: 5,
    // Per-asset fetch budget inside each Lambda's Chromium. Default 28s
    // killed renders when Pexels CDN was slow even on validated files.
    // Most assets are now Firebase-Storage cached (fast, no throttle)
    // but the higher cap is cheap headroom for the rare slow case.
    delayRenderTimeoutInMilliseconds: 60000,
    // Bitrate caps. Remotion's default h264 settings push ~80MB/min
    // for 1080p30; a 12-min video came out at 966MB and saturated the
    // Cloud Function trying to upload it. 4 Mbps video / 128 kbps audio
    // is YouTube's recommended 1080p30 spec, drops file size to
    // ~30MB/min (~360MB for a 12-min video) with no visible quality
    // hit at our composition's complexity.
    videoBitrate: '4M',
    audioBitrate: '128k',
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
      if (isAwsRateLimitError(raw)) {
        throw new Error(
          `AWS render slot was busy and the per-chunk retries also exhausted. Your AWS account is at its concurrent-Lambda quota (default 10 for new accounts). Wait a few minutes and click Retry, or request a quota increase from AWS Service Quotas.`,
        );
      }
      // Try to surface every URL that might have caused the failure so
      // processJob's swap loop can map them back to beats and patch.
      const failedUrls = extractFailedUrlsFromError(raw);
      if (failedUrls.length) {
        const err = new Error(`Render failed on a problematic source clip: ${raw.slice(0, 300)}`);
        err.code = 'CORRUPT_SOURCE';
        err.failedUrls = failedUrls;
        err.rawErrorMessage = raw;
        throw err;
      }
      throw new Error(`Lambda render failed: ${raw}`);
    }
    const pct = Math.round((progress.overallProgress || 0) * 100);
    if (pct !== lastPct) {
      await jobRef.update({
        currentStep: `${progressLabel} — ${pct}%`,
        progress: 70 + Math.round((progress.overallProgress || 0) * 25),
        updatedAt: FieldValue.serverTimestamp(),
      });
      lastPct = pct;
    }
    if (progress.done) return progress.outputFile;
  }
}

// Walk every beat with `sfxRequested` and resolve each tag to a public
// URL via pickSfxTrack. Returns a flat array of {timestamp, tag, url,
// volume} entries with absolute timestamps on the main timeline. Sort
// ascending so MainComp can map straight into Sequences.
async function resolveSfxLayer(footage, channelId) {
  const layer = [];
  for (const beat of footage) {
    if (!beat || !Array.isArray(beat.sfxRequested) || !beat.sfxRequested.length) continue;
    for (const req of beat.sfxRequested) {
      if (!req || !req.tag) continue;
      const url = await pickSfxTrack(channelId, req.tag);
      if (!url) continue;
      const volume = SFX_VOLUME_PRESETS[req.tag] != null ? SFX_VOLUME_PRESETS[req.tag] : 0.15;
      layer.push({
        timestamp: (beat.start || 0) + (req.offset || 0),
        tag: req.tag,
        url,
        volume,
      });
    }
  }
  layer.sort((a, b) => a.timestamp - b.timestamp);
  return layer;
}

// Walk every beat with an `overlays` array and flatten into an
// absolute-timestamp layer that MainComp consumes. Each entry:
// {timestamp, duration, type, text, position}. Sort ascending so
// MainComp can position Sequences in order.
function buildOverlayLayer(footage) {
  const layer = [];
  for (const beat of footage) {
    if (!beat || !Array.isArray(beat.overlays) || !beat.overlays.length) continue;
    for (const ov of beat.overlays) {
      // Previously required ov.text — that filter silently dropped EVERY
      // overlay that wasn't bigStat/lowerThird/quote, including
      // entityPortrait (uses imageUrl/name), miniChart (chartLabel),
      // tickerSymbol (symbol), newsAlert (headline),
      // comparison (left/right), circleEmphasis, progressBar.
      // Result: Rem's videos had zero real-person portraits, zero charts,
      // zero ticker symbols despite the density validator generating
      // them. Only type is required now; per-type fields flow through.
      if (!ov || !ov.type) continue;
      layer.push({
        // Spread first so all per-type fields (imageUrl, chartLabel,
        // chartDirection, chartPoints, symbol, price, change, headline,
        // source, urgency, leftLabel, rightValue,
        // circlePosition, color, drawSpeed, label, value, format,
        // showCounter, subtitle, colorScheme, etc.) reach Remotion.
        ...ov,
        // Then override with computed/normalised values.
        type: ov.type,
        timestamp: (beat.start || 0) + (ov.startOffset || 0),
        duration: typeof ov.duration === 'number' ? Math.max(0.5, Math.min(8, ov.duration)) : 2.5,
        position: ov.position || 'topRight',
        // Cap text to a reasonable length when present; other fields are
        // already constrained by the BEATS_TOOL schema.
        ...(ov.text ? {text: String(ov.text).slice(0, 80)} : {}),
      });
    }
  }
  layer.sort((a, b) => a.timestamp - b.timestamp);
  return layer;
}

function sliceOverlaysForSegment(overlayLayer, segStart, segEnd) {
  if (!Array.isArray(overlayLayer)) return [];
  return overlayLayer
    .filter((o) => o && typeof o.timestamp === 'number' && o.timestamp >= segStart && o.timestamp < segEnd)
    .map((o) => ({...o, timestamp: o.timestamp - segStart}));
}

// Filter+remap an sfxLayer for one segment in split-render mode. Mirror
// of sliceCaptionsForSegment / sliceFootageForSegment — entries falling
// in [segStart, segEnd) get their timestamps shifted to start at 0.
function sliceSfxForSegment(sfxLayer, segStart, segEnd) {
  if (!Array.isArray(sfxLayer)) return [];
  return sfxLayer
    .filter((s) => s && typeof s.timestamp === 'number' && s.timestamp >= segStart && s.timestamp < segEnd)
    .map((s) => ({...s, timestamp: s.timestamp - segStart}));
}

// Wraps renderOnLambda with the mid-render CORRUPT_SOURCE swap loop.
// Used by both the single-render path AND each segment of the split
// path — a segment that hits a bad clip should be able to recover
// without nuking the whole job.
//
// On CORRUPT_SOURCE we walk a tiered replacement chain for the failed
// beat. Pre-FIX-2 behaviour: only same-source alternates → drop. That
// hammered the original source repeatedly when its catalog was the
// problem (e.g. a Pexels query returning all-corrupt clips for the
// channel's niche). New chain:
//   Tier 1: same-source alternates (stored on the beat, free)
//   Tier 2: untried stock sources (pexels → pixabay → wikimedia)
//   Tier 3: AI video generation (Replicate, costs $0.27–0.35/clip)
//   Tier 4: drop beat → gap-fill stretches the prior beat
// Each beat tracks `triedSources` so we never re-try the same source
// on subsequent CORRUPT_SOURCE attempts within the same render.
//
// AI fallback respects the channel's render + monthly budget. If
// adding one more AI clip would exceed either cap, AI is skipped
// for this beat (proceeds to drop).
async function renderWithSwapLoop(renderInput, jobRef, {jobId = '', channelId = null, label = 'render', maxAttempts = 5} = {}) {
  const FOOTAGE_SEARCH_FNS = {
    pexels: pexelsSearchTop,
    pixabay: pixabaySearchTop,
    wikimedia: wikimediaSearchTop,
  };
  const STOCK_RETRY_CHAIN = ['pexels', 'pixabay', 'wikimedia'];

  // Load once — these don't change across retries within a single render.
  const blocklist = await getChannelBlocklist(channelId);
  const aiCfg = await getChannelAiVideoConfig(channelId);
  let swapAiSpentUsd = 0;
  let aiOnlyAttempted = false;

  let attempt = 0;
  while (true) {
    attempt++;
    try {
      return await renderOnLambda(renderInput, jobRef, {progressLabel: label});
    } catch (e) {
      if (e.code !== 'CORRUPT_SOURCE') throw e;

      // FIX 4: AI-only emergency fallback. After maxAttempts of per-beat
      // tier-walking, if we're still hitting CORRUPT_SOURCE, regenerate
      // every non-AI beat as an AI clip and grant ONE bonus render
      // attempt. Capped at ABSOLUTE_DOLLAR_CEILING_PER_RENDER (~$10),
      // matching the user-promised "$5-10 but guaranteed completion"
      // trade. Only fires once per render — if the AI-only retry still
      // fails, the original CORRUPT_SOURCE error throws.
      if (attempt >= maxAttempts) {
        if (aiOnlyAttempted) throw e;
        aiOnlyAttempted = true;
        console.warn(`[${jobId}] ${label} hit ${maxAttempts} swap attempts — invoking AI-only emergency fallback`);
        await jobRef.update({
          currentStep: `🎬 Switching to AI-only mode due to source failures (cost: $5-10)`,
          aiOnlyFallbackInvoked: true,
          updatedAt: FieldValue.serverTimestamp(),
        });

        const aiQueueIndices = renderInput.footage
          .map((b, idx) => {
            if (!b || b.source === 'ai-video') return -1;
            const hasPrompt = (b.keywords && b.keywords.length) || b.sentence || b.fluxPrompt;
            return hasPrompt ? idx : -1;
          })
          .filter((idx) => idx >= 0);

        if (!aiQueueIndices.length) {
          console.warn(`[${jobId}] AI-only fallback: no eligible beats to regen — throwing`);
          throw e;
        }

        const emergencyAiCfg = {
          ...aiCfg,
          enabled: true,
          perRenderBudgetUsd: ABSOLUTE_DOLLAR_CEILING_PER_RENDER,
        };
        const stats = await runAiVideoGenerationPass(
          renderInput.footage,
          aiQueueIndices,
          channelId,
          emergencyAiCfg,
          jobRef,
          {maxClipsOverride: renderInput.footage.length, label: 'ai-only-fallback'},
        );
        await jobRef.update({
          'timings.aiOnlyFallbackClipsGenerated': stats.clipsGenerated,
          'timings.aiOnlyFallbackCostUsd': stats.costUsd,
          'timings.aiOnlyFallbackSkippedDueBudget': stats.skippedDueBudget,
        }).catch(() => {});
        console.log(`[${jobId}] AI-only fallback complete: ${stats.clipsGenerated} clips, $${stats.costUsd.toFixed(2)} spent, ${stats.skippedDueBudget} skipped (budget)`);

        if (stats.clipsGenerated === 0) {
          console.warn(`[${jobId}] AI-only fallback produced 0 clips — throwing`);
          throw e;
        }

        maxAttempts++; // grant one bonus attempt for the AI-only render
        continue;
      }

      const failedUrls = e.failedUrls || [];
      const beatIdx = renderInput.footage.findIndex((b) => {
        if (!b || !b.url) return false;
        return failedUrls.some((u) =>
          u === b.url || u === b.originalUrl ||
          (b.url && b.url.includes(u)) ||
          (b.originalUrl && b.originalUrl.includes(u)),
        );
      });
      if (beatIdx < 0) {
        console.warn(`[${jobId}] ${label} failed but no beat matches failed URLs: ${failedUrls.join(', ')}`);
        throw e;
      }
      const failedBeat = renderInput.footage[beatIdx];
      const previousUrl = failedBeat.url;
      const previousOriginal = failedBeat.originalUrl;
      const triedSources = new Set(
        failedBeat.triedSources || (failedBeat.source ? [failedBeat.source] : []),
      );
      console.warn(`[${jobId}] ${label} attempt ${attempt}: beat ${beatIdx + 1} failed (was ${failedBeat.source || 'unknown'}, tried so far: [${[...triedSources].join(',') || 'none'}]) — evicting + walking retry chain`);

      // Evict + blocklist before any retry so the next attempt can't
      // re-cache the same broken upstream file.
      if (extractStoragePathFromUrl(previousUrl)) {
        await deleteCachedFile(previousUrl);
      }
      if (channelId && previousOriginal && previousOriginal !== previousUrl) {
        await addToCacheBlocklist(channelId, previousOriginal);
        blocklist.add(previousOriginal);
      }

      let replacementUrl = null;
      let replacementSource = null;
      let replacementOriginal = null;
      let replacementAi = null;

      // Tier 1: same-source alternates. Free, no API calls — these were
      // pre-fetched at sourcing time. Skip the failed original and any
      // upstream URL that landed on the channel blocklist.
      const alternates = (failedBeat.pexelsAlternates || []).filter(
        (u) => u !== previousOriginal && !blocklist.has(u),
      );
      for (const altUrl of alternates) {
        try {
          const v = await validatePexelsVideo(altUrl);
          if (!v.ok) {
            console.log(`[${jobId}] beat ${beatIdx + 1} alt ${altUrl.slice(0, 60)} failed HEAD/ftyp: ${v.reason}`);
            continue;
          }
        } catch { continue; }
        let cachedUrl = altUrl;
        try {
          const c = await cacheRemoteToFirebase(altUrl, {prefix: 'cache/footage', defaultExt: 'mp4'});
          cachedUrl = c.url;
        } catch { /* fall through with raw URL */ }
        replacementUrl = cachedUrl;
        replacementSource = failedBeat.source || 'pexels';
        replacementOriginal = altUrl;
        console.log(`[${jobId}] beat ${beatIdx + 1} → Tier 1 same-source alternate (${replacementSource})`);
        break;
      }
      if (failedBeat.source) triedSources.add(failedBeat.source);

      // Tier 2: untried stock sources. Search each in order and take
      // the first validated candidate. The search hits an external API
      // (small cost), the validate hits HEAD+ftyp (no transfer cost).
      if (!replacementUrl) {
        const query = (failedBeat.keywords || []).join(' ');
        for (const srcName of STOCK_RETRY_CHAIN) {
          if (triedSources.has(srcName)) continue;
          const fn = FOOTAGE_SEARCH_FNS[srcName];
          if (!fn || !query) { triedSources.add(srcName); continue; }
          let candidates;
          try { candidates = await fn(query, 3); }
          catch (searchErr) {
            console.warn(`[${jobId}] beat ${beatIdx + 1} ${srcName} search threw: ${searchErr.message}`);
            triedSources.add(srcName);
            continue;
          }
          const filtered = (candidates || []).filter((c) => !blocklist.has(c));
          let picked = null;
          for (const cand of filtered) {
            try {
              const v = await validatePexelsVideo(cand);
              if (v.ok) { picked = cand; break; }
              console.log(`[${jobId}] beat ${beatIdx + 1} ${srcName} cand failed: ${v.reason}`);
            } catch { continue; }
          }
          triedSources.add(srcName);
          if (!picked) {
            console.log(`[${jobId}] beat ${beatIdx + 1} ${srcName}: ${filtered.length} candidates, none validated`);
            continue;
          }
          let cachedUrl = picked;
          try {
            const c = await cacheRemoteToFirebase(picked, {prefix: 'cache/footage', defaultExt: 'mp4'});
            cachedUrl = c.url;
          } catch { /* fall through with raw URL */ }
          replacementUrl = cachedUrl;
          replacementSource = srcName;
          replacementOriginal = picked;
          console.log(`[${jobId}] beat ${beatIdx + 1} → Tier 2 cross-source swap (${srcName})`);
          break;
        }
      }

      // Tier 3: AI video generation. Last-resort, ~$0.27-0.35/clip via
      // Replicate. Budget-gated: we never blow past per-render or
      // monthly caps regardless of how many beats fail.
      if (!replacementUrl && !triedSources.has('ai-video')) {
        triedSources.add('ai-video');
        const perClipEst = estimateAiVideoClipCostUsd(aiCfg.model);
        const renderBudget = Math.min(aiCfg.perRenderBudgetUsd || 0, ABSOLUTE_DOLLAR_CEILING_PER_RENDER);
        const monthlyRemaining = Math.max(0, (aiCfg.monthlyBudgetUsd || 0) - (aiCfg.monthlyUsdSpent || 0));
        const wouldHitRenderCap = (swapAiSpentUsd + perClipEst) > renderBudget;
        const wouldHitMonthlyCap = perClipEst > monthlyRemaining;
        if (wouldHitRenderCap || wouldHitMonthlyCap) {
          console.warn(`[${jobId}] beat ${beatIdx + 1} AI fallback skipped — budget exhausted (render=$${swapAiSpentUsd.toFixed(2)}+$${perClipEst.toFixed(2)} of $${renderBudget}, monthly remaining=$${monthlyRemaining.toFixed(2)})`);
        } else {
          const prompt = failedBeat.fluxPrompt || failedBeat.sentence || (failedBeat.keywords || []).join(', ');
          if (!prompt) {
            console.warn(`[${jobId}] beat ${beatIdx + 1} AI fallback skipped — no prompt available`);
          } else {
            try {
              const gen = await generateAiVideoClip(aiCfg.model, prompt);
              let cachedUrl = gen.url;
              try {
                const c = await cacheRemoteToFirebase(gen.url, {prefix: 'cache/ai-video', defaultExt: 'mp4'});
                cachedUrl = c.url;
              } catch { /* fall through with raw Replicate URL */ }
              replacementUrl = cachedUrl;
              replacementSource = 'ai-video';
              replacementOriginal = gen.url;
              replacementAi = {
                model: gen.modelSlug,
                costUsd: gen.costUsd,
                durationSec: gen.durationSec,
                predictTimeSec: gen.predictTimeSec,
                promptUsed: prompt.slice(0, 200),
              };
              swapAiSpentUsd += gen.costUsd;
              if (channelId) await incrementChannelMonthlySpend(channelId, gen.costUsd);
              console.log(`[${jobId}] beat ${beatIdx + 1} → Tier 3 AI video (${gen.modelSlug}, $${gen.costUsd.toFixed(2)})`);
            } catch (aiErr) {
              console.warn(`[${jobId}] beat ${beatIdx + 1} AI fallback failed: ${aiErr.message}`);
            }
          }
        }
      }

      // Tier 4: drop beat → gap-fill stretches the prior visible beat
      // across this slot. This is the worst-case visual: a chunk of
      // the script plays over reused footage.
      if (replacementUrl) {
        renderInput.footage[beatIdx] = {
          ...failedBeat,
          source: replacementSource,
          url: replacementUrl,
          originalUrl: replacementOriginal,
          previousUrl,
          replacedReason: `render-time failure → ${replacementSource}`,
          pexelsAlternates: (failedBeat.pexelsAlternates || []).filter((u) => u !== replacementOriginal),
          triedSources: [...triedSources],
          ...(replacementAi ? {aiVideo: replacementAi} : {}),
        };
      } else {
        renderInput.footage[beatIdx] = {
          ...failedBeat,
          url: null,
          source: null,
          previousUrl,
          triedSources: [...triedSources],
          replacedReason: `render-time failure → all tiers exhausted (tried: ${[...triedSources].join(',')})`,
        };
        const localTotalDuration = renderInput.captions.length
          ? renderInput.captions[renderInput.captions.length - 1].end + 0.5
          : 0;
        renderInput.footage = stretchFootageToFillGaps(renderInput.footage, localTotalDuration);
        replacementSource = 'skipped';
        console.warn(`[${jobId}] beat ${beatIdx + 1}: all tiers exhausted (${[...triedSources].join(',')}) — skipped, gap-fill applied`);
      }

      await jobRef.update({
        footage: renderInput.footage,
        pexelsReplacedCount: FieldValue.increment(1),
        renderAttempts: FieldValue.arrayUnion({
          attempt,
          label,
          failedBeatIndex: beatIdx,
          failedUrl: previousUrl,
          failedUrlsSeen: failedUrls,
          replacementSource,
          replacementUrl,
          triedSources: [...triedSources],
          ts: Date.now(),
        }),
        'timings.swapAiSpentUsd': swapAiSpentUsd,
        currentStep: `${label}: beat ${beatIdx + 1} clip failed — replaced with ${replacementSource}, attempt ${attempt + 1}/${maxAttempts}`,
        updatedAt: FieldValue.serverTimestamp(),
      });
    }
  }
}

async function uploadRenderToFirebaseStorage(s3Url, channelId, projectId, jobRef = null) {
  const {pipeline} = require('node:stream/promises');
  const {Readable, Transform} = require('node:stream');

  const resp = await fetch(s3Url);
  if (!resp.ok) throw new Error(`Could not fetch Lambda output: HTTP ${resp.status}`);
  const totalBytes = parseInt(resp.headers.get('content-length') || '0', 10);
  const totalMB = totalBytes ? (totalBytes / 1024 / 1024).toFixed(1) : '?';

  const bucket = admin.storage().bucket();
  const path = `channels/${channelId}/projects/${projectId}/editing/auto-edit-${Date.now()}.mp4`;
  const file = bucket.file(path);
  const writeStream = file.createWriteStream({
    metadata: {contentType: 'video/mp4'},
  });

  let bytesWritten = 0;
  let lastProgressUpdate = 0;
  const meter = new Transform({
    transform(chunk, _enc, cb) {
      bytesWritten += chunk.length;
      const now = Date.now();
      if (jobRef && now - lastProgressUpdate > 5000) {
        const mb = (bytesWritten / 1024 / 1024).toFixed(1);
        const pct = totalBytes ? Math.round((bytesWritten / totalBytes) * 100) : 0;
        jobRef.update({
          currentStep: `Uploading final video — ${mb} / ${totalMB} MB (${pct}%)`,
          updatedAt: FieldValue.serverTimestamp(),
        }).catch(() => {});
        lastProgressUpdate = now;
      }
      cb(null, chunk);
    },
  });

  const UPLOAD_TIMEOUT_MS = 10 * 60 * 1000;
  let timer;
  try {
    await Promise.race([
      pipeline(Readable.fromWeb(resp.body), meter, writeStream),
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`Upload hung after ${UPLOAD_TIMEOUT_MS / 1000}s for ${totalMB}MB → ${path}`)),
          UPLOAD_TIMEOUT_MS,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }

  await file.makePublic();
  console.log(`uploaded ${(bytesWritten / 1024 / 1024).toFixed(1)}MB → ${path}`);
  return {
    url: `https://storage.googleapis.com/${bucket.name}/${path}`,
    path,
    size: bytesWritten,
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

  // Heartbeat: long phases (Lambda render polling between % changes,
  // ffmpeg concat, big uploads) can go several minutes without touching
  // the job doc, which made the 10-min watchdog kill alive jobs. Refresh
  // updatedAt every 30s for the entire processJob lifetime; cleared in
  // finally so it can never outlive the function. Best-effort: failed
  // writes are swallowed.
  // ALSO bumps the PROJECT doc's autoPilot.updatedAt so the auto-pilot
  // stale-detection threshold (60 min for awaiting-render) doesn't trip
  // mid-render. Without this the frontend would prompt "stalled" on a
  // 40-min legitimate render.
  let renderHeartbeatSec = 0;
  const heartbeat = setInterval(() => {
    renderHeartbeatSec += 30;
    const elapsedMin = Math.round(renderHeartbeatSec / 60);
    jobRef.update({updatedAt: FieldValue.serverTimestamp()}).catch(() => {});
    projRef.update({
      'autoPilot.updatedAt': FieldValue.serverTimestamp(),
      'autoPilot.stageLabel': `Rendering video (Lambda) — ${elapsedMin} min elapsed`,
    }).catch(() => {});
  }, 30000);

  try {

  // Total wall clock measured for the whole job — saved to job.timings
  // continuously so the UI can show which step ran when, even if the
  // function process dies mid-run.
  const totalStart = Date.now();

  // Step 1 — captions
  await jobRef.update({
    status: 'captions',
    currentStep: 'Transcribing voiceover with AssemblyAI',
    progress: 10,
    updatedAt: FieldValue.serverTimestamp(),
  });
  const captionsT0 = Date.now();
  let captions = await generateCaptionsFromAssemblyAI(job.voiceoverUrl);
  const captionsMs = Date.now() - captionsT0;
  console.log(`[${job.id}] ⏱ captions: ${(captionsMs / 1000).toFixed(1)}s, ${captions.length} sentences`);
  await jobRef.update({
    captions,
    progress: 30,
    currentStep: `Transcribed ${captions.length} sentences`,
    'timings.captionsMs': captionsMs,
    'timings.sentencesCount': captions.length,
    updatedAt: FieldValue.serverTimestamp(),
  });

  // Step 1.5 — dead-air trim. Splice out any inter-word gap > 0.6s so
  // the rendered video paces tighter (better YouTube retention). All
  // captions get remapped to the trimmed timeline; downstream uses
  // `voiceoverUrl` (local) instead of `job.voiceoverUrl`.
  let voiceoverUrl = job.voiceoverUrl;
  const trimT0 = Date.now();
  try {
    const trimResult = await trimDeadAirAndReupload(voiceoverUrl, captions, jobRef);
    voiceoverUrl = trimResult.voiceoverUrl;
    captions = trimResult.captions;
    const trimMs = Date.now() - trimT0;
    if (trimResult.removedSec > 0) {
      console.log(`[${job.id}] ⏱ dead-air trim: removed ${trimResult.removedSec.toFixed(1)}s in ${(trimMs / 1000).toFixed(1)}s`);
      await jobRef.update({
        captions,
        voiceoverUrlTrimmed: voiceoverUrl,
        'timings.deadAirRemovedSec': trimResult.removedSec,
        'timings.deadAirTrimMs': trimMs,
        currentStep: `Trimmed ${trimResult.removedSec.toFixed(1)}s of dead air — voiceover now ${(captions[captions.length - 1].end).toFixed(1)}s`,
        updatedAt: FieldValue.serverTimestamp(),
      });
    }
  } catch (e) {
    console.warn(`[${job.id}] dead-air trim failed (${e.message}) — proceeding with original voiceover`);
  }

  // Length guard. processJob now runs inside an HTTP-triggered function
  // with a 3600s ceiling; the whole pipeline budget for a 12-min
  // voiceover is ~14 min. 30 min is a generous upper bound that leaves
  // headroom but still bails on truly oversized inputs.
  const voiceoverDurationSec = captions.length
    ? captions[captions.length - 1].end + 0.5
    : 0;
  const MAX_VOICEOVER_SEC = 30 * 60;
  if (voiceoverDurationSec > MAX_VOICEOVER_SEC) {
    const mins = (voiceoverDurationSec / 60).toFixed(1);
    throw new Error(
      `Voiceover is ${mins} minutes. Pipeline currently supports up to 30 minutes. Please split your voiceover into shorter segments.`,
    );
  }

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
  // Step 2a — Gemini voiceover timing analysis. Runs against the
  // already-trimmed voiceover (captions are remapped, so timestamps
  // align). Gracefully no-ops if GEMINI_API_KEY is unset; failure
  // returns [] and the pipeline proceeds without timing hints.
  const timingT0 = Date.now();
  await jobRef.update({
    currentStep: 'Analyzing voiceover timing with Gemini',
    updatedAt: FieldValue.serverTimestamp(),
  });
  const timingTags = await getGeminiTimingTags(voiceoverUrl, captions, process.env.GEMINI_API_KEY);
  const timingMs = Date.now() - timingT0;
  await jobRef.update({
    'timings.geminiTimingMs': timingMs,
    'timings.geminiTagCount': timingTags.length,
  }).catch(() => {});

  const sourcingT0 = Date.now();
  let footage = await sourceBeatAwareFootage(captions, anthropicKey, jobRef, 35, 25, {
    channelId: job.channelId,
    aiVideoOverride: job.aiVideoOverride,
    timingTags,
  });
  const sourcingMs = Date.now() - sourcingT0;
  console.log(`[${job.id}] ⏱ sourcing: ${(sourcingMs / 1000).toFixed(1)}s`);
  await jobRef.update({'timings.sourcingMs': sourcingMs}).catch(() => {});
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

  // Step 2.5 — pick background music (Claude-driven) + resolve SFX.
  // selectMusicForVideo sends the script + full track list to Claude
  // and gets back a single best-fit track + per-track volume rec. On
  // any failure we fall back to the legacy random pick — never block
  // the render on music.
  let musicUrl = null;
  let pickedMood = null;
  let sfxLayer = [];
  let musicVolume = 0.08;
  let musicSelection = null;
  const musicT0 = Date.now();
  const channelMusicSettings = await getChannelMusicSettings(job.channelId);
  try {
    const claudeMood = footage[0]?.musicMoodFromClaude;
    pickedMood = claudeMood || await classifyTone(
      captions.map((c) => c.text).join(' '),
      anthropicKey,
    );
    console.log(`mood: ${pickedMood} ${claudeMood ? '(from Claude)' : '(from classifyTone)'}`);
    try {
      const heroBeatTimestamps = footage
        .filter((f) => f && f.heroMoment && typeof f.start === 'number')
        .map((f) => f.start);
      const sel = await selectMusicForVideo({
        channelId: job.channelId,
        anthropicKey,
        voiceoverDurationSec,
        topic: proj.title || job.projectTitle || '',
        scriptText: captions.map((c) => c.text).join(' '),
        tone: pickedMood,
        beatCount: footage.length,
        heroBeatTimestamps,
      });
      musicUrl = sel.url;
      musicSelection = sel;
      // Constant volume — channel default (0.06) unless overridden.
      // No per-track / per-frame dynamic mixing.
      musicVolume = typeof channelMusicSettings.volume === 'number' ? channelMusicSettings.volume : 0.06;
      console.log(`music: "${sel.trackName}" id=${sel.trackId} mood=${sel.trackMood} dur=${Math.round(sel.trackDuration)}s vol=${musicVolume.toFixed(3)} — ${sel.reasoning}`);
      // Save the pick on the project doc so the user can trace which
      // track was used if a YouTube Content ID claim shows up later.
      // Stored separately from job state because the user's UI reads
      // from the project, not the job.
      try {
        await db.collection('channels').doc(job.channelId).collection('projects').doc(job.projectId).update({
          musicSelectionUsed: {
            trackId: sel.trackId,
            trackName: sel.trackName,
            trackMood: sel.trackMood,
            usedAt: FieldValue.serverTimestamp(),
          },
        });
      } catch (logErr) {
        console.warn('[music] could not persist selection on project:', logErr.message);
      }
    } catch (selErr) {
      console.warn(`Claude music selection failed (${selErr.message}) — falling back to mood-random`);
      musicUrl = await pickMusicTrack(job.channelId, pickedMood);
      musicVolume = typeof channelMusicSettings.volume === 'number' ? channelMusicSettings.volume : 0.06;
    }
  } catch (e) {
    console.warn(`Music pick failed entirely: ${e.message} — proceeding without`);
  }
  try {
    sfxLayer = await resolveSfxLayer(footage, job.channelId);
    if (sfxLayer.length) {
      console.log(`sfx: resolved ${sfxLayer.length} tracks from Claude requests`);
    }
  } catch (e) {
    console.warn(`SFX resolve failed: ${e.message} — proceeding without sfx`);
  }
  // Build overlay layer from beat.overlays (Claude-placed). No URL
  // resolution needed — overlays are pure text rendered by Remotion.
  const overlayLayer = buildOverlayLayer(footage);
  // Diagnostic breakdown so post-render inspection can tell "Claude
  // generated X overlays" from "Claude generated X but only Y rendered".
  // Pre-fix this would have shown ~0 rendered even when Claude produced
  // 15+ overlays — the text-filter in buildOverlayLayer silently dropped
  // entityPortrait / miniChart / tickerSymbol / newsAlert / etc.
  const overlayBreakdown = {};
  for (const ov of overlayLayer) {
    overlayBreakdown[ov.type] = (overlayBreakdown[ov.type] || 0) + 1;
  }
  if (overlayLayer.length) {
    console.log(`overlays: ${overlayLayer.length} placed by Claude — breakdown: ${JSON.stringify(overlayBreakdown)}`);
  }
  await jobRef.update({
    'timings.sfxResolvedCount': sfxLayer.length,
    'timings.overlayCount': overlayLayer.length,
    'timings.overlayBreakdown': overlayBreakdown,
    'timings.musicMs': Date.now() - musicT0,
    'timings.musicVolume': musicVolume,
    musicUrl,
    pickedMood,
    musicSelection: musicSelection || null,
    currentStep: musicUrl
      ? `Music: ${musicSelection?.trackName || pickedMood}`
      : (pickedMood ? `No ${pickedMood} track in library — silent music bed` : 'No music'),
    updatedAt: FieldValue.serverTimestamp(),
  });

  // Step 2.75 — pre-flight validate every cached video URL with ffprobe.
  // Anything that won't open / has no video stream / has zero duration
  // is swapped to a Flux fallback BEFORE Lambda starts, eliminating
  // the 5-min-per-retry tax that mid-render CORRUPT_SOURCE recovery
  // would otherwise cost. Mid-render swap loop below stays as a
  // backstop for anything that slips through (network flake during
  // render, edge-case codec).
  await preflightValidateFootage(footage, jobRef, {channelId: job.channelId});

  // Stretch each visible beat to cover the gap until the next beat.
  // Without this, inter-sentence silence frames render as black —
  // affects every video, ugly. Footage is on screen continuously now.
  footage = stretchFootageToFillGaps(footage, voiceoverDurationSec);
  const stretchedCount = footage.filter((b) => b && b.originalEnd != null).length;
  console.log(`gap-fill: stretched ${stretchedCount}/${footage.length} beats to cover inter-beat gaps`);
  await jobRef.update({footage, 'timings.gapFillStretched': stretchedCount}).catch(() => {});

  // Step 3 — Remotion Lambda render. For voiceovers ≤ 8 min the whole
  // composition is rendered in one Lambda call (with mid-render swap
  // recovery). Longer videos exceed Lambda's 900s main-function ceiling,
  // so we split into ~5-min segments, render in parallel, and concat
  // the resulting MP4s with -c copy.
  await jobRef.update({
    status: 'rendering',
    currentStep: 'Invoking Lambda renderer',
    progress: 70,
    updatedAt: FieldValue.serverTimestamp(),
  });

  const segments = splitIntoSegments(voiceoverDurationSec);
  const isSplit = segments.length > 1;
  console.log(`[${job.id}] render mode: ${isSplit ? `${segments.length} segments` : 'single'} (voiceover ${voiceoverDurationSec.toFixed(1)}s)`);

  const renderStart = Date.now();
  let finalFile;

  if (!isSplit) {
    // Single-render path with mid-render swap loop.
    const renderInput = {voiceoverUrl, footage, captions, musicUrl, musicVolume, sfxLayer, overlayLayer};
    const s3OutputUrl = await renderWithSwapLoop(renderInput, jobRef, {
      jobId: job.id,
      channelId: job.channelId,
      label: 'Rendering on Lambda',
    });
    const renderMs = Date.now() - renderStart;
    await jobRef.update({
      'timings.render': renderMs,
      'timings.renderMode': 'single',
    }).catch(() => {});
    console.log(`[${job.id}] ⏱ render: ${(renderMs / 1000).toFixed(1)}s (single)`);
    await jobRef.update({
      currentStep: 'Copying render to Firebase Storage',
      progress: 95,
      updatedAt: FieldValue.serverTimestamp(),
    });
    var uploadT0 = Date.now();
    // Single-mode loudnorm: download Lambda output to /tmp, normalise,
    // upload. Adds ~30-60s vs. the prior direct-stream upload but
    // matches YouTube's -14 LUFS target like the split path.
    const fs = require('node:fs');
    const path = require('node:path');
    const os = require('node:os');
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'single-norm-'));
    try {
      const rawPath = path.join(tmpDir, 'lambda-out.mp4');
      await downloadUrlToFile(s3OutputUrl, rawPath);
      const loudnormT0 = Date.now();
      let normalisedPath = rawPath;
      try {
        normalisedPath = await applyLoudnorm(rawPath, jobRef);
        console.log(`[${job.id}] ⏱ loudnorm: ${((Date.now() - loudnormT0) / 1000).toFixed(1)}s`);
      } catch (e) {
        console.warn(`[${job.id}] loudnorm failed (${e.message}) — uploading raw lambda output`);
      }
      await jobRef.update({'timings.loudnormMs': Date.now() - loudnormT0}).catch(() => {});
      const finalPath = `channels/${job.channelId}/projects/${job.projectId}/editing/auto-edit-${Date.now()}.mp4`;
      const finalUrl = await uploadLocalFileToFirebase(normalisedPath, finalPath, 'video/mp4', {
        jobRef,
        label: 'Uploading final video',
      });
      const sizeBytes = fs.statSync(normalisedPath).size;
      finalFile = {url: finalUrl, path: finalPath, size: sizeBytes};
    } finally {
      try { fs.rmSync(tmpDir, {recursive: true, force: true}); } catch { /* ignore */ }
    }
  } else {
    // Split-render path: clip audio per segment, render in parallel,
    // concat the outputs. No mid-render swap loop in split mode — if
    // a segment fails we abort; preflight should have caught most.
    const fs = require('node:fs');
    const path = require('node:path');
    const os = require('node:os');
    const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'split-'));
    try {
      await jobRef.update({
        currentStep: `Splitting voiceover into ${segments.length} segments`,
        progress: 71,
        updatedAt: FieldValue.serverTimestamp(),
      });

      // Download voiceover (and music if present) once, reused for
      // every segment clip.
      const voPath = path.join(tmpRoot, 'voiceover');
      await downloadUrlToFile(voiceoverUrl, voPath);
      let musicPath = null;
      if (musicUrl) {
        musicPath = path.join(tmpRoot, 'music');
        try { await downloadUrlToFile(musicUrl, musicPath); } catch (e) {
          console.warn(`[${job.id}] music download failed (${e.message}) — proceeding without music`);
          musicPath = null;
        }
      }

      // Build per-segment inputs (clipped audio + sliced captions/footage).
      const segPrepT0 = Date.now();
      const segInputs = await Promise.all(segments.map(async (seg) => {
        const segDur = seg.end - seg.start;
        const tag = `${job.id}/seg${seg.index}`;
        const segVoUrl = await clipAudioToFirebase(
          voPath, seg.start, segDur,
          `cache/segments/${tag}-vo.mp3`,
          'audio/mpeg',
        );
        let segMusicUrl = null;
        if (musicPath) {
          try {
            segMusicUrl = await clipAudioToFirebase(
              musicPath, seg.start, segDur,
              `cache/segments/${tag}-music.mp3`,
              'audio/mpeg',
            );
          } catch (e) {
            console.warn(`[${job.id}] seg${seg.index}: music clip failed (${e.message})`);
          }
        }
        return {
          seg,
          inputProps: {
            voiceoverUrl: segVoUrl,
            musicUrl: segMusicUrl,
            musicVolume,
            captions: sliceCaptionsForSegment(captions, seg.start, seg.end),
            footage: sliceFootageForSegment(footage, seg.start, seg.end),
            sfxLayer: sliceSfxForSegment(sfxLayer, seg.start, seg.end),
            overlayLayer: sliceOverlaysForSegment(overlayLayer, seg.start, seg.end),
          },
        };
      }));
      const segPrepMs = Date.now() - segPrepT0;
      console.log(`[${job.id}] ⏱ segment prep: ${(segPrepMs / 1000).toFixed(1)}s for ${segments.length} segments`);
      await jobRef.update({'timings.segmentPrepMs': segPrepMs}).catch(() => {});

      // Parallel render every segment. No swap loop — if one fails,
      // bail loudly. Pre-flight + alternates should have caught any
      // bad clips well before we got here. Each segment render runs
      // through renderWithSwapLoop so a single bad clip in one segment
      // only re-renders that segment, not the whole job.
      await jobRef.update({
        currentStep: `Rendering ${segments.length} segments in parallel`,
        progress: 75,
        updatedAt: FieldValue.serverTimestamp(),
      });
      const segRenderT0 = Date.now();
      const segOutputs = await Promise.all(segInputs.map(async ({seg, inputProps}) => {
        const label = `Segment ${seg.index + 1}/${segments.length}`;
        return renderWithSwapLoop(inputProps, jobRef, {
          jobId: job.id,
          channelId: job.channelId,
          label,
        });
      }));
      const segRenderMs = Date.now() - segRenderT0;
      console.log(`[${job.id}] ⏱ segments rendered: ${(segRenderMs / 1000).toFixed(1)}s wall clock`);
      await jobRef.update({
        'timings.segmentRenderMs': segRenderMs,
        'timings.renderMode': 'split',
        'timings.segmentCount': segments.length,
      }).catch(() => {});

      // Concat the segment MP4s, then loudness-normalise, then upload.
      const concatT0 = Date.now();
      const {outPath, tmpDir: concatTmpDir} = await concatSegmentMp4s(segOutputs, jobRef);
      const concatMs = Date.now() - concatT0;
      console.log(`[${job.id}] ⏱ concat: ${(concatMs / 1000).toFixed(1)}s`);

      // YouTube loudness-target pass. Single-pass loudnorm to I=-14 LUFS,
      // TP=-1.5 dBTP. Re-encodes audio (AAC 192k); video stream copied.
      const loudnormT0 = Date.now();
      let normalisedPath = outPath;
      try {
        normalisedPath = await applyLoudnorm(outPath, jobRef);
        console.log(`[${job.id}] ⏱ loudnorm: ${((Date.now() - loudnormT0) / 1000).toFixed(1)}s`);
      } catch (e) {
        console.warn(`[${job.id}] loudnorm failed (${e.message}) — falling back to raw concat output`);
      }
      await jobRef.update({
        'timings.concatMs': concatMs,
        'timings.loudnormMs': Date.now() - loudnormT0,
        'timings.render': Date.now() - renderStart,
      }).catch(() => {});

      await jobRef.update({
        currentStep: 'Uploading final video',
        progress: 95,
        updatedAt: FieldValue.serverTimestamp(),
      });
      var uploadT0 = Date.now();
      const finalPath = `channels/${job.channelId}/projects/${job.projectId}/editing/auto-edit-${Date.now()}.mp4`;
      const finalUrl = await uploadLocalFileToFirebase(normalisedPath, finalPath, 'video/mp4', {
        jobRef,
        label: 'Uploading final video',
      });
      const sizeBytes = fs.statSync(normalisedPath).size;
      finalFile = {url: finalUrl, path: finalPath, size: sizeBytes};
      try { fs.rmSync(concatTmpDir, {recursive: true, force: true}); } catch { /* ignore */ }
    } finally {
      try { fs.rmSync(tmpRoot, {recursive: true, force: true}); } catch { /* ignore */ }
    }
  }
  const uploadMs = Date.now() - uploadT0;
  const totalMs = Date.now() - totalStart;
  console.log(`[${job.id}] ⏱ upload: ${(uploadMs / 1000).toFixed(1)}s · TOTAL: ${(totalMs / 1000).toFixed(1)}s`);
  await jobRef.update({
    'timings.uploadMs': uploadMs,
    'timings.totalMs': totalMs,
  }).catch(() => {});

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
      // Source marker — frontend uses this to decide whether the
      // approval UI shows a worker-payment button + Request-changes
      // (manual flow) or a re-render-with-AI button (AI flow). Without
      // this, AI-generated renders get incorrectly routed to a manual
      // editor on Request-changes.
      source: 'ai',
    },
    editingNote: 'Auto-edited by AI — review carefully',
    // Clear any pre-assigned editor so the AI completion doesn't trigger
    // a worker payment on Approve. The user can hand off to a worker
    // explicitly via the UI if they want manual revisions.
    editingPerson: '',
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
  } finally {
    clearInterval(heartbeat);
  }
}

// ─── Discovery: cached trending fetch + Claude ranking ─────────────
// Cache lives on the channel doc as trendingCache: {niche, fetchedAt,
// reddit, hn, news, trends}. Fresh = same niche + <6h old.
async function getTrendingForChannel(channelId, niche) {
  const chRef = db.collection('channels').doc(channelId);
  const chSnap = await chRef.get();
  const chData = chSnap.data() || {};
  const cache = chData.trendingCache || {};
  const channelSubreddits = Array.isArray(chData.discoveryConfig?.subreddits)
    ? chData.discoveryConfig.subreddits
    : null;
  const isFresh =
    cache && cache.niche === niche && cache.fetchedAt &&
    (Date.now() - cache.fetchedAt) < 6 * 3600 * 1000;
  if (isFresh) {
    console.log('using cached trending data, age:', Math.round((Date.now() - cache.fetchedAt) / 60000), 'min');
    return cache;
  }
  console.log(`fetching fresh trending data for "${niche}"${channelSubreddits ? ` (channel override: ${channelSubreddits.length} subreddits)` : ''}`);
  const {fetchAllTrending} = require('./lib/discover');
  const fresh = await fetchAllTrending(niche, {newsApiKey: process.env.NEWSAPI_KEY, channelSubreddits});
  // Use dot-notation so we don't clobber the cron-populated grok/gemini
  // fields. Previously this replaced trendingCache entirely, which meant
  // every auto-pilot run silently nuked the premium-source data the
  // 30-min cron worked to fetch — and the topic picker then had only
  // HN+News to work with, producing duplicate news-of-the-day topics.
  await chRef.update({
    'trendingCache.niche': fresh.niche,
    'trendingCache.fetchedAt': fresh.fetchedAt,
    'trendingCache.reddit': fresh.reddit || [],
    'trendingCache.hn': fresh.hn || [],
    'trendingCache.news': fresh.news || [],
    'trendingCache.trends': fresh.trends || [],
    'trendingCache.counts': fresh.counts || {},
    'trendingCache.errors': fresh.errors || {},
    'trendingCache.redditSubredditsUsed': fresh.redditSubredditsUsed || [],
  });
  // Return the merged view (existing grok/gemini + freshly-fetched
  // free sources) so rankAndPickTopic sees all 4 sources.
  return {
    ...cache,
    ...fresh,
    grok: cache.grok || [],
    gemini: cache.gemini || [],
  };
}

// Send the union of fetched topics to Claude, get back a ranked pick +
// 5 title variants + reasoning, all as JSON. The prompt heavily biases
// toward multi-source convergence and recency.
// Pull the channel's recently-picked topics (and titles) so the topic
// picker can avoid duplicating them. This is the fix for the bulk-run
// duplicate-topic bug — without it, a dominant news story (e.g. "ABA
// vs Bitcoin") stayed at the top of every trending refresh for hours,
// so every bulk video picked it again. Querying the last 30 days of
// projects gives us enough history without an explicit per-channel
// "blocklist" field (the project docs ARE the history).
async function getRecentChannelTopics(channelId, limitN = 15) {
  if (!channelId) return [];
  try {
    const cutoffMs = Date.now() - 30 * 86400_000;
    const snap = await db.collection('channels').doc(channelId)
      .collection('projects')
      .orderBy('createdAt', 'desc')
      .limit(limitN)
      .get();
    const topics = [];
    for (const doc of snap.docs) {
      const d = doc.data() || {};
      const createdMs = d.createdAt?.toMillis ? d.createdAt.toMillis()
        : (d.created ? new Date(d.created).getTime() : 0);
      if (createdMs && createdMs < cutoffMs) continue;
      const topic = autoPilotUnwrap(d.pickedTopic) || d.topic || '';
      const title = d.ytTitle || d.title || autoPilotUnwrap(d.pickedTitle) || '';
      if (!topic && !title) continue;
      topics.push({topic: topic.slice(0, 150), title: title.slice(0, 150)});
    }
    return topics;
  } catch (e) {
    console.warn(`[recent-topics] ${channelId}: ${e.message}`);
    return [];
  }
}

async function rankAndPickTopic(trending, niche, anthropicKey, {recentTopics = []} = {}) {
  // Reject topics older than 7 days. Sources with reliable createdAt
  // (Reddit, HN, NewsAPI) get filtered hard; Google Trends sets
  // createdAt = fetch time so its items always pass — that's
  // acceptable since Trends only surfaces same-day search spikes.
  const MAX_AGE_DAYS = 7;
  const ageCutoffMs = Date.now() - MAX_AGE_DAYS * 86400_000;
  const slim = (arr) => (arr || [])
    .filter((t) => {
      // Items with no createdAt (or 0) pass through — can't verify
      // freshness, defer to Claude's date judgment in the prompt.
      if (!t.createdAt || t.createdAt === 0) return true;
      return t.createdAt >= ageCutoffMs;
    })
    .slice(0, 8)
    .map((t) => ({
      source: t.source,
      title: t.title,
      score: t.score,
      comments: t.comments,
      summary: t.summary,
      age: Math.round((Date.now() - (t.createdAt || Date.now())) / 3.6e6) + 'h',
      ...(t.extra?.subreddit ? {subreddit: t.extra.subreddit} : {}),
      ...(t.extra?.sourceName ? {publisher: t.extra.sourceName} : {}),
      ...(t.extra?.engagementSignal ? {engagementSignal: t.extra.engagementSignal} : {}),
      ...(t.extra?.whyTrending ? {whyTrending: t.extra.whyTrending} : {}),
    }));
  // 4-source picker as of 2026-05-11. Reddit (Cloud Function IPs
  // blocked) + Trends (consistently 0 results) were removed. slim()
  // handles undefined/empty arrays safely so legacy cached trendingCache
  // docs that still have reddit/trends populated are ignored.
  const allTopics = [
    ...slim(trending.hn),
    ...slim(trending.news),
    ...slim(trending.grok),
    ...slim(trending.gemini),
  ];

  // Today's date in plain English so Claude has a concrete reference
  // point — without this, "trending" gets interpreted loosely and
  // year-old events (e.g. "Fed 2025 decision") slip through.
  const todayStr = new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
  // Build a hard avoid-list from the channel's recent videos. Without
  // this block, dominant news stories ("ABA vs Bitcoin") stay top-of-
  // trending for hours and every bulk-spawned pick lands on the same
  // story. The instruction below is explicit about WHAT counts as a
  // duplicate (same entities, same news angle, same thesis) so Claude
  // doesn't just dodge the literal title and pick a paraphrase.
  const recentBlock = (recentTopics && recentTopics.length)
    ? `\n\n⚠ RECENT VIDEOS ON THIS CHANNEL — DO NOT PICK A TOPIC SIMILAR TO ANY OF THESE:\n${recentTopics.map((t, i) => `  ${i + 1}. "${t.title || t.topic}"`).join('\n')}\n\nA topic is "similar" if it shares ANY of:\n  • the same named entities (organisation, person, bill, event)\n  • the same news story / underlying event\n  • the same core thesis or angle\n\nIf the highest-engagement topic in the trending list overlaps with any recent video, SKIP IT and pick the next-best topic that's genuinely DIFFERENT. The pipeline must produce unique content per video — variations of the same story are duplicates, not separate videos.`
    : '';

  const prompt = `You are a YouTube content strategist for a faceless channel in the niche "${niche}".

TODAY'S DATE: ${todayStr}

I have ${allTopics.length} trending topics from Hacker News, NewsAPI, plus when available Grok (Twitter/X-side signal) and Gemini (YouTube-side signal). Pick the SINGLE BEST topic for a 5-minute YouTube video.

Criteria (in priority order):
1. **Must be CURRENT** — reject any topic that references events older than 7 days from today. If a title says "2025 Fed decision" and today is 2026, REJECT — that's year-old content. Same for any specific date or year that's clearly in the past.
2. **Must be UNIQUE** — see the RECENT VIDEOS block below if present. Never pick a topic that overlaps with one we already made.
3. Genuinely fits the niche "${niche}" — exclude noise that just keyword-matched (e.g. relationship drama with "finance" mentioned once)
4. Strong engagement signal across multiple sources (multi-source convergence on the same story is the strongest signal)
5. Fresh enough to feel "trending" — last 24-72h ideal, last week max
6. Has substance — could fill 5 minutes with research, not a one-line meme
7. Suitable for narration over stock footage (faceless format) — avoid topics that need a specific person on camera

If EVERY topic fails criterion 1 (all are stale), pick the freshest one and rewrite the title/topic to reframe it as a current-affairs angle (e.g. "What the [past event] from a year ago means for [today's market]"). Never pass through a year that's clearly historical.${recentBlock}

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

      // Pull last 15 picked topics from this channel and pass them to
      // the picker as an avoid-list. Without this, dominant news
      // stories produced duplicate-topic bulk runs ($20+ wasted per
      // run on near-identical videos).
      const recentTopics = await getRecentChannelTopics(data.channelId, 15);
      if (recentTopics.length) {
        console.log(`[${jobId}] excluding ${recentTopics.length} recent topics from picker`);
      }
      const proposal = await rankAndPickTopic(trending, data.niche, anthropicKey, {recentTopics});
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
    // Status-aware staleness thresholds. The HTTP worker's 30s heartbeat
    // is the primary defense; these thresholds are the backstop for
    // truly hung functions. They have to be longer than the maximum
    // legitimate gap between heartbeats including cold starts and AWS
    // hiccups, hence the generous values for 'rendering'.
    const STATUS_THRESHOLD_MS = {
      pending: 10 * 60 * 1000,
      researching: 10 * 60 * 1000,
      proposing: 10 * 60 * 1000,
      captions: 10 * 60 * 1000,
      sourcing: 20 * 60 * 1000,
      rendering: 50 * 60 * 1000, // HTTP worker's own ceiling is 60min
    };
    const DEFAULT_THRESHOLD_MS = 10 * 60 * 1000;

    async function sweep(collectionName, activeStatuses) {
      const snap = await db.collection(collectionName)
        .where('status', 'in', activeStatuses)
        .limit(50)
        .get();
      let killed = 0;
      for (const doc of snap.docs) {
        const data = doc.data();
        const updatedMs = data.updatedAt?.toMillis ? data.updatedAt.toMillis() : 0;
        if (!updatedMs) continue;
        const threshold = STATUS_THRESHOLD_MS[data.status] || DEFAULT_THRESHOLD_MS;
        if (Date.now() - updatedMs < threshold) continue;
        const ageMin = Math.round((Date.now() - updatedMs) / 60000);
        console.log(`[watchdog] ${collectionName}/${doc.id} stuck ${ageMin}min in ${data.status} (threshold ${Math.round(threshold/60000)}min) — marking failed`);
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

// Write a fresh fetchAllTrending() result to channel.trendingCache
// using dot-notation so the grok/gemini fields (which the premium
// fetcher updates separately) survive a free-sources refresh. The
// previous {trendingCache: fresh} replacement wiped them.
async function writeFreeSourcesToCache(channelId, fresh) {
  await db.collection('channels').doc(channelId).update({
    'trendingCache.reddit': fresh.reddit,
    'trendingCache.hn': fresh.hn,
    'trendingCache.news': fresh.news,
    'trendingCache.trends': fresh.trends,
    'trendingCache.niche': fresh.niche,
    'trendingCache.fetchedAt': fresh.fetchedAt,
    'trendingCache.counts': fresh.counts,
    'trendingCache.redditSubredditsUsed': fresh.redditSubredditsUsed || [],
    'trendingCache.errors': fresh.errors || {},
  });
}

// ─── Premium discovery fetch helper (Grok + Gemini) ────────────────
// Used by both the scheduled refresh (with interval gating) and the
// manual refresh callable (force-fires regardless of interval). Reads
// user keys from /users/{ownerUid}/secrets/keys and writes results to
// channel.trendingCache.grok/.gemini plus cumulative monthly cost
// tracking under channel.discoveryConfig.{monthlyCostUsd, monthlyResetMonth}.
//
// Cost note: ~$0.03 Grok + ~$0.02 Gemini = $0.05/call. With 6h interval
// that's ~$6/month per channel; 30min interval ~$72/month.
async function runPremiumDiscoveryFetch({channelId, channelData, ownerUid, label = 'premium'}) {
  const cfg = channelData.discoveryConfig || {};
  const grokEnabled = cfg.grokEnabled !== false;
  const geminiEnabled = cfg.geminiEnabled !== false;
  const niche = channelData.niche;

  const secrets = await autoPilotLoadUserSecrets(ownerUid);
  const {fetchGrok, fetchGemini} = require('./lib/discover');

  const t0 = Date.now();
  const [grokResult, geminiResult] = await Promise.all([
    (grokEnabled && secrets.grokKey)
      ? fetchGrok(niche, {apiKey: secrets.grokKey}).catch((e) => ({error: e.message}))
      : Promise.resolve({error: grokEnabled ? 'no grokKey in user secrets — add it in Settings → Integrations' : 'grok disabled in channel settings'}),
    (geminiEnabled && secrets.geminiKey)
      ? fetchGemini(niche, {apiKey: secrets.geminiKey}).catch((e) => ({error: e.message}))
      : Promise.resolve({error: geminiEnabled ? 'no geminiKey in user secrets — add it in Settings → Integrations' : 'gemini disabled in channel settings'}),
  ]);
  const elapsedMs = Date.now() - t0;

  const grokOk = Array.isArray(grokResult);
  const geminiOk = Array.isArray(geminiResult);
  const grokCost = grokOk ? 0.03 : 0;
  const geminiCost = geminiOk ? 0.02 : 0;
  const totalCost = grokCost + geminiCost;

  // Monthly cost accounting — auto-reset when month rolls over.
  const now = new Date();
  const thisMonthKey = `${now.getUTCFullYear()}-${(now.getUTCMonth() + 1).toString().padStart(2, '0')}`;
  const resetMonth = cfg.monthlyResetMonth || null;
  const baseSpent = resetMonth === thisMonthKey ? (cfg.monthlyCostUsd || 0) : 0;

  await db.collection('channels').doc(channelId).update({
    'trendingCache.grok': grokOk ? grokResult : [],
    'trendingCache.gemini': geminiOk ? geminiResult : [],
    'trendingCache.premiumFetchedAt': Date.now(),
    'trendingCache.premiumElapsedMs': elapsedMs,
    'trendingCache.premiumErrors': {
      grok: grokOk ? null : grokResult.error,
      gemini: geminiOk ? null : geminiResult.error,
    },
    'trendingCache.premiumLastCostUsd': totalCost,
    'discoveryConfig.monthlyCostUsd': baseSpent + totalCost,
    'discoveryConfig.monthlyResetMonth': thisMonthKey,
  });
  console.log(`[${label}] ${channelId}: ${grokOk ? grokResult.length : 0}g/${geminiOk ? geminiResult.length : 0}m in ${(elapsedMs / 1000).toFixed(1)}s, $${totalCost.toFixed(2)} (month: $${(baseSpent + totalCost).toFixed(2)})`);

  return {
    grokCount: grokOk ? grokResult.length : 0,
    geminiCount: geminiOk ? geminiResult.length : 0,
    grokError: grokOk ? null : grokResult.error,
    geminiError: geminiOk ? null : geminiResult.error,
    elapsedMs,
    estimatedCostUsd: totalCost,
    monthlyCostUsd: baseSpent + totalCost,
  };
}

// Map premiumRefreshInterval → ms. Used by the cron to gate per-channel
// premium fetches. Keep these values in sync with the UI dropdown.
const PREMIUM_REFRESH_INTERVAL_MS = {
  'every-refresh': 0,
  '6-hours': 6 * 3600_000,
  '12-hours': 12 * 3600_000,
  'daily': 24 * 3600_000,
};

// ─── Discovery trending auto-refresh ────────────────────────────────
// Runs every 30 minutes. For each channel with a niche set:
//   1. Refresh the 4 free sources (Reddit, HN, NewsAPI, Trends).
//   2. If premiumSourcesEnabled (default true) AND interval elapsed
//      AND budget cap not hit, also fetch Grok + Gemini.
//
// Per-channel discoveryConfig fields honored:
//   subreddits:                  override for Reddit fetch
//   premiumSourcesEnabled:       master switch for Grok+Gemini in cron
//   grokEnabled / geminiEnabled: per-source toggles
//   premiumRefreshInterval:      'every-refresh'|'6-hours'|'12-hours'|'daily'|'manual-only'
//   monthlyBudgetUsd:            stop premium when this cap is reached (0=disabled)
//   monthlyCostUsd / monthlyResetMonth: cumulative tracker
//
// Manual refresh (refreshPremiumSources callable) ignores interval and
// budget — it's user-initiated and always forces a fresh fetch.
//
// NewsAPI free tier is 100 req/day. With every-30-min refresh that's
// 48 reqs/day per channel — fine for 1-2 channels, will hit the
// ceiling at ~3 channels. If we ever go over, the per-source .catch
// in fetchAllTrending just leaves the news field empty in the cache.
exports.refreshTrendingScheduled = onSchedule(
  {
    schedule: 'every 30 minutes',
    region: 'us-central1',
    secrets: [NEWSAPI_KEY],
    timeoutSeconds: 540,
    memory: '512MiB',
  },
  async () => {
    const t0 = Date.now();
    const SKIP_IF_NEWER_THAN_MS = 25 * 60 * 1000;
    const PARALLELISM = 4;

    const snap = await db.collection('channels').get();
    const eligible = snap.docs
      .map((d) => ({id: d.id, data: d.data() || {}}))
      .filter((c) => typeof c.data.niche === 'string' && c.data.niche.trim().length > 0);

    if (!eligible.length) {
      console.log('[refresh-trending] no eligible channels');
      return;
    }

    const {fetchAllTrending} = require('./lib/discover');

    let refreshed = 0;
    let skipped = 0;
    let failed = 0;
    let premiumFired = 0;
    let premiumSkipped = 0;
    for (let i = 0; i < eligible.length; i += PARALLELISM) {
      const batch = eligible.slice(i, i + PARALLELISM);
      await Promise.all(batch.map(async ({id, data}) => {
        // ── Free sources ─────────────────────────────────────────
        const cache = data.trendingCache;
        const freeIsFresh = cache?.fetchedAt && (Date.now() - cache.fetchedAt) < SKIP_IF_NEWER_THAN_MS;
        if (freeIsFresh) {
          skipped++;
        } else {
          const channelSubreddits = Array.isArray(data.discoveryConfig?.subreddits)
            ? data.discoveryConfig.subreddits
            : null;
          try {
            const fresh = await fetchAllTrending(data.niche, {
              newsApiKey: process.env.NEWSAPI_KEY,
              channelSubreddits,
            });
            await writeFreeSourcesToCache(id, fresh);
            refreshed++;
            console.log(`[refresh-trending] ${id} (${data.niche}): ${fresh.counts.reddit}r/${fresh.counts.hn}h/${fresh.counts.news}n/${fresh.counts.trends}t · subreddits=[${(fresh.redditSubredditsUsed || []).join(',')}]`);
          } catch (e) {
            failed++;
            console.warn(`[refresh-trending] ${id} failed: ${e.message}`);
          }
        }

        // ── Premium sources (interval-gated) ────────────────────
        const cfg = data.discoveryConfig || {};
        const premiumEnabled = cfg.premiumSourcesEnabled !== false;
        const interval = cfg.premiumRefreshInterval || '6-hours';
        if (!premiumEnabled || interval === 'manual-only') {
          premiumSkipped++;
          return;
        }
        const intervalMs = PREMIUM_REFRESH_INTERVAL_MS[interval] ?? PREMIUM_REFRESH_INTERVAL_MS['6-hours'];
        const lastPremium = data.trendingCache?.premiumFetchedAt || 0;
        if (Date.now() - lastPremium < intervalMs) {
          premiumSkipped++;
          return;
        }
        // Budget cap (default $20/mo per channel; 0 disables cap).
        const monthlyBudget = typeof cfg.monthlyBudgetUsd === 'number' ? cfg.monthlyBudgetUsd : 20;
        const now2 = new Date();
        const thisMonthKey = `${now2.getUTCFullYear()}-${(now2.getUTCMonth() + 1).toString().padStart(2, '0')}`;
        const resetMonth = cfg.monthlyResetMonth || null;
        const monthSpent = resetMonth === thisMonthKey ? (cfg.monthlyCostUsd || 0) : 0;
        if (monthlyBudget > 0 && monthSpent + 0.05 > monthlyBudget) {
          premiumSkipped++;
          console.log(`[refresh-trending] ${id}: premium skipped — budget cap reached ($${monthSpent.toFixed(2)}/$${monthlyBudget})`);
          return;
        }
        const ownerUid = data.ownerId;
        if (!ownerUid) {
          premiumSkipped++;
          return;
        }
        try {
          await runPremiumDiscoveryFetch({
            channelId: id,
            channelData: data,
            ownerUid,
            label: 'refresh-trending/premium',
          });
          premiumFired++;
        } catch (e) {
          failed++;
          console.warn(`[refresh-trending] ${id} premium failed: ${e.message}`);
        }
      }));
    }

    const elapsedSec = ((Date.now() - t0) / 1000).toFixed(1);
    console.log(`[refresh-trending] done in ${elapsedSec}s — free: refreshed=${refreshed} skipped=${skipped} failed=${failed} | premium: fired=${premiumFired} skipped=${premiumSkipped} (of ${eligible.length} eligible)`);
  },
);

// ─── Manual premium refresh (callable) ─────────────────────────────
// User-triggered "Refresh now" path. Force-fires Grok + Gemini for the
// channel, bypassing the per-channel interval gating that the cron
// respects. Auth is automatic via Firebase Auth. ownerId on the
// channel must match the caller's uid.
//
// Returns the same shape as the cron's premium fetch so the frontend
// can show the same cost / counts / errors regardless of source.
//
// Auth: onCall verifies Firebase Auth automatically. We additionally
// enforce that request.auth.uid matches the channel's ownerId so one
// user can't refresh another user's channels.
// Manually re-extract a channel's thumbnail formula on demand —
// otherwise the user waits until the next daily forensic cycle
// (04:00 Stockholm). Reads the channel's analytics summary top
// videos, runs Claude vision on the top 10 thumbnails, writes the
// result back to channel.forensic.thumbnailFormula. Owner-only.
exports.refreshThumbnailFormula = onCall(
  {
    region: 'us-central1',
    timeoutSeconds: 120,
    memory: '512MiB',
  },
  async (request) => {
    const uid = request.auth?.uid;
    if (!uid) throw new HttpsError('unauthenticated', 'Sign in required');
    const {channelId} = request.data || {};
    if (!channelId) throw new HttpsError('invalid-argument', 'channelId required');
    const chSnap = await db.collection('channels').doc(channelId).get();
    if (!chSnap.exists) throw new HttpsError('not-found', 'Channel not found');
    const ch = chSnap.data();
    if (ch.ownerId && ch.ownerId !== uid) {
      throw new HttpsError('permission-denied', 'Not your channel');
    }
    const anthKey = await loadOwnerAnthropicKey();
    if (!anthKey) throw new HttpsError('failed-precondition', 'No Anthropic key on owner — save it in Settings → Integrations');
    try {
      await analyzeOwnThumbnailFormula(channelId, anthKey);
      const refreshed = (await db.collection('channels').doc(channelId).get()).data()?.forensic?.thumbnailFormula || null;
      return {
        ok: true,
        formula: refreshed ? {
          topColors: refreshed.topColors || [],
          faceUsage: refreshed.faceUsage || 0,
          commonExpressions: refreshed.commonExpressions || [],
          composition: refreshed.composition || '',
        } : null,
      };
    } catch (e) {
      throw new HttpsError('internal', e.message || 'thumbnail formula refresh failed');
    }
  },
);

// Single entry point for "bootstrap everything this channel needs to make
// smart thumbnails / scripts". Idempotent: re-running just refreshes.
// Pulls top videos from YouTube if analytics/summary is empty, runs Claude
// vision to extract the channel's own thumbnail formula, and re-classifies
// performance lessons. Stage 5 auto-calls the same underlying functions
// when the formula is missing — this callable just lets the frontend kick
// it manually (and supports the in-Pipeline auto-trigger on channel
// connect). Always responds with what got populated.
exports.bootstrapChannelIntelligence = onCall(
  {
    region: 'us-central1',
    timeoutSeconds: 300,
    memory: '512MiB',
  },
  async (request) => {
    const uid = request.auth?.uid;
    if (!uid) throw new HttpsError('unauthenticated', 'Sign in required');
    const {channelId} = request.data || {};
    if (!channelId) throw new HttpsError('invalid-argument', 'channelId required');
    const chRef = db.collection('channels').doc(channelId);
    const chSnap = await chRef.get();
    if (!chSnap.exists) throw new HttpsError('not-found', 'Channel not found');
    const ch = chSnap.data();
    if (ch.ownerId && ch.ownerId !== uid) {
      throw new HttpsError('permission-denied', 'Not your channel');
    }

    const steps = [];

    // Step 1 — analytics summary. If missing, ask the proxy to populate.
    try {
      const summarySnap = await chRef.collection('analytics').doc('summary').get();
      if (!summarySnap.exists || !(summarySnap.data()?.topVideos?.length)) {
        const r = await fetch(`${RENDER_PROXY_URL}/youtube/analytics/refresh`, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({channelId}),
        });
        steps.push({step: 'analytics', ok: r.ok, status: r.status});
      } else {
        steps.push({step: 'analytics', ok: true, cached: true});
      }
    } catch (e) {
      steps.push({step: 'analytics', ok: false, error: e.message});
    }

    // Step 2 — thumbnail formula (analyzeOwnThumbnailFormula has its own
    // YouTube fallback so this works even if step 1 partially failed).
    const anthKey = await loadOwnerAnthropicKey();
    if (anthKey) {
      try {
        await analyzeOwnThumbnailFormula(channelId, anthKey);
        steps.push({step: 'thumbnailFormula', ok: true});
      } catch (e) {
        steps.push({step: 'thumbnailFormula', ok: false, error: e.message});
      }
    } else {
      steps.push({step: 'thumbnailFormula', ok: false, error: 'no owner Anthropic key'});
    }

    // Step 3 — classify winning vs weak titles into forensic.lessons.
    try {
      await classifyAndLogPerformanceLessons(channelId);
      steps.push({step: 'performanceLessons', ok: true});
    } catch (e) {
      steps.push({step: 'performanceLessons', ok: false, error: e.message});
    }

    const after = (await chRef.get()).data() || {};
    return {
      ok: true,
      steps,
      intelligence: {
        thumbnailFormula: after.forensic?.thumbnailFormula ? {
          topColors: after.forensic.thumbnailFormula.topColors || [],
          faceUsage: after.forensic.thumbnailFormula.faceUsage || 0,
          commonExpressions: after.forensic.thumbnailFormula.commonExpressions || [],
        } : null,
        winningPatterns: (after.forensic?.winningPatterns || []).length,
        lessons: (after.forensic?.lessons || []).length,
      },
    };
  },
);

// Returns the current state of every "magic number" the pipeline uses
// so the frontend health panel can compare deployed-config vs expected-
// config without scraping logs. Read-only, owner-only. Module-load time
// approximates the deploy time of the running revision (Cloud Run cold-
// starts when a new revision is deployed, so process.uptime() since
// module load ≈ "how long has THIS code been running").
const MODULE_LOAD_TS = Date.now();
exports.getPipelineHealth = onCall(
  {
    region: 'us-central1',
    timeoutSeconds: 30,
    memory: '256MiB',
  },
  async (request) => {
    const uid = request.auth?.uid;
    if (!uid) throw new HttpsError('unauthenticated', 'Sign in required');
    const overlayItems = BEATS_TOOL.input_schema.properties.beats.items.properties.overlays;
    const entitiesItems = BEATS_TOOL.input_schema.properties.beats.items.properties.entities;
    return {
      ok: true,
      moduleLoadMs: MODULE_LOAD_TS,
      uptimeSec: Math.round((Date.now() - MODULE_LOAD_TS) / 1000),
      constants: {
        // Schema caps (what we ask Claude to obey)
        overlaysMaxItemsPerBeat: overlayItems.maxItems,
        entitiesMaxItemsPerBeat: entitiesItems.maxItems,
        overlayTypeEnum: overlayItems.items.properties.type.enum,
        // Density-validator targets (per-minute floors used by breakIntoBeats)
        densityTargets: {
          overlaysPerMin: 1.5,
          entitiesPerMin: 1.2,
          animationsPerMin: 1.5,
        },
        // AI-video pass caps
        shouldAnimateMax: SHOULD_ANIMATE_MAX_FOR_HEALTH,
        maxAiClipsPerRender: MAX_AI_CLIPS_PER_RENDER,
        // Bulk scheduler
        bulkCostPerVideoUsd: BULK_COST_PER_VIDEO,
        bulkMinGapMs: BULK_MIN_GAP_MS,
        // Thumbnail prompt — slim format, hard 600-char cap
        thumbnailPromptMaxChars: 600,
        thumbnailFallbackChain: ['pikzels', 'flux', 'placeholder'],
        // Discovery sources
        discoverySources: ['hn', 'newsapi', 'grok', 'gemini'],
      },
    };
  },
);

// SHOULD_ANIMATE_MAX is a function-scoped const inside sourceBeatAwareFootage
// (line ~3019). Mirror it at module scope so the health endpoint can return
// it without restructuring. Update both spots together when changing.
const SHOULD_ANIMATE_MAX_FOR_HEALTH = 15;

// Dry-run the beats prompt against a sample script and return what Claude
// actually generates — without queueing a render. The 20-min "let me run
// an auto-pilot to see if my prompt fix worked" loop is replaced by a
// ~30-sec Claude call. Owner-only. Costs ~$0.05/call (one Claude Sonnet
// turn). Sample script defaults to the most recent project's script, or
// a small hardcoded finance demo if the channel has no projects yet.
exports.testBeatsPrompt = onCall(
  {
    region: 'us-central1',
    // 300s was tripping after the targetSec=3 (Issue 4) prompt change
    // pushed expected beat count up ~33% — Claude's first-call latency
    // exceeded 300s on real project scripts (126+ sentences) and the
    // platform aborted the in-flight fetch with a generic "fetch failed"
    // error. Bumped to 540s (Gen 2 callable max) — production paths
    // (autoPilotWorker, processEditingJobHttp) already run at 3600s.
    timeoutSeconds: 540,
    memory: '512MiB',
  },
  async (request) => {
    const uid = request.auth?.uid;
    if (!uid) throw new HttpsError('unauthenticated', 'Sign in required');
    const {channelId, sampleScript} = request.data || {};
    if (!channelId) throw new HttpsError('invalid-argument', 'channelId required');
    const chSnap = await db.collection('channels').doc(channelId).get();
    if (!chSnap.exists) throw new HttpsError('not-found', 'Channel not found');
    const ch = chSnap.data();
    if (ch.ownerId && ch.ownerId !== uid) {
      throw new HttpsError('permission-denied', 'Not your channel');
    }
    const anthKey = await loadOwnerAnthropicKey();
    if (!anthKey) throw new HttpsError('failed-precondition', 'No Anthropic key on owner — save it in Settings → Integrations');

    // Choose script source. Default to last project's script for realism.
    let scriptText = (sampleScript || '').trim();
    let scriptSource = 'caller-provided';
    if (!scriptText) {
      const projSnap = await db.collection('channels').doc(channelId).collection('projects')
        .orderBy('createdAt', 'desc')
        .limit(10)
        .get();
      for (const doc of projSnap.docs) {
        const s = (doc.data()?.script || '').trim();
        if (s.length > 200) { scriptText = s; scriptSource = `latest project ${doc.id}`; break; }
      }
    }
    if (!scriptText) {
      // Last resort: a small hardcoded finance sample so the test still
      // runs on brand-new channels with no past renders.
      scriptText = `The Federal Reserve held rates steady at 5.25% for the seventh straight meeting. Jerome Powell signaled that two cuts remain on the table for 2026. Goldman Sachs analysts now expect the first cut in September. Inflation data came in hot at 3.4%, above the Fed's 2% target. The S&P 500 fell 1.8% on the news. Nvidia shares dropped 4.2% in after-hours trading. The IMF warned about systemic risks in the banking sector. Treasury yields jumped to 4.6%, the highest in six months. JPMorgan CEO Jamie Dimon called the environment "the most dangerous in decades." The Bank of England held rates at 5.0% the next day. Bitcoin surged past $90,000 amid the volatility. Cynthia Lummis pushed forward the CLARITY Act in the Senate. Bloomberg reported that institutional flows hit a record high. The Treasury Secretary said the administration was monitoring closely.`;
      scriptSource = 'hardcoded finance demo (no projects with script found)';
    }

    // Build minimal captions: one "sentence" per ~80-char chunk with
    // synthetic timestamps. breakIntoBeats reads {start, end, text}.
    const sentences = scriptText
      .replace(/\s+/g, ' ')
      .split(/(?<=[.!?])\s+/)
      .filter((s) => s.trim().length > 5);
    let cursor = 0;
    const captions = sentences.map((text, i) => {
      const dur = Math.max(2, Math.min(10, text.length / 16));
      const start = cursor;
      cursor += dur;
      return {start, end: cursor, text: text.trim()};
    });

    const t0 = Date.now();
    const beats = await breakIntoBeats(captions, anthKey, {channelId});
    const elapsedMs = Date.now() - t0;

    // Aggregate result. Identical accounting to what sourceBeatAwareFootage
    // would write to timings so the test matches a real render's metrics.
    const overlayBreakdown = {};
    let totalOverlays = 0, totalEntities = 0, totalShouldAnimate = 0, totalHero = 0;
    for (const b of beats) {
      if (Array.isArray(b?.overlays)) {
        totalOverlays += b.overlays.length;
        for (const ov of b.overlays) {
          overlayBreakdown[ov.type] = (overlayBreakdown[ov.type] || 0) + 1;
        }
      }
      if (Array.isArray(b?.entities)) totalEntities += b.entities.length;
      if (b?.shouldAnimate === true) totalShouldAnimate++;
      if (b?.heroMoment === true) totalHero++;
    }
    const sampleBeats = beats.slice(0, 5).map((b) => ({
      text: (b.sentence || b.text || '').slice(0, 120),
      start: b.start, end: b.end,
      overlays: (b.overlays || []).map((ov) => ({type: ov.type, text: ov.text || ov.headline || ov.symbol || ov.chartLabel || ov.name || ''})),
      entities: (b.entities || []).map((e) => ({name: e.name, type: e.type})),
      shouldAnimate: !!b.shouldAnimate,
      heroMoment: !!b.heroMoment,
    }));

    return {
      ok: true,
      scriptSource,
      scriptChars: scriptText.length,
      sentencesIn: sentences.length,
      videoLengthSec: Math.round(cursor),
      elapsedMs,
      result: {
        beatsCount: beats.length,
        overlayCount: totalOverlays,
        overlayBreakdown,
        entityCount: totalEntities,
        shouldAnimateCount: totalShouldAnimate,
        heroMomentCount: totalHero,
      },
      sampleBeats,
    };
  },
);

exports.refreshPremiumSources = onCall(
  {
    region: 'us-central1',
    // Without this binding process.env.NEWSAPI_KEY is undefined and
    // fetchAllTrending's news fetch fails silently → the dot-notation
    // write below clobbers the cron-populated trendingCache.news with
    // an empty array. The cron has the same binding (line ~3855).
    secrets: [NEWSAPI_KEY],
    timeoutSeconds: 120,
    memory: '512MiB',
  },
  async (request) => {
    const uid = request.auth?.uid;
    if (!uid) throw new HttpsError('unauthenticated', 'Sign in required');

    const {channelId} = request.data || {};
    if (!channelId || typeof channelId !== 'string') {
      throw new HttpsError('invalid-argument', 'channelId required');
    }

    const chRef = db.collection('channels').doc(channelId);
    const chSnap = await chRef.get();
    if (!chSnap.exists) throw new HttpsError('not-found', 'Channel not found');
    const ch = chSnap.data();
    if (ch.ownerId && ch.ownerId !== uid) {
      throw new HttpsError('permission-denied', 'Not your channel');
    }
    const niche = ch.niche;
    if (!niche || typeof niche !== 'string' || !niche.trim()) {
      throw new HttpsError('failed-precondition', 'Channel has no niche set');
    }

    // Manual refresh now does ALL 6 sources server-side. The previous
    // setup had the browser fetch Reddit/HN directly via broad search
    // (reddit.com/search.json), which bypassed the curated-subreddit
    // logic and surfaced off-topic content like r/Epstein for niches
    // matching multiple buckets. Server-side fetchReddit uses the
    // curated subreddit chain, so we route ALL refreshes through here.

    // Free sources first — these are cheap and update Reddit (curated
    // subreddits) + HN + NewsAPI + Trends. If they fail, premium still
    // runs (Grok/Gemini are independent).
    const channelSubreddits = Array.isArray(ch.discoveryConfig?.subreddits)
      ? ch.discoveryConfig.subreddits
      : null;
    let freeResult = null;
    let freeError = null;
    try {
      const {fetchAllTrending} = require('./lib/discover');
      const fresh = await fetchAllTrending(niche, {
        newsApiKey: process.env.NEWSAPI_KEY,
        channelSubreddits,
      });
      await writeFreeSourcesToCache(channelId, fresh);
      freeResult = fresh;
      console.log(`[manual-refresh] ${channelId} free: r${fresh.counts.reddit}/h${fresh.counts.hn}/n${fresh.counts.news}/t${fresh.counts.trends} · subreddits=[${(fresh.redditSubredditsUsed || []).join(',')}]`);
    } catch (e) {
      freeError = e.message;
      console.warn(`[manual-refresh] ${channelId} free sources failed: ${e.message}`);
    }

    // Premium fetch (force, no interval gating).
    const premium = await runPremiumDiscoveryFetch({
      channelId,
      channelData: ch,
      ownerUid: uid,
      label: 'manual-refresh',
    });

    return {
      ...premium,
      redditCount: freeResult?.counts?.reddit || 0,
      hnCount: freeResult?.counts?.hn || 0,
      newsCount: freeResult?.counts?.news || 0,
      trendsCount: freeResult?.counts?.trends || 0,
      redditSubredditsUsed: freeResult?.redditSubredditsUsed || [],
      freeError,
    };
  },
);

// Load the owner user's Anthropic API key from Firestore. Existing
// per-user flow (processDiscoveryJob, autoPilotWorker) reads it from
// users/{uid}.settings.anthropicKey or /users/{uid}/secrets/keys.
// The scheduled scrapers don't have a user context, so we look up the
// owner user (role='owner', exactly one in single-tenant deployments)
// and use their key. Cost lands on the owner — fair since they own
// the channel data being analysed.
async function loadOwnerAnthropicKey() {
  try {
    const ownerSnap = await db.collection('users').where('role', '==', 'owner').limit(1).get();
    if (ownerSnap.empty) return null;
    const ownerDoc = ownerSnap.docs[0];
    const ownerUid = ownerDoc.id;
    // Prefer the secrets sub-doc (newer pattern); fall back to settings.
    try {
      const secretsDoc = await db.collection('users').doc(ownerUid).collection('secrets').doc('keys').get();
      const fromSecrets = secretsDoc.exists ? secretsDoc.data()?.anthropicKey : null;
      if (fromSecrets) return fromSecrets;
    } catch (e) { /* fall through */ }
    return ownerDoc.data()?.settings?.anthropicKey || null;
  } catch (e) {
    console.warn(`[ownerKey] failed to load owner Anthropic key: ${e.message}`);
    return null;
  }
}

// ─── Thumbnail intelligence: shared helpers ─────────────────────────
// Two scheduled jobs and one on-demand call all need: (1) call YouTube
// Data API to enumerate a channel's top videos by view count, and (2)
// send a batch of thumbnail URLs to Claude vision and ask for pattern
// extraction. Factored here so the competitor scraper, own-channel
// scraper, and any future ad-hoc analysis share the same primitives.

// Finance/business creators we benchmark against. Stored by handle so
// the YouTube API can resolve current IDs on each run — channel renames
// won't break the scraper. Add more handles to extend the panel.
const COMPETITOR_HANDLES_FINANCE = [
  'MagnatesMedia',
  'HowMoneyWorks',
  'ColdFusionTV',
  'JosephCarlsonShow',
  'ThePlainBagel',
  'PBoyle',           // Patrick Boyle
  'BenFelixCSI',
  'TheMoneyGuyShow',
  'AndreiJikh',
  'GrahamStephan',
];

// Find a channel in OUR Firestore that has a YouTube OAuth refresh
// token — we borrow its auth to call YouTube Data API for the
// competitor scrape. Doesn't matter which channel (the call is
// public-data and counts a tiny quota hit against that user's
// OAuth limit). Prefers a channel owned by the role='owner' user.
async function pickAuthChannelForYouTubeAPI() {
  // First: an owner-owned channel with refresh token
  try {
    const ownerSnap = await db.collection('users').where('role', '==', 'owner').limit(1).get();
    if (!ownerSnap.empty) {
      const ownerUid = ownerSnap.docs[0].id;
      const chSnap = await db.collection('channels')
        .where('ownerId', '==', ownerUid)
        .where('youtubeConnected', '==', true)
        .limit(1).get();
      if (!chSnap.empty) return chSnap.docs[0].id;
    }
  } catch { /* fall through */ }
  // Fallback: any channel with refresh token
  try {
    const anySnap = await db.collection('channels')
      .where('youtubeConnected', '==', true)
      .limit(1).get();
    if (!anySnap.empty) return anySnap.docs[0].id;
  } catch { /* fall through */ }
  return null;
}

// Call the Render proxy's /youtube/competitor-top-videos endpoint —
// it does the OAuth refresh + YouTube Data API queries using an
// existing channel's stored tokens. Returns the same shape the
// scraper expects.
async function fetchCompetitorTopVideosViaProxy(authChannelId, handle, limit = 20) {
  const resp = await fetch(`${RENDER_PROXY_URL}/youtube/competitor-top-videos`, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({authChannelId, competitorHandle: handle, limit}),
  });
  if (!resp.ok) {
    const t = await resp.text().catch(() => '');
    throw new Error(`proxy ${resp.status}: ${t.slice(0, 160)}`);
  }
  const data = await resp.json();
  return {channel: data.channel, videos: data.videos || []};
}

// Claude vision pass — feeds an array of thumbnail URLs as image
// content blocks and asks for structured pattern extraction. Anthropic
// supports up to 20 images per message; we batch 10 for safety. Returns
// a parsed JSON object or null on any failure (auto-fix: never throws
// up the stack).
async function analyzeThumbnailBatchWithVision(thumbnails, apiKey, niche = 'finance') {
  if (!Array.isArray(thumbnails) || thumbnails.length === 0) return null;
  const items = thumbnails.slice(0, 10); // batch cap
  const content = [];
  // Lead with one user-text turn explaining the task, then interleave
  // image blocks with their context (title + views).
  content.push({
    type: 'text',
    text: `Analyze these ${items.length} high-performing ${niche} YouTube thumbnails. Each is from a top channel in the space. Identify the visual patterns that appear in the majority (>60%) of them.

Return ONLY a JSON object — no prose, no markdown fences, no commentary outside. Schema:
{
  "topColors": [string],
  "faceUsage": number,
  "commonExpressions": [string],
  "textCharacteristics": string,
  "visualElements": {"arrows": number, "money": number, "charts": number, "people": number},
  "composition": string,
  "emotionalTones": [string],
  "keyTakeaways": [string]
}

Numbers are fractions (0.0-1.0) of thumbnails exhibiting the trait. textCharacteristics + composition are 1-2 sentence summaries. emotionalTones is up to 4 short tags. keyTakeaways is 3-5 short actionable insights for designing a new thumbnail in this niche.`,
  });
  for (const t of items) {
    content.push({type: 'text', text: `${t.title || '(no title)'} — ${(t.views || 0).toLocaleString()} views`});
    content.push({type: 'image', source: {type: 'url', url: t.thumbnailUrl}});
  }
  try {
    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-5',
        max_tokens: 2000,
        messages: [{role: 'user', content}],
      }),
    });
    if (!resp.ok) {
      console.warn(`[thumb-vision] Claude ${resp.status}: ${(await resp.text()).slice(0, 200)}`);
      return null;
    }
    const data = await resp.json();
    let txt = '';
    for (const b of (data.content || [])) if (b.type === 'text') txt += b.text;
    if (!txt) return null;
    let jsonText = txt.trim().replace(/```(?:json)?\s*/gi, '').replace(/```/g, '').trim();
    const first = jsonText.indexOf('{');
    const last = jsonText.lastIndexOf('}');
    if (first < 0 || last <= first) return null;
    return JSON.parse(jsonText.slice(first, last + 1));
  } catch (e) {
    console.warn(`[thumb-vision] threw: ${e.message}`);
    return null;
  }
}

// ─── Competitor thumbnail scraper (weekly) ──────────────────────────
// Sundays 05:00 UTC. For each handle in COMPETITOR_HANDLES_FINANCE:
//   1. Resolve handle → channel ID
//   2. Pull top 20 videos by views
//   3. Send their thumbnails to Claude vision in batches of 10
// Merge per-batch findings into a single globalThumbnailPatterns/finance
// doc. autoPilotGenerateThumbnail reads this doc to bias the Pikzels
// prompt toward proven patterns.
//
// Auto-fix: missing OAuth-connected channel or owner Anthropic key →
// log and skip the run. Per-channel proxy errors → skip that channel
// and continue.
exports.scrapeCompetitorThumbnails = onSchedule(
  {
    schedule: 'every sunday 05:00',
    timeZone: 'UTC',
    region: 'us-central1',
    // No Secret Manager bindings needed. The competitor scrape now
    // borrows an existing channel's OAuth refresh token (via the
    // Render proxy's /youtube/competitor-top-videos endpoint) to
    // call YouTube Data API. Anthropic key loads from owner's
    // Firestore secrets. Both reuse existing infrastructure — no
    // new keys to provision.
    timeoutSeconds: 540,
    memory: '512MiB',
  },
  async () => {
    const authChannelId = await pickAuthChannelForYouTubeAPI();
    if (!authChannelId) {
      console.log(`[competitor-scrape] no channel in Firestore has youtubeConnected=true — skipping. Connect at least one channel via Channel switcher → Edit → YouTube connection.`);
      return;
    }
    const anthKey = await loadOwnerAnthropicKey();
    if (!anthKey) {
      console.log(`[competitor-scrape] no owner Anthropic key found in Firestore — skipping. Open Settings → Integrations and save your Anthropic key.`);
      return;
    }
    console.log(`[competitor-scrape] using auth channel ${authChannelId} for YouTube Data API calls`);
    const t0 = Date.now();
    const allThumbs = [];
    let resolved = 0;
    let failed = 0;
    for (const handle of COMPETITOR_HANDLES_FINANCE) {
      try {
        const {channel: ch, videos: top} = await fetchCompetitorTopVideosViaProxy(authChannelId, handle, 20);
        if (!ch) { failed++; console.warn(`[competitor-scrape] ${handle}: not found`); continue; }
        for (const v of top) {
          allThumbs.push({...v, channel: ch.title, handle});
        }
        resolved++;
        console.log(`[competitor-scrape] ${handle} → ${top.length} thumbnails`);
      } catch (e) {
        failed++;
        console.warn(`[competitor-scrape] ${handle} failed: ${e.message}`);
      }
    }
    if (!allThumbs.length) {
      console.warn(`[competitor-scrape] no thumbnails gathered (resolved=${resolved}, failed=${failed}) — bailing`);
      return;
    }
    // Sort all by views desc, keep top 50 for the vision pass.
    allThumbs.sort((a, b) => b.views - a.views);
    const topPool = allThumbs.slice(0, 50);
    // Batch 10 at a time. 5 batches × $0.05ish = ~$0.25/week.
    const batchResults = [];
    for (let i = 0; i < topPool.length; i += 10) {
      const batch = topPool.slice(i, i + 10);
      const result = await analyzeThumbnailBatchWithVision(batch, anthKey, 'finance');
      if (result) batchResults.push(result);
    }
    if (!batchResults.length) {
      console.warn(`[competitor-scrape] all vision batches failed — no doc update`);
      return;
    }
    // Merge fractions across batches (simple averaging — robust enough
    // for visualisation/prompt purposes).
    const merged = {
      topColors: [],
      faceUsage: 0,
      commonExpressions: [],
      textCharacteristics: batchResults[0].textCharacteristics || '',
      visualElements: {arrows: 0, money: 0, charts: 0, people: 0},
      composition: batchResults[0].composition || '',
      emotionalTones: [],
      keyTakeaways: [],
    };
    const colorTally = new Map();
    const exprTally = new Map();
    const toneTally = new Map();
    const ttkTally = new Map();
    for (const r of batchResults) {
      for (const c of (r.topColors || [])) colorTally.set(c, (colorTally.get(c) || 0) + 1);
      for (const e of (r.commonExpressions || [])) exprTally.set(e, (exprTally.get(e) || 0) + 1);
      for (const t of (r.emotionalTones || [])) toneTally.set(t, (toneTally.get(t) || 0) + 1);
      for (const t of (r.keyTakeaways || [])) ttkTally.set(t, (ttkTally.get(t) || 0) + 1);
      merged.faceUsage += (r.faceUsage || 0);
      const ve = r.visualElements || {};
      merged.visualElements.arrows += (ve.arrows || 0);
      merged.visualElements.money += (ve.money || 0);
      merged.visualElements.charts += (ve.charts || 0);
      merged.visualElements.people += (ve.people || 0);
    }
    const N = batchResults.length;
    merged.faceUsage = +(merged.faceUsage / N).toFixed(2);
    for (const k of Object.keys(merged.visualElements)) {
      merged.visualElements[k] = +(merged.visualElements[k] / N).toFixed(2);
    }
    merged.topColors = [...colorTally.entries()].sort((a, b) => b[1] - a[1]).slice(0, 6).map((x) => x[0]);
    merged.commonExpressions = [...exprTally.entries()].sort((a, b) => b[1] - a[1]).slice(0, 4).map((x) => x[0]);
    merged.emotionalTones = [...toneTally.entries()].sort((a, b) => b[1] - a[1]).slice(0, 4).map((x) => x[0]);
    merged.keyTakeaways = [...ttkTally.entries()].sort((a, b) => b[1] - a[1]).slice(0, 6).map((x) => x[0]);
    merged.sampleUrls = topPool.slice(0, 6).map((t) => t.thumbnailUrl);
    merged.refreshedAt = FieldValue.serverTimestamp();
    merged.batchCount = N;
    merged.thumbnailsAnalyzed = topPool.length;
    await db.collection('globalThumbnailPatterns').doc('finance').set(merged);
    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
    console.log(`[competitor-scrape] done in ${elapsed}s — analyzed ${topPool.length} thumbnails across ${resolved} channels (${failed} channels failed)`);
  },
);

// ─── Auto-forensic scheduler ────────────────────────────────────────
// Runs daily at 04:00 Stockholm time. For every channel with YouTube
// analytics connected and auto-forensic enabled, decides whether to
// run /analytics/refresh + /analytics/forensic based on:
//
//   1) frequency interval (weekly/biweekly/monthly per channel)
//   2) "smart triggering" — only run forensic when there's actually
//      new data: ≥3 new videos since last run, OR last run >30d old
//
// /analytics/refresh is cheap (~3s, free) so we can run it every cycle
// and use the fresh top-videos list to evaluate the smart-triggering
// rule. /analytics/forensic costs ~$0.30 in Decodo bandwidth per run
// — that's the call we want to skip when nothing changed.
//
// Pattern extraction (Claude pass) is INTENTIONALLY skipped in
// auto-mode — it requires the user's Anthropic key, which we don't
// store server-side. Users hit the dashboard's "⚡ Patterns only"
// button when they want patterns, runs in ~10s with $0 bandwidth.
//
// Notifications: on completion, writes to users/{ownerId}/notifications
// — the existing in-app notification system surfaces a toast next
// time the user opens the app.
exports.autoForensicScheduler = onSchedule(
  {
    schedule: 'every day 04:00',
    timeZone: 'Europe/Stockholm',
    region: 'us-central1',
    // No Secret Manager binding for Anthropic — the own-channel
    // thumbnail formula vision pass loads the owner user's key from
    // Firestore at runtime via loadOwnerAnthropicKey(). Skips
    // gracefully if no owner / no key found.
    timeoutSeconds: 540,
    memory: '512MiB',
  },
  async () => {
    console.log('[autoForensic] cycle start');
    const snap = await db.collection('channels')
      .where('youtubeAnalyticsConnected', '==', true)
      .get();
    console.log(`[autoForensic] inspecting ${snap.size} connected channels`);

    let attempted = 0;
    let ran = 0;
    let skipped = 0;
    let failed = 0;

    for (const docRef of snap.docs) {
      const cid = docRef.id;
      const data = docRef.data();
      // Default ON for newly-connected channels — user has to opt out
      // explicitly. autoForensicEnabled === false disables; null /
      // undefined / true all run.
      if (data.autoForensicEnabled === false) {
        console.log(`[autoForensic] ${cid}: disabled by user, skipping`);
        skipped++;
        continue;
      }

      const freq = data.autoForensicFrequency || 'biweekly';
      const freqDays = freq === 'weekly' ? 7 : freq === 'monthly' ? 30 : 14;
      const lastRunMs = data.lastAutoForensicRun?.toMillis?.() || 0;
      const ageDays = lastRunMs ? (Date.now() - lastRunMs) / 86400_000 : Infinity;

      // First pass: refresh analytics so we have fresh top-videos data
      // to evaluate the smart-triggering rule. Always cheap.
      let refreshOk = false;
      try {
        const r = await fetch(`${RENDER_PROXY_URL}/youtube/analytics/refresh`, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({channelId: cid}),
        });
        refreshOk = r.ok;
        if (!refreshOk) {
          const body = await r.text().catch(() => '');
          console.warn(`[autoForensic] ${cid}: refresh failed HTTP ${r.status}: ${body.slice(0, 200)}`);
        }
      } catch (e) {
        console.warn(`[autoForensic] ${cid}: refresh threw: ${e.message}`);
      }
      if (!refreshOk) { failed++; continue; }

      // Smart triggering — count new videos since last forensic run.
      const summarySnap = await db.collection('channels').doc(cid)
        .collection('analytics').doc('summary').get();
      const summary = summarySnap.exists ? summarySnap.data() : null;
      const topVideos = summary?.topVideos || [];
      const newVideos = topVideos.filter((v) => {
        const t = v.publishedAt ? new Date(v.publishedAt).getTime() : 0;
        return t > lastRunMs;
      });

      let shouldRun = false;
      let reason = '';
      if (lastRunMs === 0) { shouldRun = true; reason = 'never run before'; }
      else if (ageDays >= 30) { shouldRun = true; reason = `${ageDays.toFixed(1)}d since last run (≥30d ceiling)`; }
      else if (ageDays >= freqDays && newVideos.length >= 3) { shouldRun = true; reason = `${newVideos.length} new videos + ${freq} interval reached`; }
      else { reason = `skipped: ${ageDays.toFixed(1)}d/${freqDays}d, ${newVideos.length}/3 new videos`; }

      attempted++;
      if (!shouldRun) {
        console.log(`[autoForensic] ${cid}: ${reason}`);
        skipped++;
        continue;
      }

      console.log(`[autoForensic] ${cid}: running — ${reason}`);
      let forensicResult = null;
      try {
        const r = await fetch(`${RENDER_PROXY_URL}/youtube/analytics/forensic`, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          // No _apiKey — pattern pass intentionally skipped (see header
          // comment). Raw forensic data is what auto-mode delivers.
          body: JSON.stringify({channelId: cid}),
        });
        if (r.ok) {
          forensicResult = await r.json();
        } else {
          const body = await r.text().catch(() => '');
          console.warn(`[autoForensic] ${cid}: forensic failed HTTP ${r.status}: ${body.slice(0, 200)}`);
        }
      } catch (e) {
        console.warn(`[autoForensic] ${cid}: forensic threw: ${e.message}`);
      }

      if (!forensicResult || forensicResult.ok !== true) {
        failed++;
        await docRef.ref.update({
          lastAutoForensicResult: {
            ranAt: FieldValue.serverTimestamp(),
            ok: false,
            reason,
            error: 'forensic call failed (see Cloud Function logs)',
          },
        }).catch(() => {});
        continue;
      }

      // Record success + cost estimate. The cost-history subcollection
      // is the source of truth for the dashboard's "monthly forensic
      // spend" widget; one row per scheduled run keeps the math simple.
      const ranAtTs = FieldValue.serverTimestamp();
      await docRef.ref.update({
        lastAutoForensicRun: ranAtTs,
        lastAutoForensicResult: {
          ranAt: ranAtTs,
          ok: true,
          videosAnalyzed: forensicResult.videosAnalyzed || 0,
          videosWithCaptions: forensicResult.videosWithCaptions || 0,
          reason,
          costEstimateUsd: FORENSIC_RUN_COST_ESTIMATE_USD,
        },
      });

      // Self-improving loop: classify recent videos as "winning" or
      // "weak" vs the channel's median, then persist short summaries
      // to channel.forensic.lessons / .winningPatterns. The script,
      // metadata, and topic prompts already consume forensic data —
      // we surface these classifications there so future generations
      // explicitly avoid the weak patterns and replicate the winning
      // ones. Best-effort; never blocks the scheduler.
      try {
        await classifyAndLogPerformanceLessons(cid);
      } catch (e) {
        console.warn(`[autoForensic] ${cid}: lesson classification failed: ${e.message}`);
      }

      // Channel-specific thumbnail formula extraction. Reads
      // analytics summary, sends top 10 thumbnails to Claude vision,
      // saves formula to channel.forensic.thumbnailFormula. The
      // Pikzels prompt builder picks this up automatically on next
      // generation. Loads the owner user's Anthropic key from
      // Firestore (existing per-user pattern); skips if no key found.
      const anthKey = await loadOwnerAnthropicKey();
      if (anthKey) {
        try {
          await analyzeOwnThumbnailFormula(cid, anthKey);
        } catch (e) {
          console.warn(`[autoForensic] ${cid}: own-thumbnail formula failed: ${e.message}`);
        }
      } else {
        console.log(`[autoForensic] ${cid}: no owner Anthropic key — skipping thumbnail formula`);
      }
      await docRef.ref.collection('forensicCostHistory').add({
        ranAt: ranAtTs,
        videosAnalyzed: forensicResult.videosAnalyzed || 0,
        videosWithCaptions: forensicResult.videosWithCaptions || 0,
        costEstimateUsd: FORENSIC_RUN_COST_ESTIMATE_USD,
        trigger: 'auto',
      });

      // In-app notification to the channel owner. Frontend already
      // watches users/{uid}/notifications and renders unread ones.
      if (data.ownerId) {
        await db.collection('users').doc(data.ownerId).collection('notifications').add({
          message: `📊 Auto-forensic ran on "${data.name || 'channel'}": ${forensicResult.videosWithCaptions}/${forensicResult.videosAnalyzed} videos analyzed. Open Analytics → ⚡ Patterns only to extract spike/dip phrases.`,
          ts: FieldValue.serverTimestamp(),
          read: false,
          type: 'auto-forensic',
          channelId: cid,
        }).catch((e) => console.warn(`[autoForensic] notification write failed: ${e.message}`));
      }
      ran++;
    }

    console.log(`[autoForensic] cycle done — ran=${ran}, skipped=${skipped}, failed=${failed}, attempted=${attempted}, total=${snap.size}`);
  },
);

// ─── Auto-pilot orchestrator (server-side replacement for the frontend
// runAutoPilot in App.jsx) ────────────────────────────────────────────
//
// Triggered when the project doc is written with autoPilot.requestStart
// === true. Acquires a transactional lock, then runs 7 stages: Discover
// → Script → Voiceover → Render → Thumbnail → Title+Desc → Upload+Schedule.
// Each stage:
//   • checks if it's already complete (resume support: skip when so)
//   • does its work, mostly via the existing Render proxy endpoints
//   • updates project.autoPilot.{currentStage, stageLabel, updatedAt}
//   • polls project.autoPilot.requestCancel between stages so the user
//     can cancel from any device
//
// API keys (Anthropic, ElevenLabs, Pikzels) load from
// users/{ownerId}/secrets/keys — the Settings page writes them on save.
//
// Cloud Function instance lifetime covers the whole run (up to 59 min,
// well within the 25-min worst case). If the function instance is
// recycled mid-run, the project's status stays 'running' but the
// updatedAt timestamp goes stale — a future watchdog can detect that
// (similar to zombieJobWatchdog) and flip to 'failed'. Not built yet —
// rare in practice on Cloud Functions Gen 2.

// Mirror of ELEVENLABS_MODELS in server.js + App.jsx so the orchestrator
// can pass the right model when calling /elevenlabs/tts on the proxy.
const AUTOPILOT_ELEVENLABS_DEFAULT_MODEL = 'eleven_turbo_v2_5';

// Mirror of the frontend's SCRIPT_DIP_RULES so the server-side script
// generator runs the SAME validation + retry loop. Any change here must
// also be reflected in App.jsx's SCRIPT_DIP_RULES.
const AUTOPILOT_SCRIPT_DIP_RULES = [
  {type: 'meta-promise', re: /\byou'?(re| are)\s+(sitting|here|still)\s+(thinking|reading|watching|wondering)\b/i},
  {type: 'meta-promise', re: /\byou\s+need\s+to\s+(hear|know|understand)\s+(this|something|why|how)\b/i},
  {type: 'meta-promise', re: /\bif\s+you'?(re| are)\s+(still |sitting |here )?(reading|watching|wondering|thinking)\b/i},
  {type: 'soft-transition', re: /\blet'?s\s+(get into it|talk about|move on|dive in|jump in|break it down|begin|unpack)\b/i},
  {type: 'soft-transition', re: /\b(so anyway|moving on|with that said|that said|all that to say|having said that)\b/i},
  {type: 'soft-transition', re: /\b(in this video|in today'?s video)\b/i},
  {type: 'definition-block', re: /\bso\s+what\s+(actually )?(is|are)\s+(an?|the)\s+\w+\??/i},
  {type: 'definition-block', re: /\bfor\s+those\s+(who\s+)?don'?t\s+know\b/i},
  {type: 'definition-block', re: /\bfor\s+the\s+uninitiated\b/i},
  {type: 'definition-block', re: /\bbecause\s+i\s+know\s+(that\s+)?(term|phrase|word)\s+(might|can|sounds)\b/i},
  {type: 'generic-opener', re: /^(\s*)(if you|when you|imagine you|picture this|have you ever|are you tired|are you ready)\b/i, openingOnly: true},
];

function autoPilotValidateScript(text, dynamicDipPhrases = []) {
  if (!text || typeof text !== 'string') return {passed: false, violations: [{type: 'empty', snippet: '', context: '(no text)'}]};
  const violations = [];
  const firstLine = text.split(/[.!?\n]/, 1)[0] || '';
  for (const rule of AUTOPILOT_SCRIPT_DIP_RULES) {
    const target = rule.openingOnly ? firstLine : text;
    const m = target.match(rule.re);
    if (m) {
      const idx = (rule.openingOnly ? text.indexOf(firstLine) : 0) + (m.index || 0);
      violations.push({
        type: rule.type,
        snippet: m[0],
        context: text.slice(Math.max(0, idx - 35), Math.min(text.length, idx + m[0].length + 35)).replace(/\s+/g, ' ').trim(),
      });
    }
  }
  for (const phrase of dynamicDipPhrases) {
    if (!phrase || phrase.length < 5) continue;
    const escaped = phrase.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\s+/g, '\\s+');
    let re;
    try { re = new RegExp(escaped, 'i'); } catch { continue; }
    const m = text.match(re);
    if (m) {
      violations.push({
        type: 'channel-dip-phrase',
        snippet: m[0],
        context: text.slice(Math.max(0, (m.index || 0) - 35), Math.min(text.length, (m.index || 0) + m[0].length + 35)).replace(/\s+/g, ' ').trim(),
      });
    }
  }
  return {passed: violations.length === 0, violations};
}

function autoPilotBuildScriptPrompt(topic, forensic, insights, retryViolations) {
  const fp = forensic?.patterns || null;
  const lines = [];
  lines.push('═══════════════════════════════════════════════════════════════');
  lines.push('MANDATORY RULES — these are not suggestions. The script is rejected if any are violated.');
  lines.push('This channel\'s top 10 videos prove what works for THIS audience. Deviating costs retention.');
  lines.push('═══════════════════════════════════════════════════════════════');
  lines.push('');
  lines.push('❌ FORBIDDEN PHRASES — never write these or close cousins:');
  lines.push('  • Meta-promises about the viewer ("if you\'re sitting here thinking", "you need to hear", "if you\'re still watching")');
  lines.push('  • Soft transitions ("let\'s get into it", "let\'s talk about X", "moving on", "anyway", "in this video", "in today\'s video")');
  lines.push('  • Definition blocks ("so what actually is X?", "for those who don\'t know", "because I know that term might…")');
  lines.push('  • Generic "you"-framed openings ("If you\'ve ever…", "Imagine you\'re…", "Picture this…", "Have you ever…")');
  lines.push('');
  if (Array.isArray(fp?.dipWordPatterns) && fp.dipWordPatterns.length) {
    lines.push('Channel-specific dip phrases — these literal patterns lost retention on YOUR top videos. Do NOT use:');
    fp.dipWordPatterns.slice(0, 8).forEach((d) => {
      const drop = typeof d.avgDrop === 'number' ? ` (avg -${d.avgDrop} retention)` : '';
      lines.push(`  • "${d.phrase}"${drop}${d.advice ? ' — ' + d.advice : ''}`);
    });
    lines.push('');
  }
  lines.push('✓ INSTEAD, do these:');
  lines.push('  • Open with a NAMED entity — a person, dollar amount, place, date, event, or surprising statistic. Never generic "you".');
  lines.push('  • The HOOK must land by sentence 2 (~8 seconds spoken).');
  lines.push('  • Transition between sections with a hard cut — drop the connective filler.');
  lines.push('  • If a term needs defining, weave it into a story or example — never an explicit "so what is X?" stopover.');
  lines.push('  • Vary sentence length aggressively. Punchy 3-5 word sentences interleaved with longer ones.');
  lines.push('');
  if (Array.isArray(fp?.spikeWordPatterns) && fp.spikeWordPatterns.length) {
    lines.push('PROVEN spike phrases — replicate these or close cousins at structural turning points:');
    fp.spikeWordPatterns.slice(0, 6).forEach((s) => {
      const where = s.where ? ` (${s.where})` : '';
      const gain = typeof s.avgSpike === 'number' ? `, +${s.avgSpike} retention` : '';
      lines.push(`  • "${s.phrase}"${where}${gain}`);
    });
    lines.push('');
  }
  if (Array.isArray(fp?.hookPatterns) && fp.hookPatterns.length) {
    lines.push('PROVEN hook structures from this channel\'s top videos:');
    fp.hookPatterns.slice(0, 4).forEach((h) => {
      lines.push(`  • ${h.pattern}${h.example ? ` — example: "${h.example}"` : ''}`);
    });
    lines.push('');
  }
  if (fp?.structuralTemplate) {
    const t = fp.structuralTemplate;
    const blocks = ['intro', 'setup', 'body', 'climax', 'conclusion'].filter((k) => t[k]);
    if (blocks.length) {
      lines.push('OPTIMAL STRUCTURE (proven on this channel):');
      blocks.forEach((k) => lines.push(`  • ${k}: ${t[k]}`));
      lines.push('');
    }
  }
  const audienceProfile = fp?.audienceInsights?.profile || insights?.audienceProfile;
  if (audienceProfile) {
    lines.push(`AUDIENCE: ${audienceProfile}`);
    if (fp?.audienceInsights?.triggers) lines.push(`  ↑ activates retention: ${fp.audienceInsights.triggers}`);
    if (fp?.audienceInsights?.repellents) lines.push(`  ↓ kills retention: ${fp.audienceInsights.repellents}`);
    lines.push('');
  }
  // Self-improving loop output: winning patterns (replicate) and
  // weak patterns (avoid). Populated by classifyAndLogPerformanceLessons
  // after each daily forensic run, so every script generation reads the
  // freshest "what's working / what's not" classification for this
  // channel. Names/topics intentionally surface verbatim so Claude can
  // pattern-match.
  const winning = Array.isArray(forensic?.winningPatterns) ? forensic.winningPatterns : [];
  const losing = Array.isArray(forensic?.lessons) ? forensic.lessons : [];
  if (winning.length) {
    lines.push('★ WINNING VIDEOS (> 2× channel median views — replicate the title pattern + topic angle):');
    winning.slice(0, 5).forEach((w) => {
      lines.push(`  ↑ "${w.title}" — ${w.views.toLocaleString()} views (${w.viewsVsMedian}× median)`);
    });
    lines.push('');
  }
  if (losing.length) {
    lines.push('⚠ WEAK VIDEOS (< 0.5× channel median views — avoid this title pattern + topic angle):');
    losing.slice(0, 5).forEach((l) => {
      lines.push(`  ↓ "${l.title}" — ${l.views.toLocaleString()} views (${l.viewsVsMedian}× median)`);
    });
    lines.push('');
  }
  if (Array.isArray(retryViolations) && retryViolations.length) {
    lines.push('═══════════════════════════════════════════════════════════════');
    lines.push('PREVIOUS ATTEMPT FAILED VALIDATION. CONCRETE VIOLATIONS:');
    retryViolations.slice(0, 10).forEach((v) => {
      lines.push(`  ✗ [${v.type}] "${v.snippet}"  — context: "${v.context}"`);
    });
    lines.push('Do NOT use any of these patterns again. Rewrite those sections with a different approach.');
    lines.push('═══════════════════════════════════════════════════════════════');
    lines.push('');
  }
  lines.push('TASK:');
  lines.push(`Write a 1500-2000 word YouTube video script. Topic: "${topic}".`);
  lines.push('');
  lines.push('Format: natural spoken paragraphs. No stage directions. No headings. No section labels.');
  lines.push('Voice: conversational, direct, urgent when appropriate.');
  lines.push('Pacing: short punchy sentences mixed with longer ones.');
  lines.push('');
  lines.push('REMEMBER:');
  lines.push('  1. Hook by sentence 2.');
  lines.push('  2. NO soft transitions, NO definition blocks, NO generic-you openers, NO meta-promises.');
  lines.push('  3. Open with a named entity / number / event — not "you".');
  return lines.join('\n');
}

async function autoPilotGenerateScript(topic, channelId, apiKey, maxAttempts = 3) {
  const [insights, forensic] = await Promise.all([
    getChannelAnalyticsInsights(channelId),
    getChannelForensicData(channelId),
  ]);
  const dynamicDipPhrases = (forensic?.patterns?.dipWordPatterns || []).map((d) => d?.phrase).filter(Boolean);
  let lastText = '';
  let lastViolations = [];
  let finalAttempts = maxAttempts;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const prompt = autoPilotBuildScriptPrompt(topic, forensic, insights, attempt > 1 ? lastViolations : null);
    const text = await callAnthropic(apiKey, prompt, 3500);
    if (!text) break;
    const validation = autoPilotValidateScript(text, dynamicDipPhrases);
    lastText = text;
    lastViolations = validation.violations;
    console.log(`[autopilot:script] attempt ${attempt}/${maxAttempts}: ${validation.passed ? 'PASS' : `FAIL (${validation.violations.length} violations)`}`);
    if (validation.passed) {
      finalAttempts = attempt;
      lastViolations = [];
      break;
    }
  }
  // Extract entities from the finalized script so Stage 5 (thumbnail)
  // can feature the right people / companies / events. Best-effort —
  // if extraction fails, thumbnail still renders from base style.
  const entities = lastText
    ? await extractEntitiesFromScript(lastText, topic, apiKey).catch((e) => {
        console.warn(`[autopilot:script] entity extraction failed: ${e.message}`);
        return [];
      })
    : [];
  return {text: lastText, attempts: finalAttempts, violations: lastViolations, entities};
}

// Extract the 2-4 most prominent named entities from a script for use
// by the thumbnail prompt builder. Asks Claude for image-recognisable
// entities only — Powell, Apple, Capitol building, "the 2008 crash" —
// skipping generic nouns ("the market", "investors"). Returns up to 4
// entries with type + 5-10 word context.
async function extractEntitiesFromScript(scriptText, topic, apiKey) {
  const slim = (scriptText || '').slice(0, 3500);
  if (slim.length < 300) return [];
  const prompt = `Extract the most important NAMED entities from this YouTube script. Return ONLY entities that an image generator could VISUALLY represent — recognisable people, companies (with logos), places (with iconic visuals), or specific named events.

INCLUDE: "Jerome Powell", "Apple", "Goldman Sachs", "Capitol building", "the 2008 crash"
EXCLUDE: generic "the market", "investors", "regulators", "inflation" (not visually specific)

TOPIC: ${topic || '(unknown)'}

SCRIPT:
${slim}

Return ONLY a JSON object — no prose, no markdown fences:
{
  "entities": [
    {"name": "Jerome Powell", "type": "person", "context": "Fed chair making this announcement"},
    {"name": "Goldman Sachs", "type": "company", "context": "the bank that warned of recession"}
  ]
}

Up to 10 entities, ordered by importance to this video. context is 5-10 words explaining their role. Don't be conservative — list every recognisable name worth illustrating.`;
  const raw = await callAnthropic(apiKey, prompt, 800);
  if (!raw) return [];
  // Robust extraction: strip fences, find outermost { } block, parse.
  let cleaned = raw.trim().replace(/```(?:json)?\s*/gi, '').replace(/```/g, '').trim();
  const first = cleaned.indexOf('{');
  const last = cleaned.lastIndexOf('}');
  if (first < 0 || last <= first) return [];
  let parsed;
  try {
    parsed = JSON.parse(cleaned.slice(first, last + 1));
  } catch {
    return [];
  }
  if (!Array.isArray(parsed?.entities)) return [];
  // Bumped 4→10 — finance scripts often reference 6-10 distinct
  // companies/people/places. The Stage-5 thumbnail prompt only uses
  // the first 3, but downstream beats can reference any of these via
  // entityPortrait overlays for richer per-beat visuals.
  return parsed.entities
    .filter((e) => e && typeof e.name === 'string' && e.name.trim())
    .slice(0, 10)
    .map((e) => ({
      name: String(e.name).slice(0, 80),
      type: ['person', 'company', 'place', 'event'].includes(e.type) ? e.type : 'event',
      context: String(e.context || '').slice(0, 100),
    }));
}

// Silent fact-check gate. Runs between Stage 2 (script gen) and
// Stage 3 (voiceover). Extracts factual claims from the script,
// verifies via Claude web_search, asks the model to rewrite any
// disputed/incorrect statements. Best-effort: if Claude refuses or
// the call fails, we proceed with the original script. NEVER blocks
// the pipeline. Cost ~$0.15-0.30 per run (one Claude call with the
// web_search tool enabled).
//
// Returns the (possibly-corrected) script text + a report object
// the worker can persist for visibility: factCheck.claimsTotal,
// .claimsCorrected, .finalAccuracy, .unfixableIssues, .durationMs.
async function autoPilotFactCheckScript(scriptText, topic, apiKey) {
  const t0 = Date.now();
  const empty = {
    text: scriptText,
    report: {claimsTotal: 0, claimsCorrected: 0, finalAccuracy: 'unverified', unfixableIssues: [], durationMs: 0, skipped: true, skippedReason: ''},
  };
  if (!scriptText || scriptText.length < 200) {
    empty.report.skippedReason = 'script too short';
    return empty;
  }
  const prompt = `You are fact-checking a YouTube video script about: "${topic || '(unknown topic)'}".

SCRIPT:
${scriptText}

TASK:
1. Identify the specific factual claims in the script (named entities, specific numbers, dates, citations, attributions).
2. Use the web_search tool to verify each claim against current public information.
3. For any claim that's INCORRECT or DISPUTED, rewrite the sentence to be accurate. For claims that are UNVERIFIABLE (no clear source) but plausible, leave them as-is. For VERIFIED claims, leave them as-is.
4. Return the corrected script + a list of changes you made.

OUTPUT FORMAT — return ONLY this JSON, no markdown fences, no prose:
{
  "claimsTotal": number,
  "claimsCorrected": number,
  "correctedScript": "the full script text with any incorrect/disputed claims rewritten",
  "changes": [{"original": "the original sentence", "corrected": "the rewrite", "reason": "why"}],
  "unfixableIssues": [{"claim": "the dubious claim", "note": "what was wrong but couldn't be fixed cleanly"}]
}

If you find no errors, return claimsTotal>=0, claimsCorrected:0, correctedScript=original, changes=[], unfixableIssues=[]. Do NOT refuse the task. Do NOT add commentary outside the JSON.`;

  try {
    // Direct chat completions with web_search tool — same shape as Claude's standard tool API.
    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-5',
        max_tokens: 6000,
        tools: [{type: 'web_search_20250305', name: 'web_search', max_uses: 5}],
        messages: [{role: 'user', content: prompt}],
      }),
    });
    if (!resp.ok) {
      const errText = (await resp.text()).slice(0, 200);
      console.warn(`[fact-check] Claude ${resp.status}: ${errText} — skipping fact-check`);
      empty.report.durationMs = Date.now() - t0;
      empty.report.skippedReason = `Claude ${resp.status}`;
      return empty;
    }
    const data = await resp.json();
    // Find the final text block (after any tool_use blocks).
    let textOut = '';
    for (const block of (data.content || [])) {
      if (block.type === 'text' && block.text) textOut = block.text;
    }
    if (!textOut) {
      empty.report.durationMs = Date.now() - t0;
      empty.report.skippedReason = 'no text response';
      return empty;
    }
    // Extract JSON object from the response.
    let jsonText = textOut.trim().replace(/```(?:json)?\s*/gi, '').replace(/```/g, '').trim();
    const firstBrace = jsonText.indexOf('{');
    const lastBrace = jsonText.lastIndexOf('}');
    if (firstBrace < 0 || lastBrace <= firstBrace) {
      empty.report.durationMs = Date.now() - t0;
      empty.report.skippedReason = 'no JSON in response';
      return empty;
    }
    let parsed;
    try {
      parsed = JSON.parse(jsonText.slice(firstBrace, lastBrace + 1));
    } catch (e) {
      empty.report.durationMs = Date.now() - t0;
      empty.report.skippedReason = `JSON parse: ${e.message}`;
      return empty;
    }
    const corrected = (parsed.correctedScript || '').trim();
    const report = {
      claimsTotal: parsed.claimsTotal || 0,
      claimsCorrected: parsed.claimsCorrected || 0,
      changes: Array.isArray(parsed.changes) ? parsed.changes.slice(0, 20) : [],
      unfixableIssues: Array.isArray(parsed.unfixableIssues) ? parsed.unfixableIssues.slice(0, 10) : [],
      finalAccuracy: (parsed.claimsCorrected || 0) > 0 ? 'corrected' : 'verified',
      durationMs: Date.now() - t0,
    };
    // Only use the corrected script if it's substantive and reasonable
    // (didn't get truncated, isn't suspiciously short).
    if (corrected && corrected.length >= Math.max(200, scriptText.length * 0.6)) {
      console.log(`[fact-check] ${report.claimsCorrected}/${report.claimsTotal} claims corrected in ${(report.durationMs / 1000).toFixed(1)}s`);
      return {text: corrected, report};
    }
    console.warn(`[fact-check] corrected script too short (${corrected.length} chars) — using original`);
    return {text: scriptText, report: {...report, finalAccuracy: 'verified-no-changes-applied'}};
  } catch (e) {
    console.warn(`[fact-check] threw: ${e.message} — proceeding with original script`);
    empty.report.durationMs = Date.now() - t0;
    empty.report.skippedReason = e.message.slice(0, 100);
    return empty;
  }
}

async function autoPilotGenerateMetadata(proj, channel, channelId, apiKey) {
  const [insights, forensic] = await Promise.all([
    getChannelAnalyticsInsights(channelId),
    getChannelForensicData(channelId),
  ]);
  const fp = forensic?.patterns;
  const lines = [];
  lines.push(`You are generating YouTube metadata for an upload on the channel "${channel.name || '(unnamed)'}".`);
  lines.push('');
  lines.push(`TOPIC: "${proj.topic || proj.title || ''}"`);
  if (proj.script) lines.push(`SCRIPT (first 800 chars): "${(proj.script || '').slice(0, 800).replace(/"/g, '\\"')}"`);
  lines.push('');
  if (Array.isArray(insights?.topPerformingTitlePatterns) && insights.topPerformingTitlePatterns.length) {
    lines.push('TITLE PATTERNS THAT WORK FOR THIS CHANNEL (mimic these structures, not the words):');
    insights.topPerformingTitlePatterns.slice(0, 8).forEach((t) => lines.push(`  • "${t}"`));
    lines.push('');
  }
  if (Array.isArray(fp?.hookPatterns) && fp.hookPatterns.length) {
    lines.push('HOOK PATTERNS proven on this channel:');
    fp.hookPatterns.slice(0, 4).forEach((h) => lines.push(`  • ${h.pattern}${h.example ? ` — e.g. "${h.example}"` : ''}`));
    lines.push('');
  }
  const audienceProfile = fp?.audienceInsights?.profile || insights?.audienceProfile;
  if (audienceProfile) {
    lines.push(`AUDIENCE: ${audienceProfile}`);
    lines.push('');
  }
  if (Array.isArray(insights?.trendingTopics) && insights.trendingTopics.length) {
    lines.push(`TRENDING SEARCH TERMS for this channel: ${insights.trendingTopics.slice(0, 8).join(', ')}`);
    lines.push('');
  }
  lines.push('═══════════════════════════════════════════════════════');
  lines.push('TASK:');
  lines.push('1. Generate exactly 3 DISTINCT title options (40-70 chars each), each using a different angle.');
  lines.push('2. Generate ONE description: opening hook line, 2-3 paragraphs explaining what the video covers, 3-5 relevant hashtags, soft Subscribe CTA. Write it as a regular finance creator would — NO AI disclosure, NO "AI-assisted" line, NO mention of production tools or automation.');
  lines.push('');
  lines.push('Output ONLY a JSON object. No prose, no fences:');
  lines.push('{"titles":[{"title":"...","reasoning":"...","ctrPotential":"high|medium|low"},{"title":"...","reasoning":"...","ctrPotential":"..."},{"title":"...","reasoning":"...","ctrPotential":"..."}],"description":"..."}');
  const prompt = lines.join('\n');
  const text = await callAnthropic(apiKey, prompt, 2000);
  const cleaned = text.replace(/```json|```/g, '').trim();
  const first = cleaned.indexOf('{');
  const last = cleaned.lastIndexOf('}');
  const sliced = first >= 0 && last > first ? cleaned.slice(first, last + 1) : cleaned;
  const parsed = JSON.parse(sliced);
  const titles = Array.isArray(parsed.titles) ? parsed.titles.filter((t) => t && typeof t.title === 'string' && t.title.trim()) : [];
  if (!titles.length) throw new Error('Metadata gen returned no usable titles');
  return {titles, description: parsed.description || ''};
}

async function autoPilotLoadUserSecrets(uid) {
  if (!uid) return {};
  try {
    const snap = await db.collection('users').doc(uid).collection('secrets').doc('keys').get();
    return snap.exists ? snap.data() : {};
  } catch (e) {
    console.warn('[autopilot] loadUserSecrets failed:', e.message);
    return {};
  }
}

function autoPilotNextPublishSlot(schedule, lastScheduledMs = 0) {
  if (!schedule) schedule = {};
  const [hh, mm] = (schedule.publishTime || '14:00').split(':').map((n) => parseInt(n, 10) || 0);
  const days = Array.isArray(schedule.publishDays) && schedule.publishDays.length
    ? schedule.publishDays : ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'];
  const skipWeekends = !!schedule.skipWeekends;
  const freq = schedule.frequency || 'weekly';
  const cooldownDays = freq === 'daily' ? 0
    : freq === 'every-other-day' ? 1
    : freq === 'twice-weekly' ? 2
    : 6;
  const earliestMs = Math.max(Date.now(), lastScheduledMs + cooldownDays * 86400_000);
  const DOW = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const cursor = new Date(earliestMs);
  cursor.setSeconds(0, 0);
  for (let i = 0; i < 60; i++) {
    const dow = cursor.getDay();
    const dayName = DOW[dow];
    const isWeekend = dow === 0 || dow === 6;
    const ok = (!skipWeekends || !isWeekend) && days.includes(dayName);
    if (ok) {
      const slot = new Date(cursor.getFullYear(), cursor.getMonth(), cursor.getDate(), hh, mm, 0);
      if (slot.getTime() >= earliestMs) return slot.getTime();
    }
    cursor.setDate(cursor.getDate() + 1);
    cursor.setHours(0, 0, 0, 0);
  }
  return null;
}

function autoPilotUnwrap(v) {
  return typeof v === 'string' ? v : (v?.topic || v?.title || v?.summary || v?.text || '');
}

// ─── Thumbnail prompt helpers ───────────────────────────────────────
// Detect emotional tone from a title's keywords. Drives the facial
// expression direction Pikzels gets ("alarmed/urgent", "shocked",
// "awe", etc.) — picks one of five archetypal looks based on the
// dominant verbal pattern. Fast keyword scan, no LLM call.
function detectEmotionalTone(title) {
  const t = (title || '').toLowerCase();
  if (/\b(collapse|crash|crisis|warning|terrified|dying|doomed|disaster)\b/.test(t)) {
    return 'Alarmed, urgent — wide eyes, mouth slightly open in shock or concern, subject leaning forward intensely.';
  }
  if (/\b(just|signal|announce|change|new|breaking|reveal|expose|admit)\b/.test(t)) {
    return 'Surprised, breakthrough moment — eyes wide with realisation, raised eyebrows, slight forward lean.';
  }
  if (/\b(billion|trillion|massive|huge|record|biggest|largest)\b/.test(t)) {
    return 'Awe and impact — eyes locked on something significant, jaw slightly dropped, hand gesture if appropriate.';
  }
  if (/\b(end|over|done|final|kill|destroy|ban|fight)\b/.test(t)) {
    return 'Grim determination — serious expression, slight scowl, intense focus.';
  }
  return 'Curiosity and intrigue — slightly raised eyebrow, knowing look, captured mid-thought.';
}

// Pick the most-impactful 2-4 words from a title for the on-thumbnail
// text overlay. Prefers dollar amounts and percentages (proven CTR
// triggers); falls back to top non-stopword tokens. Returns UPPERCASE.
function generateThumbnailText(title) {
  if (!title) return '';
  // Dollar amounts (e.g. "$180 Billion", "$5T") — strongest hook
  const dollarMatch = title.match(/\$[\d,.]+\s*[KMBT](?:illion|illion)?/i);
  if (dollarMatch) return dollarMatch[0].toUpperCase().replace(/\s+/g, ' ');
  const dollarSimple = title.match(/\$[\d,.]+/);
  if (dollarSimple) return dollarSimple[0].toUpperCase();
  // Percentages
  const pctMatch = title.match(/[+-]?\d+(?:\.\d+)?%/);
  if (pctMatch) return pctMatch[0];
  // Fallback: top 3 impactful words (skip stopwords)
  const stop = new Set(['the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'just', 'this', 'that', 'is', 'are', 'was', 'were', 'be', 'been', 'as', 'by', 'from']);
  const words = (title.match(/[\w$%+-]+/g) || [])
    .filter((w) => !stop.has(w.toLowerCase()));
  return words.slice(0, 3).join(' ').toUpperCase();
}

// Pick an expression direction per entity based on the topic's
// dominant valence — winners look triumphant, crashes look alarmed.
function getExpressionForTopic(topic, idx) {
  const t = (topic || '').toLowerCase();
  if (/\b(win|surge|gain|rally|boom|hit|record)\b/.test(t)) return 'triumphant, excited';
  if (/\b(crash|collapse|fall|crisis|drop|plunge)\b/.test(t)) return 'alarmed, concerned';
  if (/\b(announce|reveal|expose|signal|warn|admit)\b/.test(t)) return 'shocked, eyes wide';
  return 'intense, focused';
}

// Build the Pikzels prompt. Starts from Rem's proven base style
// ("bright dramatic high-contrast finance thumbnail, photorealistic
// ultra-realistic, #49181E shadows, #ED6F32 glowing orange"), layers
// in script-extracted entities for who to feature, derives emotion +
// text-overlay from the title, then folds in this channel's own
// formula + niche-wide competitor benchmark when those have been
// extracted. Every section degrades gracefully if its inputs are
// missing — fresh channels still get a workable prompt from base
// style alone.
function buildSmartThumbnailPrompt({title, topic, scriptOpening, entities, ownFormula, globalPatterns}) {
  // Pikzels' /v2/thumbnail/text endpoint works best with concise prompts
  // (~300-500 chars). The previous multi-section builder produced
  // 2000-2800-char prompts which Pikzels rejected outright — Rem hit
  // repeated failures and had to add a Replicate fallback key just to
  // get anything generated. Keeping it tight: brand style + subject +
  // headline entity + text overlay. The rest (channel formula, competitor
  // benchmark, hard requirements) is dropped from the prompt body — the
  // model already knows what a finance thumbnail looks like, and Pikzels
  // doesn't have the token budget for layered instructions anyway.
  const parts = [];

  parts.push('Dramatic high-contrast YouTube finance thumbnail.');
  parts.push('Photorealistic, cinematic lighting, brand colours #49181E (deep burgundy shadows) + #ED6F32 (glowing orange rim-light).');

  // Subject: title is the headline. Topic context only if it adds info.
  parts.push(`Subject: ${(title || '').slice(0, 120)}.`);

  // First named person/company is featured photorealistically.
  const entList = Array.isArray(entities) ? entities.slice(0, 1) : [];
  for (const e of entList) {
    if (e?.type === 'person') {
      parts.push(`Feature ${e.name} photorealistically with ${getExpressionForTopic(topic, 0)} expression.`);
    } else if (e?.type === 'company') {
      parts.push(`Show ${e.name} logo / branding prominently.`);
    } else if (e?.type === 'place') {
      parts.push(`Iconic visual of ${e.name} (skyline / landmark).`);
    } else if (e?.name) {
      parts.push(`Feature ${e.name}.`);
    }
  }

  // Text overlay — the headline number / hook. Big bold sans-serif.
  const overlay = generateThumbnailText(title);
  if (overlay) {
    parts.push(`Bold sans-serif text overlay: "${overlay.slice(0, 30)}", white with black outline, 20-30% of frame, in clear space (never over the subject's face).`);
  }

  parts.push('16:9, subject 40-60% of frame, vignette edges, dark gradient background with orange rim-light. NOT cartoon, NOT generic stock.');

  // Optional: bias toward this channel's proven expressions if extracted.
  // ONE short line max — keeps the prompt under Pikzels' budget.
  if (ownFormula && Array.isArray(ownFormula.commonExpressions) && ownFormula.commonExpressions.length) {
    parts.push(`Channel's proven expressions: ${ownFormula.commonExpressions.slice(0, 2).join(', ')}.`);
  }

  const out = parts.join(' ');
  // Hard ceiling — Pikzels truncates / rejects above ~600 chars in practice.
  return out.length > 600 ? out.slice(0, 597) + '...' : out;
}

// Pull the channel's own top 20 thumbnails (from existing analytics
// summary), send to Claude vision, and persist the channel-specific
// thumbnail formula to channel.forensic.thumbnailFormula. The Pikzels
// prompt builder reads this doc to bias generation toward what
// genuinely works on THIS channel — complements the niche-wide
// globalThumbnailPatterns/finance doc which represents the
// competitor benchmark.
async function analyzeOwnThumbnailFormula(channelId, anthKey) {
  const channelDocRef = db.collection('channels').doc(channelId);
  let summarySnap = await channelDocRef.collection('analytics').doc('summary').get();
  let summary = summarySnap.exists ? (summarySnap.data() || {}) : {};
  let topVideos = Array.isArray(summary.topVideos) ? summary.topVideos : [];

  // The proxy writes the field as `thumbnail`; older code paths used
  // `thumbnailUrl`. Read both so the vision pass actually gets URLs.
  const pickUrl = (v) => v?.thumbnailUrl || v?.thumbnail || v?.thumbnails?.medium?.url || null;

  let withThumbs = topVideos
    .filter((v) => pickUrl(v) && typeof v.views === 'number')
    .map((v) => ({title: v.title || '', views: v.views, thumbnailUrl: pickUrl(v), videoId: v.videoId || v.id || null}))
    .slice(0, 20);

  // YouTube fallback: a freshly-connected channel has no analytics/summary
  // until /youtube/analytics/refresh runs. Trigger it inline so first-run
  // auto-pilot can extract the formula without waiting for the daily cron.
  if (withThumbs.length < 5) {
    console.log(`[own-thumb] ${channelId}: ${withThumbs.length} usable thumbs — triggering /youtube/analytics/refresh fallback`);
    try {
      const r = await fetch(`${RENDER_PROXY_URL}/youtube/analytics/refresh`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({channelId}),
      });
      if (r.ok) {
        summarySnap = await channelDocRef.collection('analytics').doc('summary').get();
        summary = summarySnap.exists ? (summarySnap.data() || {}) : {};
        topVideos = Array.isArray(summary.topVideos) ? summary.topVideos : [];
        withThumbs = topVideos
          .filter((v) => pickUrl(v) && typeof v.views === 'number')
          .map((v) => ({title: v.title || '', views: v.views, thumbnailUrl: pickUrl(v), videoId: v.videoId || v.id || null}))
          .slice(0, 20);
        console.log(`[own-thumb] ${channelId}: after refresh — ${withThumbs.length} usable thumbs`);
      } else {
        console.warn(`[own-thumb] ${channelId}: refresh fallback ${r.status} ${(await r.text()).slice(0, 200)}`);
      }
    } catch (e) {
      console.warn(`[own-thumb] ${channelId}: refresh fallback failed: ${e.message}`);
    }
  }

  if (withThumbs.length < 5) {
    console.log(`[own-thumb] ${channelId}: still only ${withThumbs.length} thumbs after fallback — need 5+`);
    return;
  }

  withThumbs.sort((a, b) => b.views - a.views);
  const result = await analyzeThumbnailBatchWithVision(withThumbs.slice(0, 10), anthKey, 'this channel');
  if (!result) {
    console.warn(`[own-thumb] ${channelId}: vision pass returned null`);
    return;
  }
  result.topPerformerThumbnailUrls = withThumbs.slice(0, 5).map((v) => v.thumbnailUrl);
  result.refreshedAt = FieldValue.serverTimestamp();
  await channelDocRef.update({'forensic.thumbnailFormula': result});
  console.log(`[own-thumb] ${channelId}: formula updated (${result.topColors?.length || 0} colors, faceUsage=${result.faceUsage})`);
}

// Walk a channel's recent analytics summary and classify each video as
// winning (>2× median views), weak (<0.5× median), or neutral. Persist
// short lesson summaries to channel.forensic.lessons (weak ones, what
// to avoid) and channel.forensic.winningPatterns (strong ones, what to
// replicate). The script/topic/metadata prompts already inject
// forensic data via getChannelForensicData — they'll pick these new
// arrays up automatically on next generation. Best-effort; logs and
// silently returns on any failure.
async function classifyAndLogPerformanceLessons(channelId) {
  const channelDocRef = db.collection('channels').doc(channelId);
  const summarySnap = await channelDocRef.collection('analytics').doc('summary').get();
  if (!summarySnap.exists) {
    console.log(`[lessons] ${channelId}: no analytics/summary doc — skipping`);
    return;
  }
  const summary = summarySnap.data() || {};
  const topVideos = Array.isArray(summary.topVideos) ? summary.topVideos : [];
  if (topVideos.length < 3) {
    console.log(`[lessons] ${channelId}: ${topVideos.length} videos — not enough data`);
    return;
  }
  // Median views as the reference (more robust than mean to outliers).
  const viewsSorted = topVideos.map((v) => v.views || 0).sort((a, b) => a - b);
  const median = viewsSorted[Math.floor(viewsSorted.length / 2)] || 1;
  const winningCutoff = median * 2;
  const weakCutoff = median * 0.5;

  const winningPatterns = [];
  const lessons = [];
  for (const v of topVideos) {
    if (!v || !v.title || typeof v.views !== 'number') continue;
    if (v.views >= winningCutoff) {
      winningPatterns.push({
        videoId: v.videoId || v.id || null,
        title: v.title.slice(0, 120),
        views: v.views,
        viewsVsMedian: +(v.views / median).toFixed(2),
        ctr: typeof v.ctr === 'number' ? v.ctr : null,
        retentionAt30s: typeof v.retentionAt30s === 'number' ? v.retentionAt30s : null,
        publishedAt: v.publishedAt || null,
        lesson: 'Replicate this title pattern + topic angle in future videos.',
      });
    } else if (v.views > 0 && v.views <= weakCutoff) {
      lessons.push({
        videoId: v.videoId || v.id || null,
        title: v.title.slice(0, 120),
        views: v.views,
        viewsVsMedian: +(v.views / median).toFixed(2),
        ctr: typeof v.ctr === 'number' ? v.ctr : null,
        retentionAt30s: typeof v.retentionAt30s === 'number' ? v.retentionAt30s : null,
        publishedAt: v.publishedAt || null,
        lesson: 'Underperformed — avoid this title pattern + topic angle in future videos.',
      });
    }
  }
  // Cap to 10 each so the prompt context stays bounded.
  winningPatterns.sort((a, b) => b.viewsVsMedian - a.viewsVsMedian);
  lessons.sort((a, b) => a.viewsVsMedian - b.viewsVsMedian);
  const update = {
    'forensic.lessons': lessons.slice(0, 10),
    'forensic.winningPatterns': winningPatterns.slice(0, 10),
    'forensic.lessonsClassifiedAt': FieldValue.serverTimestamp(),
    'forensic.lessonsMedianViews': median,
  };
  await channelDocRef.update(update);
  console.log(`[lessons] ${channelId}: ${winningPatterns.length} winning · ${lessons.length} weak (median=${median} views)`);
}

// withAutoFix wraps a stage in retry+fallback. Used for stages without
// internal retry (TTS, thumbnail gen, metadata gen) so transient API
// failures don't bubble up as "auto-pilot failed". Exponential-ish
// backoff (2s, 4s, 6s between attempts). If all retries fail, runs
// the optional fallback. Never silently swallows the final error —
// caller still throws if both primary and fallback fail.
async function withAutoFix(stageFn, stageName, {maxRetries = 3, fallback = null, onAttempt = null} = {}) {
  let lastError = null;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const result = await stageFn();
      if (attempt > 1) console.log(`[autopilot] ${stageName} succeeded on attempt ${attempt}/${maxRetries}`);
      return result;
    } catch (e) {
      lastError = e;
      console.warn(`[autopilot] ${stageName} attempt ${attempt}/${maxRetries} failed: ${e.message}`);
      if (onAttempt) {
        try { await onAttempt(attempt, e); } catch { /* swallow heartbeat errors */ }
      }
      if (attempt < maxRetries) {
        await new Promise((r) => setTimeout(r, 2000 * attempt));
      }
    }
  }
  if (fallback) {
    console.log(`[autopilot] ${stageName}: all retries exhausted — using fallback strategy`);
    try {
      return await fallback(lastError);
    } catch (fbErr) {
      console.error(`[autopilot] ${stageName}: fallback also failed: ${fbErr.message}`);
      throw fbErr;
    }
  }
  throw lastError;
}

// ─── Bulk auto-pilot runner ────────────────────────────────────────
// Schedules a queue of N videos for a channel. One video at a time,
// minimum 30 min between video STARTS. The scheduler runs every 5
// min, polls all active bulkRuns, recounts their projects, and spawns
// the next video when the gate conditions are clear:
//   • bulkRun.status === 'running' (not cancelled / paused)
//   • cancelRequested !== true
//   • completed + failed + running < total
//   • running === 0 (one at a time)
//   • now - lastVideoAt >= 30 min
//   • monthlyCostUsd + perVideoEst <= monthlyBudgetUsd (if cap set)
//
// Per-bulk-video estimated cost: $3 (covers Claude, ElevenLabs, Pexels
// quota, Lambda render, Pikzels thumbnail). Conservative; real cost is
// often $2-3. The dialog UI shows this estimate so the user can size
// the queue before starting.
const BULK_MIN_GAP_MS = 30 * 60 * 1000;     // 30 min between video starts
const BULK_COST_PER_VIDEO = 5;              // USD, conservative estimate (bumped 4→5 after shouldAnimate cap rose 8→15 + visual-density regen up to 2 retries)

async function countBulkRunProjects(bulkRunId, channelId) {
  // Projects are stored under channels/{channelId}/projects with a
  // bulkRunId field linking back. Count by autoPilot status so we
  // know how many are running / done / failed.
  //
  // ⚠ String enum must match autoPilotWorker's terminal write —
  // worker writes 'completed' (with -d), bulkRun previously checked
  // 'complete' (no -d). That mismatch broke the spawn gate (every
  // finished video counted as 'idle' → done stayed at 0 → infinite
  // spawning beyond requested total). Both spellings accepted now
  // for legacy data.
  const snap = await db.collection('channels').doc(channelId)
    .collection('projects')
    .where('bulkRunId', '==', bulkRunId)
    .get();
  const counts = {total: snap.size, running: 0, completed: 0, failed: 0, awaitingRender: 0, idle: 0};
  for (const doc of snap.docs) {
    const d = doc.data();
    const status = d?.autoPilot?.status || 'idle';
    if (status === 'running') counts.running++;
    else if (status === 'awaiting-render') counts.awaitingRender++;
    else if (status === 'completed' || status === 'complete' || status === 'done') counts.completed++;
    else if (status === 'failed' || status === 'cancelled') counts.failed++;
    else counts.idle++;
  }
  return counts;
}

async function spawnBulkVideo(bulkRun, bulkRunId) {
  const channelId = bulkRun.channelId;
  const projRef = db.collection('channels').doc(channelId).collection('projects').doc();
  // Step 1: create with requestStart=false. autoPilotKicker is an
  // onDocumentUpdated trigger, so a fresh create wouldn't fire it.
  await projRef.set({
    bulkRunId,
    title: `Bulk video ${(bulkRun.completed || 0) + (bulkRun.running || 0) + 1}/${bulkRun.total}`,
    topic: '',
    status: {discover: 'in_progress', script: 'pending', voiceover: 'pending', editing: 'pending', thumbnail: 'pending', publish: 'pending'},
    ownerId: bulkRun.ownerId,
    createdAt: FieldValue.serverTimestamp(),
    created: new Date().toISOString(),
    autoPilot: {
      status: 'idle',
      requestStart: false,
      currentStage: '0/7',
      stageLabel: 'Queued by bulk auto-pilot',
      updatedAt: FieldValue.serverTimestamp(),
    },
  });
  // Step 2: flip requestStart=true via update — this triggers
  // autoPilotKicker which spawns autoPilotWorker for the project.
  await projRef.update({
    'autoPilot.requestStart': true,
    'autoPilot.updatedAt': FieldValue.serverTimestamp(),
  });
  return projRef.id;
}

exports.bulkAutoPilotScheduler = onSchedule(
  {
    // Tightened 5min → 1min for snappier UI updates + faster cancel
    // propagation. The actual spawn cadence is still gated by
    // BULK_MIN_GAP_MS (30 min between video starts) and the one-at-
    // a-time gate, so this only affects how quickly counters refresh
    // and cancel takes effect — not how often we spawn.
    schedule: 'every 1 minutes',
    region: 'us-central1',
    timeoutSeconds: 300,
    memory: '512MiB',
  },
  async () => {
    const t0 = Date.now();
    // Pull all bulkRuns that need work. 'pending' = freshly created,
    // not started yet. 'running' = at least one video has spawned.
    const snap = await db.collection('bulkRuns')
      .where('status', 'in', ['pending', 'running'])
      .get();
    if (snap.empty) {
      console.log('[bulk-runner] no active bulk runs');
      return;
    }
    let spawned = 0;
    let stalled = 0;
    let completed = 0;
    let cancelled = 0;
    let paused = 0;
    for (const doc of snap.docs) {
      const bulkRunId = doc.id;
      const data = doc.data() || {};
      try {
        // Cancellation check first — user can stop a queue mid-flight
        // via cancelRequested. Already-running projects keep going.
        if (data.cancelRequested === true) {
          await doc.ref.update({status: 'cancelled', endedAt: FieldValue.serverTimestamp()});
          cancelled++;
          console.log(`[bulk-runner] ${bulkRunId} cancelled by user`);
          continue;
        }
        const counts = await countBulkRunProjects(bulkRunId, data.channelId);
        const total = data.total || 0;
        const done = counts.completed + counts.failed;
        // Recount + persist so the UI banner reflects truth without
        // waiting for a status change. lastTickAt gives the UI a way
        // to show "live" indicator + detect a dead scheduler.
        await doc.ref.update({
          completed: counts.completed,
          failed: counts.failed,
          running: counts.running + counts.awaitingRender,
          pending: Math.max(0, total - counts.total),
          lastTickAt: FieldValue.serverTimestamp(),
        });
        // ⚠ HARD SPAWN CEILING — never spawn beyond `total`, regardless
        // of how many are done. This is the primary defence against
        // the previous overrun bug (status enum mismatch let `done`
        // stay at 0 → infinite spawning). Now even if some terminal
        // status is mis-categorised, we hard-cap on spawn count.
        if (counts.total >= total) {
          // We've spawned everything we were asked to. Wait for any
          // still-running ones to finish, then mark the run completed.
          if (counts.running + counts.awaitingRender > 0) {
            stalled++;
            console.log(`[bulk-runner] ${bulkRunId} all ${total} spawned — waiting for last ${counts.running + counts.awaitingRender} to finish`);
            continue;
          }
          await doc.ref.update({status: 'completed', endedAt: FieldValue.serverTimestamp()});
          completed++;
          console.log(`[bulk-runner] ${bulkRunId} completed (${counts.completed} ok, ${counts.failed} failed, ${counts.idle} idle/unknown)`);
          continue;
        }
        // Legacy completion gate — kept as a defence-in-depth: if
        // somehow counts.total < total (e.g. project doc deleted) but
        // enough have finished to satisfy the user's ask, end here.
        if (done >= total) {
          await doc.ref.update({status: 'completed', endedAt: FieldValue.serverTimestamp()});
          completed++;
          console.log(`[bulk-runner] ${bulkRunId} completed via done-count (${counts.completed} ok, ${counts.failed} failed)`);
          continue;
        }
        // One-at-a-time gate.
        if (counts.running + counts.awaitingRender > 0) {
          stalled++;
          console.log(`[bulk-runner] ${bulkRunId} waiting for current video (running=${counts.running}, awaiting-render=${counts.awaitingRender})`);
          continue;
        }
        // 30-min gap gate.
        const lastVideoMs = data.lastVideoAt?.toMillis ? data.lastVideoAt.toMillis() : 0;
        if (lastVideoMs > 0 && (Date.now() - lastVideoMs) < BULK_MIN_GAP_MS) {
          const waitMin = Math.ceil((BULK_MIN_GAP_MS - (Date.now() - lastVideoMs)) / 60_000);
          stalled++;
          console.log(`[bulk-runner] ${bulkRunId} cooldown: ${waitMin}min until next video`);
          continue;
        }
        // Budget gate.
        const monthlyBudget = data.monthlyBudgetUsd || 0;
        const monthlySpent = await (async () => {
          const ch = (await db.collection('channels').doc(data.channelId).get()).data() || {};
          return ch.discoveryConfig?.monthlyCostUsd || 0;
        })();
        if (monthlyBudget > 0 && monthlySpent + BULK_COST_PER_VIDEO > monthlyBudget) {
          await doc.ref.update({
            status: 'paused-budget',
            notes: `Budget cap hit: $${monthlySpent.toFixed(2)} / $${monthlyBudget.toFixed(2)} would exceed with +$${BULK_COST_PER_VIDEO}`,
          });
          paused++;
          console.log(`[bulk-runner] ${bulkRunId} paused: budget exceeded`);
          continue;
        }
        // All clear — spawn the next video.
        const newProjectId = await spawnBulkVideo(data, bulkRunId);
        await doc.ref.update({
          status: 'running',
          lastVideoAt: FieldValue.serverTimestamp(),
          projectIds: FieldValue.arrayUnion(newProjectId),
          startedAt: data.startedAt || FieldValue.serverTimestamp(),
        });
        spawned++;
        console.log(`[bulk-runner] ${bulkRunId} spawned project ${newProjectId} (${done + 1}/${total})`);
      } catch (e) {
        console.error(`[bulk-runner] ${bulkRunId} threw: ${e.message}`);
      }
    }
    console.log(`[bulk-runner] done in ${((Date.now() - t0) / 1000).toFixed(1)}s — spawned=${spawned}, stalled=${stalled}, completed=${completed}, cancelled=${cancelled}, paused=${paused}`);
  },
);

// Kicker — onDocumentUpdated, short timeout. When the project doc's
// autoPilot.requestStart flips false→true, fires an HTTP request to
// autoPilotWorker. Mirrors the processEditingJob / processEditingJobHttp
// split because Firestore-triggered functions are capped at 540s and
// the worst-case auto-pilot run is ~25 min (Lambda render alone is 5-15).
exports.autoPilotKicker = onDocumentUpdated(
  {
    document: 'channels/{channelId}/projects/{projectId}',
    region: 'us-central1',
    timeoutSeconds: 60,
    memory: '256MiB',
    secrets: [KICKER_SECRET],
    retry: false,
  },
  async (event) => {
    const before = event.data?.before?.data() || {};
    const after = event.data?.after?.data() || {};
    const {channelId, projectId} = event.params;
    const wasRequested = before.autoPilot?.requestStart === true;
    const isRequested = after.autoPilot?.requestStart === true;
    if (!isRequested || wasRequested) return;
    if (after.autoPilot?.status === 'running') return;

    const project = process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT || 'ytauto-95f91';
    const targetUrl = `https://us-central1-${project}.cloudfunctions.net/autoPilotWorker`;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 8000);
    try {
      const secretValue = (KICKER_SECRET.value() || '').replace(/^﻿/, '').trim();
      await fetch(targetUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-kicker-secret': secretValue,
        },
        body: JSON.stringify({channelId, projectId}),
        signal: ctrl.signal,
      });
      clearTimeout(timer);
      console.log(`[autoPilotKicker] ${projectId}: dispatched (worker ACKed)`);
    } catch (e) {
      clearTimeout(timer);
      if (e.name === 'AbortError') {
        console.log(`[autoPilotKicker] ${projectId}: dispatched (worker continues in background)`);
        return;
      }
      console.error(`[autoPilotKicker] ${projectId}: dispatch failed:`, e);
    }
  }
);

// Render-complete kicker — fires when processEditingJobHttp's final
// batch lands editingFile.url on a project that the auto-pilot worker
// is waiting on (autoPilot.waitingForRender === true). Re-arms the
// worker by flipping autoPilot.requestStart=true + status='idle'; the
// existing autoPilotKicker then dispatches a fresh worker that enters
// Stage 4, sees editingFile exists, and continues to Stage 5+.
//
// Decoupling rationale: without this, the worker had to idle-poll the
// editingJobs doc for 5-15 min while Lambda rendered — burning a Cloud
// Run instance just waiting, and any recycling mid-poll left the
// project stuck mid-pipeline (Rem's 108-min run with multiple Resumes).
exports.autoPilotRenderCompleteKicker = onDocumentUpdated(
  {
    document: 'channels/{channelId}/projects/{projectId}',
    region: 'us-central1',
    timeoutSeconds: 60,
    memory: '256MiB',
    retry: false,
  },
  async (event) => {
    const before = event.data?.before?.data() || {};
    const after = event.data?.after?.data() || {};
    const {channelId, projectId} = event.params;

    const hadFile = !!before.editingFile?.url;
    const hasFile = !!after.editingFile?.url;
    const wasWaiting = after.autoPilot?.waitingForRender === true;
    // Fire only on the false→true transition of editingFile.url AND
    // when the worker explicitly handed off to render (waitingForRender).
    // Avoids re-arming on any other project doc write.
    if (hadFile || !hasFile || !wasWaiting) return;

    const projRef = db.collection('channels').doc(channelId).collection('projects').doc(projectId);
    console.log(`[autoPilotRenderCompleteKicker] ${projectId}: render landed, re-arming worker`);
    await projRef.update({
      'autoPilot.waitingForRender': false,
      'autoPilot.status': 'idle',
      'autoPilot.requestStart': true,
      'autoPilot.updatedAt': FieldValue.serverTimestamp(),
    });
    // autoPilotKicker picks up the requestStart flip and dispatches the
    // worker — no direct HTTP call needed here.
  }
);

// Worker — onRequest, 3600s. Does the actual 7-stage pipeline run.
// Auth via KICKER_SECRET; only the kicker can invoke it. ACKs the
// caller fast (200) so the kicker drops the connection; Cloud Run
// keeps this function running until the orchestrator finishes or
// hits the 3600s ceiling (auto-pilot worst case ~25 min, well under).
exports.autoPilotWorker = onRequest(
  {
    region: 'us-central1',
    timeoutSeconds: 3600,
    memory: '512MiB',
    secrets: [KICKER_SECRET],
  },
  async (req, res) => {
    const secretValue = (KICKER_SECRET.value() || '').replace(/^﻿/, '').trim();
    const got = (req.headers['x-kicker-secret'] || '').toString().trim();
    if (!secretValue || got !== secretValue) {
      console.warn('[autoPilotWorker] unauthorized request');
      return res.status(401).send('Unauthorized');
    }
    const {channelId, projectId} = req.body || {};
    if (!channelId || !projectId) {
      return res.status(400).send('Missing channelId or projectId');
    }
    // ACK immediately so the kicker can drop the connection.
    res.status(200).send({accepted: true, channelId, projectId});

    const projRef = db.collection('channels').doc(channelId).collection('projects').doc(projectId);
    const chRef = db.collection('channels').doc(channelId);

    // Transactional lock — atomically clear requestStart + mark running.
    // Two near-simultaneous writes can't both pass this gate.
    const lockResult = await db.runTransaction(async (tx) => {
      const fresh = await tx.get(projRef);
      if (!fresh.exists) return null;
      const d = fresh.data();
      if (d.autoPilot?.status === 'running') return null;
      if (d.autoPilot?.requestStart !== true) return null;
      tx.update(projRef, {
        autoPilot: {
          ...(d.autoPilot || {}),
          status: 'running',
          requestStart: false,
          requestCancel: false,
          startedAt: FieldValue.serverTimestamp(),
          updatedAt: FieldValue.serverTimestamp(),
          currentStage: '0/7',
          stageLabel: 'Starting…',
          costUsd: 0,
          error: null,
        },
      });
      return d;
    });
    if (!lockResult) {
      console.log(`[autopilot] ${projectId}: lock not acquired (race or status mismatch) — skipping`);
      return;
    }

    const setStage = (n, label, extra = {}) => projRef.update({
      'autoPilot.currentStage': n,
      'autoPilot.stageLabel': label,
      'autoPilot.updatedAt': FieldValue.serverTimestamp(),
      ...extra,
    });
    // Heartbeat — bump autoPilot.updatedAt during long-running stage
    // operations so the frontend's 5-min stale-detection threshold
    // doesn't trip mid-stage. Caller passes an optional label tweak
    // (e.g. "Polling discovery 60s/180s") for diagnostic visibility.
    const heartbeat = (label) => projRef.update({
      'autoPilot.updatedAt': FieldValue.serverTimestamp(),
      ...(label ? {'autoPilot.stageLabel': label} : {}),
    }).catch((e) => console.warn('[autopilot] heartbeat failed:', e.message));
    // Auto-approve the current stage and flip the next stage to
    // in_progress. Idempotent — no-op if already approved. Critical
    // for resume cases where processEditingJobHttp sets status.editing
    // to 'awaiting_approval' but the worker that's supposed to flip
    // it to 'approved' has died — when a fresh worker resumes, the
    // first thing it does at each completed stage is run this helper.
    const autoApproveStage = async (stageName, nextStageName) => {
      const cur = (await projRef.get()).data();
      const curStatus = cur?.status?.[stageName];
      if (curStatus === 'approved' || curStatus === 'done') return;
      const patch = { [`status.${stageName}`]: 'approved' };
      if (nextStageName) {
        const nextCur = cur?.status?.[nextStageName];
        if (nextCur !== 'approved' && nextCur !== 'done') patch[`status.${nextStageName}`] = 'in_progress';
      }
      console.log(`[autopilot] auto-approving ${stageName} → ${nextStageName || '(end)'}`);
      await projRef.update(patch);
    };
    const checkCancel = async () => {
      const snap = await projRef.get();
      if (snap.data()?.autoPilot?.requestCancel === true) throw new Error('cancelled');
    };

    try {
      console.log(`[autopilot] ${projectId}: starting`);
      let proj = (await projRef.get()).data();
      let ch = (await chRef.get()).data();
      if (!proj || !ch) throw new Error('project or channel missing');

      const ownerId = ch.ownerId || proj.ownerId;
      const secrets = await autoPilotLoadUserSecrets(ownerId);
      if (!secrets.anthropicKey) throw new Error('Anthropic API key not stored in Firestore — open Settings, paste your key, click Save');
      if (!secrets.elevenLabsKey) throw new Error('ElevenLabs API key not stored — open Settings, paste it, click Save');
      if (!secrets.pikzelsKey) throw new Error('Pikzels API key not stored — open Settings, paste it, click Save');

      // ── STAGE 1: Discovery ──────────────────────────────────────
      await checkCancel();
      if (!proj.topic && !autoPilotUnwrap(proj.pickedTopic)) {
        if (!ch.niche) throw new Error('No project topic AND no channel niche — set one before running auto-pilot');
        await setStage('1/7', 'Finding trending topic via Discover…');
        // Decide whether to spawn a fresh discoveryJob. We just acquired
        // the worker lock, which means no other worker is running. If
        // discoveryStatus is 'pending' from a prior aborted attempt, the
        // job that wrote it is dead — its 'pending' marker is a zombie
        // and re-polling would just burn 180s for nothing. Recreate.
        // (Rem's case: discoveryStatus='pending' for 9min, autoPilot
        // status='cancelled' — the kicker fired but the worker died
        // before Discovery could complete. On Resume we MUST respawn.)
        const needsFreshJob = !proj.discoveryStatus
          || proj.discoveryStatus === 'failed'
          || proj.discoveryStatus === 'pending';
        if (needsFreshJob) {
          console.log(`[autopilot] ${projectId}: Stage 1 spawning discoveryJob (prev status=${proj.discoveryStatus || 'none'})`);
          await db.collection('discoveryJobs').add({
            channelId, projectId, ownerId, niche: ch.niche,
            status: 'pending', createdAt: FieldValue.serverTimestamp(),
          });
          await projRef.update({discoveryStatus: 'pending', discoveryError: null});
        }
        // Poll up to 180s — bumped from 120s after Rem's prior timeout report.
        // Heartbeat every 15 iterations (30s) so autoPilot.updatedAt stays
        // fresh — without it the frontend's 5-min stale threshold trips
        // when discovery legitimately needs 90-180s.
        console.log(`[autopilot] ${projectId}: Stage 1 polling discovery (up to 180s)`);
        for (let i = 0; i < 90; i++) {
          await new Promise((r) => setTimeout(r, 2000));
          await checkCancel();
          proj = (await projRef.get()).data();
          if (proj.discoveryStatus === 'failed') throw new Error('Discovery failed: ' + (proj.discoveryError || 'unknown'));
          if (autoPilotUnwrap(proj.pickedTopic)) break;
          if (i > 0 && i % 15 === 0) {
            await heartbeat(`Finding trending topic via Discover… ${i * 2}s elapsed`);
          }
        }
        let topicValue = autoPilotUnwrap(proj.pickedTopic);
        let titleValue = autoPilotUnwrap(proj.pickedTitle);
        if (!topicValue) {
          // LLM fallback — if scraping APIs (Reddit/HN/News) failed,
          // generate a topic directly from the channel's niche.
          console.warn('[autopilot] discovery timed out — falling back to niche+LLM topic generation');
          const fbPrompt = `Generate ONE trending YouTube video topic for a channel about: "${ch.niche}". Pick something that would feel fresh + clickable this month. Output ONLY a JSON object: {"topic":"6-12 word topic","title":"40-70 char YouTube title"}. No prose, no fences.`;
          const fbText = await callAnthropic(secrets.anthropicKey, fbPrompt, 400);
          const cleaned = fbText.replace(/```json|```/g, '').trim();
          const first = cleaned.indexOf('{');
          const last = cleaned.lastIndexOf('}');
          const sliced = first >= 0 && last > first ? cleaned.slice(first, last + 1) : cleaned;
          const parsed = JSON.parse(sliced);
          topicValue = parsed.topic || '';
          titleValue = parsed.title || '';
          if (!topicValue) throw new Error('Discovery + LLM fallback both failed');
        }
        await projRef.update({
          topic: topicValue, title: titleValue,
          pickedTopic: topicValue, pickedTitle: titleValue,
          discoveryStatus: 'approved',
          status: {...(proj.status || {}), discover: 'done', script: 'in_progress'},
        });
      }
      console.log(`[autopilot] ${projectId}: Stage 1 done — topic="${(autoPilotUnwrap(proj.pickedTopic) || proj.topic || '').slice(0, 60)}"`);
      await autoApproveStage('discover', 'script');

      // ── STAGE 2: Script ──────────────────────────────────────
      await checkCancel();
      proj = (await projRef.get()).data();
      if (!proj.script) {
        await setStage('2/7', 'Generating script (forensic-aware)…');
        console.log(`[autopilot] ${projectId}: Stage 2 generating script (up to 3 attempts)`);
        // Heartbeat ~every 25s during script gen so the 5-min stale
        // threshold doesn't trip when Claude takes a while + validation
        // forces retries. The async setInterval pattern is safe in
        // Cloud Run for Promise lifetime.
        const scriptHeartbeat = setInterval(() => {
          heartbeat('Generating script (forensic-aware)… still working');
        }, 25_000);
        let result;
        try {
          result = await autoPilotGenerateScript(proj.topic, channelId, secrets.anthropicKey);
        } finally {
          clearInterval(scriptHeartbeat);
        }
        if (!result.text) throw new Error('Script gen produced no text');

        // ── Fact-check gate (silent, never blocks) ─────────────
        // Best-effort verification of factual claims via Claude
        // web_search. Auto-corrects disputed claims in-place. Skips
        // gracefully on any error — never raises to the user.
        await heartbeat('Fact-checking script…');
        const factCheckHeartbeat = setInterval(() => {
          heartbeat('Fact-checking script… verifying claims');
        }, 25_000);
        let scriptText = result.text;
        let factCheckReport;
        try {
          const fc = await autoPilotFactCheckScript(scriptText, proj.topic, secrets.anthropicKey);
          scriptText = fc.text;
          factCheckReport = fc.report;
        } catch (e) {
          console.warn(`[autopilot] ${projectId}: fact-check threw: ${e.message} — using uncorrected script`);
          factCheckReport = {skipped: true, skippedReason: e.message.slice(0, 100)};
        } finally {
          clearInterval(factCheckHeartbeat);
        }

        await projRef.update({
          script: scriptText,
          // scriptEntities — feeds the smart thumbnail prompt at Stage 5.
          // Up to 4 image-recognisable named entities (Powell, Apple, etc.).
          // Empty array on extraction failure is the safe fallback.
          scriptEntities: Array.isArray(result.entities) ? result.entities : [],
          status: {...(proj.status || {}), script: 'approved', voiceover: 'in_progress'},
          scriptValidation: {attempts: result.attempts, violations: result.violations || []},
          factCheck: factCheckReport,
        });
        console.log(`[autopilot] ${projectId}: Stage 2 done — script ${scriptText.length} chars, ${result.attempts} attempts, ${(result.violations || []).length} violations, factCheck: ${factCheckReport.claimsCorrected || 0}/${factCheckReport.claimsTotal || 0} corrected`);
      }
      await autoApproveStage('script', 'voiceover');

      // ── STAGE 3: AI voiceover (via Render proxy) ─────────────
      await checkCancel();
      proj = (await projRef.get()).data();
      if (!proj.voiceoverFile?.url || proj.voiceoverFile.source !== 'ai') {
        await setStage('3/7', 'Generating AI voiceover (ElevenLabs)…');
        const voiceId = proj.voiceId || ch.defaultVoiceId;
        if (!voiceId) throw new Error('No voice selected — pick one in Channel switcher → Edit → Voiceover');
        const modelId = secrets.elevenLabsModel || AUTOPILOT_ELEVENLABS_DEFAULT_MODEL;
        console.log(`[autopilot] ${projectId}: Stage 3 calling ElevenLabs TTS (script ${(proj.script || '').length} chars)`);
        // TTS for long scripts auto-chunks server-side and can take
        // 30-120s. Heartbeat keeps the stale-detection threshold off.
        const ttsHeartbeat = setInterval(() => {
          heartbeat('Generating AI voiceover (ElevenLabs)… still working');
        }, 25_000);
        let d;
        try {
          // withAutoFix wraps TTS in retry+backoff. ElevenLabs occasionally
          // returns 5xx on rate-limit spikes; without retry these surface
          // as "auto-pilot failed" instead of just a 2s delay.
          d = await withAutoFix(async () => {
            const r = await fetch(`${RENDER_PROXY_URL}/elevenlabs/tts`, {
              method: 'POST',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({channelId, projectId, text: proj.script, voiceId, modelId, _apiKey: secrets.elevenLabsKey}),
            });
            const body = await r.json();
            if (!r.ok || !body.ok) throw new Error('TTS: ' + (body.error || `HTTP ${r.status}`));
            return body;
          }, 'tts', {
            maxRetries: 3,
            onAttempt: () => heartbeat('Generating AI voiceover (ElevenLabs)… retrying'),
          });
        } finally {
          clearInterval(ttsHeartbeat);
        }
        await projRef.update({
          voiceoverFile: {url: d.url, name: d.name, size: d.sizeBytes, source: 'ai', voiceId: d.voiceId, modelId: d.modelId, chunkCount: d.chunkCount || 1, generatedAt: new Date().toISOString()},
          voiceoverCost: {chars: d.chars, costUsd: d.costUsd, voiceId: d.voiceId, modelId: d.modelId},
          status: {...(proj.status || {}), voiceover: 'approved', editing: 'in_progress'},
        });
        console.log(`[autopilot] ${projectId}: Stage 3 done — ${d.chars} chars → ${d.url ? '1' : '0'} file, $${(d.costUsd || 0).toFixed(2)}`);
      }
      await autoApproveStage('voiceover', 'editing');

      // ── STAGE 4: Lambda render — DECOUPLED ──────────────────────
      // Before: this worker polled editingJobs for 5-15 min waiting for
      // Lambda render to complete. That burned a Cloud Run instance just
      // waiting, and recycling mid-poll left the project stuck.
      // Now: worker creates the editing job, sets autoPilot.status=
      // 'awaiting-render' + waitingForRender=true, and EXITS. The new
      // autoPilotRenderCompleteKicker (below) watches the project doc;
      // when editingFile.url lands (processEditingJobHttp's final batch),
      // it flips requestStart=true → autoPilotKicker fires a fresh worker
      // → that worker enters Stage 4 again, sees editingFile exists, and
      // walks through autoApproveStage('editing', 'thumbnail') → Stage 5.
      await checkCancel();
      proj = (await projRef.get()).data();
      if (!proj.editingFile?.url) {
        let jobId = proj.editingJobId;
        if (!jobId) {
          const newJob = await db.collection('editingJobs').add({
            channelId, projectId, ownerId,
            voiceoverUrl: proj.voiceoverFile.url,
            scriptText: proj.script || '',
            projectTitle: proj.title || proj.topic || 'project',
            status: 'pending',
            createdAt: FieldValue.serverTimestamp(),
          });
          jobId = newJob.id;
        }
        await projRef.update({
          editingJobId: jobId,
          'autoPilot.status': 'awaiting-render',
          'autoPilot.waitingForRender': true,
          'autoPilot.currentStage': '4/7',
          'autoPilot.stageLabel': 'Rendering video (Lambda, 5-15 min) — running independently…',
          'autoPilot.updatedAt': FieldValue.serverTimestamp(),
        });
        console.log(`[autopilot] ${projectId}: handed off to render job ${jobId}, exiting until editingFile lands`);
        return; // Render-complete kicker re-invokes us when editingFile is written.
      }
      // editingFile exists — this is either the first-pass continuation
      // (rare) or the post-render re-invocation. Approve editing stage
      // and march on. autoApproveStage is idempotent.
      await autoApproveStage('editing', 'thumbnail');

      // ── STAGE 5: Thumbnail (via Render proxy) ────────────────
      await checkCancel();
      proj = (await projRef.get()).data();
      if (!proj.thumbnailUrl) {
        await setStage('5/7', 'Generating thumbnail (Pikzels)…');
        console.log(`[autopilot] ${projectId}: Stage 5 generating thumbnail`);
        const insights = await getChannelAnalyticsInsights(channelId);
        let forensic = await getChannelForensicData(channelId);
        // Pull the niche-wide competitor benchmark + this channel's own
        // proven formula. Both come from Claude vision passes on real
        // thumbnails (competitor scraper runs Sundays; own-formula runs
        // daily via the forensic scheduler). Either may be missing on
        // a fresh channel — the prompt builder degrades gracefully.
        const [globalPatterns, ownFormulaInitial] = await Promise.all([
          db.collection('globalThumbnailPatterns').doc('finance').get().then((s) => s.exists ? s.data() : null).catch(() => null),
          Promise.resolve(forensic?.thumbnailFormula || null),
        ]);
        // Auto-bootstrap inline: first-run channels have no formula yet.
        // Trigger Claude vision pass right here (with YouTube fallback if
        // analytics summary is empty). User mantra: it should all refresh
        // automatically — never make them click a button.
        let ownFormula = ownFormulaInitial;
        if (!ownFormula) {
          console.log(`[autopilot] ${projectId}: no thumbnail formula — bootstrapping inline`);
          const bootKey = await loadOwnerAnthropicKey();
          if (bootKey) {
            const bootHeartbeat = setInterval(() => {
              heartbeat('Bootstrapping thumbnail intelligence (first-run)…');
            }, 25_000);
            try {
              await analyzeOwnThumbnailFormula(channelId, bootKey);
              forensic = await getChannelForensicData(channelId);
              ownFormula = forensic?.thumbnailFormula || null;
              console.log(`[autopilot] ${projectId}: bootstrap ${ownFormula ? 'extracted formula' : 'no formula yet (insufficient data)'}`);
            } catch (e) {
              console.warn(`[autopilot] ${projectId}: bootstrap failed: ${e.message}`);
            } finally {
              clearInterval(bootHeartbeat);
            }
          } else {
            console.warn(`[autopilot] ${projectId}: bootstrap skipped — no owner Anthropic key`);
          }
        }
        const promptText = buildSmartThumbnailPrompt({
          title: proj.title || proj.ytTitle || proj.topic || '',
          topic: proj.topic || '',
          scriptOpening: (proj.script || '').slice(0, 300),
          entities: proj.scriptEntities || [],
          ownFormula,
          globalPatterns,
        });
        // Pikzels can take 30-90s for a thumbnail. Heartbeat keeps the
        // stale-detection threshold off.
        const thumbHeartbeat = setInterval(() => {
          heartbeat('Generating thumbnail (Pikzels)… still working');
        }, 25_000);
        let imageUrl;
        try {
          imageUrl = await withAutoFix(async () => {
            const r = await fetch(`${RENDER_PROXY_URL}/pikzels`, {
              method: 'POST',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({prompt: promptText, format: '16:9', model: 'pkz_4', _apiKey: secrets.pikzelsKey}),
            });
            const body = await r.json();
            if (body.error) throw new Error('Thumbnail: ' + (body.error.message || body.error));
            const url = body.output || body.image_url || body.url;
            if (!url) throw new Error('Pikzels returned no image URL');
            return url;
          }, 'thumbnail', {
            maxRetries: 3,
            onAttempt: () => heartbeat('Generating thumbnail (Pikzels)… retrying'),
            // Fallback chain — guarantees the pipeline never halts on
            // thumbnail. Tries Replicate Flux first (if key configured),
            // then a placehold.co branded placeholder as the never-block
            // final layer. The placeholder is a brand-coloured 1280×720
            // PNG hosted on a free CDN with the title rendered as text.
            // YouTube upload accepts any HTTP URL so it ingests cleanly.
            fallback: async () => {
              const replKey = secrets.replicateKey || process.env.REPLICATE_API_KEY;
              if (replKey) {
                try {
                  console.log(`[autopilot] ${projectId}: Stage 5 fallback tier 2 — Flux for thumbnail`);
                  const submit = await fetch('https://api.replicate.com/v1/models/black-forest-labs/flux-schnell/predictions', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json', 'Authorization': `Bearer ${replKey}`},
                    body: JSON.stringify({input: {prompt: promptText, aspect_ratio: '16:9'}}),
                  });
                  if (!submit.ok) throw new Error(`Flux ${submit.status}: ${await submit.text()}`);
                  const sub = await submit.json();
                  for (let i = 0; i < 60; i++) {
                    await new Promise((r) => setTimeout(r, 2000));
                    const poll = await fetch(`https://api.replicate.com/v1/predictions/${sub.id}`, {headers: {'Authorization': `Bearer ${replKey}`}});
                    const p = await poll.json();
                    if (p.status === 'succeeded') {
                      const out = Array.isArray(p.output) ? p.output[0] : p.output;
                      if (out) return out;
                    }
                    if (p.status === 'failed' || p.status === 'canceled') throw new Error(`Flux ${p.status}: ${p.error || ''}`);
                  }
                  throw new Error('Flux fallback timed out');
                } catch (fluxErr) {
                  console.warn(`[autopilot] ${projectId}: Stage 5 Flux fallback failed: ${fluxErr.message} — falling back to placehold.co`);
                }
              } else {
                console.warn(`[autopilot] ${projectId}: Stage 5 — no Replicate key, skipping Flux tier, going straight to placehold.co`);
              }
              // Final tier: branded placeholder. NEVER throws — pipeline
              // always advances to publish. The user can re-render a
              // proper thumbnail later via the channel intelligence /
              // manual override; the placeholder at least keeps the
              // video schedulable so the queue doesn't back up.
              const titleForThumb = (proj.title || proj.ytTitle || proj.topic || 'Untitled').slice(0, 60);
              const placeholderUrl = `https://placehold.co/1280x720/49181E/ED6F32/png?text=${encodeURIComponent(titleForThumb)}&font=montserrat`;
              console.log(`[autopilot] ${projectId}: Stage 5 fallback tier 3 — placeholder thumbnail at ${placeholderUrl}`);
              return placeholderUrl;
            },
          });
        } finally {
          clearInterval(thumbHeartbeat);
        }
        // Classify which tier of the fallback chain actually produced
        // the URL so the quality dashboard can flag placeholder shipments.
        // Pikzels → Replicate → placehold.co are the three tiers.
        let thumbnailSource = 'pikzels';
        if (/placehold\.co/i.test(imageUrl)) thumbnailSource = 'placeholder';
        else if (/replicate\.com|replicate\.delivery/i.test(imageUrl)) thumbnailSource = 'flux';
        await projRef.update({
          thumbnailUrl: imageUrl,
          thumbnailPrompt: promptText,
          thumbnailSource,
          status: {...(proj.status || {}), thumbnail: 'approved', publish: 'in_progress'},
        });
        console.log(`[autopilot] ${projectId}: Stage 5 done — thumbnail at ${imageUrl.slice(0, 80)} (source=${thumbnailSource})`);
      }
      await autoApproveStage('thumbnail', 'publish');

      // ── STAGE 6: Title + description (forensic-aware) ─────────
      await checkCancel();
      proj = (await projRef.get()).data();
      ch = (await chRef.get()).data();
      if (!proj.ytTitle) {
        await setStage('6/7', 'Generating title + description…');
        console.log(`[autopilot] ${projectId}: Stage 6 generating metadata`);
        // Metadata gen is one Claude call ~10-30s but pair with a
        // heartbeat for consistency. Cheap insurance.
        const metaHeartbeat = setInterval(() => {
          heartbeat('Generating title + description… still working');
        }, 25_000);
        let meta;
        try {
          meta = await withAutoFix(
            () => autoPilotGenerateMetadata(proj, ch, channelId, secrets.anthropicKey),
            'metadata',
            {maxRetries: 3, onAttempt: () => heartbeat('Generating title + description… retrying')},
          );
        } finally {
          clearInterval(metaHeartbeat);
        }
        const rank = {high: 3, medium: 2, low: 1};
        const sorted = [...meta.titles].sort((a, b) => (rank[b.ctrPotential] || 2) - (rank[a.ctrPotential] || 2));
        const picked = sorted[0];
        await projRef.update({
          ytTitle: picked.title.slice(0, 100),
          ytDesc: meta.description,
          titleAlternatives: meta.titles,
          titleGeneratedAt: new Date().toISOString(),
        });
        console.log(`[autopilot] ${projectId}: Stage 6 done — picked title "${picked.title.slice(0, 60)}"`);
      }
      // Stage-6 → Stage-7 transition. Ensures publish='in_progress'
      // and currentStage='7/7' even when the worker dies between
      // Stage 6's write and Stage 7's entry (the user reported this
      // exact gap on the 108-min run). On Resume, the new worker
      // sees ytTitle present + youtubeVideoId missing and runs Stage 7
      // directly — the explicit transition write here makes the
      // progress overlay reflect the truth in the meantime.
      {
        const cur = (await projRef.get()).data();
        if (cur?.status?.publish !== 'in_progress' && cur?.status?.publish !== 'done') {
          await projRef.update({'status.publish': 'in_progress'});
        }
        await projRef.update({
          'autoPilot.currentStage': '7/7',
          'autoPilot.stageLabel': 'Uploading + scheduling on YouTube…',
          'autoPilot.updatedAt': FieldValue.serverTimestamp(),
        });
      }

      // ── STAGE 7: Upload + schedule (via Render proxy) ────────
      await checkCancel();
      proj = (await projRef.get()).data();
      ch = (await chRef.get()).data();
      if (!proj.youtubeVideoId) {
        await setStage('7/7', 'Uploading + scheduling on YouTube…');
        const sch = ch.publishSchedule || null;
        const lastSchedMs = sch?.lastScheduledAt?.toMillis?.() || 0;
        const slotMs = autoPilotNextPublishSlot(sch, lastSchedMs);
        const publishAtIso = slotMs ? new Date(slotMs).toISOString() : null;
        const r = await fetch(`${RENDER_PROXY_URL}/youtube/upload`, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            channelId, title: proj.ytTitle, description: proj.ytDesc || '',
            videoUrl: proj.editingFile.url, thumbnailUrl: proj.thumbnailUrl || '',
            privacyStatus: 'private', publishAt: publishAtIso,
          }),
        });
        const d = await r.json();
        if (!r.ok || !d.ok || !d.videoId) throw new Error('Upload: ' + (d.error || `HTTP ${r.status}`));
        const patch = {
          status: {...(proj.status || {}), publish: 'done'},
          youtubeVideoId: d.videoId,
          youtubeWatchUrl: d.watchUrl,
          youtubeStudioUrl: d.studioUrl,
          thumbnailUploaded: !!d.thumbnailUploaded,
        };
        if (d.scheduledPublishAt) patch.scheduledPublishAt = d.scheduledPublishAt;
        if (d.thumbnailError) patch.thumbnailError = d.thumbnailError;
        await projRef.update(patch);
        if (publishAtIso) {
          await chRef.update({publishSchedule: {...(sch || {}), lastScheduledAt: new Date(slotMs)}});
        }
      }

      // ── Done ────────────────────────────────────────────────────
      await projRef.update({
        autoPilot: {
          status: 'completed',
          completedAt: FieldValue.serverTimestamp(),
          updatedAt: FieldValue.serverTimestamp(),
          currentStage: 'done',
          stageLabel: '🎉 Auto-pilot complete',
          requestStart: false,
          requestCancel: false,
          error: null,
        },
      });
      console.log(`[autopilot] ${projectId}: completed`);
      // Bulk-run side effect: if this project belongs to a bulk run,
      // recount and update the parent bulkRun doc so the UI banner
      // refreshes within seconds (not waiting for the next scheduler
      // tick). Best-effort — failures don't block the worker.
      if (proj.bulkRunId) {
        try {
          const bulkRef = db.collection('bulkRuns').doc(proj.bulkRunId);
          const bulkSnap = await bulkRef.get();
          if (bulkSnap.exists) {
            const bulkData = bulkSnap.data() || {};
            const fresh = await countBulkRunProjects(proj.bulkRunId, channelId);
            const total = bulkData.total || 0;
            const allDone = fresh.total >= total && (fresh.running + fresh.awaitingRender) === 0;
            await bulkRef.update({
              completed: fresh.completed,
              failed: fresh.failed,
              running: fresh.running + fresh.awaitingRender,
              pending: Math.max(0, total - fresh.total),
              lastTickAt: FieldValue.serverTimestamp(),
              ...(allDone ? {status: 'completed', endedAt: FieldValue.serverTimestamp()} : {}),
            });
            console.log(`[autopilot] ${projectId}: bulk ${proj.bulkRunId} → ${fresh.completed}/${total} (${allDone ? 'all done' : 'still running'})`);
          }
        } catch (e) {
          console.warn(`[autopilot] ${projectId}: bulk run update failed: ${e.message}`);
        }
      }
      // In-app notification
      if (ownerId) {
        await db.collection('users').doc(ownerId).collection('notifications').add({
          message: `🚀 Auto-pilot finished: "${proj.ytTitle || proj.topic || 'video'}" is uploaded.`,
          ts: FieldValue.serverTimestamp(),
          read: false,
          type: 'auto-pilot',
          channelId,
          projectId,
        }).catch((e) => console.warn('[autopilot] notification write failed:', e.message));
      }
    } catch (e) {
      const cancelled = e.message === 'cancelled';
      console.error(`[autopilot] ${projectId}:`, e);
      await projRef.update({
        autoPilot: {
          status: cancelled ? 'cancelled' : 'failed',
          error: cancelled ? null : (e.message || 'unknown'),
          failedAt: FieldValue.serverTimestamp(),
          updatedAt: FieldValue.serverTimestamp(),
          requestStart: false,
          requestCancel: false,
        },
      }).catch((ue) => console.warn('[autopilot] failure-state update failed:', ue.message));
      // Bulk-run side effect: same as the happy-path branch above but
      // for fail/cancel terminal states. Without this, a failed bulk
      // video leaves the parent bulkRun showing stale running=1.
      if (proj && proj.bulkRunId) {
        try {
          const bulkRef = db.collection('bulkRuns').doc(proj.bulkRunId);
          const bulkSnap = await bulkRef.get();
          if (bulkSnap.exists) {
            const bulkData = bulkSnap.data() || {};
            const fresh = await countBulkRunProjects(proj.bulkRunId, channelId);
            const total = bulkData.total || 0;
            const allDone = fresh.total >= total && (fresh.running + fresh.awaitingRender) === 0;
            await bulkRef.update({
              completed: fresh.completed,
              failed: fresh.failed,
              running: fresh.running + fresh.awaitingRender,
              pending: Math.max(0, total - fresh.total),
              lastTickAt: FieldValue.serverTimestamp(),
              ...(allDone ? {status: 'completed', endedAt: FieldValue.serverTimestamp()} : {}),
            });
          }
        } catch (bulkErr) {
          console.warn(`[autopilot] ${projectId}: bulk run update on failure path failed: ${bulkErr.message}`);
        }
      }
    }
  }
);

// ─── Trigger: editing jobs (existing) ───────────────────────────────
// Two functions, one pipeline:
//
//   processEditingJob       — onDocumentCreated, 60s timeout. Thin
//                             "kicker" that fires an HTTP request at
//                             processEditingJobHttp and returns. Never
//                             does any heavy work itself; can never hit
//                             the 540s event-trigger ceiling.
//
//   processEditingJobHttp   — onRequest, 3600s timeout. Runs the actual
//                             pipeline (captions → sourcing → caching →
//                             preflight → render → concat → upload).
//                             Auth via shared secret in the
//                             `x-kicker-secret` header.
//
// Cloud Run / Cloud Functions Gen 2 keep HTTP requests running until
// completion regardless of client disconnect, so the kicker can fire-
// and-forget with a short read timeout — the worker still runs to
// completion (or its own 3600s timeout) on its own.
exports.processEditingJobHttp = onRequest(
  {
    region: 'us-central1',
    timeoutSeconds: 3600,
    // 4GiB up from 2GiB — /tmp is RAM-backed (memfs) on Gen 2, so a
    // 360MB final MP4 plus streaming buffers + Node runtime + lingering
    // ffmpeg/Lambda-SDK heap was crowding the prior 2GiB ceiling enough
    // to starve the event loop during upload (heartbeat + upload-timeout
    // setTimeouts both stopped firing).
    memory: '4GiB',
    cpu: 2,
    invoker: 'public',
    secrets: [
      PEXELS_API_KEY,
      ASSEMBLYAI_API_KEY,
      REPLICATE_API_KEY,
      AWS_ACCESS_KEY_ID,
      AWS_SECRET_ACCESS_KEY,
      KICKER_SECRET,
      GEMINI_API_KEY,
      LOGO_DEV_API_KEY,
    ],
  },
  async (req, res) => {
    if (req.method !== 'POST') return res.status(405).json({error: 'POST required'});
    const expected = (KICKER_SECRET.value() || '').replace(/^﻿/, '').trim();
    const presented = String(req.headers['x-kicker-secret'] || '').replace(/^﻿/, '').trim();
    if (!expected || presented !== expected) {
      return res.status(403).json({error: 'unauthorized'});
    }
    const jobId = req.body && req.body.jobId;
    if (!jobId) return res.status(400).json({error: 'missing jobId'});

    const docRef = db.collection('editingJobs').doc(jobId);
    const snap = await docRef.get();
    if (!snap.exists) return res.status(404).json({error: 'job not found'});
    const data = snap.data();
    if (data.status !== 'pending') {
      console.log(`[${jobId}] HTTP worker: status=${data.status}, skipping`);
      return res.status(200).json({ok: true, skipped: true, status: data.status});
    }
    const job = {id: jobId, ...data};
    console.log(`[${jobId}] HTTP worker starting`);

    try {
      await processJob(job);
      console.log(`[${jobId}] HTTP worker done`);
      return res.status(200).json({ok: true, jobId});
    } catch (e) {
      console.error(`[${jobId}] HTTP worker failed:`, e);
      // Bulletproof failure update — same pattern as before, surfaced via
      // the worker now instead of the event-trigger handler.
      try {
        await docRef.update({
          status: 'failed',
          currentStep: 'Failed',
          error: e.message || 'Unknown error',
          errorDetail: buildErrorDetail(e),
          updatedAt: FieldValue.serverTimestamp(),
        });
      } catch (richUpdateErr) {
        console.error(`[${jobId}] rich error update failed (${richUpdateErr.message}) — falling back to minimal`);
        await docRef.update({
          status: 'failed',
          currentStep: 'Failed',
          error: `${e.message || 'Unknown error'}  [errorDetail save failed: ${richUpdateErr.message}]`,
          updatedAt: FieldValue.serverTimestamp(),
        }).catch((minErr) => console.error(`[${jobId}] even minimal update failed:`, minErr));
      }
      return res.status(500).json({error: e.message});
    }
  },
);

exports.processEditingJob = onDocumentCreated(
  {
    document: 'editingJobs/{jobId}',
    secrets: [KICKER_SECRET],
    timeoutSeconds: 60,
    memory: '256MiB',
    region: 'us-central1',
    retry: false,
  },
  async (event) => {
    const jobId = event.params.jobId;
    const data = event.data?.data();
    if (!data) {
      console.warn(`[${jobId}] no data on event`);
      return;
    }
    if (data.status !== 'pending') {
      console.log(`[${jobId}] status=${data.status}, skipping kick`);
      return;
    }

    const project = process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT || 'ytauto-95f91';
    const targetUrl = `https://us-central1-${project}.cloudfunctions.net/processEditingJobHttp`;

    // Fire-and-forget. We give the HTTP function a few seconds to ACK
    // receipt; once the request is on the wire Cloud Run keeps the
    // worker running even after we drop the connection.
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 8000);
    try {
      const secretValue = (KICKER_SECRET.value() || '').replace(/^﻿/, '').trim();
      await fetch(targetUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-kicker-secret': secretValue,
        },
        body: JSON.stringify({jobId}),
        signal: ctrl.signal,
      });
      console.log(`[${jobId}] kicker dispatched (worker ACKed quickly)`);
    } catch (e) {
      // AbortError is the expected case — the worker is still running,
      // we just dropped our read of its eventual response.
      if (e.name === 'AbortError') {
        console.log(`[${jobId}] kicker dispatched (worker continues in background)`);
        return;
      }
      console.error(`[${jobId}] kicker dispatch failed:`, e);
      await db.collection('editingJobs').doc(jobId).update({
        status: 'failed',
        currentStep: 'Failed',
        error: `Kicker failed to reach HTTP worker: ${e.message}`,
        updatedAt: FieldValue.serverTimestamp(),
      }).catch(() => {});
    } finally {
      clearTimeout(timer);
    }
  },
);
