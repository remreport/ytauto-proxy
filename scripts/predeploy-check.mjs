#!/usr/bin/env node
// Pre-deploy syntax + reference check for ytauto-proxy/functions/index.js.
//
// Catches the failure mode that took production down on 2026-05-15:
// `enforceOverlaySpacing` was CALLED but never DEFINED — silent at
// parse time (a callable name reference is just an identifier in the
// source), only blew up when the call line ran in production after a
// $5 Lambda render had already started.
//
// What this catches:
//   1. Syntax errors  — `node --check` rejects the file.
//   2. Pipeline-helper definition gaps — every name in EXPECTED_HELPERS
//      must have a matching `function NAME(` declaration in the file.
//   3. Pipeline-call sanity — every helper MUST also appear as a call
//      site (`NAME(`) somewhere, otherwise the helper is dead code OR
//      the pipeline order changed silently.
//
// What this does NOT catch:
//   - Runtime exceptions inside helpers (still need integration tests).
//   - Wrong arguments at the call site.
//   - Helpers added without updating EXPECTED_HELPERS — keep this list
//     in sync with the actual pipeline order in sourceBeatAwareFootage.
//
// Wire into deploy:
//   node ytauto-proxy/scripts/predeploy-check.mjs && firebase deploy --only functions...
//
// Exits 0 on pass, non-zero on any failure.

import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const indexPath = path.resolve(__dirname, '..', 'functions', 'index.js');

// Helpers that MUST be defined AND called somewhere. Mirrors the
// 10-pass pipeline order in sourceBeatAwareFootage. Update this when
// adding/removing pipeline passes.
const EXPECTED_HELPERS = [
  'enrichBeatsWithEntityImages',
  'validateBigStatContent',
  'anchorOverlaysToTimingTags',
  'trimOverlayDurationsToNextAnchor',
  'pairQuotesWithEntityPortraits',
  'dropRedundantLowerThirds',
  'dropNonSourceLowerThirds',
  'clampOverlayDurationsToBeat',
  'enforceOverlaySpacing',
  'stretchFootageToFillGaps',
  // Lookups invoked from the helpers above:
  'fetchCompanyLogo',
  'fetchWikipediaImage',
  'parseStatNumber',
  'normalizeEntityName',
  'getGeminiTimingTags',
  'callAnthropicWithTool',
  'breakIntoBeats',
];

const errors = [];
const warnings = [];

// ── Step 1: syntax check ──────────────────────────────────────────
try {
  execFileSync('node', ['--check', indexPath], {stdio: ['ignore', 'ignore', 'pipe']});
  console.log(`✓ syntax: node --check passed`);
} catch (e) {
  errors.push(`SYNTAX: ${(e.stderr || e.message || '').toString().slice(0, 600)}`);
}

// ── Step 2: definition + call site checks per helper ──────────────
const src = fs.readFileSync(indexPath, 'utf8');
for (const fn of EXPECTED_HELPERS) {
  // Definitions: function fn( OR async function fn( (non-capturing
  // group + global flag so .match() returns one entry per definition).
  const defRe = new RegExp(`^(?:async\\s+)?function\\s+${fn}\\s*\\(`, 'gm');
  // Call sites: bare `fn(` at word boundary, NOT preceded by `function `
  // (so the definition itself isn't counted as a call).
  const callRe = new RegExp(`(?<!function\\s)\\b${fn}\\s*\\(`, 'g');
  const defs = (src.match(defRe) || []).length;
  const calls = (src.match(callRe) || []).length;

  if (defs === 0) {
    errors.push(`MISSING DEFINITION: ${fn}() is referenced but never defined`);
  } else if (defs > 1) {
    warnings.push(`DUPLICATE DEFINITION: ${fn}() defined ${defs} times — second wins, first is dead code`);
  }
  if (calls === 0) {
    warnings.push(`UNCALLED: ${fn}() defined but never invoked — dead code or stale pipeline`);
  }
}
console.log(`✓ helpers: ${EXPECTED_HELPERS.length} checked`);

// ── Step 3: secret bindings ───────────────────────────────────────
// Both must be defineSecret'd AND bound to processEditingJobHttp.
const REQUIRED_SECRETS = ['GEMINI_API_KEY', 'LOGO_DEV_API_KEY', 'PEXELS_API_KEY', 'ASSEMBLYAI_API_KEY'];
for (const secret of REQUIRED_SECRETS) {
  const declared = src.includes(`defineSecret('${secret}')`);
  if (!declared) {
    errors.push(`SECRET NOT DECLARED: ${secret} is read via process.env but no defineSecret('${secret}') call found`);
  }
}
console.log(`✓ secrets: ${REQUIRED_SECRETS.length} required secrets declared`);

// ── Report ────────────────────────────────────────────────────────
if (warnings.length) {
  console.log('\n⚠ WARNINGS:');
  for (const w of warnings) console.log(`   ${w}`);
}
if (errors.length) {
  console.error('\n✗ ERRORS — DO NOT DEPLOY:');
  for (const e of errors) console.error(`   ${e}`);
  process.exit(1);
}
console.log('\n✓ predeploy-check OK — safe to deploy');
