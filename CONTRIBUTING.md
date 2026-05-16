# Contributing — branch & deploy workflow

Production launched 2026-05-15. From now on we develop in parallel without
breaking what's already shipping.

## Repos

The project is **3 separate git repos**, NOT one monorepo:

| Repo | Branch | Remote |
|---|---|---|
| `ytauto-proxy/` | `main` (canonical) → `develop` (active dev) | `github.com/remreport/ytauto-proxy` |
| `remotion/` | `master` (single branch) | local only |
| `ytapp-online/` | `master` (single branch) | local only |

Only `ytauto-proxy` uses the `main`/`develop` model. The other two stay
linear because the dual-Lambda-site mechanism (below) is a runtime
feature flag, not a git branch. Whatever is bundled to v6 OR v7 is what
runs — branch is purely organizational.

## Two Lambda sites — production stays stable

| Site | Purpose | Deploy from |
|---|---|---|
| `rem-report-v6` | **Production.** Don't break. | `remotion/master` (after testing) |
| `rem-report-v7` | **Staging.** Where new visuals land first. | `remotion/master` (active dev) |

Both sites live in the same S3 bucket
(`remotionlambda-useast1-1zsfacnzw9`). Cloud Functions selects which
one to render to per job — see "Feature flag" below.

## Feature flag — `useStagingRender`

Set on the editingJob document at creation time:

```js
{
  // ...standard job fields...
  useStagingRender: true,    // → renders to rem-report-v7
  // useStagingRender: false (or omit) → renders to rem-report-v6 (default)
}
```

`functions/index.js` resolves the serve URL via `getRemotionServeUrl()`.
Every render logs which site it used:
```
[lambda] rendering to site=rem-report-v7 (STAGING) (useStagingRender=true)
[lambda] rendering to site=rem-report-v6 (PROD) (useStagingRender=false)
```

## Pre-flight validation

Before invoking Lambda, the pipeline runs `validatePipelineOutputBeforeLambda`:
- HEAD/GET-probe every overlay imageUrl, footage URL, voiceover URL (3s timeout)
- Validate beat shape, no duplicate startTimes, overlay timestamps within voiceover
- On any error → set `job.status = 'failed'`, do NOT invoke Lambda

This catches "bad data → $5 wasted Lambda render" failures. Sample log:
```
[preflight] <jobId>: ok=true probes=12 errors=0 warnings=2
```

## Auto-cancel timeouts

`functions/index.js` has hard timeouts to prevent runaway renders:

| What | Limit | On timeout |
|---|---|---|
| `renderMediaOnLambda` invocation | 25 min | throw, mark job failed |
| `getRenderProgress` poll loop | 25 min wall clock | throw, mark job failed |
| Generic `withHardTimeout(promise, ms, label)` helper | as specified | throw with `code='PIPELINE_TIMEOUT'` |

The in-flight Lambda render in AWS keeps running after timeout (consumes
budget until completion). Future work: call AWS Step Functions cancel
API to actively stop it.

## Deploy commands

### Production deploy (main → v6)

```bash
# In ytauto-proxy/
git checkout main
git pull
node scripts/predeploy-check.mjs
firebase deploy --only functions:processEditingJobHttp,functions:autoPilotWorker --project ytauto-95f91

# In remotion/ (if visuals changed too)
cd ../remotion
npx remotion lambda sites create src/index.js --site-name=rem-report-v6 --region=us-east-1
```

### Staging deploy (develop → v7)

```bash
# In ytauto-proxy/
git checkout develop
node scripts/predeploy-check.mjs
firebase deploy --only functions:processEditingJobHttp,functions:autoPilotWorker --project ytauto-95f91

# In remotion/
cd ../remotion
npx remotion lambda sites create src/index.js --site-name=rem-report-v7 --region=us-east-1
```

**Important:** Cloud Functions are ONE deployment serving BOTH sites.
There's no "deploy functions to staging vs production" — the function
selects the site at runtime via the feature flag. So when you deploy
functions from `develop`, the new code path runs for ALL renders
(both staging AND production). If you want to test functions changes
without affecting prod renders, gate the new behavior behind
`useStagingRender` checks inside the function itself.

## Gating rule for new features (HARD REQUIREMENT)

**Every new feature on `develop` that touches `functions/index.js`
MUST be gated behind `useStagingRender` until explicitly approved
for production.** This rule exists because of day-11: the music-volume
fix was deployed from `develop` and silently changed production
behavior even though only the staging Lambda site was bundled with
the new visuals.

Two acceptable shapes:

**A) Gate inside the function** (any code-path change):

```js
// At the start of the section that branches behavior:
const useStaging = !!job.useStagingRender;

// Conditional default
const newFeatureValue = useStaging ? NEW_BEHAVIOR : OLD_BEHAVIOR;

// OR conditional execution
if (useStaging) {
  // new code path — only runs for v7 renders
} else {
  // existing code path — v6 production unchanged
}
```

(Example placeholder — no live gated default exists right now. Earlier
day-12 Gemini chunking gate was removed after the v6 "entity timing
off on long voiceovers" issue was confirmed to need the same fix.
Chunking is now the default for any voiceover >300s, both v6 and v7.
When a new gated default is added, it should follow the same shape:
a helper that branches on `useStagingRender`, with per-channel UI
override still winning in both branches.)

**B) Remotion-side only** (visual changes that don't touch
`functions/index.js`):

These are AUTO-isolated by the dual Lambda site. v7 site bundles the
new Remotion code; v6 site keeps the previous bundle. No gating code
needed — the site selection at render time is the gate. Example:
day-11 captions revert affected `Captions.jsx` only, so v7 rendered
with full-sentence captions while v6 stayed on sliding window —
zero functions/index.js change.

**Promotion workflow** when staging looks good:
1. Merge `develop` → `main` in `ytauto-proxy`
2. Remove the gate inside the function (or invert it so the new
   behavior becomes the default for both)
3. Run predeploy-check
4. Deploy Cloud Functions from `main`
5. Bundle Remotion to v6 (production site) — `npx remotion lambda
   sites create src/index.js --site-name=rem-report-v6`

## Pre-deploy check

`ytauto-proxy/scripts/predeploy-check.mjs` runs:
1. `node --check` syntax validation
2. Verify all 17 expected pipeline helpers DEFINED + CALLED
3. Verify 4 required secrets declared via `defineSecret()`

Run it before EVERY deploy. Exits non-zero on failure → don't deploy.

## Feature workflow

```
1. Start on develop:
   cd ytauto-proxy && git checkout develop

2. Make changes. Run local short test:
   node ../remotion/scripts/test-short-video.mjs

3. (If satisfied) deploy to staging:
   node scripts/predeploy-check.mjs
   firebase deploy --only functions:processEditingJobHttp,functions:autoPilotWorker

4. Trigger a staging render from the app (set useStagingRender: true on the job doc)

5. Verify visually + via logs ([lambda] should show "site=rem-report-v7")

6. (If staging looks good) merge to main:
   git checkout main && git merge develop

7. Deploy production:
   node scripts/predeploy-check.mjs
   firebase deploy --only functions:processEditingJobHttp,functions:autoPilotWorker
   cd ../remotion && npx remotion lambda sites create src/index.js --site-name=rem-report-v6 --region=us-east-1

8. Watch the next prod render's logs for regressions.
```

## What NOT to do

- Don't push directly to `main` without testing on `develop` first.
- Don't deploy without running `scripts/predeploy-check.mjs`.
- Don't skip the staging render — it's there because production cost
  $5 per failed Lambda render and we hit two such failures on day 10.
- Don't `git init` at the project root — three separate repos is the
  current architecture; merging them is a separate decision.
- **Don't add Cloud Functions code that changes prod behavior without
  a `useStagingRender` gate.** Day-11 example: a music-volume bump
  was deployed from `develop` and silently changed production
  behavior. (The gate has since been reverted because 0.06 turned
  out to be the correct value — but the rule still stands; the next
  prod-behavior change must be gated.) The dual Lambda site only
  isolates Remotion code; Cloud Functions is shared. See the
  "Gating rule" section above.
