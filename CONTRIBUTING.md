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
