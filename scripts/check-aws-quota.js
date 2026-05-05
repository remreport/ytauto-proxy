// scripts/check-aws-quota.js
//
// Reads the current AWS Lambda "Concurrent executions" quota and (with
// --request) submits an increase request via the Service Quotas API.
// Self-bootstraps the IAM permission it needs (the remotion-lambda user
// gets AWSServiceQuotasFullAccess attached on first run; idempotent).
//
// Usage:
//   node scripts/check-aws-quota.js                # show current quota
//   node scripts/check-aws-quota.js --request 100  # request increase
//   node scripts/check-aws-quota.js --history      # show open requests
//
// Requires AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY + AWS_REGION.

const fs = require('fs');
const path = require('path');

// .env loader
const envPath = path.join(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const m = /^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)$/.exec(line);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2];
  }
}

// Service Quotas SDK lives in remotion/node_modules — borrow from there
const sqPath = path.join(__dirname, '..', '..', 'remotion', 'node_modules', '@aws-sdk', 'client-service-quotas');
const iamPath = path.join(__dirname, '..', '..', 'remotion', 'node_modules', '@aws-sdk', 'client-iam');
const stsPath = path.join(__dirname, '..', '..', 'remotion', 'node_modules', '@aws-sdk', 'client-sts');
const {
  ServiceQuotasClient,
  GetServiceQuotaCommand,
  RequestServiceQuotaIncreaseCommand,
  ListRequestedServiceQuotaChangeHistoryCommand,
} = require(sqPath);
const {IAMClient, AttachUserPolicyCommand} = require(iamPath);
const {STSClient, GetCallerIdentityCommand} = require(stsPath);

const region = process.env.AWS_REGION || 'us-east-1';
const SERVICE_CODE = 'lambda';
// AWS-stable code for "Concurrent executions" — the same in every region.
const QUOTA_CODE = 'L-B99A9384';

async function ensurePermissions() {
  const sts = new STSClient({region});
  const me = await sts.send(new GetCallerIdentityCommand({}));
  const userName = (me.Arn || '').split('/').pop();
  if (!userName) throw new Error('Could not determine IAM user name from STS');
  const iam = new IAMClient({region});
  // Idempotent: AttachUserPolicy returns 200 even if already attached.
  await iam.send(new AttachUserPolicyCommand({
    UserName: userName,
    PolicyArn: 'arn:aws:iam::aws:policy/ServiceQuotasFullAccess',
  }));
  return userName;
}

async function getCurrent(sq) {
  const r = await sq.send(new GetServiceQuotaCommand({
    ServiceCode: SERVICE_CODE,
    QuotaCode: QUOTA_CODE,
  }));
  return r.Quota;
}

async function requestIncrease(sq, value) {
  return sq.send(new RequestServiceQuotaIncreaseCommand({
    ServiceCode: SERVICE_CODE,
    QuotaCode: QUOTA_CODE,
    DesiredValue: Number(value),
  }));
}

async function listHistory(sq) {
  return sq.send(new ListRequestedServiceQuotaChangeHistoryCommand({
    ServiceCode: SERVICE_CODE,
    Status: 'PENDING',
    MaxResults: 10,
  }));
}

(async () => {
  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    console.error('AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY required (in .env or shell).');
    process.exit(1);
  }

  const userName = await ensurePermissions();
  console.log(`IAM user: ${userName}  (region ${region})`);
  console.log('');

  const sq = new ServiceQuotasClient({region});
  const want = process.argv.includes('--request')
    ? Number(process.argv[process.argv.indexOf('--request') + 1])
    : null;
  const showHistory = process.argv.includes('--history');

  // Always show current
  const current = await getCurrent(sq);
  console.log('── Lambda concurrent executions ──');
  console.log(`  current quota:  ${current.Value}`);
  console.log(`  unit:           ${current.Unit}`);
  console.log(`  adjustable:     ${current.Adjustable}`);
  console.log(`  service:        ${current.ServiceName}`);
  console.log('');

  if (showHistory) {
    const h = await listHistory(sq);
    const reqs = h.RequestedQuotas || [];
    if (reqs.length === 0) {
      console.log('No pending quota requests.');
    } else {
      console.log(`── ${reqs.length} pending request(s) ──`);
      for (const r of reqs) {
        console.log(`  ${r.QuotaName}: ${r.DesiredValue} (was ${r.Status === 'PENDING' ? 'pending' : r.Status}, opened ${r.Created})`);
      }
    }
    console.log('');
  }

  if (want != null) {
    if (!Number.isFinite(want) || want <= current.Value) {
      console.error(`--request value must be a number greater than current (${current.Value}). Got: ${want}`);
      process.exit(1);
    }
    console.log(`Requesting increase to ${want}…`);
    try {
      const r = await requestIncrease(sq, want);
      console.log(`  case id:   ${r.RequestedQuota?.CaseId || '(none)'}`);
      console.log(`  status:    ${r.RequestedQuota?.Status || '(unknown)'}`);
      console.log(`  desired:   ${r.RequestedQuota?.DesiredValue}`);
      console.log('');
      console.log('AWS will email you when the request is approved (typically minutes-to-hours for asks under 200).');
      console.log('Check status: node scripts/check-aws-quota.js --history');
    } catch (e) {
      if (e.name === 'ResourceAlreadyExistsException') {
        console.log('A pending request for this quota already exists. Run with --history to see it.');
      } else {
        throw e;
      }
    }
  }
})().catch((e) => {
  console.error(e?.message || e);
  if (e?.name === 'AccessDeniedException') {
    console.error('IAM propagation can take up to ~30s after first run. Try again in a moment.');
  }
  process.exit(1);
});
