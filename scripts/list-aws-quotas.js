// Lists every AWS Lambda quota and its current value. Used to find the
// right QuotaCode for "function memory size" (or any other limit) when
// the canonical code we hardcoded turned out to be wrong.

const fs = require('fs');
const path = require('path');
const envPath = path.join(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, 'utf8').split(/\r?\n/)) {
    const m = /^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)$/.exec(line);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2];
  }
}
const sqPath = path.join(__dirname, '..', '..', 'remotion', 'node_modules', '@aws-sdk', 'client-service-quotas');
const {ServiceQuotasClient, ListServiceQuotasCommand} = require(sqPath);

const sq = new ServiceQuotasClient({region: process.env.AWS_REGION || 'us-east-1'});

(async () => {
  let nextToken;
  const all = [];
  do {
    const r = await sq.send(new ListServiceQuotasCommand({
      ServiceCode: 'lambda',
      MaxResults: 100,
      NextToken: nextToken,
    }));
    all.push(...(r.Quotas || []));
    nextToken = r.NextToken;
  } while (nextToken);
  console.log(`${all.length} Lambda quotas in this account:\n`);
  for (const q of all) {
    const flag = q.Adjustable ? ' ' : 'X';
    console.log(`  [${flag}] ${q.QuotaCode.padEnd(14)}  ${String(q.Value).padEnd(8)}  ${q.Unit.padEnd(8)}  ${q.QuotaName}`);
  }
})().catch((e) => { console.error(e?.message || e); process.exit(1); });
