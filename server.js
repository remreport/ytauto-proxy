const http = require('http');
const https = require('https');

const PORT = process.env.PORT || 3001;

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  if (req.method === 'GET') {
    res.writeHead(200, {'Content-Type':'text/plain'});
    res.end('YT Automation proxy is running');
    return;
  }

  let body = '';
  req.on('data', chunk => { body += chunk; });
  req.on('end', () => {
    let parsed;
    try { parsed = JSON.parse(body); } catch(e) {
      res.writeHead(400); res.end('Bad JSON'); return;
    }

    const apiKey = parsed._apiKey || '';
    delete parsed._apiKey;

    // Pikzels endpoint
    if (req.url === '/pikzels') {
      console.log('Calling Pikzels API...');
      const payload = JSON.stringify(parsed);
      const options = {
        hostname: 'api.pikzels.com',
        path: '/v2/thumbnail/text',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload),
          'X-Api-Key': apiKey,
        }
      };
      const apiReq = https.request(options, apiRes => {
        let data = '';
        apiRes.on('data', chunk => { data += chunk; });
        apiRes.on('end', () => {
          console.log('Pikzels status:', apiRes.statusCode);
          res.writeHead(apiRes.statusCode, { 'Content-Type': 'application/json' });
          res.end(data);
        });
      });
      apiReq.on('error', err => { res.writeHead(500); res.end(JSON.stringify({ error: err.message })); });
      apiReq.write(payload);
      apiReq.end();
      return;
    }

    // Default: Anthropic
    console.log('Calling Anthropic API...');
    const payload = JSON.stringify(parsed);
    const options = {
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload),
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
      }
    };
    const apiReq = https.request(options, apiRes => {
      let data = '';
      apiRes.on('data', chunk => { data += chunk; });
      apiRes.on('end', () => {
        console.log('Anthropic status:', apiRes.statusCode);
        res.writeHead(apiRes.statusCode, { 'Content-Type': 'application/json' });
        res.end(data);
      });
    });
    apiReq.on('error', err => { res.writeHead(500); res.end(JSON.stringify({ error: { message: err.message } })); });
    apiReq.write(payload);
    apiReq.end();
  });
});

server.listen(PORT, () => console.log('✓ Proxy running on port ' + PORT));
