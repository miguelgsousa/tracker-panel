import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import vm from 'node:vm';
import express from 'express';
const require = createRequire(import.meta.url);
const { createPanelSecurity, publicAccountData } = require('../lib/panel-security');
const origin = 'https://tracker.example.com';
const authorization = 'Basic ' + Buffer.from('regression:temporary-password').toString('base64');

async function listen(app, fn) {
 const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
 try { await fn((path, options = {}) => fetch(`http://127.0.0.1:${server.address().port}${path}`, options)); }
 finally { await new Promise(resolve => server.close(resolve)); }
}
test('whole-origin gate fails closed for incomplete or explicitly enabled configuration', async () => {
 for (const env of [{METRICS_ENABLED:'true'}, {METRICS_USERNAME:'owner'}, {META_APP_ID:'test'}, {DATA_DIR:'/tmp/test'}]) {
  const app = express(); app.use(createPanelSecurity(env)); app.use((req,res) => res.send('unsafe'));
  await listen(app, async request => { for (const p of ['/', '/index.html', '/api/accounts', '/api/metrics/accounts', '/auth/instagram/callback']) assert.equal((await request(p)).status,503); });
 }
});
test('whole panel protects legacy writes; cross-origin and missing-origin writes are 403; cookies are write-only including fetch responses', async () => {
 const dir = await mkdtemp(tmpdir()+'/tracker-security-');
 const keys = ['DB_PATH','METRICS_ENABLED','METRICS_USERNAME','METRICS_PASSWORD','PUBLIC_BASE_URL'];
 const prior = Object.fromEntries(keys.map(k => [k,process.env[k]]));
 Object.assign(process.env, {DB_PATH:dir,METRICS_ENABLED:'true',METRICS_USERNAME:'regression',METRICS_PASSWORD:'temporary-password',PUBLIC_BASE_URL:origin});
 const secret = 'STORED_CREDENTIAL_MUST_NOT_ESCAPE';
 await writeFile(dir+'/accounts.json', JSON.stringify({facebook:[{id:'fb',handle:'mock-page',cookie:secret,access_token:secret,metrics:{pageAccessToken:secret,followers:12},recentContent:[{title:'public',token:secret}]}],instagram:[],youtube:[],_settings:{instagramToken:secret,facebookToken:secret},_folders:{}}));
 delete require.cache[require.resolve('../server.js')];
 const { app, metricsIntegration } = require('../server.js');
 const puppeteer = require('puppeteer-core'); const launch = puppeteer.launch;
 puppeteer.launch = async () => { throw new Error('MOCK browser disabled; no provider calls'); };
 try {
  await listen(app, async request => {
   const req = (p,o={}) => request(p,{...o,headers:{authorization,origin,...o.headers}});
   for (const p of ['/', '/index.html', '/metrics.js', '/api/accounts','/api/folders','/api/metrics/config']) {
    const r = await request(p); assert.equal(r.status,401,p); assert.match(r.headers.get('www-authenticate'),/Basic/); assert.equal(r.headers.get('access-control-allow-origin'),null);
   }
   for (const [method,p] of [['POST','/api/accounts/youtube'],['PATCH','/api/accounts/facebook/fb/cookie'],['DELETE','/api/accounts/facebook/fb'],['POST','/api/fetch/facebook/fb'],['POST','/api/folders/youtube'],['PUT','/api/folders/youtube/reorder']]) {
    assert.equal((await request(p,{method})).status,401);
    assert.equal((await req(p,{method,headers:{origin:'https://evil.example'}})).status,403);
    assert.equal((await request(p,{method,headers:{authorization}})).status,403);
   }
   assert.equal((await req('/')).status,200);
   for (const p of ['/api/accounts','/api/accounts/facebook']) {
    const text = await (await req(p)).text(); assert.ok(!text.includes(secret)); assert.ok(!/cookie|access_token|pageAccessToken|instagramToken/.test(text));
   }
   const added = await req('/api/accounts/youtube',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({handle:'mock',name:'<img src=x onerror=alert(1)>',cookie:secret})});
   assert.equal(added.status,201); assert.ok(!(await added.text()).includes(secret));
   assert.equal((await req('/api/accounts/facebook/fb/cookie',{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({cookie:'UPDATED_WRITE_ONLY'})})).status,200);
   for (const p of ['/api/fetch/facebook/fb','/api/fetch-all/facebook']) {
    const response = await req(p,{method:'POST'}); assert.equal(response.status,200); const text = await response.text(); assert.ok(!text.includes(secret)); assert.ok(!text.includes('UPDATED_WRITE_ONLY')); assert.ok(!/pageAccessToken|access_token/.test(text));
   }
   assert.equal((await req('/api/fetch/instagram/unused',{method:'POST'})).status,410);
   const ig = await req('/api/accounts/instagram',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({handle:'no-provider-call'})}); assert.equal(ig.status,201);
   const disk = JSON.parse(await readFile(dir+'/accounts.json','utf8')); assert.equal(disk.facebook[0].cookie,'UPDATED_WRITE_ONLY'); assert.equal(disk._settings.facebookToken,secret); assert.equal(disk.facebook[0].access_token,secret);
  });
 } finally {
  puppeteer.launch = launch; await metricsIntegration.close();
  for (const k of keys) if (prior[k] === undefined) delete process.env[k]; else process.env[k] = prior[k];
  delete require.cache[require.resolve('../server.js')]; await rm(dir,{recursive:true,force:true});
 }
});
test('recursive response redaction never mutates stored credential fields', () => {
 const value = {cookie:'secret', nested:[{refreshToken:'secret',password:'secret',title:'visible'}]};
 assert.deepEqual(publicAccountData(value),{nested:[{title:'visible'}]}); assert.equal(value.cookie,'secret');
});
test('legacy stored-XSS payloads are inert in rendered text, attributes, JS IDs and URLs', async () => {
 const source = await readFile(new URL('../index.html',import.meta.url),'utf8');
 const helpers = source.slice(source.indexOf('        function escapeHtml'),source.indexOf('        // --- Server check ---'));
 const render = source.slice(source.indexOf('        function renderPlatformTab'),source.indexOf('        // --- Reports ---'));
 const payload = `<img src=x onerror="globalThis.pwned=1">'&`;
 const account = {id:payload,handle:payload,name:payload,metrics:{avatar:'javascript:alert(1)',followers:1},recentContent:[{url:'javascript:alert(1)',thumbnail:'https://example.com/" onerror="alert(1)',title:payload,durationStr:payload}]};
 const container = {innerHTML:'',querySelector:()=>null};
 const ctx = vm.createContext({URL,document:{getElementById:()=>container},window:{},lucide:{createIcons(){}},allAccounts:{youtube:[account]},allFolders:{youtube:[{id:payload,name:payload}]},PLATFORMS:{youtube:{name:'YouTube'}},platformState:{youtube:{selectedProfile:payload,viewMode:'grid',selectedItems:[],openFolder:null}},fetchingAccounts:new Set()});
 vm.runInContext(helpers+render+';renderPlatformTab("youtube");',ctx);
 assert.ok(!container.innerHTML.includes(payload)); assert.ok(container.innerHTML.includes('&lt;img')); assert.ok(!container.innerHTML.includes('javascript:'));
 assert.equal(vm.runInContext('safeUrl("data:text/html,attack")',ctx),'');
 assert.equal(vm.runInContext('safeUrl("https://user:pass@example.com")',ctx),'');
 assert.equal(vm.runInContext('safeUrl("https://example.com/video")',ctx),'https://example.com/video');
 ctx.payload = payload;
 assert.equal(vm.runInContext('eval("\\\'" + inlineValue(payload) + "\\\'")',ctx),payload);
 assert.equal(ctx.pwned,undefined);
 assert.ok(!source.includes('btoa(account.cookie'));
 const backend = await readFile(new URL('../server.js',import.meta.url),'utf8');
 assert.ok(!/getInstagramToken|fetchInstagramGraphMetrics|facebookToken|instagramToken|access_token=/.test(backend));
});
