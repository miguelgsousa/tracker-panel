// All app credentials below are deliberately fake TEST fixtures. No provider calls.
import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn, spawnSync} from 'node:child_process';
import {mkdtempSync, mkdirSync, writeFileSync, readFileSync, statSync, existsSync, rmSync, symlinkSync, chmodSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {parseEnv} from 'node:util';
const root = fileURLToPath(new URL('../', import.meta.url));
const script = path.join(root, 'scripts/setup-metrics.mjs');
const cleanEnv = {PATH: process.env.PATH, HOME: process.env.HOME};
const fake = {META_APP_ID: '123456789', META_APP_SECRET: 'TEST_ONLY_FAKE_INSTAGRAM_SECRET', FACEBOOK_APP_ID: '987654321', FACEBOOK_APP_SECRET: 'TEST_ONLY_FAKE_FACEBOOK_SECRET'};
function fixture(t) {
  const dir = mkdtempSync(path.join(tmpdir(), 'tracker-setup-test-'));
  t.after(() => rmSync(dir, {recursive:true, force:true}));
  const secrets = path.join(dir, 'test-app.json');
  writeFileSync(secrets, JSON.stringify(fake), {mode:0o600});
  const output = path.join(dir, 'runtime');
  const run = (args = [], base = true) => spawnSync(process.execPath, [script, ...(base ? ['--origin', 'https://tracker.test', '--secrets-file', secrets, '--output-dir', output] : []), ...args], {cwd:root, env:cleanEnv, encoding:'utf8'});
  return {dir,secrets,output,run};
}
test('setup creates private separate stores and unique secrets, without logging values; refuses repeat', t => {
  const {dir,secrets,output,run} = fixture(t);
  const result = run(); assert.equal(result.status, 0, result.stderr);
  const filename = path.join(output, 'metrics.env');
  const env = parseEnv(readFileSync(filename, 'utf8'));
  assert.equal(env.PUBLIC_BASE_URL, 'https://tracker.test');
  assert.equal(env.METRICS_SECRETS_FILE, secrets);
  assert.equal(env.METRICS_ENABLED, 'true');
  assert.match(env.METRICS_USERNAME, /^tracker-/);
  assert.ok(env.METRICS_PASSWORD.length >= 40);
  assert.match(env.TOKEN_ENCRYPTION_KEY, /^[a-f0-9]{64}$/);
  assert.notEqual(env.DATA_DIR, env.DB_PATH);
  for (const p of [output,env.DATA_DIR,env.DB_PATH]) assert.equal(statSync(p).mode & 0o777, 0o700);
  assert.equal(statSync(filename).mode & 0o777, 0o600);
  for (const value of [env.METRICS_USERNAME,env.METRICS_PASSWORD,env.TOKEN_ENCRYPTION_KEY,...Object.values(fake)]) assert.ok(!(result.stdout+result.stderr).includes(value));
  assert.ok(result.stdout.includes(filename)); assert.match(result.stdout, /--env-file=/);
  const before = readFileSync(filename, 'utf8');
  assert.notEqual(run().status, 0); assert.equal(readFileSync(filename,'utf8'),before);
  const other = path.join(dir,'another');
  const again = run(['--origin','https://tracker.example.test','--secrets-file',secrets,'--output-dir',other], false);
  assert.equal(again.status,0,again.stderr);
  const env2 = parseEnv(readFileSync(path.join(other,'metrics.env'),'utf8'));
  for (const key of ['METRICS_USERNAME','METRICS_PASSWORD','TOKEN_ENCRYPTION_KEY']) assert.notEqual(env[key],env2[key]);
});
test('invalid origins, arguments, repository targets and unsafe secrets fail without output', t => {
  const {dir,secrets,output,run} = fixture(t);
  for (const origin of ['http://tracker.test','http://localhost:3000','http://127.0.0.1:3000','http://[::1]:3000',"https://tracker'bad.test",'https://user:pass@tracker.test','https://tracker.test/path','https://tracker.test/','https://tracker.test?x=1','https://tracker.test#fragment','ftp://tracker.test','https://tracker.test\n','http://127.1','https://github.com/miguelgsousa/tracker-panel']) {
    assert.notEqual(run(['--origin',origin,'--secrets-file',secrets,'--output-dir',output],false).status,0,origin);
    assert.equal(existsSync(output),false);
  }
  for (const args of [[],['--origin'],['--help','--unexpected'],['--origin','https://tracker.test','--origin','https://other.test','--secrets-file',secrets]]) assert.notEqual(run(args,false).status,0);
  const alias = path.join(dir,'repo-alias'); symlinkSync(root,alias);
  for (const target of [path.join(root,'DO_NOT_CREATE'),path.join(alias,'DO_NOT_CREATE'),'relative-output']) assert.notEqual(run(['--origin','https://tracker.test','--secrets-file',secrets,'--output-dir',target],false).status,0);
  writeFileSync(secrets, JSON.stringify({...fake,ACCESS_TOKEN:'TEST_PRIVATE_VALUE'}));
  let bad = run(); assert.notEqual(bad.status,0); assert.ok(!(bad.stdout+bad.stderr).includes('TEST_PRIVATE_VALUE')); assert.equal(existsSync(output),false);
  writeFileSync(secrets,JSON.stringify(fake)); chmodSync(secrets,0o644);
  assert.notEqual(run().status,0); assert.equal(existsSync(output),false);
  chmodSync(secrets,0o600); writeFileSync(secrets,JSON.stringify({META_APP_ID:'123'}));
  assert.notEqual(run().status,0); assert.equal(existsSync(output),false);
  writeFileSync(secrets,JSON.stringify(fake)); mkdirSync(output,{mode:0o700}); writeFileSync(path.join(output,'keep'),'DO NOT DESTROY');
  assert.notEqual(run().status,0); assert.equal(readFileSync(path.join(output,'keep'),'utf8'),'DO NOT DESTROY');
});
test('npm entry point defaults to a new private home folder and accepts one complete provider', t => {
  const {dir,secrets} = fixture(t);
  writeFileSync(secrets,JSON.stringify({META_APP_ID:fake.META_APP_ID,META_APP_SECRET:fake.META_APP_SECRET}));
  const result = spawnSync('npm',['run','--silent','setup:metrics','--','--origin','https://tracker.test','--secrets-file',secrets],{cwd:root,env:{...cleanEnv,HOME:dir},encoding:'utf8'});
  assert.equal(result.status,0,result.stderr);
  const lines = result.stdout.trim().split('\n'); assert.equal(lines.length,2);
  const filename = lines[0].slice('Private env: '.length);
  assert.equal(path.dirname(path.dirname(filename)),dir);
  assert.match(path.basename(path.dirname(filename)),/^tracker-metrics-[a-f0-9]+$/);
  const env = parseEnv(readFileSync(filename,'utf8'));
  assert.equal(env.META_APP_ID,undefined); assert.equal(env.FACEBOOK_APP_ID,undefined);
});
test('generated env boots actual Tracker: Basic gate, config, empty SQLite and OAuth redirects (TEST apps only)', async t => {
  const {output,run} = fixture(t); const result = run(); assert.equal(result.status,0,result.stderr);
  const filename = path.join(output,'metrics.env'); const env = parseEnv(readFileSync(filename,'utf8'));
  const child = spawn(process.execPath, ['--env-file='+filename,'-e', `global.fetch=()=>{throw new Error('TEST: provider network forbidden')};const {app}=require('./server.js');const s=app.listen(0,'127.0.0.1',()=>process.send(s.address().port));`], {cwd:root,env:cleanEnv,stdio:['ignore','pipe','pipe','ipc']});
  t.after(async () => { if(child.exitCode === null) { const done = new Promise(r=>child.once('exit',r)); child.kill(); await done; } });
  const port = await new Promise((resolve,reject) => {
    const timer = setTimeout(()=>reject(new Error('TEST server startup timeout')),10000);
    child.once('message',p=>{clearTimeout(timer);resolve(p);});
    child.once('exit',()=>{clearTimeout(timer);reject(new Error('TEST server failed to start'));});
  });
  const auth = 'Basic '+Buffer.from(env.METRICS_USERNAME+':'+env.METRICS_PASSWORD).toString('base64');
  const request = (p,authenticated=true) => fetch('http://127.0.0.1:'+port+p,{redirect:'manual',headers:authenticated?{authorization:auth}:{}});
  assert.equal((await request('/',false)).status,401);
  assert.equal((await request('/api/metrics/config',false)).status,401);
  const config = await request('/api/metrics/config'); assert.equal(config.status,200);
  const text = await config.text(); assert.ok(!text.includes('TEST_ONLY')); assert.ok(!text.includes(env.TOKEN_ENCRYPTION_KEY));
  const configured = JSON.parse(text); assert.equal(configured.providers.instagram.configured,true); assert.equal(configured.providers.facebook.configured,true);
  const accounts = await request('/api/metrics/accounts'); assert.equal(accounts.status,200); assert.deepEqual((await accounts.json()).accounts,[]);
  for (const provider of ['instagram','facebook']) {
    const r = await request('/api/metrics/auth/start?provider='+provider); assert.equal(r.status,302);
    const url = new URL(r.headers.get('location'));
    assert.equal(url.searchParams.get('redirect_uri'),'https://tracker.test/auth/'+provider+'/callback');
    assert.equal(url.searchParams.get('client_id'),provider==='instagram'?fake.META_APP_ID:fake.FACEBOOK_APP_ID);
  }
  assert.ok(existsSync(path.join(env.DATA_DIR,'lume.sqlite')));
});
