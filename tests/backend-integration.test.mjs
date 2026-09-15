// Provider responses are explicit mocks. Only temporary directories are written.
import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import {createRequire} from 'node:module';
import {mkdtemp,rm,readFile,writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {PersonalStore} from '../lib/backend/personal-store.mjs';
const require=createRequire(import.meta.url);
const {createMetricsIntegration}=require('../lib/metrics-integration.js');
const origin='https://tracker.example.com';
const auth='Basic '+Buffer.from('test-owner:test-password-only').toString('base64');
async function fixture(fn,{configured=true,seed=true}={}){
 const dir=await mkdtemp(tmpdir()+'/tracker-backend-');
 let now=Date.parse('2026-09-15T12:00Z'),failure=false,calls=0;
 const key='ab'.repeat(32);
 if(seed){const s=new PersonalStore(dir,Buffer.from(key,'hex'));s.data.workspace.connections['42']={id:'42',profile:{username:'mock_account'},token:'PROVIDER_TOKEN_NEVER_PUBLIC',tokenIssuedAt:now,expiresAt:now+86400000*10,status:'connected',cache:{}};s.save();s.close();}
 const env={METRICS_USERNAME:'test-owner',...(configured?{METRICS_PASSWORD:'test-password-only'}:{}),META_APP_ID:'123',META_APP_SECRET:'APP_SECRET_NEVER_PUBLIC',FACEBOOK_APP_ID:'456',FACEBOOK_APP_SECRET:'FB_SECRET_NEVER_PUBLIC',TOKEN_ENCRYPTION_KEY:key,PUBLIC_BASE_URL:origin,DATA_DIR:dir};
 const provider=async(input)=>{calls++;const u=new URL(input);if(failure)return Response.json({error:{code:failure==='auth'?190:2}},{status:400});if(u.pathname.endsWith('/42'))return Response.json({id:'42',username:'mock_account',followers_count:4});if(u.pathname.endsWith('/media'))return Response.json({data:[]});const m=u.searchParams.get('metric');return Response.json({data:[{name:m,period:'day',total_value:{value:m==='views'?123:0},values:[{value:0,end_time:'2026-09-14T07:00:00Z'}]}]});};
 const middleware=createMetricsIntegration(env,{now:()=>now,fetch:provider});
 const app=express();app.use(middleware);app.get('/',(req,res)=>res.send('tracker'));
 const server=await new Promise(r=>{const s=app.listen(0,'127.0.0.1',()=>r(s));});
 const request=(p,o={})=>fetch('http://127.0.0.1:'+server.address().port+p,{redirect:'manual',...o,headers:{authorization:auth,...o.headers}});
 try{await fn({request,dir,calls:()=>calls,advance:n=>now+=n,fail:v=>failure=v});}finally{await new Promise(r=>server.close(r));await middleware.close();await rm(dir,{recursive:true,force:true});}
}
test('mounted metrics fail closed with no access credentials (including callbacks)',()=>fixture(async({request,calls})=>{
 for(const p of ['/api/metrics/config','/api/metrics/accounts','/api/metrics/dashboard','/api/metrics/auth/start','/auth/instagram/callback','/auth/login'])assert.equal((await request(p)).status,503);
 assert.equal(calls(),0);
},{configured:false}));
test('all mounted private routes enforce auth, expose no CORS and no secrets',()=>fixture(async({request})=>{
 for(const p of ['/api/metrics/config','/api/metrics/accounts','/api/metrics/dashboard','/api/metrics/today-views','/api/metrics/media-details','/api/metrics/diagnostics','/api/metrics/auth/start','/auth/facebook/callback']){
  const r=await request(p,{headers:{authorization:''}});assert.equal(r.status,401,p);assert.match(r.headers.get('www-authenticate'),/Basic/);assert.equal(r.headers.get('access-control-allow-origin'),null);
 }
 const r=await request('/api/metrics/accounts');const text=await r.text();assert.equal(r.headers.get('cache-control'),'no-store');assert.ok(!text.includes('NEVER_PUBLIC'));assert.equal(JSON.parse(text).accounts[0].id,'42');
 assert.equal((await request('/auth/login')).headers.get('location'),'/');
}));
test('mounted OAuth uses Tracker callback and provider-bound cookie state, never a Lume proxy',()=>fixture(async({request,calls})=>{
 const r=await request('/api/metrics/auth/start?provider=instagram');assert.equal(r.status,302);const u=new URL(r.headers.get('location'));assert.equal(u.hostname,'www.instagram.com');assert.equal(u.searchParams.get('redirect_uri'),origin+'/auth/instagram/callback');assert.equal(u.searchParams.get('force_reauth'),'false');assert.match(r.headers.get('set-cookie'),/Secure; SameSite=Lax/);
 const fb=await request('/auth/start?provider=facebook');assert.equal(new URL(fb.headers.get('location')).searchParams.get('redirect_uri'),origin+'/auth/facebook/callback');
 const bad=await request('/auth/instagram/callback?state='+u.searchParams.get('state')+'&code=MOCK',{headers:{cookie:fb.headers.get('set-cookie').split(';')[0]}});assert.match(bad.headers.get('location'),/invalid_state/);assert.equal(calls(),0);
}));
test('mounted exact-range cache reuses results, preserves stale snapshot, guards today and disconnect',()=>fixture(async({request,calls,advance,fail,dir})=>{
 const get=async days=>{const r=await request('/api/metrics/dashboard?provider=instagram&account=42&days='+days);assert.equal(r.status,200);return r.json();};
 const good=await get(7);assert.equal(good.metrics.views,123);const count=calls();assert.deepEqual(await get(7),good);assert.equal(calls(),count);await get(30);assert.equal((await get(7)).lastSynced,good.lastSynced);
 advance(301000);fail(true);const stale=await get(7);assert.equal(stale.metrics.views,123);assert.equal(stale.stale,true);assert.equal(stale.partial,true);assert.equal(stale.lastSynced,good.lastSynced);assert.notEqual(stale.refreshAttemptedAt,good.lastSynced);assert.equal((await get(1)).metrics.views,null);
 fail(false);const today=await (await request('/api/metrics/today-views?account=42')).json();assert.equal(today.metrics.views,null);assert.equal(today.accountViews[0].error,'current_day_window_unverified');
 assert.equal((await request('/api/metrics/media-details?account=42&media=99')).status,404);
 assert.equal((await request('/api/metrics/dashboard?provider=evil')).status,400);
 assert.equal((await request('/api/metrics/disconnect?account=42',{method:'POST',headers:{origin:'https://evil.example'}})).status,403);
 assert.equal((await request('/auth/disconnect?account=42',{method:'POST',headers:{origin}})).status,200);
 assert.deepEqual((await (await request('/api/metrics/accounts')).json()).accounts,[]);
 const disk=await readFile(dir+'/lume.sqlite');assert.ok(!disk.includes(Buffer.from('PROVIDER_TOKEN_NEVER_PUBLIC')));
}));
test('Tracker static allowlist blocks repo/secrets and existing other-platform CRUD works',async()=>{
 const dir=await mkdtemp(tmpdir()+'/tracker-legacy-');const prior=process.env.DB_PATH;process.env.DB_PATH=dir;
 await writeFile(dir+'/accounts.json',JSON.stringify({youtube:[],_settings:{instagramToken:'LEGACY_SECRET_NEVER_RETURN'},_folders:{youtube:[]}}));
 const {app,metricsIntegration}=require('../server.js');const server=await new Promise(r=>{const s=app.listen(0,'127.0.0.1',()=>r(s));});
 const request=(p,o)=>fetch('http://127.0.0.1:'+server.address().port+p,o);
 try{
  assert.equal((await request('/')).status,200);
  for(const p of ['/server.js','/accounts.json','/.env','/.env.metrics.example','/package.json','/lib/lume-server.mjs','/.git/config','/ig_scraper.py','/node_modules/express/package.json'])assert.equal((await request(p)).status,404,p);
  const all=await (await request('/api/accounts')).text();assert.ok(!all.includes('LEGACY_SECRET'));assert.equal((await request('/api/accounts/_settings')).status,400);
  assert.equal((await request('/api/settings/instagram-token',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({token:'NO_WRITE'})})).status,410);
  const added=await request('/api/accounts/youtube',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({handle:'mock-channel',name:'Mock'})});assert.equal(added.status,201);const a=await added.json();assert.equal((await (await request('/api/accounts/youtube')).json()).length,1);
  assert.equal((await request('/api/accounts/youtube/'+a.id,{method:'DELETE'})).status,200);assert.deepEqual(await (await request('/api/accounts/youtube')).json(),[]);
 }finally{await new Promise(r=>server.close(r));await metricsIntegration.close();if(prior===undefined)delete process.env.DB_PATH;else process.env.DB_PATH=prior;await rm(dir,{recursive:true,force:true});}
});
