// Explicit MOCK responses only. No real Facebook authorization or production storage.
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,readFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {createServer} from '../../lib/lume-server.mjs';
import {Store} from '../../lib/backend/store.mjs';
const now=Date.parse('2026-09-15T12:00:00Z');
async function fixture(fn,overrides={}){
 const dir=await mkdtemp(tmpdir()+'/lume-facebook-');const calls=[];
 const env={META_APP_ID:'123',META_APP_SECRET:'IG_MOCK_SECRET',FACEBOOK_APP_ID:'456',FACEBOOK_APP_SECRET:'FB_MOCK_SECRET',PUBLIC_BASE_URL:'https://example.com',DATA_DIR:dir,TOKEN_ENCRYPTION_KEY:'ab'.repeat(32),...overrides};
 const provider=async(input,init={})=>{const u=new URL(input);calls.push({u,init});assert.equal(u.hostname,'graph.facebook.com');let d;
 if(u.pathname.endsWith('/oauth/access_token'))d={access_token:'MOCK_FB_USER',expires_in:5184000};
 else if(u.pathname.endsWith('/me/permissions'))d={data:['pages_show_list','pages_read_engagement','read_insights'].map(permission=>({permission,status:'granted'}))};
 else if(u.pathname.endsWith('/me/accounts'))d={data:[{id:'42',name:'Mock Facebook Page',access_token:'MOCK_PAGE_TOKEN',tasks:['ANALYZE']}]};
 else if(u.pathname.endsWith('/42'))d={id:'42',name:'Mock Facebook Page',followers_count:125};
 else if(u.pathname.endsWith('/42_99'))d={id:'42_99',shares:{count:3},comments:{data:[],summary:{total_count:2}}};
 else if(u.pathname.endsWith('/42/posts'))d={data:[{id:'42_99',message:'Mock post',created_time:'2026-09-14T12:00:00Z',permalink_url:'https://www.facebook.com/42/posts/99'}]};
 else if(u.pathname.endsWith('/insights')){const metric=u.searchParams.get('metric');d={data:[{name:metric,period:u.searchParams.get('period'),values:metric.startsWith('page_')?Array.from({length:7},(_,i)=>({value:2,end_time:new Date(Date.parse('2026-09-08T07:00:00Z')+i*86400000).toISOString()})):[{value:0}]}]};}
 else throw Error('Unexpected MOCK endpoint');
 return new Response(JSON.stringify(d));};
 let server,base;async function start(){server=createServer(env,{fetch:provider,now:()=>now});await new Promise(r=>server.listen(0,'127.0.0.1',r));base='http://127.0.0.1:'+server.address().port;}await start();
 const request=(path,opts={})=>fetch(base+path,{redirect:'manual',...opts});
 const begin=async(provider='facebook')=>{const r=await request('/api/auth/start?provider='+provider);assert.equal(r.status,302);return {r,cookie:r.headers.get('set-cookie').split(';')[0],state:new URL(r.headers.get('location')).searchParams.get('state')};};
 try{await fn({env,dir,calls,request,begin,restart:async()=>{await new Promise(r=>server.close(r));await start();}});}finally{await new Promise(r=>server.close(r));await rm(dir,{recursive:true,force:true});}
}
test('Facebook optional config is separate; no assumption that Instagram app is Facebook app',()=>fixture(async({request,calls})=>{const c=await (await request('/api/config')).json();assert.equal(c.configured,true);assert.equal(c.providers.facebook.configured,false);assert.ok(c.providers.facebook.missingConfiguration.includes('FACEBOOK_APP_ID'));assert.equal((await request('/api/auth/start?provider=facebook')).status,503);assert.equal(calls.length,0);},{FACEBOOK_APP_ID:'',FACEBOOK_APP_SECRET:''}));
test('Facebook OAuth uses own app, provider-bound one-use state and encrypted Page token, not user or Instagram token',()=>fixture(async({request,begin,calls,dir,restart})=>{
 const a=await begin(),ig=await begin('instagram');const u=new URL(a.r.headers.get('location'));assert.equal(u.hostname,'www.facebook.com');assert.equal(u.searchParams.get('client_id'),'456');assert.equal(u.searchParams.get('redirect_uri'),'https://example.com/auth/facebook/callback');assert.equal(u.searchParams.get('scope'),'pages_show_list,pages_read_engagement,read_insights');
 assert.match((await request('/auth/instagram/callback?state='+a.state+'&code=mock',{headers:{cookie:a.cookie}})).headers.get('location'),/invalid_state/);
 assert.match((await request('/auth/facebook/callback?state='+ig.state+'&code=mock',{headers:{cookie:ig.cookie}})).headers.get('location'),/invalid_state/);assert.equal(calls.length,0);
 assert.equal((await request('/auth/facebook/callback?state='+a.state+'&code=mock',{headers:{cookie:a.cookie}})).headers.get('location'),'/?connected=1&provider=facebook');
 assert.match((await request('/auth/facebook/callback?state='+a.state+'&code=mock',{headers:{cookie:a.cookie}})).headers.get('location'),/invalid_state/);
 const text=await readFile(dir+'/store.enc','utf8');assert.ok(!text.includes('MOCK_PAGE_TOKEN'));const disk=new Store(dir,Buffer.from('ab'.repeat(32),'hex'));const saved=Object.values(disk.data.sessions).flatMap(s=>Object.values(s.connections));assert.equal(saved[0].id,'facebook:42');assert.equal(saved[0].token,'MOCK_PAGE_TOKEN');assert.ok(!JSON.stringify(disk.data).includes('MOCK_FB_USER'));
 await restart();const headers={cookie:a.cookie};const d=await (await request('/api/dashboard?account=facebook:42&days=7',{headers})).json();assert.equal(d.accounts[0].provider,'facebook');assert.equal(d.accounts[0].status,'connected');assert.equal(d.accounts[0].providerId,'42');assert.equal(d.metrics.followers,125);assert.equal(d.metrics.views,14);assert.equal(d.metrics.reach,null);assert.equal(d.content.length,1);assert.equal(d.content[0].id,'facebook:42_99');assert.equal(d.content[0].comments,2);assert.equal(d.content[0].shares,3);assert.equal(d.content[0].views,0);assert.equal(d.content[0].saved,null);assert.equal(d.source,'Facebook Pages API');assert.ok(!JSON.stringify(d).includes('MOCK_PAGE_TOKEN'));
 const filtered=await (await request('/api/dashboard?provider=instagram&days=7',{headers})).json();assert.equal(filtered.content.length,0);assert.deepEqual(filtered.selectedAccountIds,[]);assert.equal((await request('/api/dashboard?provider=unknown',{headers})).status,400);
 const count=calls.length;const today=await (await request('/api/today-views?account=facebook:42',{headers})).json();assert.equal(today.metrics.views,null);assert.ok(today.errors.some(x=>x.code==='current_day_window_unverified'));
 assert.equal((await request('/api/media-details?account=facebook:42&media=facebook:42_99',{headers})).status,422);assert.equal(calls.length,count+1);
 assert.equal((await request('/api/dashboard?account=facebook:42')).status,404);
 assert.equal((await request('/api/disconnect?account=facebook:42',{method:'POST',headers:{...headers,origin:'https://evil.test'}})).status,403);
 assert.equal((await request('/api/disconnect?account=facebook:42',{method:'POST',headers:{...headers,origin:'https://example.com'}})).status,200);
}));
test('Facebook can be configured alone without disabling shared encrypted storage',()=>fixture(async({request})=>{const c=await (await request('/api/config')).json();assert.equal(c.configured,false);assert.equal(c.providers.facebook.configured,true);assert.equal((await request('/api/auth/start?provider=facebook')).status,302);},{META_APP_ID:'',META_APP_SECRET:''}));
test('Facebook collector is bounded, never substitutes deprecated reach or partial daily sums',async()=>{
 const {Facebook,collectFacebook}=await import('../../lib/backend/facebook.mjs');const calls=[];const api=new Facebook({FACEBOOK_APP_ID:'456',FACEBOOK_APP_SECRET:'mock'},async(input,init)=>{const u=new URL(input);calls.push(u);assert.equal(init.headers.Authorization,'Bearer PAGE_ONLY');if(u.pathname.endsWith('/42'))return Response.json({id:'42',name:'Page',followers_count:0});if(u.pathname.endsWith('/posts'))return Response.json({data:Array.from({length:25},(_,i)=>({id:'42_'+(100+i),created_time:'2026-09-14T12:00:00Z'})),paging:{next:'https://evil.test',cursors:{after:'cursor'}}});const metric=u.searchParams.get('metric');return Response.json({data:[{name:metric,period:u.searchParams.get('period'),values:[{value:0,end_time:'2026-09-14T07:00:00Z'}]}]});});
 const c={id:'facebook:42',provider:'facebook',providerId:'42',token:'PAGE_ONLY',expiresAt:now+86400000,profile:{}};const d=await collectFacebook(api,c,90,now);assert.equal(d.metrics.followers,0);assert.equal(d.metrics.views,null);assert.equal(d.metrics.reach,null);assert.equal(d.content.length,12);assert.equal(d.truncated,true);assert.ok(calls.length<=60);assert.ok(calls.every(u=>u.hostname==='graph.facebook.com'));assert.ok(calls.every(u=>!u.search.includes('impressions')));assert.ok(d.errors.some(e=>e.code==='incomplete_daily_series'));
 calls.length=0;await collectFacebook(api,{...c,expiresAt:now-1},7,now);assert.equal(calls.length,0);
});
