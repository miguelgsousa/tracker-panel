import test from 'node:test';
import assert from 'node:assert/strict';
import {Instagram} from '../../lib/backend/instagram.mjs';
import {createServer} from '../../lib/lume-server.mjs';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
test('MOCK callback validates short-token profile and persists visible temporary account',async()=>{
 const dir=await mkdtemp(tmpdir()+'/lume-short-');const server=createServer({META_APP_ID:'123',META_APP_SECRET:'secret',PUBLIC_BASE_URL:'https://example.com',TOKEN_ENCRYPTION_KEY:'ab'.repeat(32),DATA_DIR:dir,PERSONAL_PASSWORD:'test'}, {fetch:async(url)=>{
  if(url.includes('api.instagram.com/oauth/'))return new Response(JSON.stringify({access_token:'valid-short'}));
  if(url.includes('graph.instagram.com/access_token'))return new Response(JSON.stringify({error:{code:100,message:'Unsupported request - method type: get'}}),{status:400});
  return new Response(JSON.stringify({user_id:'456',username:'mock_profile'}));
 }});await new Promise(r=>server.listen(0,'127.0.0.1',r));
 try{const base='http://127.0.0.1:'+server.address().port,headers={authorization:'Basic '+Buffer.from('miguel:test').toString('base64')};const start=await fetch(base+'/api/auth/start',{headers,redirect:'manual'});headers.cookie=start.headers.get('set-cookie').split(';')[0];const state=new URL(start.headers.get('location')).searchParams.get('state');const callback=await fetch(base+'/auth/instagram/callback?code=test&state='+state,{headers,redirect:'manual'});assert.equal(callback.headers.get('location'),'/?connected=1&warning=short_lived_token');const data=await(await fetch(base+'/api/accounts',{headers})).json();assert.equal(data.accounts[0].tokenLifetime,'short');assert.ok(!JSON.stringify(data).includes('valid-short'));const diagnostics=await(await fetch(base+'/api/diagnostics',{headers})).json();assert.equal(diagnostics.lastOAuthError.provider.endpoint,'long_token');}
 finally{await new Promise(r=>server.close(r));await rm(dir,{recursive:true,force:true});}
});
test('MOCK preserves short token only when long exchange fails, with bounded local validity',async()=>{
 let n=0;const api=new Instagram({META_APP_SECRET:'secret'},async()=>++n===1?new Response(JSON.stringify({access_token:'short-valid'})):new Response(JSON.stringify({error:{code:100,message:'Unsupported request - method type: get'}}),{status:400}));
 const result=await api.exchange('code');assert.equal(result.access_token,'short-valid');assert.equal(result.tokenLifetime,'short');assert.ok(result.expires_in<=1800);assert.equal(result.upgradeError.details.endpoint,'long_token');
});
test('MOCK rejected initial authorization never becomes connected',async()=>{
 const api=new Instagram({},async()=>new Response(JSON.stringify({error:{code:100,message:'Bad authorization'}}),{status:400}));await assert.rejects(api.exchange('bad'));
});
