import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,readFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {Store} from '../../lib/backend/store.mjs';
import {createServer} from '../../lib/lume-server.mjs';
const auth='Basic '+Buffer.from('miguel:TestPassword123').toString('base64');
test('personal SQLite migrates accounts and shares them across authenticated browsers only',async()=>{
 const dir=await mkdtemp(tmpdir()+'/lume-personal-'),key='ab'.repeat(32);const legacy=new Store(dir,Buffer.from(key,'hex'));
 legacy.data.sessions.old={expiresAt:Date.now()+86400000,connections:{'42':{id:'42',profile:{username:'private_test'},token:'TOKEN_NEVER_PUBLIC',tokenIssuedAt:Date.now(),expiresAt:Date.now()+86400000,status:'connected',cache:{}}},states:{}};legacy.save();
 const env={META_APP_ID:'123',META_APP_SECRET:'mock',PUBLIC_BASE_URL:'https://example.com',TOKEN_ENCRYPTION_KEY:key,DATA_DIR:dir,PERSONAL_PASSWORD:'TestPassword123',PERSONAL_USERNAME:'miguel'};
 let server,base;const start=async()=>{server=createServer(env);await new Promise(r=>server.listen(0,'127.0.0.1',r));base=`http://127.0.0.1:${server.address().port}`};await start();
 try{
 assert.equal((await fetch(base+'/api/config')).status,401);
 assert.equal((await fetch(base+'/api/config',{headers:{authorization:'Basic bad'}})).status,401);
 for(const cookie of ['', '__Host-lume_session=unrelated']){const r=await fetch(base+'/api/config',{headers:{authorization:auth,cookie}});assert.equal(r.status,200);const d=await r.json();assert.equal(d.connected,true);assert.equal(d.sessionOwnership,'personal');assert.equal(d.accountCount,1);assert.ok(!JSON.stringify(d).includes('TOKEN_NEVER_PUBLIC'));}
 assert.equal((await fetch(base+'/api/disconnect?account=all',{method:'POST',headers:{authorization:auth,origin:'https://evil.example'}})).status,403);
 const startA=await fetch(base+'/api/auth/start?provider=instagram',{redirect:'manual',headers:{authorization:auth}}),startB=await fetch(base+'/api/auth/start?provider=instagram',{redirect:'manual',headers:{authorization:auth}});const stateA=new URL(startA.headers.get('location')).searchParams.get('state');
 const wrongCallback=await fetch(base+'/auth/instagram/callback?code=mock&state='+stateA,{redirect:'manual',headers:{authorization:auth,cookie:startB.headers.get('set-cookie').split(';')[0]}});assert.match(wrongCallback.headers.get('location'),/invalid_state/);
 assert.equal((await (await fetch(base+'/api/diagnostics',{headers:{authorization:auth}})).json()).lastOAuthError.code,'invalid_state');
 const db=await readFile(dir+'/lume.sqlite');assert.ok(!db.includes(Buffer.from('TOKEN_NEVER_PUBLIC')));
 await new Promise(r=>server.close(r));await start();assert.equal((await (await fetch(base+'/api/config',{headers:{authorization:auth}})).json()).accountCount,1);
 const d=await fetch(base+'/api/disconnect?account=42',{method:'POST',headers:{authorization:auth,origin:'https://example.com'}});assert.equal(d.status,200);assert.equal((await (await fetch(base+'/api/config',{headers:{authorization:auth}})).json()).connected,false);
 }finally{await new Promise(r=>server.close(r));await rm(dir,{recursive:true,force:true})}
});
