import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {createServer} from '../../lib/lume-server.mjs';
test('Instagram authorization allows existing login session',async()=>{
 const dir=await mkdtemp(tmpdir()+'/lume-reauth-');
 const server=createServer({META_APP_ID:'123',META_APP_SECRET:'mock',PUBLIC_BASE_URL:'https://example.com',DATA_DIR:dir,TOKEN_ENCRYPTION_KEY:'ab'.repeat(32)});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 try{const response=await fetch(`http://127.0.0.1:${server.address().port}/api/auth/start?provider=instagram`,{redirect:'manual'});assert.equal(response.status,302);const u=new URL(response.headers.get('location'));assert.equal(u.searchParams.get('force_reauth'),'false');assert.ok(u.searchParams.get('state'));assert.equal(u.searchParams.get('response_type'),'code');}
 finally{await new Promise(r=>server.close(r));await rm(dir,{recursive:true,force:true});}
});
