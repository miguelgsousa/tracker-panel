import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {Store} from '../../lib/backend/store.mjs';
import {createServer} from '../../lib/lume-server.mjs';
const auth='Basic '+Buffer.from('test:password').toString('base64');
test('media details authenticate, validate cached ownership, bound/cache calls and preserve partial/null semantics',async()=>{
 const dir=await mkdtemp(tmpdir()+'/lume-media-'),key='ab'.repeat(32),now=Date.now();
 const store=new Store(dir,Buffer.from(key,'hex'));
 const media={id:'111',accountId:'42',type:'reel',caption:'Test',url:'https://www.instagram.com/reel/test/',views:100};
 store.data.sessions.old={expiresAt:now+86400000,states:{},connections:{'42':{id:'42',profile:{username:'test'},token:'PRIVATE_TOKEN',expiresAt:now+86400000,cache:{30:{savedAt:now,data:{content:[media,{...media,id:'112',type:'image'}]}}}},'43':{id:'43',profile:{username:'other'},token:'OTHER_TOKEN',expiresAt:now+86400000,cache:{}}}};store.save();
 let calls=0;
 const server=createServer({META_APP_ID:'123',META_APP_SECRET:'mock',PUBLIC_BASE_URL:'https://example.com',TOKEN_ENCRYPTION_KEY:key,DATA_DIR:dir,PERSONAL_USERNAME:'test',PERSONAL_PASSWORD:'password'},{fetch:async u=>{calls++;const m=new URL(u).searchParams.get('metric');await new Promise(r=>setTimeout(r,15));return new Response(JSON.stringify(m==='reels_skip_rate'?{error:{code:10,message:'denied'}}:{data:[{name:m,values:[{value:m==='ig_reels_avg_watch_time'?0:20923211}]}]}),{status:m==='reels_skip_rate'?400:200});}});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}/api/media-details`,get=(query,headers={authorization:auth})=>fetch(base+query,{headers});
 try{
 assert.equal((await get('?account=42&media=111',{})).status,401);
 assert.equal((await get('?account=all&media=111')).status,400);
 assert.equal((await get('?account=42&media=../111')).status,400);
 assert.equal((await get('?account=99&media=111')).status,404);
 assert.equal((await get('?account=43&media=111')).status,404);
 assert.equal((await get('?account=42&media=999')).status,404);assert.equal(calls,0);
 assert.equal((await fetch(base+'?account=42&media=111',{method:'POST',headers:{authorization:auth}})).status,405);
 const results=await Promise.all([get('?account=42&media=111'),get('?account=42&media=111')]);
 for(const r of results){assert.equal(r.status,200);const d=await r.json();assert.equal(d.metrics.ig_reels_avg_watch_time.value,0);assert.equal(d.metrics.ig_reels_avg_watch_time.unit,'milliseconds');assert.equal(d.metrics.reels_skip_rate.value,null);assert.equal(d.partial,true);assert.equal(d.retentionCurve.available,false);assert.equal(d.viewCountries.available,false);assert.ok(!JSON.stringify(d).includes('PRIVATE_TOKEN'));}
 assert.equal(calls,3);await get('?account=42&media=111');assert.equal(calls,3);
 const photo=await (await get('?account=42&media=112')).json();assert.equal(photo.metrics.ig_reels_avg_watch_time.value,null);assert.equal(calls,3);
 }finally{await new Promise(r=>server.close(r));await rm(dir,{recursive:true,force:true});}
});
