import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {Store} from '../../lib/backend/store.mjs';
import {createServer} from '../../lib/lume-server.mjs';
const auth='Basic '+Buffer.from('test:password').toString('base64');
test('today account views: auth, ownership, UTC bounds, zero/null, all totals, caching and midnight rollover',async()=>{
 const dir=await mkdtemp(tmpdir()+'/lume-today-'),key='ab'.repeat(32);let now=Date.parse('2026-09-13T20:00:00Z');
 const store=new Store(dir,Buffer.from(key,'hex'));
 store.data.sessions.old={expiresAt:now+864000000,states:{},connections:Object.fromEntries(['42','43','44'].map(id=>[id,{id,profile:{username:'test'+id},token:'PRIVATE_'+id,expiresAt:now+864000000,cache:{}}]))};store.save();
 let calls=[];
 const server=createServer({META_APP_ID:'123',META_APP_SECRET:'mock',PUBLIC_BASE_URL:'https://example.com',TOKEN_ENCRYPTION_KEY:key,DATA_DIR:dir,PERSONAL_USERNAME:'test',PERSONAL_PASSWORD:'password'},{now:()=>now,fetch:async u=>{const url=new URL(u);calls.push(url);await new Promise(r=>setTimeout(r,15));return Response.json({data:url.pathname.includes('/44/')?[]:[{name:'views',period:'day',total_value:{value:url.pathname.includes('/42/')?0:104}}]});}});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));const base=`http://127.0.0.1:${server.address().port}`,get=(q,headers={authorization:auth})=>fetch(base+'/api/today-views?account='+q,{headers});
 try{
 assert.equal((await get('all',{})).status,401);assert.equal((await get('../42')).status,400);assert.equal((await get('99')).status,404);assert.equal(calls.length,0);
 assert.equal((await fetch(base+'/api/today-views?account=42',{method:'POST',headers:{authorization:auth}})).status,405);
 const rs=await Promise.all([get('42'),get('42')]);for(const r of rs){assert.equal(r.status,200);const d=await r.json();assert.equal(d.metrics.views,null);assert.equal(d.accountViews[0].error,'current_day_window_unverified');assert.equal(d.range.timezone,'UTC');assert.equal(d.range.since,Date.parse('2026-09-13T00:00:00Z')/1000);assert.equal(d.range.until,now/1000);assert.equal(d.range.incomplete,true);assert.equal(d.providerDelayHours,48);}
 assert.equal(calls.length,1);await get('42');assert.equal(calls.length,1);
 const all=await (await get('all')).json();assert.equal(calls.length,3);assert.equal(all.metrics.views,null);assert.equal(all.knownSubtotal,null);assert.equal(all.availableAccounts,0);assert.equal(all.partial,true);assert.equal(all.accountViews.find(a=>a.accountId==='44').views,null);assert.ok(!JSON.stringify(all).includes('PRIVATE_'));
 for(const url of calls){assert.ok(url.pathname.endsWith('/insights'));assert.equal(url.searchParams.get('metric'),'views');assert.equal(url.searchParams.get('period'),'day');assert.equal(url.searchParams.get('metric_type'),'total_value');assert.equal(url.searchParams.get('since'),String(Date.parse('2026-09-13T00:00:00Z')/1000));assert.equal(url.searchParams.get('until'),String(now/1000));}
 now=Date.parse('2026-09-14T00:00:01Z');const next=await(await get('42')).json();assert.equal(calls.length,4);assert.equal(next.range.since,Date.parse('2026-09-14T00:00:00Z')/1000);
 }finally{await new Promise(r=>server.close(r));await rm(dir,{recursive:true,force:true});}
});
