import test from 'node:test';
import assert from 'node:assert/strict';
import {Instagram} from '../../lib/backend/instagram.mjs';
import {todayViews,todayPayload} from '../../lib/backend/today-views.mjs';
test('today provider cannot turn total_value, time_series or malformed buckets into current-day views',async()=>{
 const now=Date.parse('2026-09-13T20:00:00Z'),c={id:'42',token:'secret',expiresAt:now+1000};
 for(const data of [[{name:'views',period:'day',total_value:{value:104}}],[{name:'views',period:'day',total_value:{value:0}}],[{name:'views',period:'day',values:[{value:104,end_time:'2026-09-13T07:00:00Z'}]}],[{name:'views',period:'day',total_value:{value:-1}}]]){
 const r=await todayViews(new Instagram({},async()=>Response.json({data})),c,now);assert.equal(r.views,null);assert.ok(r.error);
 }
});
test('today provider bounds budget/time and preserves expiry, permission and timeout failures',async()=>{
 const now=Date.now(),c={id:'42',token:'secret',expiresAt:now+1000};let calls=0;
 const expired=await todayViews(new Instagram({},async()=>{calls++;}),{...c,expiresAt:now},now);assert.equal(expired.error,'token_expired');assert.equal(calls,0);
 const denied=await todayViews(new Instagram({},async(u,init)=>{calls++;assert.ok(init.signal instanceof AbortSignal);assert.equal(init.redirect,'error');return Response.json({error:{code:10,message:'permission denied'}},{status:400});}),c,now);assert.equal(denied.error,'permission_denied');assert.equal(calls,1);
 const timeout=await todayViews(new Instagram({},async()=>{throw new DOMException('timeout','TimeoutError')}),c,now);assert.equal(timeout.error,'provider_timeout');assert.equal(timeout.views,null);
});
test('today aggregate query end is earliest cached window, not newly requested time',()=>{
 const now=Date.parse('2026-09-13T20:00:00Z'),until=Math.floor(now/1000)-200;
 const d=todayPayload([{accountId:'42',views:null,range:{until},checkedAt:new Date(until*1000).toISOString()}],[],now);
 assert.equal(d.range.until,until);assert.equal(d.requestedRange.until,Math.floor(now/1000));
});
