import test from 'node:test';
import assert from 'node:assert/strict';
import {dailyTotal} from '../../lib/backend/facebook.mjs';
test('complete daily buckets accept adjacent-window paging links without following them',()=>{const start=Date.parse('2026-09-08T00:00:00Z')/1000,end=start+7*86400;const d={data:[{name:'page_media_view',period:'day',values:Array.from({length:7},(_,i)=>({value:2,end_time:new Date((start+i*86400+7*3600)*1000).toISOString()}))}],paging:{next:'https://graph.facebook.com/next-window',previous:'https://graph.facebook.com/previous-window'}};assert.equal(dailyTotal(d,'page_media_view',start,end),14);d.data[0].values.pop();assert.equal(dailyTotal(d,'page_media_view',start,end),null);});

test('padded daily responses select only complete in-range end_time buckets, never adjacent/future totals',()=>{
 const start=Date.parse('2026-09-08T00:00Z')/1000,end=start+7*86400;
 const values=Array.from({length:8},(_,i)=>({value:i===7?999:2,end_time:new Date((start+i*86400+7*3600)*1000).toISOString()}));
 const data=v=>({data:[{name:'page_media_view',period:'day',values:v}]});
 assert.equal(dailyTotal(data(values),'page_media_view',start,end),14);
 assert.equal(dailyTotal(data(values.slice(1)),'page_media_view',start,end),null);
 assert.equal(dailyTotal(data([...values,values[0]]),'page_media_view',start,end),null);
 assert.equal(dailyTotal(data([...values,{value:1,end_time:'invalid'}]),'page_media_view',start,end),null);
});
test('collector pads provider query then validates requested buckets (MOCK of live one-day shifted response)',async()=>{
 const {Facebook,collectFacebook}=await import('../../lib/backend/facebook.mjs');const now=Date.parse('2026-09-15T03:00Z'),until=Math.floor(now/86400000)*86400;const calls=[];
 const api=new Facebook({FACEBOOK_APP_ID:'mock',FACEBOOK_APP_SECRET:'mock'},async input=>{const u=new URL(input);if(u.pathname.endsWith('/42'))return Response.json({id:'42',followers_count:1});if(u.pathname.endsWith('/posts'))return Response.json({data:[]});calls.push(u);const start=Number(u.searchParams.get('since')),end=Number(u.searchParams.get('until'))+1,metric=u.searchParams.get('metric');return Response.json({data:[{name:metric,period:'day',values:Array.from({length:(end-start)/86400},(_,i)=>({value:2,end_time:new Date((start+(i+1)*86400+7*3600)*1000).toISOString()}))}]});});
 for(const days of [1,7,30,90]){calls.length=0;const d=await collectFacebook(api,{id:'facebook:42',provider:'facebook',providerId:'42',profile:{},token:'MOCK',expiresAt:now+60000},days,now);assert.equal(d.metrics.views,days*2);assert.equal(d.range.since,until-days*86400);assert.equal(d.range.until,until);assert.ok(!d.errors.some(e=>e.code==='incomplete_daily_series'));assert.ok(calls.every(u=>Number(u.searchParams.get('until'))-Number(u.searchParams.get('since'))<30*86400));}
});
