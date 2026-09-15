// MOCK provider payloads, not a live Facebook authorization.
import test from 'node:test';
import assert from 'node:assert/strict';
import {Facebook,collectFacebook} from '../../lib/backend/facebook.mjs';
test('real-shaped Page post IDs collect lifetime metrics; foreign/malformed IDs never queried',async()=>{
 const id='123456789012345',post=id+'_987654321098765',now=Date.parse('2026-09-15T12:00:00Z'),calls=[];
 const api=new Facebook({FACEBOOK_APP_ID:'mock',FACEBOOK_APP_SECRET:'mock'},async input=>{
  const u=new URL(input);calls.push(u);const path=u.pathname;
  if(path.endsWith('/'+id))return Response.json({id,name:'Mock Page',followers_count:10});
  if(path.endsWith('/posts'))return Response.json({data:[post,post,'999_123',id+'_\\d',id+'_123/insights'].map(id=>({id,message:'Mock post',created_time:'2026-09-14T12:34:56+0000',permalink_url:'https://www.facebook.com/'+post}))});
  if(path.endsWith('/'+post))return Response.json({id:post,shares:{count:0},comments:{data:[],summary:{total_count:5}}});
  const metric=u.searchParams.get('metric');return Response.json({data:[{name:metric,period:u.searchParams.get('period'),values:[{value:metric==='post_media_view'?123:7}]}]});
 });
 const result=await collectFacebook(api,{id:'facebook:'+id,provider:'facebook',providerId:id,profile:{},token:'MOCK_PAGE',expiresAt:now+86400000},7,now);
 assert.equal(result.content.length,1);const m=result.content[0];
 assert.equal(m.id,'facebook:'+post);assert.equal(m.views,123);assert.equal(m.likes,7);assert.equal(m.comments,5);assert.equal(m.shares,0);assert.equal(m.saved,null);assert.equal(m.metricsScope,'lifetime');assert.equal(m.detailsSupported,false);
 assert.equal(calls.filter(u=>u.pathname.includes(post+'/insights')).length,2);
 assert.ok(calls.every(u=>!u.pathname.includes('999_123')&&!u.pathname.includes('%5C')&&!u.pathname.includes('/123/')));
});
