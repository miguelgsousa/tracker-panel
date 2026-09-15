import test from 'node:test';
import assert from 'node:assert/strict';
import {Instagram} from '../../lib/backend/instagram.mjs';
test('MOCK provider diagnostic distinguishes exchange steps and redacts credentials',async()=>{
 const api=new Instagram({META_APP_ID:'app',META_APP_SECRET:'secret-value',PUBLIC_BASE_URL:'https://example.com'},async()=>new Response(JSON.stringify({error_type:'OAuthException',code:400,error_message:'Invalid platform app; secret-value code-value'}),{status:400}));
 await assert.rejects(api.exchange('code-value'),e=>{assert.equal(e.details.endpoint,'short_token');assert.equal(e.details.httpStatus,400);assert.equal(e.details.providerCode,400);assert.match(e.details.message,/Invalid platform app/);assert.ok(!JSON.stringify(e.details).includes('secret-value'));assert.ok(!JSON.stringify(e.details).includes('code-value'));return true;});
 let i=0;const second=new Instagram({META_APP_SECRET:'secret-value'},async()=>new Response(JSON.stringify({error:{code:100,message:'Cannot exchange short-token-value https://example.com?access_token=private'}}),{status:400}));
 await assert.rejects(second.upgrade('short-token-value'),e=>{assert.equal(e.details.endpoint,'long_token');assert.ok(!JSON.stringify(e.details).includes('short-token-value'));assert.ok(!JSON.stringify(e.details).includes('access_token=private'));return true;});
});
