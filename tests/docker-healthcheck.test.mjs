import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

// Exercise the actual Docker CMD with a mocked local HTTP transport.
const dockerfile=readFileSync(new URL('../Dockerfile',import.meta.url),'utf8');
const command=dockerfile.match(/CMD node -e "([^\n]+)"/)[1];
async function probe(env,{status=200,reject=false}={}){
 let request,exitCode;
 vm.runInNewContext(command,{
  process:{env,exit(code){exitCode=code;}},Buffer,
  fetch:async(url,options)=>{request={url,options};if(reject)throw Error('local transport failure');return {ok:status>=200&&status<300};}
 });
 await new Promise(resolve=>setImmediate(resolve));
 return {request,exitCode};
}
test('Docker healthcheck authenticates privately in configured mode',async()=>{
 const {request,exitCode}=await probe({PORT:'3456',METRICS_USERNAME:'fixture-owner',METRICS_PASSWORD:'fixture-password'});
 assert.equal(request.url,'http://127.0.0.1:3456/api/accounts');
 assert.equal(request.options.headers.Authorization,'Basic '+Buffer.from('fixture-owner:fixture-password').toString('base64'));
 assert.equal(exitCode,0);
});
test('Docker healthcheck preserves legacy mode and reports failures',async()=>{
 const legacy=await probe({});assert.equal(legacy.request.options.headers.Authorization,undefined);assert.equal(legacy.exitCode,0);
 assert.equal((await probe({},{status:503})).exitCode,1);
 assert.equal((await probe({},{status:401})).exitCode,1);
 assert.equal((await probe({},{reject:true})).exitCode,1);
});
