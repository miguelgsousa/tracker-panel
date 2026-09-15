// Local-only verification servers. No real credentials or provider connections.
import {spawn} from 'node:child_process';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
export async function startLocal(authenticated=false){
 const dir=await mkdtemp(join(tmpdir(),'tracker-browser-'));
 const child=spawn(process.execPath,['-e',`const {app,metricsIntegration}=require('./server.js');const server=app.listen(0,'127.0.0.1',()=>process.send({port:server.address().port}));process.on('message',async()=>{await metricsIntegration.close();server.close(()=>process.exit(0))});`],{cwd:new URL('../',import.meta.url),env:{PATH:process.env.PATH,HOME:process.env.HOME,DB_PATH:dir,...(authenticated?{DATA_DIR:join(dir,'metrics'),METRICS_USERNAME:'browser-test',METRICS_PASSWORD:'local-test-only-not-a-secret',TOKEN_ENCRYPTION_KEY:'11'.repeat(32)}:{})},stdio:['ignore','ignore','inherit','ipc']});
 const port=await new Promise((resolve,reject)=>{const t=setTimeout(()=>reject(Error('Local server startup timeout')),10000);child.once('message',m=>{clearTimeout(t);resolve(m.port)});child.once('exit',c=>{clearTimeout(t);reject(Error('Local server exited '+c))})});
 return {base:`http://127.0.0.1:${port}`,async close(){child.send('close');await new Promise(r=>child.once('exit',r));await rm(dir,{recursive:true,force:true})}};
}
