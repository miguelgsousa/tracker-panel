// Real, read-only verification against a configured deployment; no mock metrics.
import assert from 'node:assert/strict';
import {readFile,mkdir,writeFile} from 'node:fs/promises';
import puppeteer from 'puppeteer-core';
const file=process.env.TRACKER_VERIFY_ENV;
if(!file)throw Error('Set TRACKER_VERIFY_ENV to a private server environment file');
const env=Object.fromEntries((await readFile(file,'utf8')).split('\n').filter(l=>l&&!l.startsWith('#')&&l.includes('=')).map(l=>[l.slice(0,l.indexOf('=')),l.slice(l.indexOf('=')+1)]));
const base=env.PUBLIC_BASE_URL;
const authorization='Basic '+Buffer.from(env.METRICS_USERNAME+':'+env.METRICS_PASSWORD).toString('base64');
async function api(path){const r=await fetch(base+path,{headers:{Authorization:authorization},signal:AbortSignal.timeout(60000)});assert.equal(r.status,200,path);const d=await r.json();const serialized=JSON.stringify(d);for(const value of Object.values(JSON.parse(env.ZERNIO_API_KEYS||'{}')))assert.ok(!serialized.includes(value),'credential must not appear');return d;}
assert.equal((await fetch(base+'/api/metrics/accounts')).status,401);
for(const path of ['/lib/zernio-metrics.js','/.env','/accounts.json']){const r=await fetch(base+path,{headers:{Authorization:authorization}});assert.equal(r.status,404,path);}
const config=await api('/api/metrics/config');assert.equal(config.source,'zernio');assert.equal(config.readOnlyConnections,true);
const registry=await api('/api/metrics/accounts');assert.equal(registry.partial,false);assert.ok(registry.accounts.length>0);assert.equal(new Set(registry.accounts.map(a=>a.providerId)).size,registry.accounts.length);
const summary={source:config.source,accounts:registry.accounts.map(a=>({id:a.id,provider:a.provider,name:a.name})),dashboards:[]};
for(const provider of ['instagram','facebook']){
 const expected=registry.accounts.filter(a=>a.provider===provider);if(!expected.length)continue;
 const d=await api('/api/metrics/dashboard?'+new URLSearchParams({provider,account:'all',days:'30'}));
 assert.equal(d.mode,'live');assert.equal(d.selectedAccountIds.length,expected.length);assert.ok(d.content.length>0);assert.ok(d.accountMetrics.some(a=>Number.isFinite(a.metrics.views)));assert.equal(d.metrics.reach,null);assert.ok(d.content.every(p=>d.selectedAccountIds.includes(p.accountId)));
 summary.dashboards.push({provider,accounts:d.selectedAccountIds.length,posts:d.content.length,metrics:d.metrics,partial:d.partial,stale:d.stale,truncated:d.truncated,errors:d.errors});
 const today=await api('/api/metrics/today-views?provider='+provider);assert.equal(today.metrics.views,null);assert.equal(today.partial,true);
}
const denied=await fetch(base+'/api/metrics/sync?provider=instagram',{method:'POST',headers:{Authorization:authorization}});assert.equal(denied.status,403);
const browser=await puppeteer.launch({executablePath:process.env.CHROMIUM_PATH||'/usr/bin/chromium-browser',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
const out=process.env.TRACKER_VERIFY_OUTPUT||'/tmp/tracker-zernio-evidence';await mkdir(out,{recursive:true});
try{
 const page=await browser.newPage();await page.authenticate({username:env.METRICS_USERNAME,password:env.METRICS_PASSWORD});await page.setViewport({width:1440,height:1100});const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(base,{waitUntil:'domcontentloaded'});
 for(const provider of ['instagram','facebook']){
  await page.evaluate(p=>window.switchTab(p,null),provider);
  await page.waitForFunction(p=>document.querySelector('#metrics-'+p+' .tm-kpis')&&document.querySelector('#metrics-'+p)?.getAttribute('aria-busy')==='false',{timeout:60000},provider);
  const root='#metrics-'+provider;
  assert.equal(await page.$$eval(root+' [data-disconnect]',a=>a.length),0);
  const opts=await page.$$eval(root+' [data-account] option',a=>a.map(o=>o.value));assert.equal(opts.length,registry.accounts.filter(a=>a.provider===provider).length+1);
  await page.screenshot({path:out+'/'+provider+'-desktop.png',fullPage:true});
 }
 await page.evaluate(()=>window.openAddModal('instagram'));
 await page.waitForFunction(()=>document.querySelector('#tracker-connect-dialog')?.open&&document.querySelector('#tracker-connect-dialog')?.textContent.includes('Zernio'));
 assert.equal(await page.$eval('#tracker-connect-dialog',e=>e.innerText.includes('Continuar com Instagram')),false);
 await page.screenshot({path:out+'/connections.png'});await page.keyboard.press('Escape');
 await page.setViewport({width:390,height:844,isMobile:true,hasTouch:true});
 await page.evaluate(()=>{window.switchTab('instagram',null);window.updateMobileNav('instagram');window.scrollTo(0,0)});
 await page.waitForFunction(()=>document.querySelector('#metrics-instagram .tm-kpis')&&document.querySelector('#metrics-instagram')?.getAttribute('aria-busy')==='false',{timeout:60000});
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
 await page.screenshot({path:out+'/instagram-mobile.png',fullPage:true});
 assert.deepEqual(errors,[]);summary.browser={desktop:true,mobile:true,noOverflow:true,noErrors:true};
}finally{await browser.close();}
await writeFile(out+'/verification.json',JSON.stringify(summary,null,2));console.log(JSON.stringify(summary,null,2));
