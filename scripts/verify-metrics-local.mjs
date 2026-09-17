// Real local Express + embedded metrics backend; empty isolated stores, no Meta calls.
import puppeteer from 'puppeteer-core';
import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import {startLocal} from './metrics-browser-server.mjs';
const browser=await puppeteer.launch({executablePath:process.env.CHROMIUM_PATH||'/usr/bin/chromium-browser',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
try{
 for(const authenticated of [false,true]){
  const local=await startLocal(authenticated);const context=await browser.createBrowserContext();
  try{
   const page=await context.newPage();page.setDefaultTimeout(10000);const errors=[];page.on('pageerror',e=>errors.push(e.message));
   await page.setRequestInterception(true);page.on('request',r=>{const u=new URL(r.url());if(u.origin===local.base)return r.continue();return r.respond({status:200,contentType:u.hostname==='unpkg.com'?'application/javascript':'text/css',body:u.hostname==='unpkg.com'?'window.lucide={createIcons(){}}':''})});
   assert.equal((await fetch(local.base+'/api/metrics/config')).status,authenticated?401:503);
   if(authenticated)await page.authenticate({username:'browser-test',password:'local-test-only-not-a-secret'});
   await page.goto(local.base,{waitUntil:'domcontentloaded'});await page.evaluate(()=>switchTab('instagram',null));
   await page.waitForFunction(()=>document.querySelector('#metrics-instagram')?.getAttribute('aria-busy')==='false');
   const state=await page.evaluate(async()=>({config:await fetch('/api/metrics/config').then(async r=>({status:r.status,body:await r.json()})),accounts:await fetch('/api/metrics/accounts').then(async r=>({status:r.status,body:await r.json()}))}));
   assert.equal(state.config.status,authenticated?200:503);
   assert.equal(await page.$('#metrics-instagram [data-export]'),null);
   assert.equal(await page.$('#metrics-instagram .tm-toolbar'),null);
   await page.click('#instagram .tracker-platform-heading .btn-primary');
   await page.waitForSelector('#tracker-connect-dialog[open]');
   await page.waitForFunction(()=>!document.querySelector('[data-connect-status]').textContent.includes('Verificando'));
   assert.equal(await page.$eval('[data-oauth]',e=>e.disabled),true);
   assert.match(await page.$eval('[data-connect-status]',e=>e.textContent),authenticated?/configuração pendente/:/503/);
   assert.match(await page.$eval('[data-connect-status]',e=>e.textContent),/README/);
   await page.keyboard.press('Escape');
   if(authenticated){assert.deepEqual(state.accounts.body.accounts,[]);assert.equal(await page.$eval('#metrics-instagram',e=>e.hidden),true);assert.match(await page.$eval('#instagram .tracker-empty',e=>e.textContent),/Nenhum perfil cadastrado/);await page.goto(local.base+'/auth/login',{waitUntil:'domcontentloaded'});assert.equal(new URL(page.url()).pathname,'/');await page.evaluate(()=>switchTab('instagram',null));await page.waitForSelector('#metrics-instagram');}
   else assert.equal(await page.$('#metrics-instagram .tm-kpis'),null);
   await page.setViewport({width:390,height:844,isMobile:true});await page.evaluate(()=>{switchTab('instagram',null);updateMobileNav('instagram');window.scrollTo(0,0)});await page.waitForFunction(()=>document.querySelector('#metrics-instagram')?.getAttribute('aria-busy')==='false');assert.equal(await page.$eval('#instagram',e=>e.classList.contains('active')),true);assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
   await mkdir('/tmp/tracker-metrics-evidence',{recursive:true});await page.screenshot({path:`/tmp/tracker-metrics-evidence/real-local-${authenticated?'authenticated-empty':'failclosed'}-mobile.png`,fullPage:true});
   assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,mode:authenticated?'REAL_LOCAL_AUTHENTICATED_EMPTY':'REAL_LOCAL_FAILCLOSED',configStatus:state.config.status,noMetaConnections:true,mobileNoOverflow:true}));
  }finally{await context.close();await local.close()}
 }
}finally{await browser.close()}
