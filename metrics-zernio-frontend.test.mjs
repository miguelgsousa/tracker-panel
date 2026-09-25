import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
import * as presentation from './metrics-presentation.mjs';
import {createPeriodCache} from './metrics-cache.mjs';

// Minimal DOM adapter exercises the real controller without a browser dependency.
const source=(await readFile(new URL('./metrics.js',import.meta.url),'utf8')).replace(/^import .*;\n/gm,'');
function harness(config={source:'zernio',readOnlyConnections:true}){
 const nodes=new Map(),calls=[],markup=[];
 class Element{
  constructor(){this.children=new Map();this.classList={contains:()=>false,add(){},toggle(){}};this.dataset={};this.isConnected=true;this.open=false;}
  set innerHTML(value){this.html=value;this.children.clear();markup.push(value)}get innerHTML(){return this.html||''}
  querySelector(selector){if(selector.startsWith('[')&&!this.innerHTML.includes(selector.slice(1,-1)))return null;if(!this.children.has(selector))this.children.set(selector,new Element());return this.children.get(selector)}
  setAttribute(){}toggleAttribute(){}addEventListener(){}focus(){}append(){}showModal(){this.open=true}close(){this.open=false}
 }
 const document={getElementById:id=>nodes.get(id),querySelector:s=>nodes.get(s.slice(1)),createElement:()=>new Element(),body:{append:e=>nodes.set(e.id,e)}};
 const account={id:'zernio:test:1',provider:'instagram',name:'Test',credentialLabel:'Scope <unsafe>'};
 const data={mode:'live',source:'zernio',accounts:[account],metrics:{views:123,reach:45,interactions:6},content:[{id:'p1',accountId:account.id,caption:'Post <unsafe>',views:1000,likes:2,url:'https://example.com/post',timestamp:'2026-09-01T00:00:00Z'}]};
 const context=vm.createContext({...presentation,createPeriodCache,renderMediaDetails:()=>'<p>Legacy details</p>',document,window:{addEventListener(){}},location:{search:'',assign:url=>calls.push(url)},history:{},URL,URLSearchParams,Intl,Date,setInterval(){},setTimeout(){},fetch:async url=>{calls.push(url);return {ok:true,json:async()=>url.endsWith('/config')?config:url.endsWith('/accounts')?{accounts:[account]}:data}}});
 vm.runInContext(source+'\nglobalThis.api={boot,paint,detail,today,openConnect,getConfig:()=>config};',context);
 return {api:context.api,nodes,calls,markup,account,data,Element};
}
test('Zernio mode and scope helpers, safe escaped basic detail links',()=>{
 assert.equal(presentation.isZernio({metricsProvider:'zernio'}),true);
 assert.equal(presentation.connectionScope({scope:'Workspace'}),'Workspace');
 for(const url of ['javascript:alert(1)','data:text/html,evil','/relative'])assert.equal(presentation.safePostURL(url),'');
 const html=presentation.renderZernioDetails({title:'<img src=x onerror=alert(1)>',url:'https://example.com/?x="&y=1',views:0});
 assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img/);assert.match(html,/noopener noreferrer/);assert.match(html,/>0</);assert.match(html,/lifetime/);assert.match(html,/não são suportados/);
 assert.doesNotMatch(presentation.renderZernioDetails({url:'javascript:alert(1)'}),/href=/);
});
test('Zernio connect auto-imports, refreshes registry, and never renders OAuth or token input',async()=>{
 const h=harness();await h.api.openConnect('instagram');
 const dialog=h.nodes.get('tracker-connect-dialog');
 assert.match(dialog.innerHTML,/https:\/\/zernio.com\/dashboard/);
 assert.match(dialog.querySelector('[data-connect-status]').textContent,/1 contas Instagram importadas/);
 assert.ok(h.calls.includes('/api/metrics/accounts'));
 await dialog.querySelector('[data-import]').onclick();
 assert.equal(h.calls.filter(x=>x.endsWith('/accounts')).length,2);
 // Reopening with cached config must also never flash Meta messaging.
 await h.api.openConnect('facebook');
 assert.doesNotMatch(h.markup.join('\n'),/Meta|data-oauth|<input|auth\/start/);
});
test('legacy config still renders and enables official OAuth',async()=>{
 const h=harness({providers:{instagram:{configured:true}},storageAvailable:true});await h.api.openConnect('instagram');
 const dialog=h.nodes.get('tracker-connect-dialog');assert.match(dialog.innerHTML,/página oficial da Meta/);
 const button=dialog.querySelector('[data-oauth]');assert.equal(button.disabled,false);button.onclick();assert.ok(h.calls.includes('/api/metrics/auth/start?provider=instagram'));
});
test('Zernio dashboard labels account period vs post lifetime and hides disconnect',async()=>{
 const h=harness();await h.api.boot();const root=new h.Element();
 const d=presentation.normalizeLive(h.data,30,'all');
 h.api.paint({root,provider:'instagram',data:d,days:30,account:'all',type:'all',query:'',sort:'date'});
 assert.match(root.innerHTML,/Conta · período selecionado/);assert.match(root.innerHTML,/Acumulado dos posts retornados/);
 assert.match(root.innerHTML,/Fonte: Zernio/);assert.match(root.innerHTML,/Scope &lt;unsafe&gt;/);
 assert.doesNotMatch(root.innerHTML,/data-disconnect|configurações da Meta/);
 assert.equal(d.metrics.views,123);assert.equal(d.content[0].views,1000);assert.equal(d.metrics.likes,2);
 const today=h.api.today({metrics:{views:999}});assert.match(today,/Hoje indisponível/);assert.match(today,/não foi verificado/);assert.doesNotMatch(today,/999/);
});
test('Zernio Instagram and Facebook details use returned content with no advanced request',async()=>{
 const h=harness();await h.api.boot();
 for(const provider of ['instagram','facebook']){
  const item={...h.data.content[0],provider,title:'Returned post'};
  await h.api.detail({provider,data:{content:[item]}},{dataset:{media:item.id,owner:item.accountId}});
  assert.match(h.nodes.get('tracker-metrics-dialog').querySelector('[data-detail]').innerHTML,/Returned post/);
 }
 assert.equal(h.calls.some(x=>x.includes('media-details')),false);
});
