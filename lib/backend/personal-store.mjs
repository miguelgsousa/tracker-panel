import {DatabaseSync} from 'node:sqlite';
import {randomBytes,createCipheriv,createDecipheriv} from 'node:crypto';
import {mkdirSync,chmodSync,existsSync} from 'node:fs';
import {join} from 'node:path';
import {Store} from './store.mjs';
// Private single-owner SQLite. Every payload is authenticated/encrypted; cookies own only OAuth state.
export class PersonalStore{
 constructor(dir,key){this.key=key;mkdirSync(dir,{recursive:true,mode:0o700});this.path=join(dir,'lume.sqlite');this.db=new DatabaseSync(this.path);chmodSync(this.path,0o600);this.db.exec('PRAGMA journal_mode=DELETE; PRAGMA secure_delete=ON; CREATE TABLE IF NOT EXISTS records (id TEXT PRIMARY KEY, payload TEXT NOT NULL)');
 const rows=this.db.prepare('SELECT id,payload FROM records').all();
 this.data={sessions:{},workspace:{connections:{},nextSyncAt:0}};
 if(rows.length){for(const row of rows){const data=this.decode(row.id,row.payload);if(row.id==='workspace')Object.assign(this.data.workspace,data);else if(row.id.startsWith('session:'))this.data.sessions[row.id.slice(8)]=data;else if(row.id.startsWith('account:'))this.data.workspace.connections[row.id.slice(8)]=data;}}
 else{if(existsSync(join(dir,'store.enc'))){const old=new Store(dir,key);for(const [id,s] of Object.entries(old.data.sessions||{})){for(const [aid,c] of Object.entries(s.connections||{})){const prior=this.data.workspace.connections[aid];if(!prior||(c.tokenIssuedAt||0)>(prior.tokenIssuedAt||0))this.data.workspace.connections[aid]=c;}this.data.sessions[id]={expiresAt:s.expiresAt,states:s.states||{},connections:{}};}}this.save();}
 }
 encode(id,value){const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',this.key,iv);cipher.setAAD(Buffer.from('lume-sqlite-v1:'+id));const encrypted=Buffer.concat([cipher.update(JSON.stringify(value)),cipher.final()]);return JSON.stringify({iv:iv.toString('base64'),tag:cipher.getAuthTag().toString('base64'),data:encrypted.toString('base64')});}
 decode(id,raw){const x=JSON.parse(raw),d=createDecipheriv('aes-256-gcm',this.key,Buffer.from(x.iv,'base64'));d.setAAD(Buffer.from('lume-sqlite-v1:'+id));d.setAuthTag(Buffer.from(x.tag,'base64'));return JSON.parse(Buffer.concat([d.update(Buffer.from(x.data,'base64')),d.final()]).toString());}
 save(){this.db.exec('BEGIN IMMEDIATE');try{this.db.exec('DELETE FROM records');const put=this.db.prepare('INSERT INTO records(id,payload) VALUES(?,?)');const {connections,...meta}=this.data.workspace;put.run('workspace',this.encode('workspace',meta));for(const[id,s]of Object.entries(this.data.sessions))put.run('session:'+id,this.encode('session:'+id,s));for(const[id,c]of Object.entries(connections))put.run('account:'+id,this.encode('account:'+id,c));this.db.exec('COMMIT');}catch(e){this.db.exec('ROLLBACK');throw e;}}
 close(){this.db.close();}
}
