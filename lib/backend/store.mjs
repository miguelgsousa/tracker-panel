import {randomBytes,createCipheriv,createDecipheriv} from 'node:crypto';
import {mkdirSync,readFileSync,writeFileSync,renameSync,chmodSync} from 'node:fs';
import {join} from 'node:path';
export function encryptionKey(value=''){if(/^[a-fA-F0-9]{64}$/.test(value))return Buffer.from(value,'hex');if(/^[A-Za-z0-9+/]{43}=$/.test(value)){const b=Buffer.from(value,'base64');if(b.length===32)return b;}return null;}
// Single-process store. Atomic encrypted replacement; never reset corrupted data silently.
export class Store {
 constructor(dir,key){this.key=key;mkdirSync(dir,{recursive:true,mode:0o700});this.path=join(dir,'store.enc');try{const x=JSON.parse(readFileSync(this.path,'utf8'));if(x.v!==1)throw Error();const d=createDecipheriv('aes-256-gcm',key,Buffer.from(x.iv,'base64'));d.setAAD(Buffer.from('lume-store-v1'));d.setAuthTag(Buffer.from(x.tag,'base64'));this.data=JSON.parse(Buffer.concat([d.update(Buffer.from(x.data,'base64')),d.final()]).toString());if(!this.data.sessions)throw Error();}catch(e){if(e.code!=='ENOENT')throw new Error('STORE_UNAVAILABLE');this.data={sessions:{}};this.save();}}
 save(){const iv=randomBytes(12),c=createCipheriv('aes-256-gcm',this.key,iv);c.setAAD(Buffer.from('lume-store-v1'));const data=Buffer.concat([c.update(JSON.stringify(this.data)),c.final()]);const tmp=this.path+'.tmp';writeFileSync(tmp,JSON.stringify({v:1,iv:iv.toString('base64'),tag:c.getAuthTag().toString('base64'),data:data.toString('base64')}),{mode:0o600});chmodSync(tmp,0o600);renameSync(tmp,this.path);}
}
