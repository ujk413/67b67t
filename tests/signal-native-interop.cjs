const fs=require('fs'),path=require('path'),vm=require('vm'),assert=require('node:assert/strict'),{execFileSync}=require('child_process');
const requireMobile=require('module').createRequire(path.resolve(__dirname,'../mobile/package.json'));
const libsignal=requireMobile('@privacyresearch/libsignal-protocol-typescript');
const {Curve25519Wrapper}=requireMobile('@privacyresearch/curve25519-typescript');
const {webcrypto}=require('crypto');
const root=path.resolve(__dirname,'..'),jdk=process.env.JAVA_HOME || 'C:/Program Files/Eclipse Adoptium/jdk-17.0.18.8-hotspot';
const cache=path.join(process.env.USERPROFILE,'.gradle/caches/modules-2/files-2.1');
function jar(group,name,version){const dir=path.join(cache,group,name,version);for(const hash of fs.readdirSync(dir)){const file=path.join(dir,hash,`${name}-${version}.jar`);if(fs.existsSync(file))return file;}throw Error('Resolve native Gradle dependencies first');}
const jars=[jar('org.whispersystems','signal-protocol-java','2.8.1'),jar('org.whispersystems','curve25519-java','0.5.0'),jar('com.google.protobuf','protobuf-javalite','3.25.9')];
const out=path.join(root,'builds/signal-test');fs.mkdirSync(out,{recursive:true});const cp=[out,...jars].join(path.delimiter),props=path.join(out,'synthetic.properties');
function readProps(){return Object.fromEntries(fs.readFileSync(props,'utf8').split(/\r?\n/).filter(l=>l&&!l.startsWith('#')).map(l=>{const i=l.indexOf('=');return [l.slice(0,i),l.slice(i+1).replace(/\\=/g,'=')];}));}
(async()=>{
 const generated=['state/StorageProtos.java','protocol/SignalProtos.java','fingerprint/FingerprintProtos.java'].map(file=>path.join(root,'mobile/modules/room-key/android/src/main/java/org/whispersystems/libsignal',file));
 execFileSync(path.join(jdk,'bin/javac'),['-encoding','UTF-8','-cp',cp,'-d',out,...generated,path.join(root,'tests/SignalInterop.java')]);
 execFileSync(path.join(jdk,'bin/java'),['-cp',cp,'SignalInterop','generate',props]);
 const bob=readProps(),context={window:{},libsignal,Curve25519Wrapper,crypto:webcrypto,ArrayBuffer,Uint8Array,TextEncoder,TextDecoder,btoa,atob,console};
 vm.runInNewContext(fs.readFileSync(path.join(root,'web/crypto.js'),'utf8'),context);
 const alice=new context.window.E2EManager();await alice.init('alice','7',null);
 await alice.buildSession('bob','9',{identityKey:bob.identityPublic,registrationId:123,signedPreKey:{keyId:1,publicKey:bob.signedPublic,signature:bob.signature},preKey:{keyId:1,publicKey:bob.prePublic}});
 const text='Привет Android: проверка 🌍',encrypted=await alice.encryptMessage('bob','9',text);assert.equal(encrypted.type,3);assert.ok(encrypted.body.length>100);
 execFileSync(path.join(jdk,'bin/java'),['-cp',cp,'SignalInterop','decrypt',props,encrypted.body]);
 const result=readProps();assert.equal(Buffer.from(result.received,'base64').toString(),text);
 assert.equal(await alice.decryptMessage('bob','9',{type:Number(result.replyType),body:result.reply}),'Ответ Android: проверка 🔑');
 console.log('PASS: real web Signal → Java v3 → web, Unicode, signature verification and wire message types');
 fs.unlinkSync(props);
})().catch(error=>{console.error(error);process.exitCode=1;});
