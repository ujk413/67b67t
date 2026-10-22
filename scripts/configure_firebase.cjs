// Run locally after firebase login. Uses the official CLI's authorized session;
// only the restricted delivery service account is copied to the app server.
const fs = require('node:fs/promises');
const path = require('node:path');
const {setTimeout:delay}=require('node:timers/promises');
const root=path.resolve(__dirname,'..');
const cli=path.join(root,'builds/firebase-cli/node_modules/firebase-tools/lib');
const project=process.argv[2];
if(!project||!/^[a-z][a-z0-9-]{4,28}[a-z0-9]$/.test(project))throw new Error('Pass the intended Firebase project ID');
const destination=path.join(root,'secrets/firebase-service-account.json');
async function main(){
  const auth=require(path.join(cli,'auth.js')),account=auth.getGlobalDefaultAccount();
  if(!account)throw new Error('Run firebase login first');
  await require(path.join(cli,'requireAuth.js')).requireAuth({project,user:account.user,tokens:account.tokens});
  const access=await require(path.join(cli,'apiv2.js')).getAccessToken();
  async function request(origin,resource,method='GET',body){
    const response=await fetch(`https://${origin}/${resource}`,{method,headers:{authorization:`Bearer ${access}`,'content-type':'application/json'},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(30000)});
    const value=await response.json();
    if(!response.ok)throw Object.assign(new Error(`${origin}: HTTP ${response.status} ${value.error?.status||''}`),{status:response.status});
    return value;
  }
  const info=await request('cloudresourcemanager.googleapis.com',`v1/projects/${project}`);
  for(const service of ['iam.googleapis.com','fcm.googleapis.com']){
    const resource=`v1/projects/${info.projectNumber}/services/${service}`;
    const state=await request('serviceusage.googleapis.com',resource);
    if(state.state!=='ENABLED'){
      const operation=await request('serviceusage.googleapis.com',resource+':enable','POST',{});
      let ready=false;
      for(let attempt=0;attempt<30;attempt++){
        const poll=await request('serviceusage.googleapis.com','v1/'+operation.name);
        if(poll.error)throw new Error('API enable failed: '+service);
        if(poll.done){ready=true;break;}await delay(2000);
      }
      if(!ready)throw new Error('API enable still pending: '+service);
    }
  }
  const email=`chat67-push@${project}.iam.gserviceaccount.com`;
  const accountPath=`v1/projects/${project}/serviceAccounts/${email}`;
  try{await request('iam.googleapis.com',accountPath);}catch(error){
    if(error.status!==404)throw error;
    await request('iam.googleapis.com',`v1/projects/${project}/serviceAccounts`,'POST',{accountId:'chat67-push',serviceAccount:{displayName:'67chat notification delivery'}});
  }
  const role='roles/firebasecloudmessaging.admin',member=`serviceAccount:${email}`;
  let granted=false;
  for(let attempt=0;attempt<3;attempt++){
    const policy=await request('cloudresourcemanager.googleapis.com',`v1/projects/${project}:getIamPolicy`,'POST',{options:{requestedPolicyVersion:3}});
    policy.bindings||=[];
    let binding=policy.bindings.find(binding=>binding.role===role&&!binding.condition);
    if(!binding){binding={role,members:[]};policy.bindings.push(binding);}
    if(binding.members.includes(member)){granted=true;break;}
    binding.members.push(member);
    try{await request('cloudresourcemanager.googleapis.com',`v1/projects/${project}:setIamPolicy`,'POST',{policy});granted=true;break;}
    catch(error){if(![409,412].includes(error.status))throw error;}
  }
  if(!granted)throw new Error('IAM policy changed concurrently; retry configuration');
  try{
    const existing=JSON.parse(await fs.readFile(destination,'utf8'));
    if(existing.project_id!==project||existing.client_email!==email)throw new Error('Existing Firebase credential belongs to another account');
    console.log('Restricted FCM account is configured; existing local key retained');return;
  }catch(error){if(error.code!=='ENOENT')throw error;}
  const keys=await request('iam.googleapis.com',accountPath+'/keys?keyTypes=USER_MANAGED');
  if(keys.keys?.length)throw new Error('This delivery account already has a key. Locate its saved file before creating another');
  const key=await request('iam.googleapis.com',accountPath+'/keys','POST',{privateKeyType:'TYPE_GOOGLE_CREDENTIALS_FILE',keyAlgorithm:'KEY_ALG_RSA_2048'});
  const decoded=Buffer.from(key.privateKeyData,'base64');
  try{
    const config=JSON.parse(decoded.toString('utf8'));
    if(config.project_id!==project||config.client_email!==email)throw new Error('Unexpected service-account identity');
    await fs.mkdir(path.dirname(destination),{recursive:true,mode:0o700});
    await fs.writeFile(destination,decoded,{flag:'wx',mode:0o600});
  }catch(error){
    await request('iam.googleapis.com','v1/'+key.name,'DELETE').catch(()=>{});throw error;
  }finally{decoded.fill(0);}
  console.log('FCM delivery account and key configured. Private key saved only under ignored secrets/');
}
main().catch(error=>{console.error(error.message);process.exitCode=1;});
