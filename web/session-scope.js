// A user ID alone is insufficient: logging out and back into the same account
// must also invalidate callbacks holding the previous keys and local repository.
export function createSessionScope({account,generation,currentAccount,currentGeneration}){
 const guard=()=>{
  if(!account||currentAccount()!==account||currentGeneration()!==generation)throw Error('Сеанс изменился');
 };
 return {
  guard,
  wrap:fn=>async(...args)=>{guard();try{return await fn(...args);}finally{guard();}},
  sync:fn=>(...args)=>{guard();try{return fn(...args);}finally{guard();}},
 };
}
