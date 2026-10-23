export function createSessionScope(options:{account:unknown;generation:unknown;currentAccount:()=>unknown;currentGeneration:()=>unknown}):{
 guard():void;
 wrap<A extends unknown[],R>(fn:(...args:A)=>R):(...args:A)=>Promise<Awaited<R>>;
 sync<A extends unknown[],R>(fn:(...args:A)=>R):(...args:A)=>R;
};
