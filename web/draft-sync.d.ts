export type DraftValue={text:string;reply:unknown|null};
export function draftValue(value:unknown):DraftValue;
export class DraftSync {
 constructor(deps:{state:(room:string)=>Promise<{revision:number;value:DraftValue}|null>;local:(room:string)=>Promise<unknown>;fetch:(room:string)=>Promise<{revision:number;encrypted_content:string|null}>;decode:(room:string,row:any)=>Promise<unknown>;encode:(room:string,value:DraftValue)=>Promise<string>;put:(room:string,body:any)=>Promise<{revision:number}>;apply:(room:string,value:DraftValue,expected?:DraftValue)=>Promise<void|boolean>;saveState:(room:string,value:any)=>Promise<void>;conflict:(room:string,value:any)=>Promise<void>});
 sync(room:string):Promise<void>;stop():void;
}
