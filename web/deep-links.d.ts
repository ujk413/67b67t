export type DeepLink={type:'chat';roomId:string;messageId?:string}|{type:'profile';username:string}|{type:'invite';token:string}|{type:'action';action:'compose'|'saved'};
export function parse67Link(input:unknown):DeepLink|null;
export function messageLink(roomId:string,messageId:string):string;
