export function getLanguage():'ru'|'en';
export function setLanguage(language:'ru'|'en'):void;
export function locale():'ru-RU'|'en-US';
export function isUiCopy(source:string):boolean;
export function subscribeLanguage(listener:()=>void):()=>void;
export function t(source:string,...parameters:unknown[]):string;
