import {EN} from './i18n-en.js';
let language='ru';
const listeners=new Set();
export const getLanguage=()=>language;
export const locale=()=>language==='en'?'en-US':'ru-RU';
export const isUiCopy=source=>Object.hasOwn(EN,source);
export function setLanguage(value){const next=value==='en'?'en':'ru';if(next===language)return;language=next;for(const listener of listeners)listener();}
export function subscribeLanguage(listener){listeners.add(listener);return()=>listeners.delete(listener);}
/** Only call with an explicit static UI key. Dynamic content is a parameter. */
export function t(source,...parameters){const text=language==='en'?(EN[source]??source):source;return text.replace(/\{(\d+)\}/g,(token,index)=>index<parameters.length?String(parameters[index]):token);}
