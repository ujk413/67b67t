export type MediaDetails={waveform?:number[];width?:number;height?:number;orientation?:number;rotation?:0|90|180|270;duration_ms?:number;expressive?:{kind:'sticker'|'gif';label?:string}};
export function mediaDetails(input?:Record<string,unknown>):MediaDetails;
export function recordingGesture(input:{dx:number;dy:number;locked:boolean}):'none'|'cancel'|'lock';
export function fitCrop(width:number,height:number,ratio?:number,scale?:number):{originX:number;originY:number;width:number;height:number};
