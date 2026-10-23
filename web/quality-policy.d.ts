export type QualityDownloadPolicy={wifi:Record<'image'|'video'|'voice'|'file',boolean>;mobile:Record<'image'|'video'|'voice'|'file',boolean>};
export function isQuietHours(quiet:{enabled:boolean;start:string;end:string;time_zone:string},at?:Date):boolean;
export function shouldAutoDownload(policy:QualityDownloadPolicy,type:string,network:string):boolean;
export function crashPreview(input:Record<string,unknown>):{platform:string;code:string;component:string;app_version:string;at:string};
export function clearWebMediaCache():Promise<number>;
