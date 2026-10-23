export function projectPoint(longitude:number,latitude:number):[number,number];
export function mapPaths(bundle:unknown,maxPoints?:number):string[];
export function mapViewport(latitude:number,longitude:number,span?:number):string;
export function loadMapOverview(origin:string,fetchBundle:()=>Promise<unknown>):Promise<{paths:string[];attribution:string;license_url:string;overview_only:boolean}>;
