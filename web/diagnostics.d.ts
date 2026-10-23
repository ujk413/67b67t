export type DiagnosticResult={id:string;title:string;ok:boolean;message:string;action:string};
export function diagnoseConnection(options:{request:(path:string)=>Promise<any>;wsUrl:string;token?:string;Socket?:any;Peer?:any;timeout?:number}):Promise<DiagnosticResult[]>;
