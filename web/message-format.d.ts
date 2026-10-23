export type MessageFormatToken={type:'text'|'code'|'codeblock'|'bold'|'italic'|'strike'|'spoiler'|'quote'|'link'|'mention';text:string;href?:string;username?:string;language?:string};
export function tokenizeMessage(value:unknown):MessageFormatToken[];
