// One plaintext tokenizer for DOM and native renderers. It never creates HTML.
export function tokenizeMessage(value){
 const input=String(value??''),tokens=[],pattern=/```([\w-]*)\n([\s\S]*?)```|(^|\n)> ([^\n]*)|(`[^`\n]+`|\*\*[^*\n]+\*\*|__[^_\n]+__|~~[^~\n]+~~|\|\|[^|\n]+\|\||https?:\/\/[^\s<>]+|@[A-Za-z0-9_]{3,30})/g;
 let index=0,match;
 while((match=pattern.exec(input))){
  if(match.index>index)tokens.push({type:'text',text:input.slice(index,match.index)});
  if(match[2]!==undefined)tokens.push({type:'codeblock',text:match[2],language:match[1]});
  else if(match[4]!==undefined){if(match[3])tokens.push({type:'text',text:match[3]});tokens.push({type:'quote',text:match[4]});}
  else{const part=match[5],pairs={'**':'bold','__':'italic','~~':'strike','||':'spoiler'};
   if(part.startsWith('`'))tokens.push({type:'code',text:part.slice(1,-1)});
   else if(pairs[part.slice(0,2)])tokens.push({type:pairs[part.slice(0,2)],text:part.slice(2,-2)});
   else if(part.startsWith('@'))tokens.push({type:'mention',text:part,username:part.slice(1)});
   else{const href=part.replace(/[.,!?;:)\]}]+$/,'');let safe=false;try{const url=new URL(href);safe=['http:','https:'].includes(url.protocol)&&!url.username&&!url.password;}catch{}
    tokens.push(safe?{type:'link',text:href,href}:{type:'text',text:href});if(href.length<part.length)tokens.push({type:'text',text:part.slice(href.length)});
   }
  }
  index=pattern.lastIndex;
 }
 if(index<input.length)tokens.push({type:'text',text:input.slice(index)});return tokens;
}
