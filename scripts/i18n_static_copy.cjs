// Compile-time migration of reviewed, explicit UI source keys. No runtime DOM
// traversal, message/profile translation, or arbitrary text replacement.
const fs=require('fs'),path=require('path'),ts=require('../mobile/node_modules/typescript');
const {EN}=require('../web/i18n-en.js');
const root=path.resolve(__dirname,'..');
const targets=process.argv.slice(2);
if(!targets.length)throw Error('Pass explicit reviewed UI files');
for(const name of targets){
 const file=path.resolve(root,name),source=fs.readFileSync(file,'utf8');
 const ast=ts.createSourceFile(file,source,ts.ScriptTarget.Latest,true,file.endsWith('.tsx')?ts.ScriptKind.TSX:file.endsWith('.ts')?ts.ScriptKind.TS:ts.ScriptKind.JS),edits=[];let htmlCalls=false,dateCalls=false;
 const htmlCopy=raw=>raw.replace(/(^|>)([^<>]*)(?=<|$)/g,(all,prefix,text)=>{
  const key=text.trim();if(!Object.hasOwn(EN,key))return all;
  htmlCalls=true;return prefix+text.slice(0,text.indexOf(key))+'${escapeUi(uiText('+JSON.stringify(key)+'))}'+text.slice(text.indexOf(key)+key.length);
 }).replace(/\b(title|aria-label|placeholder|alt)=(['"])([^'"]+)\2/g,(all,name,quote,key)=>{
  if(!Object.hasOwn(EN,key))return all;htmlCalls=true;return name+'='+quote+'${escapeUi(uiText('+JSON.stringify(key)+'))}'+quote;
 });
 const visit=node=>{
  if(ts.isTemplateExpression(node)){
   const key=node.head.text+node.templateSpans.map((span,index)=>'{'+index+'}'+span.literal.text).join('');
   let parent=node.parent,inError=false;while(parent&&!ts.isFunctionLike(parent)){if((ts.isCallExpression(parent)||ts.isNewExpression(parent))&&parent.expression.getText(ast)==='Error')inError=true;parent=parent.parent;}
   if(Object.hasOwn(EN,key)&&!inError){edits.push({start:node.getStart(ast),end:node.end,value:`uiText(${JSON.stringify(key)},${node.templateSpans.map(span=>span.expression.getText(ast)).join(',')})`});return;}
  }
  if(ts.isStringLiteral(node)||ts.isNoSubstitutionTemplateLiteral(node)||ts.isJsxText(node)){
   const key=node.text.trim(),parent=node.parent;
   const propertyName=(ts.isPropertyAssignment(parent)||ts.isMethodDeclaration(parent)||ts.isPropertySignature(parent))&&parent.name===node;
   const translatedCall=ts.isCallExpression(parent)&&['t','uiText'].includes(parent.expression.getText(ast));
   let scope=parent;while(scope&&!ts.isFunctionLike(scope))scope=scope.parent;
   let context=parent,inError=false;while(context&&!ts.isFunctionLike(context)){if((ts.isCallExpression(context)||ts.isNewExpression(context))&&context.expression.getText(ast)==='Error'){inError=true;break;}context=context.parent;}
   // Constants must retain source keys; translating at module import freezes
   // their language before account preferences load. Translate at use sites.
   const staticInitialization=!scope;
   if(Object.hasOwn(EN,key)&&!propertyName&&!translatedCall&&!staticInitialization&&!inError){
    const translated=`uiText(${JSON.stringify(key)})`,left=node.text.slice(0,node.text.indexOf(key)),right=node.text.slice(node.text.indexOf(key)+key.length);
    const expression=ts.isJsxText(node)||(!left&&!right)?translated:`(${JSON.stringify(left)}+${translated}+${JSON.stringify(right)})`;
    edits.push({start:node.getStart(ast),end:node.end,value:ts.isJsxText(node)||ts.isJsxAttribute(parent)?`{${expression}}`:expression});
   }
  }
  const templateSource=node.parent&&ts.isTemplateExpression(node.parent)?node.parent.head.text+node.parent.templateSpans.map(span=>span.literal.text).join(''):node.text||'';
  if(file.includes(path.sep+'web'+path.sep)&&ts.isStringLiteral(node)&&/<[A-Za-z!/]/.test(node.text)&&!edits.some(edit=>edit.start<=node.getStart(ast)&&edit.end>=node.end)){
   const raw=node.text.replaceAll('\\','\\\\').replaceAll('`','\\`').replaceAll('${','\\${'),value=htmlCopy(raw);
   if(value!==raw)edits.push({start:node.getStart(ast),end:node.end,value:'`'+value+'`'});
  }
  if(file.includes(path.sep+'web'+path.sep)&&/<[A-Za-z!/]/.test(templateSource)&&(ts.isNoSubstitutionTemplateLiteral(node)||ts.isTemplateHead(node)||ts.isTemplateMiddle(node)||ts.isTemplateTail(node))){
   const start=node.getStart(ast)+1,end=node.end-((ts.isTemplateHead(node)||ts.isTemplateMiddle(node))?2:1),raw=source.slice(start,end),value=htmlCopy(raw);
   if(value!==raw&&!edits.some(edit=>edit.start<=start&&edit.end>=end))edits.push({start,end,value});
  }
  if(ts.isCallExpression(node)&&ts.isPropertyAccessExpression(node.expression)&&['toLocaleDateString','toLocaleTimeString','toLocaleString'].includes(node.expression.name.text)){
   if(!node.arguments.length){edits.push({start:node.arguments.pos,end:node.arguments.pos,value:'uiLocale()'});dateCalls=true;}
   else if(ts.isStringLiteral(node.arguments[0])&&['ru','ru-RU'].includes(node.arguments[0].text)){edits.push({start:node.arguments[0].getStart(ast),end:node.arguments[0].end,value:'uiLocale()'});dateCalls=true;}
  }
  ts.forEachChild(node,visit);
 };
 visit(ast);if(!edits.length)continue;
 let result=source;for(const edit of edits.sort((a,b)=>b.start-a.start))result=result.slice(0,edit.start)+edit.value+result.slice(edit.end);
 if(!/import\s*\{[^}]*t\s+as\s+uiText[^}]*\}\s*from\s*['"][^'"]*i18n\.js/.test(source)){
  let relative=path.relative(path.dirname(file),path.join(root,'web/i18n.js')).replaceAll('\\','/');if(!relative.startsWith('.'))relative='./'+relative;
  result=`import {t as uiText} from ${JSON.stringify(relative)};\n`+result;
 }
 if(htmlCalls&&!source.includes('html as escapeUi'))result=`import {html as escapeUi} from './ui.js';\n`+result;
 if(dateCalls&&!source.includes('locale as uiLocale')){let relative=path.relative(path.dirname(file),path.join(root,'web/i18n.js')).replaceAll('\\','/');if(!relative.startsWith('.'))relative='./'+relative;result=`import {locale as uiLocale} from ${JSON.stringify(relative)};\n`+result;}
 fs.writeFileSync(file,result);console.log(name+': '+edits.length+' explicit UI keys');
}
