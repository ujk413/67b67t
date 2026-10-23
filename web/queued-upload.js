// Only encrypted blobs reach this transport. The account store owns retries.
export function uploadQueuedFile(file, kind, signal, progress = () => {}) {
  return new Promise((resolve, reject) => {
    const xhr=new XMLHttpRequest();
    xhr.open('POST',`/api/media/upload?kind=${encodeURIComponent(kind)}`);xhr.withCredentials=true;xhr.timeout=120000;
    const abort=()=>xhr.abort();const finish=(error,result)=>{signal?.removeEventListener('abort',abort);error?reject(error):resolve(result);};
    xhr.upload.onprogress=event=>{if(event.lengthComputable)progress(event.loaded/event.total);};
    xhr.onload=()=>{let response;try{response=JSON.parse(xhr.responseText);}catch{}
      if(xhr.status>=200 && xhr.status<300 && response?.id)finish(null,response);
      else finish(Object.assign(Error(response?.error || 'Не удалось загрузить вложение'),{status:xhr.status}));};
    xhr.onerror=()=>finish(Error('Нет соединения. Вложение в очереди.'));
    xhr.ontimeout=()=>finish(Error('timeout'));xhr.onabort=()=>finish(Error('upload_cancelled'));
    if(signal?.aborted)return finish(Error('upload_cancelled'));
    signal?.addEventListener('abort',abort,{once:true});
    const form=new FormData();form.append('file',file,'attachment.bin');xhr.send(form);
  });
}
