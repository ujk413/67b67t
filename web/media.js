import {html as escapeUi} from './ui.js';
import {t as uiText} from "./i18n.js";
import { userError } from './errors.js';
import {mediaDetails,recordingGesture} from './media-details.js';
import {editPhoto,inspectMedia,transcodeVideo} from './media-editor.js';
const MAX_FILE = 100 * 1024 * 1024 - 16;
function element(tag, className, text) { const node = document.createElement(tag); if (className) node.className = className; if (text) node.textContent = text; return node; }
function button(text, action, className = '') { const node = element('button', className, text); node.type = 'button'; node.onclick = action; return node; }
export function createMediaController({ context, enqueue, encryptFile, report }) {
  let active, viewer;
  function close() { active?.close(); }
  async function compose(kind, files = [], captured) {
    close(); const owner = captured || context(); if (!owner?.store || !owner.roomId) throw new Error('Сначала откройте чат');
    if(context()?.userId!==owner.userId)throw Error('Сеанс изменился. Выберите вложения заново.');
    const valid = () => context()?.userId === owner.userId;
    const name = `media-draft:${owner.roomId}:${kind}`, blobName = name + ':blob';
    let stream, recorder, chunks = [], blob, savedBlob, uploaded, cancelled = false, busy = false, xhr, timer, startedAt = 0, facing = 'user', urls = [],fileIds=new Map(),captionSent=false,albumId=crypto.randomUUID(),messageId=crypto.randomUUID(),audioContext,analyser,samples=[],details={},gestureLocked=false,gestureCancelled=false,pointerStart,pointerHeld=false;
    const originals=new Map(),qualities=new Map(),fileDetails=new Map();
    const dialog = element('dialog', kind === 'circle' ? 'media-dialog circle-recorder' : 'media-dialog'), heading = element('h2', '', kind === 'voice' ? uiText("Голосовое сообщение") : kind === 'circle' ? uiText("Видеокружок") : uiText("Вложения"));
    const destination = element('p', 'media-destination', (""+uiText("В чат:")+" ") + owner.title), preview = element('div', 'recording-preview'), status = element('p', 'media-status'), error = element('p', 'form-error'); error.setAttribute('role', 'alert');
    const caption = element('textarea', 'textarea'); caption.placeholder = uiText("Подпись"); caption.setAttribute('aria-label', uiText("Подпись к вложениям"));
    const progress = element('progress', 'media-progress'); progress.max = 1; progress.value = 0; progress.hidden = true;
    const controls = element('div', 'recording-controls'), send = button(uiText("Отправить"), () => void submit(), 'btn primary'); send.disabled = true;
    const discard=button(uiText("Удалить запись"),()=>void (async()=>{blob=null;savedBlob=null;uploaded=null;preview.replaceChildren();send.disabled=true;discard.disabled=true;status.textContent=uiText("Запись удалена");start.textContent=uiText("Начать запись");await owner.store.remove(name);await owner.store.remove(blobName);})());discard.hidden=kind==='files';discard.disabled=true;
    const stop = button(uiText("Остановить"), () => { if (recorder?.state === 'recording') recorder.stop(); }, 'btn'); stop.hidden = true;
    const start = button(uiText("Начать запись"), () => void record(), 'btn primary'); start.hidden = kind === 'files';
    const flip = button(uiText("Сменить камеру"), () => { facing = facing === 'user' ? 'environment' : 'user'; status.textContent = facing === 'user' ? uiText("Фронтальная камера выбрана") : uiText("Основная камера выбрана"); }); flip.hidden = kind !== 'circle';
    const cleanupStream = () => { clearInterval(timer); stream?.getTracks().forEach(track => track.stop()); stream = null;void audioContext?.close().catch(()=>{});audioContext=null;analyser=null; };
    const dispose = () => { cancelled = true; xhr?.abort(); if (recorder?.state === 'recording') recorder.stop(); cleanupStream(); for (const url of urls) URL.revokeObjectURL(url); dialog.close(); dialog.remove(); if (active?.dialog === dialog) active = null; };
    controls.append(start, stop, flip, discard, send, button(uiText("Закрыть"), dispose, 'btn')); dialog.append(heading, destination, preview, status, error, caption, progress, controls); document.body.append(dialog); dialog.showModal(); active = { dialog, close: dispose }; dialog.addEventListener('cancel', event => { event.preventDefault(); dispose(); });
    if(kind==='circle') {
      const hold=button(uiText("Удерживать для записи"),()=>{});hold.style && (hold.style.touchAction='none');controls.append(hold);
      hold.onpointerdown=event=>{event.preventDefault();hold.setPointerCapture?.(event.pointerId);pointerHeld=true;gestureLocked=false;gestureCancelled=false;pointerStart={x:event.clientX,y:event.clientY};void record(true);};
      hold.onpointermove=event=>{if(!pointerHeld || !pointerStart)return;const action=recordingGesture({dx:event.clientX-pointerStart.x,dy:event.clientY-pointerStart.y,locked:gestureLocked});if(action==='cancel'){gestureCancelled=true;pointerHeld=false;if(recorder?.state==='recording')recorder.stop();}else if(action==='lock'){gestureLocked=true;status.textContent=uiText("Запись зафиксирована. Нажмите «Остановить» для предпросмотра.");}};
      hold.onpointerup=()=>{pointerHeld=false;if(!gestureLocked && recorder?.state==='recording')recorder.stop();};
      hold.onpointercancel=()=>{pointerHeld=false;if(!gestureLocked){gestureCancelled=true;if(recorder?.state==='recording')recorder.stop();}};
    }
    function displayBlob(value) {
      preview.replaceChildren(); const url = URL.createObjectURL(value); urls.push(url);
      const player = element(kind === 'voice' ? 'audio' : 'video', kind === 'circle' ? 'circle-preview' : ''); player.controls = true; player.src = url; player.playsInline = true; preview.append(player);
      send.disabled = false;discard.disabled=false; start.textContent = uiText("Записать заново"); status.textContent = uiText("Прослушайте или просмотрите запись перед отправкой");
    }
    async function save() {
      if (!valid()) return;
      if (blob && blob!==savedBlob) {await owner.store.setBlob(blobName,blob);savedBlob=blob;}
      const manifest=[];
      if(kind==='files')for(const file of files){let id=fileIds.get(file);if(!id){id=name+':file:'+crypto.randomUUID();await owner.store.setBlob(id,file);fileIds.set(file,id);}const original=originals.get(file);if(original)await owner.store.setBlob(id+':original',original);manifest.push({id,name:file.name,type:file.type,lastModified:file.lastModified,details:fileDetails.get(file),quality:qualities.get(file),...(original?{original:{name:original.name,type:original.type,lastModified:original.lastModified}}:{})});}
      await owner.store.set(name,{caption:caption.value,uploaded,files:manifest,captionSent,albumId,messageId,details});
    }
    caption.oninput = () => void save().catch(e => { error.textContent = userError(e); });
    if (kind !== 'files') {
      start.disabled=true;
      try { const saved = await owner.store.get(name); blob = await owner.store.getBlob(blobName); if (saved) { caption.value = saved.caption || ''; uploaded = saved.uploaded;messageId=saved.messageId || messageId;details=mediaDetails(saved.details); } if (blob && !cancelled) displayBlob(blob); }
      catch (e) { error.textContent = userError(e); }
      finally{start.disabled=false;}
    } else {
      const incoming=files;files=[];
      const saved=await owner.store.get(name);if(saved){caption.value=saved.caption||'';captionSent=!!saved.captionSent;albumId=saved.albumId||albumId;for(const item of saved.files||[]){const data=await owner.store.getBlob(item.id);if(data){const file=new File([data],item.name,{type:item.type,lastModified:item.lastModified});files.push(file);fileIds.set(file,item.id);fileDetails.set(file,item.details||{});qualities.set(file,item.quality);if(item.original){const source=await owner.store.getBlob(item.id+':original');if(source)originals.set(file,new File([source],item.original.name,item.original));}}}}
      if(cancelled||!valid())return;
      files.push(...incoming.filter(file=>!files.some(old=>old.name===file.name&&old.size===file.size&&old.lastModified===file.lastModified)));
      if(files.some(file=>file.size>MAX_FILE)){files=files.filter(file=>file.size<=MAX_FILE);error.textContent=uiText("Максимальный размер одного файла — 100 МБ.");}
      await save();
      for (const file of files) {
        renderFile(file);
      }
      send.disabled = !files.length;
    }
    function renderFile(file,previousRow){
        const row = element('div', 'attachment-preview'); const url = URL.createObjectURL(file); urls.push(url);
        if (file.type.startsWith('image/')) { const image = element('img'); image.src = url; image.alt = file.name; row.append(image); }
        else if (file.type.startsWith('video/')) { const video = element('video'); video.src = url; video.controls = true; video.playsInline = true; row.append(video); }
        row.append(element('span', '', `${file.name} · ${(file.size / 1048576).toFixed(1)} ${escapeUi(uiText("МБ"))}`), button(uiText("Убрать"), () => {if(busy)return; files = files.filter(item => item !== file);row.remove();send.disabled=!files.length;const id=fileIds.get(file);void owner.store.remove(id);void owner.store.remove(id+':original');void save().catch(e=>{error.textContent=userError(e);}); }));
        async function replace(transform){if(busy)return;busy=true;const processing=new AbortController();xhr=processing;send.disabled=true;error.textContent='';try{const next=await transform(processing.signal);if(!next||cancelled||!valid())return;const id=fileIds.get(file);await owner.store.setBlob(id,next);files=files.map(item=>item===file?next:item);fileIds.set(next,id);if(file.type.startsWith('video/'))originals.set(next,originals.get(file)||file);fileDetails.set(next,await inspectMedia(next,{signal:processing.signal}));await save();renderFile(next,row);}catch(value){if(value.name!=='AbortError')error.textContent=userError(value);}finally{busy=false;send.disabled=!files.length;progress.hidden=true;}}
        if(file.type.startsWith('image/'))row.append(button(uiText("Изменить фото"),()=>void replace(signal=>editPhoto(file,{signal}))));
        if(file.type.startsWith('video/'))for(const quality of [0,720,480])row.append(button(quality?`${quality}p`:uiText("Оригинал"),()=>void replace(async signal=>{const source=originals.get(file)||file;if(!quality)return source;progress.hidden=false;status.textContent=uiText("Обработка видео до шифрования…");const result=await transcodeVideo(source,quality,{signal,progress:value=>progress.value=value});qualities.set(result,quality);return result;})));
        if(file.type.startsWith('video/'))row.append(button(uiText("Отменить обработку"),()=>xhr?.abort()));
        if(previousRow)previousRow.replaceWith(row);else preview.append(row);
    }
    async function record(fromHold=false) {
      if (busy || start.disabled || recorder?.state === 'recording') return;
      if(!fromHold)gestureCancelled=false;
      start.disabled = true; flip.disabled = true; error.textContent = ''; status.textContent = uiText("Подключаем микрофон…");
      try {
        if (!navigator.mediaDevices || !window.MediaRecorder) throw new Error('Запись недоступна в этом браузере. Прикрепите готовый файл.');
        stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: kind === 'circle' ? { facingMode: facing, width: { ideal: 480 }, height: { ideal: 480 } } : false });
        if(fromHold && !pointerHeld && !gestureLocked){cleanupStream();start.disabled=false;flip.disabled=false;return;}
        if (cancelled || !valid()) { cleanupStream(); return; }
        chunks = []; uploaded = null; const choices = kind === 'voice' ? ['audio/mp4', 'audio/webm;codecs=opus', 'audio/webm'] : ['video/mp4', 'video/webm;codecs=vp8,opus', 'video/webm'];
        samples=[];details={};
        if(kind==='voice' && (window.AudioContext || window.webkitAudioContext)) {
          audioContext=new (window.AudioContext || window.webkitAudioContext)();analyser=audioContext.createAnalyser();analyser.fftSize=2048;audioContext.createMediaStreamSource(stream).connect(analyser);
        }
        const mime = choices.find(type => MediaRecorder.isTypeSupported(type)); recorder = new MediaRecorder(stream, mime ? { mimeType: mime } : undefined);
        preview.replaceChildren(); if (kind === 'circle') { const video = element('video', 'circle-preview'); video.srcObject = stream; video.autoplay = true; video.muted = true; video.playsInline = true; if (facing === 'user') video.setAttribute('style', 'transform:scaleX(-1)'); preview.append(video); }
        recorder.ondataavailable = event => { if (event.data.size) chunks.push(event.data); };
        recorder.onstop = async () => {
          const duration = Date.now() - startedAt; cleanupStream(); stop.hidden = true; start.hidden = false; start.disabled = false; flip.disabled = false;
          if(gestureCancelled){gestureCancelled=false;status.textContent=uiText("Запись отменена");return;}
          details=mediaDetails({duration_ms:duration,...(kind==='voice'?{waveform:samples}:{width:480,height:480})});
          if (cancelled || !valid()) return;
          if (duration < 400) { error.textContent = uiText("Запись слишком короткая. Попробуйте ещё раз."); return; }
          blob = new Blob(chunks, { type: recorder.mimeType || (kind === 'voice' ? 'audio/webm' : 'video/webm') });
          try { await save(); if (!cancelled) displayBlob(blob); } catch (e) { error.textContent = userError(e); }
        };
        recorder.onerror = event => { error.textContent = userError(event.error); cleanupStream(); };
        recorder.start(250); startedAt = Date.now(); start.hidden = true; stop.hidden = false; send.disabled = true;
        timer = setInterval(() => {if(analyser){const values=new Float32Array(analyser.fftSize);analyser.getFloatTimeDomainData(values);samples.push(Math.sqrt(values.reduce((sum,n)=>sum+n*n,0)/values.length));} const seconds = Math.floor((Date.now() - startedAt) / 1000); status.textContent = uiText("● {0}:{1} · Остановите для предпросмотра",Math.floor(seconds / 60),String(seconds % 60).padStart(2, '0')); if (seconds >= (kind === 'circle' ? 60 : 180)) recorder.stop(); }, 250);
      } catch (e) { cleanupStream(); error.textContent = userError(e); start.disabled = false; flip.disabled = false; }
    }
    function upload(file, mediaKind) {
      return new Promise((resolve, reject) => {
        xhr = new XMLHttpRequest(); xhr.open('POST', `/api/media/upload?kind=${mediaKind}`); xhr.withCredentials = true; xhr.setRequestHeader('X-Account-Id',owner.userId); xhr.timeout = 120000;
        xhr.upload.onprogress = event => { if (event.lengthComputable) progress.value = event.loaded / event.total; };
        xhr.onload = () => { let response; try { response = JSON.parse(xhr.responseText); } catch {} if (xhr.status >= 200 && xhr.status < 300 && response?.id) resolve(response); else reject(Object.assign(new Error(response?.error || 'Не удалось загрузить файл'), { status: xhr.status })); };
        xhr.onerror = () => reject(new Error('Нет соединения. Запись сохранена для повтора.')); xhr.ontimeout = () => reject(new Error('Загрузка не завершилась вовремя. Повторите попытку.')); xhr.onabort = () => reject(new Error('Загрузка остановлена. Запись сохранена.'));
        const data = new FormData(); data.append('file', file, file.name || 'recording'); xhr.send(data);
      });
    }
    async function submit() {
      if (busy || !valid()) return; busy = true; send.disabled = true; start.disabled = true; error.textContent = ''; progress.hidden = false;
      try {
        if (kind === 'files') {
          for (const original of [...files]) {
            if (original.size > MAX_FILE) throw new Error('Максимальный размер одного файла — 100 МБ.');
            const measured=await inspectMedia(original);const prepared = await encryptFile(original, owner.roomId,measured); if (cancelled || !valid()) return;
            const text=captionSent?'':caption.value;
            await enqueue({ roomId: owner.roomId, clientId:fileIds.get(original), text, upload:{file:prepared.file,kind:prepared.kind}, body: { type: 'media', text, meta: { ...prepared.meta, filename: original.name,album_id:albumId } } });captionSent=true;
            files = files.filter(item => item !== original);await owner.store.remove(fileIds.get(original));await save();
          }
        } else {
          if (!blob) return;
          const ext = blob.type.includes('mp4') ? (kind === 'voice' ? 'm4a' : 'mp4') : 'webm';
          const prepared=uploaded?null:await encryptFile(new File([blob],`recording.${ext}`,{type:blob.type}),owner.roomId,details);
          if (cancelled || !valid()) return;
          await enqueue({ roomId: owner.roomId, clientId:messageId, text: caption.value, ...(prepared?{upload:{file:prepared.file,kind:prepared.kind}}:{}), body: { type: kind === 'voice' ? 'voice' : 'video_note', ...(uploaded?{media_id:uploaded.id}:{}), text: caption.value,meta:uploaded?.protectedMeta || prepared.meta } });
          await owner.store.remove(name); await owner.store.remove(blobName);
        }
        dispose();
      } catch (e) { error.textContent = userError(e); } finally { busy = false; send.disabled = false; start.disabled = false; progress.hidden = true; }
    }
  }
  async function openViewer(message, messages, source) {
    viewer?.close(); const items = messages.filter(m => m.media_id), index = Math.max(0, items.findIndex(m => m.id === message.id)); let position = index, currentUrl, scale = 1, requestId = 0;
    const dialog = element('dialog', 'media-viewer'), content = element('div', 'viewer-content'), label = element('p', 'viewer-label'), controls = element('div', 'viewer-controls');
    const download = element('a', 'btn', uiText("Скачать")); download.setAttribute('download', '');
    const zoom = element('input'); zoom.type = 'range'; zoom.min = '1'; zoom.max = '4'; zoom.step = '0.1'; zoom.value = '1'; zoom.setAttribute('aria-label', uiText("Масштаб изображения"));
    zoom.oninput = () => { scale = Number(zoom.value); const img = content.querySelector('img'); if (img) img.style.transform = `scale(${scale})`; };
    const dispose = () => { ++requestId; dialog.close(); dialog.remove(); viewer = null; };
    const previous = button('←', () => { if (position) { position--; void show(); } }); previous.setAttribute('aria-label', uiText("Предыдущее вложение"));
    const next = button('→', () => { if (position < items.length - 1) { position++; void show(); } }); next.setAttribute('aria-label', uiText("Следующее вложение"));
    controls.append(previous, label, next, zoom, download, button(uiText("Закрыть"), dispose)); dialog.append(content, controls); document.body.append(dialog); dialog.showModal(); viewer = { close: dispose }; dialog.addEventListener('cancel', e => { e.preventDefault(); dispose(); });
    dialog.addEventListener('keydown', e => { if (e.key === 'ArrowLeft') previous.click(); if (e.key === 'ArrowRight') next.click(); });
    let touchX; content.addEventListener('touchstart', e => { touchX = e.touches[0].clientX; }, { passive: true }); content.addEventListener('touchend', e => { const dx = e.changedTouches[0].clientX - touchX; if (scale === 1 && Math.abs(dx) > 70) (dx < 0 ? next : previous).click(); }, { passive: true });
    async function show() {
      const mine = ++requestId, item = items[position]; content.replaceChildren(element('p', '', uiText("Загрузка…"))); label.textContent = `${position + 1} / ${items.length}`; previous.disabled = !position; next.disabled = position === items.length - 1; scale = 1; zoom.value = '1';
      try { const resolved = await source(item); if (mine !== requestId) return; currentUrl = resolved.url; const mime = resolved.mime || item.media_mime || '';
        const media = mime.startsWith('image/') ? element('img') : mime.startsWith('video/') || item.type === 'video_note' ? element('video') : mime.startsWith('audio/') || item.type === 'voice' ? element('audio') : element('a', 'btn', uiText("Открыть файл"));
        if (media.tagName === 'A') { media.href = currentUrl; media.download = resolved.name || 'file'; } else media.src = currentUrl;
        if (['VIDEO', 'AUDIO'].includes(media.tagName)) { media.controls = true; media.playsInline = true; }
        if (media.tagName === 'IMG') media.alt = item.text || uiText("Фотография"); content.replaceChildren(media); zoom.hidden = media.tagName !== 'IMG'; download.href = currentUrl; download.download = resolved.name || item.meta?.filename || '67chat-media';
      } catch (e) { if (mine === requestId) content.replaceChildren(element('p', 'form-error', userError(e))); }
    }
    await show();
  }
  return { compose, close, openViewer, stop: () => { close(); viewer?.close(); } };
}
