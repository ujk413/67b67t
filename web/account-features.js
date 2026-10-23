import {userError} from './errors.js';
import {t as uiText} from "./i18n.js";
import {mountSecretChats} from './secret-screen.js';
// No module-level DOM access: the same backup envelope is used by Android.
const FORMAT = "messenger67-e2e-backup";
export const bytesToBase64 = (bytes) => {
  let s = "";
  for (const b of new Uint8Array(bytes)) s += String.fromCharCode(b);
  return btoa(s);
};
export const base64ToBytes = (value) =>
  Uint8Array.from(atob(value), (c) => c.charCodeAt(0));
const enc = (s) => new TextEncoder().encode(s);
const replacer = (_k, v) =>
  v instanceof ArrayBuffer
    ? { __b64: bytesToBase64(v) }
    : ArrayBuffer.isView(v)
      ? {
          __b64: bytesToBase64(
            new Uint8Array(v.buffer, v.byteOffset, v.byteLength),
          ),
        }
      : v;
const reviver = (_k, v) =>
  v &&
  typeof v === "object" &&
  typeof v.__b64 === "string" &&
  Object.keys(v).length === 1
    ? base64ToBytes(v.__b64).buffer
    : v;
const webCrypto = {
  random: (n) => crypto.getRandomValues(new Uint8Array(n)),
  derive: async (password, salt, iter) => {
    const raw = await crypto.subtle.importKey(
      "raw",
      enc(password),
      "PBKDF2",
      false,
      ["deriveBits"],
    );
    return new Uint8Array(
      await crypto.subtle.deriveBits(
        { name: "PBKDF2", hash: "SHA-256", iterations: iter, salt },
        raw,
        256,
      ),
    );
  },
  encrypt: async (key, iv, aad, data) =>
    new Uint8Array(
      await crypto.subtle.encrypt(
        { name: "AES-GCM", iv, additionalData: aad },
        await crypto.subtle.importKey("raw", key, "AES-GCM", false, [
          "encrypt",
        ]),
        data,
      ),
    ),
  decrypt: async (key, iv, aad, data) =>
    new Uint8Array(
      await crypto.subtle.decrypt(
        { name: "AES-GCM", iv, additionalData: aad },
        await crypto.subtle.importKey("raw", key, "AES-GCM", false, [
          "decrypt",
        ]),
        data,
      ),
    ),
};
export async function encryptBackup(
  keys,
  password,
  meta = {},
  provider = webCrypto,
) {
  if (typeof password !== "string" || password.length < 8)
    throw Error("Пароль резервной копии: минимум 8 символов");
  if (keys?.secretChats) throw Error("Секретные чаты нельзя экспортировать");
  const iter = 310000,
    salt = provider.random(16),
    iv = provider.random(12),
    key = await provider.derive(password, salt, iter);
  try {
    const plain = enc(
        JSON.stringify(
          {
            user_id: meta.userId ?? null,
            device_id: meta.deviceId ?? null,
            keys,
          },
          replacer,
        ),
      ),
      aad = enc(`${FORMAT}|v1|${iter}`);
    return JSON.stringify({
      format: FORMAT,
      v: 1,
      kdf: "PBKDF2",
      hash: "SHA-256",
      iter,
      salt: bytesToBase64(salt),
      iv: bytesToBase64(iv),
      ct: bytesToBase64(await provider.encrypt(key, iv, aad, plain)),
      created: new Date().toISOString(),
    });
  } finally {
    key.fill(0);
  }
}
export async function decryptBackup(
  input,
  password,
  userId,
  provider = webCrypto,
) {
  let e;
  try {
    e = typeof input === "string" ? JSON.parse(input) : input;
  } catch {
    throw Error("BAD_FORMAT");
  }
  if (
    !e ||
    e.format !== FORMAT ||
    e.v !== 1 ||
    e.kdf !== "PBKDF2" ||
    e.hash !== "SHA-256" ||
    !Number.isInteger(e.iter) ||
    e.iter < 210000 ||
    e.iter > 5000000 ||
    typeof e.ct !== "string" ||
    e.ct.length > 32 * 1024 * 1024
  )
    throw Error("BAD_FORMAT");
  let payload, key;
  try {
    const salt = base64ToBytes(e.salt),
      iv = base64ToBytes(e.iv);
    if (salt.length !== 16 || iv.length !== 12) throw Error("BAD_FORMAT");
    key = await provider.derive(password, salt, e.iter);
    payload = JSON.parse(
      new TextDecoder().decode(
        await provider.decrypt(
          key,
          iv,
          enc(`${FORMAT}|v1|${e.iter}`),
          base64ToBytes(e.ct),
        ),
      ),
      reviver,
    );
  } catch {
    throw Error("BAD_PASSPHRASE");
  } finally {
    key?.fill(0);
  }
  if (
    !payload?.keys?.identityKey ||
    payload.keys.registrationId == null ||
    !payload.keys.preKeys ||
    !payload.keys.signedPreKeys ||
    !payload.keys.sessions
  )
    throw Error("BAD_FORMAT");
  if (
    userId != null &&
    payload.user_id != null &&
    String(userId) !== String(payload.user_id)
  )
    throw Error("USER_MISMATCH");
  return payload;
}
export function assertTransport(policy, capabilities = {}, kind = "http") {
  if (!policy || policy.mode === "direct") return;
  if (!["proxy", "tor"].includes(policy.mode))
    throw Error("Неизвестный транспорт");
  if (
    !capabilities[policy.mode] ||
    !capabilities.failClosed ||
    !capabilities.dnsThroughProxy
  )
    throw Error("Защищённый транспорт недоступен; прямое соединение запрещено");
  if (kind === "call" && !capabilities.callsThroughProxy)
    throw Error("Звонки отключены для выбранного транспорта");
}
const secretMemory = new Map();
/** Snapshots belong only in the encrypted device vault, never in backups. */
export class SecretSession {
  constructor(chatId, deviceId) {
    this.chatId = chatId;
    this.deviceId = deviceId;
    this.verified = false;
    this.received = new Set();
  }
  assertOpen() {
    if(this.destroyed)throw Error('Чат закрыт');
  }
  async offer() {
    this.assertOpen();
    if(this.pair)return this.publicKey;
    if(!this.offering)this.offering=(async()=>{
      const pair=await crypto.subtle.generateKey({name:'X25519'},true,['deriveBits']);
      this.assertOpen();
      const pub=await crypto.subtle.exportKey('raw',pair.publicKey);
      this.assertOpen();
      this.pair=pair;this.publicKey=bytesToBase64(pub);
      secretMemory.set(this.chatId,this);
      return this.publicKey;
    })();
    try{return await this.offering;}finally{this.offering=null;}
  }
  async accept(peerPublicKey) {
    this.assertOpen();this.verified=false;
    if(!this.pair)await this.offer();this.assertOpen();
    const pair=this.pair,publicKey=this.publicKey,publicBytes=base64ToBytes(peerPublicKey);
    if(publicBytes.length!==32)throw Error('Некорректный ключ');
    let shared;
    try{
      const peer=await crypto.subtle.importKey('raw',publicBytes,{name:'X25519'},false,[]);this.assertOpen();
      shared=await crypto.subtle.deriveBits({name:'X25519',public:peer},pair.privateKey,256);this.assertOpen();
      const material=await crypto.subtle.importKey('raw',shared,'HKDF',false,['deriveKey']);this.assertOpen();
      const key=await crypto.subtle.deriveKey({name:'HKDF',hash:'SHA-256',salt:enc(this.chatId),info:enc('67chat-secret-v1')},material,{name:'AES-GCM',length:256},false,['encrypt','decrypt']);this.assertOpen();
      const ordered=[publicKey,peerPublicKey].sort().join('|');
      const digest=await crypto.subtle.digest('SHA-256',enc(this.chatId+'|'+ordered));this.assertOpen();
      const fingerprint=Array.from(new Uint8Array(digest),x=>x.toString(16).padStart(2,'0')).join('').match(/.{1,8}/g).join(' ');
      this.key=key;this.peerPublicKey=peerPublicKey;this.fingerprint=fingerprint;
      return fingerprint;
    }finally{if(shared)new Uint8Array(shared).fill(0);}
  }
  verify(fingerprint) {
    this.assertOpen();
    if (!this.key || fingerprint !== this.fingerprint)
      throw Error("Код безопасности не совпадает");
    this.verified = true;
  }
  async encrypt(text, expiresAt) {
    this.assertOpen();
    if (!this.key || !this.verified)
      throw Error("Сначала сравните код безопасности на обоих устройствах");
    const until = Number(expiresAt);
    if (
      !Number.isFinite(until) ||
      until <= Date.now() ||
      until > Date.now() + 86400000
    )
      throw Error("Некорректный срок");
    const iv = crypto.getRandomValues(new Uint8Array(12)),
      payload = enc(
        JSON.stringify({ text, expires_at: until, device_id: this.deviceId }),
      );
    try{
      const ciphertext=await crypto.subtle.encrypt({name:'AES-GCM',iv,additionalData:enc(this.chatId)},this.key,payload);
      this.assertOpen();
      return bytesToBase64(enc(JSON.stringify({v:1,iv:bytesToBase64(iv),ct:bytesToBase64(ciphertext)})));
    }finally{payload.fill(0);}
  }
  async decrypt(ciphertext, peerDeviceId) {
    this.assertOpen();
    if (!this.key || !this.verified)
      throw Error("Сначала проверьте код безопасности");
    const e = JSON.parse(new TextDecoder().decode(base64ToBytes(ciphertext)));
    if (e.v !== 1 || this.received.has(e.iv)) throw Error("Повтор сообщения");
    const plain = JSON.parse(
      new TextDecoder().decode(
        await crypto.subtle.decrypt(
          {
            name: "AES-GCM",
            iv: base64ToBytes(e.iv),
            additionalData: enc(this.chatId),
          },
          this.key,
          base64ToBytes(e.ct),
        ),
      ),
    );
    this.assertOpen();
    if (peerDeviceId && plain.device_id !== peerDeviceId) throw Error("Некорректное устройство");
    if (typeof plain.text !== "string" || !Number.isFinite(plain.expires_at) || plain.expires_at > Date.now()+86400000) throw Error("Некорректный срок");
    if (plain.expires_at <= Date.now()) throw Object.assign(Error("Сообщение истекло"),{code:'secret_expired'});
    if (this.received.size >= 1024) throw Error("Начните новый секретный чат");
    this.received.add(e.iv);
    return plain;
  }
  async snapshot() {
    this.assertOpen();
    const privateKey=await crypto.subtle.exportKey('jwk',this.pair.privateKey);this.assertOpen();
    return {v:1,chatId:this.chatId,deviceId:this.deviceId,privateKey,publicKey:this.publicKey,peerPublicKey:this.peerPublicKey||null,verified:this.verified,received:[...this.received]};
  }
  async restore(saved) {
    this.assertOpen();
    if(saved?.v!==1||saved.chatId!==this.chatId||saved.deviceId!==this.deviceId)throw Error('Некорректное устройство');
    const privateKey=await crypto.subtle.importKey('jwk',saved.privateKey,{name:'X25519'},true,['deriveBits']);this.assertOpen();
    this.pair={privateKey};this.publicKey=saved.publicKey;
    if(saved.peerPublicKey)await this.accept(saved.peerPublicKey);this.assertOpen();
    this.received=new Set(saved.received);
    if(saved.verified)this.verify(this.fingerprint);
  }
  destroy() {
    this.destroyed=true;this.offering=null;this.fingerprint="";this.publicKey="";this.peerPublicKey=null;
    this.pair = null;
    this.key = null;
    this.verified = false;
    this.received.clear();
    secretMemory.delete(this.chatId);
  }
}
const TITLES = {
  contacts: "Контакты 67chat",
  privacy: "Приватность",
  blocked: "Заблокированные",
  backup: "Защищённая резервная копия",
  security: "Защита аккаунта",
  aliases: "Публичные имена",
  accounts: "Аккаунты",
  delete: "Удаление аккаунта",
  qr: "Вход по QR",
  transport: "Прокси и Tor",
  secret: "Секретные чаты",
};
export async function openAccountFeature(mode, context) {
  mode = String(mode).replace(/^account[-:]/, "");
  const dialog = document.createElement("dialog");
  dialog.className = "account-feature-dialog";
  dialog.style.cssText =
    "width:min(600px,94vw);max-height:90vh;overflow:auto;background:var(--panel-bg,var(--bg,#fff));color:var(--text,#111);border:1px solid var(--border,#888);border-radius:16px;padding:24px";
  const title = document.createElement("h2");
  title.textContent = uiText(TITLES[mode] || uiText("Аккаунт"));
  const content = document.createElement("div"),
    status = document.createElement("p");
  status.setAttribute("role", "status");
  dialog.append(title, content, status);
  document.body.append(dialog);
  const say = (message) => {
      status.textContent = String(message);
    },
    run = async (action) => {
      try {
        say("");
        await action();
      } catch (e) {
        say(userError(e));
        context.report?.(e);
      }
    };
  const call = (path, method = "GET", body) =>
    context.request(path, { method, body });
  const label = (t) => {
    const p = document.createElement("p");
    p.textContent = t;
    content.append(p);
    return p;
  };
  const input = (caption, type = "text") => {
    const l = document.createElement("label"),
      i = document.createElement("input");
    l.textContent = caption + " ";
    i.type = type;
    i.autocomplete = type === "password" ? "current-password" : "off";
    i.style.cssText =
      "display:block;width:100%;box-sizing:border-box;margin:8px 0;padding:9px";
    l.append(i);
    content.append(l);
    return i;
  };
  const button = (caption, action) => {
    const b = document.createElement("button");
    b.type = "button";
    b.textContent = caption;
    b.style.cssText = "margin:6px;padding:9px";
    b.onclick = () => run(action);
    content.append(b);
    return b;
  };
  const reload = () => {
    dialog.close();
    dialog.remove();
    return openAccountFeature(mode, context);
  };
  const close = document.createElement("button");
  close.textContent = uiText("Закрыть");
  close.onclick = () => dialog.close();
  dialog.append(close);
  dialog.addEventListener("close", () => {
    dialog.remove();
  });
  dialog.showModal();
  await run(async () => {
    if (mode === "contacts" || mode === "blocked") {
      const path =
          mode === "contacts" ? "/me/account/contacts" : "/me/account/blocks",
        rows = await call(path);
      label(
        mode === "contacts"
          ? uiText("Список хранится на вашем сервере. Телефонная книга не читается.")
          : uiText("Заблокированные пользователи не могут писать вам."),
      );
      for (const u of rows) {
        label(u.name || u.display_name || u.username || u.id);
        button(
          mode === "contacts" ? uiText("Удалить контакт") : uiText("Разблокировать"),
          async () => {
            await call(path + "/" + u.id, "DELETE");
            await reload();
          },
        );
      }
      if (mode === "contacts") {
        const q = input(uiText("Найти по имени"));
        button(uiText("Найти"), async () => {
          const users = await call(
            "/users/search?q=" + encodeURIComponent(q.value),
          );
          for (const u of users) {
            label("@" + u.username);
            button((""+uiText("Добавить")+" ") + u.username, async () => {
              await call(path + "/" + u.id, "PUT", {
                name: u.display_name || u.username,
              });
              await reload();
            });
          }
        });
      }
      return;
    }
    if (mode === "privacy") {
      const p = await call("/me/account/privacy"),
        selects = {};
      for (const [k, caption] of [
        ["last_seen", uiText("Время последнего посещения")],
        ["write", uiText("Кто может писать")],
      ]) {
        label(caption);
        const s = document.createElement("select");
        for (const [value, name] of [
          ["everyone", uiText("Все")],
          ["contacts", uiText("Мои контакты")],
          ["nobody", uiText("Никто")],
        ]) {
          const o = document.createElement("option");
          o.value = value;
          o.textContent = name;
          s.append(o);
        }
        s.value = p[k];
        content.append(s);
        selects[k] = s;
      }
      label(uiText("Отметки прочтения видны, только если их разрешили оба участника."));
      const receipt = input(uiText("Отметки прочтения"), "checkbox");
      receipt.checked = p.read_receipts;
      button(uiText("Сохранить"), async () => {
        await call("/me/account/privacy", "PUT", {
          last_seen: selects.last_seen.value,
          write: selects.write.value,
          read_receipts: receipt.checked,
        });
        say(uiText("Сохранено"));
      });
      return;
    }
    if (mode === "security") {
      const s = await call("/me/account/security");
      label(s.enabled ? uiText("TOTP включён") : uiText("TOTP выключен"));
      const password = input(uiText("Пароль аккаунта"), "password"),
        code = input(uiText("Код TOTP или резервный код"));
      const credentials = () => ({
        password: password.value,
        ...(/^\d{6}$/.test(code.value)
          ? { code: code.value }
          : { backup_code: code.value }),
      });
      button(uiText("Настроить TOTP"), async () => {
        const setup = await call(
          "/me/account/totp/setup",
          "POST",
          credentials(),
        );
        label((""+uiText("Сохраните секрет в приложении аутентификатора:")+" ") + setup.secret);
        label(setup.uri);
      });
      button(uiText("Подтвердить TOTP"), async () => {
        const enabled = await call("/me/account/totp/enable", "POST", {
          code: code.value,
        });
        label(uiText("Резервные коды — показаны один раз:"));
        for (const c of enabled.backup_codes) label(c);
      });
      if (s.enabled) {
        button(uiText("Выключить TOTP"), async () => {
          await call("/me/account/totp/disable", "POST", credentials());
          await reload();
        });
        const deviceKey =
          (await context.local?.get?.("account.deviceKey")) ||
          crypto.randomUUID();
        await context.local?.set?.("account.deviceKey", deviceKey);
        button(
          s.approval_required
            ? uiText("Выключить подтверждение новых устройств")
            : uiText("Подтверждать новые устройства"),
          async () => {
            await call("/me/account/device-approval", "PUT", {
              ...credentials(),
              enabled: !s.approval_required,
              device_key: deviceKey,
              device_name: navigator.userAgent,
            });
            await reload();
          },
        );
      }
      for (const c of s.challenges) {
        label((""+uiText("Запрос входа:")+" ") + (c.device_name || c.id));
        button(uiText("Подтвердить устройство"), async () => {
          await call("/me/account/challenges/" + c.id + "/approve", "POST", {});
          await reload();
        });
      }
      label(uiText("История входов"));
      for (const a of s.audit)
        label(
          `${a.created_at}: ${a.device_name || uiText("Устройство")} · ${a.outcome} · ${a.ip || ""}`,
        );
      return;
    }
    if (mode === "aliases") {
      const data = await call("/me/public-names");
      for (const n of data.public_names || []) {
        label("@" + n.name + (n.kind === "primary_tag" ? (" "+uiText("· основной")+"") : ""));
        if (n.kind !== "primary_tag")
          button(uiText("Сделать основным"), async () => {
            await call("/me/public-names/" + n.id + "/promote", "POST", {});
            await reload();
          });
        button(uiText("Удалить @") + n.name, async () => {
          await call("/me/public-names/" + n.id, "DELETE");
          await reload();
        });
      }
      label(
        uiText("Свободные имена задаются в редакторе профиля. Имя другого вашего аккаунта переносится объединением: вход и данные второго аккаунта перейдут в текущий."),
      );
      const username = input(uiText("Второй аккаунт")),
        password = input(uiText("Пароль второго аккаунта"), "password"),
        factor = input(
          uiText("TOTP или резервный код второго аккаунта (если включён)"),
        ),
        confirm = input(uiText("Для объединения введите ОБЪЕДИНИТЬ"));
      button(uiText("Объединить аккаунты и добавить имя"), async () => {
        if (confirm.value !== uiText("ОБЪЕДИНИТЬ")) throw Error("Введите ОБЪЕДИНИТЬ");
        await call("/me/public-names/merge", "POST", {
          secondary_username: username.value,
          secondary_password: password.value,
          ...(/^\d{6}$/.test(factor.value)
            ? { secondary_totp: factor.value }
            : { secondary_backup_code: factor.value }),
        });
        await reload();
      });
      return;
    }
    if (mode === "backup") {
      label(
        uiText("Копия содержит обычные E2E-ключи. Секретные чаты исключены. Пароль копии не передаётся серверу."),
      );
      const password = input(uiText("Пароль резервной копии"), "password");
      button(uiText("Экспортировать"), async () => {
        if (!context.local?.exportKeys)
          throw Error("Хранилище ключей ещё не подключено");
        const data = await encryptBackup(
          await context.local.exportKeys(),
          password.value,
          { userId: context.me.id },
        );
        const a = document.createElement("a");
        a.href = URL.createObjectURL(
          new Blob([data], { type: "application/json" }),
        );
        a.download = "67chat-encrypted-backup.json";
        a.click();
        setTimeout(() => URL.revokeObjectURL(a.href), 1000);
      });
      const file = input(uiText("Файл резервной копии"), "file");
      file.accept = ".json,application/json";
      button(uiText("Восстановить"), async () => {
        if (!file.files?.[0]) throw Error("Выберите файл");
        if (!context.local?.restoreBackup)
          throw Error("Безопасное восстановление ещё не подключено");
        const payload = await decryptBackup(
          await file.files[0].text(),
          password.value,
          context.me.id,
        );
        if(await context.local.restoreBackup(payload.keys))dialog.close();
      });
      return;
    }
    if (mode === "delete") {
      label(
        uiText("Профиль, ключи, контакты и сеансы удаляются. Общие разговоры других участников сохраняются."),
      );
      const password = input(uiText("Пароль"), "password"),
        code = input(uiText("TOTP или резервный код")),
        confirm = input(uiText("Введите УДАЛИТЬ"));
      button(uiText("Удалить аккаунт"), async () => {
        if (confirm.value !== uiText("УДАЛИТЬ")) throw Error("Введите УДАЛИТЬ");
        await call("/me/account/delete", "POST", {
          password: password.value,
          ...(/^\d{6}$/.test(code.value)
            ? { code: code.value }
            : { backup_code: code.value }),
        });
        await context.local?.onDeleted?.();
        dialog.close();
      });
      return;
    }
    if (mode === "qr") {
      label(
        uiText("Отсканируйте запрос входа с нового устройства. Запрос действует две минуты и не содержит токен сеанса."),
      );
      const raw = input(uiText("Запрос 67chat-handoff"));
      button(uiText("Подтвердить вход на другом устройстве"), async () => {
        const q = JSON.parse(raw.value);
        if (q.type !== "67chat-handoff" || q.v !== 1)
          throw Error("Неверный запрос");
        await call(
          "/me/account/handoff/" + encodeURIComponent(q.id) + "/approve",
          "POST",
          { approval: q.approval },
        );
        say(uiText("Вход подтверждён"));
      });
      return;
    }
    if (mode === "transport") {
      label(
        uiText("Веб-браузер использует прокси/Tor, настроенный в самом браузере или ОС. Страница не может гарантировать маршрут и DNS."),
      );
      label(
        uiText("Для fail-closed режима используйте Tor Browser или проверенный нативный транспорт Android. Указание URL само по себе не подключает прокси."),
      );
      return;
    }
    if (mode === "accounts") {
      if (!context.local?.listAccounts || !context.local?.switchAccount) {
        label(
          uiText("Изоляция аккаунтов в этом браузере ещё не подключена. Используйте отдельные профили браузера."),
        );
        return;
      }
      for (const a of await context.local.listAccounts())
        button(a.username || a.userId, () => {dialog.close();return context.local.switchAccount(a.id);});
      if(context.local.addAccount)button(uiText("Добавить аккаунт"),()=>{dialog.close();return context.local.addAccount();});
      return;
    }
    if (mode === "secret") {
      await mountSecretChats({content,dialog,context,Session:SecretSession,say});
      return;
    }
    label(uiText("Неизвестный раздел"));
  });
  return dialog;
}

/** Signs a server nonce with the active Signal device identity; never signs arbitrary caller text. */
export async function createWebDeviceProof(request,e2e,deviceId,identityKey){
 const pair=await e2e?.store?.get('identityKey');if(!pair?.privKey)throw Error('Ключ устройства ещё не загружен');
 const published=identityKey||bytesToBase64(pair.pubKey),challenge=await request('/e2e/device-proof',{method:'POST',body:{device_id:String(deviceId),identity_key:published}});
 const user=e2e.address?.name||e2e.address?.getName?.(),parts=String(challenge.challenge||'').split('\n');
 const expectedHash=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(published))),b=>b.toString(16).padStart(2,'0')).join('');
 if(parts.length!==6||parts[0]!=='67chat-device-proof-v1'||parts[1]!==String(user)||parts[3]!==String(deviceId)||parts[4]!==expectedHash||parts[5].length<32)throw Error('Некорректное подтверждение устройства');
 const curve=await e2e._getCurveWrapper?.();if(!curve?.sign)throw Error('Подпись Curve25519 недоступна; секретные чаты заблокированы');
 const signature=await curve.sign(pair.privKey,new TextEncoder().encode(challenge.challenge).buffer);
 return{id:challenge.id,signature:bytesToBase64(signature)};
}
