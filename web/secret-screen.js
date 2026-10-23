import { SecretChatController, pruneSecretVault } from "./secret-vault.js";
import { t, locale } from "./i18n.js";
import { userError } from "./errors.js";
import {SecretPeerSelector,peerLabel,deviceLabel,secretChatLabels} from './feature-selectors.js';

/** Secret UI owns only temporary plaintext; the controller commits to encrypted account storage. */
export async function mountSecretChats({
  content,
  dialog,
  context,
  Session,
  say,
}) {
  let generation = 0,
    active = null,
    busy = false,
    disposed = false,
    polling = false;
  const fields = new Set(),
    list = document.createElement("div"),
    panel = document.createElement("div");
  const current = () =>
    !disposed &&
    dialog.isConnected &&
    !document.hidden &&
    context.local.isCurrent();
  const guard = () => {
    if (!current()) throw Error("Сеанс изменился");
  };
  const selector=new SecretPeerSelector(context.request,current);
  const label = (parent, value) => {
    const p = document.createElement("p");
    p.textContent = value;
    parent.append(p);
    return p;
  };
  const input = (parent, caption) => {
    const l = document.createElement("label"),
      field = document.createElement("input");
    l.textContent = t(caption);
    field.autocomplete = "off";
    field.style.cssText =
      "display:block;width:100%;box-sizing:border-box;margin:8px 0;padding:9px;background:var(--input-bg,var(--panel-bg));color:inherit;border:1px solid var(--border)";
    l.append(field);
    parent.append(l);
    fields.add(field);
    return field;
  };
  const run = async (action) => {
    if (busy) return;
    busy = true;
    try {
      guard();
      say("");
      await action();
    } catch (e) {
      if (current()) say(userError(e));
    } finally {
      busy = false;
    }
  };
  const button = (parent, caption, action) => {
    const b = document.createElement("button");
    b.type = "button";
    b.textContent = caption;
    b.style.cssText = "margin:6px;padding:9px";
    b.onclick = () => run(action);
    parent.append(b);
    return b;
  };
  const lock = () => {
    generation++;
    selector.reset();
    people.replaceChildren();devices.replaceChildren();
    active?.close();
    active = null;
    for (const field of fields) field.value = "";
    fields.clear();
    fields.add(peer);
    fields.add(ttl);
    panel.replaceChildren();
    say("");
  };
  label(
    content,
    t(
      "Только два устройства. История зашифрована на этом устройстве и удаляется по таймеру. При уходе в фон экран и черновик очищаются. Экспорт запрещён. Сравните код безопасности вне чата.",
    ),
  );
  label(content, t("Браузер не может запретить снимки и запись экрана."));
  label(
    content,
    t(
      "Это не Signal Double Ratchet: нет отдельного обновления ключа для каждого сообщения.",
    ),
  );
  if (
    !context.local?.get ||
    !context.local?.set ||
    !context.local?.deviceId ||
    !context.local?.isCurrent
  ) {
    label(
      content,
      t(
        "Защищённое хранилище устройства ещё не подключено. Создание секретного чата недоступно.",
      ),
    );
    return;
  }
  content.append(list);
  const peer = input(content, "Имя пользователя собеседника"),
    people=document.createElement('div'),devices=document.createElement('div');
  content.append(people,devices);
  peer.oninput=()=>{selector.reset();people.replaceChildren();devices.replaceChildren();};
  button(content,t('Найти собеседника'),async()=>{
    people.replaceChildren();devices.replaceChildren();
    const users=await selector.search(peer.value);
    if(!users.length)label(people,t('Пользователи не найдены'));
    for(const user of users)button(people,peerLabel(user),async()=>{
      devices.replaceChildren();
      const rows=await selector.selectUser(user.id);
      label(devices,t('Выберите устройство собеседника')+' · '+peerLabel(user));
      if(!rows.length)label(devices,t('Доступных устройств нет. Сначала откройте обычный чат и попросите собеседника войти на устройстве.'));
      const draw=()=>{for(const [index,row] of rows.entries())button(devices,deviceLabel(row,index),async()=>{selector.selectDevice(row.device_id);say(t('Выбрано устройство')+' · '+deviceLabel(row,index));});};draw();
    });
  });
  const ttl = input(content, "Срок сообщения, секунд (30–86400)");
  ttl.type = "number";
  ttl.value = "3600";
  const render = (controller) => {
    panel.replaceChildren();
    label(
      panel,
      t("Публичный ключ этого устройства:") +
        " " +
        controller.session.publicKey,
    );
    const key = input(panel, "Публичный ключ собеседника"),
      fingerprint = label(panel, controller.session.fingerprint || ""),
      code = input(panel, "Введите сравниваемый код безопасности");
    button(panel, t("Вычислить код безопасности"), async () => {
      fingerprint.textContent = await controller.accept(key.value);
    });
    button(panel, t("Подтвердить совпадение кода"), async () => {
      await controller.verify(code.value);
      code.value = "";
      say(t("Код подтверждён"));
    });
    const history = document.createElement("div");
    history.setAttribute("aria-live", "polite");
    panel.append(history);
    const paint = () => {
      guard();
      if (active !== controller) return;
      history.replaceChildren();
      for (const message of controller.history())
        label(
          history,
          message.text +
            " " +
            t("· до") +
            " " +
            new Date(message.expires_at).toLocaleTimeString(locale()),
        );
    };
    controller.paint = paint;
    paint();
    const text = input(panel, "Секретное сообщение");
    button(panel, t("Отправить шифрованное сообщение"), async () => {
      await controller.send(text.value);
      guard();
      text.value = "";
      paint();
    });
    button(panel, t("Получить и удалить с сервера"), async () => {
      await controller.refresh();
      paint();
    });
    button(panel, t("Завершить и удалить секретный чат"), async () => {
      await controller.remove();
      lock();
      await reload();
    });
  };
  const open = async (chat) => {
    lock();
    guard();
    const owner = generation,
      deviceId = String(await context.local.deviceId());
    guard();
    if (owner !== generation) return;
    const controller = await SecretChatController.open({
      chat,
      userId: context.me.id,
      deviceId,
      store: context.local.secretStore || context.local,
      request: context.request,
      Session,
      current: () => current() && owner === generation,
    });
    if (!current() || owner !== generation) {
      controller.close();
      return;
    }
    active = controller;
    render(controller);
  };
  const reload = async () => {
    const chats = await secretChatLabels(await context.request("/me/account/secret-chats"),context.me.id,context.request,current);
    guard();
    list.replaceChildren();
    for (const chat of chats)
      button(list, t("Открыть") + " " + chat._label, () => open(chat));
  };
  button(content, t("Создать секретный чат"), async () => {
    const owner = generation,
      deviceId = String(await context.local.deviceId());
    guard();
    if (owner !== generation) return;
    const chat = await context.request("/me/account/secret-chats", {
      method: "POST",
      body: selector.payload(deviceId,ttl.value),
    });
    guard();
    if (owner !== generation) return;
    await open(chat);
    await reload();
  });
  content.append(panel);
  const visibility = () => {
    if (document.hidden) lock();
  };
  document.addEventListener("visibilitychange", visibility);
  // TTL pruning also runs with no open chat. Hidden pages drop plaintext immediately.
  let tick = 0;
  const timer = setInterval(async () => {
    if (!current()) {
      lock();
      return;
    }
    const controller = active;
    if (polling) return;
    polling = true;
    try {
      if (!controller) {
        await pruneSecretVault(context.local.secretStore || context.local);
        return;
      }
      await controller.prune();
      if (controller.closed) {
        lock();
        return;
      }
      controller.paint();
      if (++tick % 5 === 0) {
        if (controller.session.verified) await controller.refresh();
        else await controller.inbox();
        controller.paint();
      }
    } catch (error) {
      if (current() && active === controller) {
        if (/secret_device_mismatch/.test(String(error))) lock();
        say(userError(error));
      }
    } finally {
      polling = false;
    }
  }, 1000);
  dialog.addEventListener("close", () => {
    disposed = true;
    lock();
    clearInterval(timer);
    document.removeEventListener("visibilitychange", visibility);
  });
  await reload();
}
