import {userError} from '../errors';
import {locale as uiLocale} from "../../../web/i18n.js";
import {t as uiText} from "../../../web/i18n.js";
import React, { useState, useEffect, useRef } from "react";
import {
  ScrollView,
  View,
  Text,
  TextInput,
  Pressable,
  Switch,
  AppState,
} from "react-native";
import * as DocumentPicker from "expo-document-picker";
import * as FileSystem from "expo-file-system/legacy";
import * as Sharing from "expo-sharing";
import { useTheme } from "../ThemeProvider";
import { request } from "../api";
import { getMobileDeviceId } from "../crypto";
import type { FeatureContext } from "./types";
import { CameraView, useCameraPermissions } from "expo-camera";
import {
  accountRegistry,
  getAccountNativeHooks,
  mobileBackupCrypto,
  currentTransport,
  configureTransport,
  transportCapabilities,
  protectSecretScreen,
  DeviceSecretSession,
  exportNativeBackupKeys,
  restoreAccountBackup,
} from "../accountRegistry";
// Portable envelope has no dependency on DOM; noble primitives provide Android crypto.
// @ts-ignore The no-build web client intentionally owns this shared JS module.
import {
  encryptBackup,
  decryptBackup,
  bytesToBase64,
  base64ToBytes,
} from "../../../web/account-features.js";
import {SecretChatController,pruneSecretVault} from "../../../web/secret-vault.js";
import {SecretPeerSelector,peerLabel,deviceLabel,secretChatLabels} from '../../../web/feature-selectors.js';
type Row = Record<string, any>;
const TITLES: Record<string, string> = {
  contacts: "Контакты 67chat",
  privacy: "Приватность",
  blocked: "Заблокированные",
  backup: "Защищённая копия",
  security: "Защита аккаунта",
  aliases: "Публичные имена",
  accounts: "Аккаунты",
  delete: "Удаление аккаунта",
  qr: "Вход по QR",
  transport: "Прокси и Tor",
  secret: "Секретные чаты",
};
export function AccountFeatures({
  mode,
  context,
}: {
  mode: string;
  context: FeatureContext;
}) {
  const { colors } = useTheme(),
    name = mode.replace(/^account[-:]/, ""),
    hooks = getAccountNativeHooks();
  const [cameraPermission, requestCameraPermission] = useCameraPermissions(),
    [scanning, setScanning] = useState(false);
  const [data, setData] = useState<any>(null),
    [status, setStatus] = useState(""),
    [busy, setBusy] = useState(false),
    [values, setValues] = useState<Record<string, string>>({}),
    [privacy, setPrivacy] = useState<Row>({}),
    [search, setSearch] = useState<Row[]>([]),
    [secretChat, setSecretChat] = useState<Row | null>(null),
    [messages, setMessages] = useState<{ text: string; expires_at: number }[]>(
      [],
    );
  const secret = useRef<DeviceSecretSession | null>(null),
    alive = useRef(true);
  const secretController=useRef<SecretChatController|null>(null);
  const secretSelector=useRef<SecretPeerSelector|null>(null),selectorEpoch=useRef(0);
  const [,redrawSelector]=useState(0);
  const secretStore=useRef(context.repository);secretStore.current=context.repository;
  const secretGeneration=useRef(0),foreground=useRef(AppState.currentState==='active');
  const clearSecret=()=>{
    secretSelector.current?.reset();redrawSelector(n=>n+1);
    secretGeneration.current++;secretController.current?.close();secretController.current=null;secret.current?.destroy();secret.current=null;
    setMessages([]);setSecretChat(null);
    setValues(old=>Object.fromEntries(Object.entries(old).filter(([key])=>!key.startsWith('secret_')&&key!=='fingerprint')));
    void protectSecretScreen(false).catch(()=>{});
  };
  const set = (key: string, value: string) =>
    setValues((old) => ({ ...old, [key]: value }));
  const api = <T = any,>(path: string, method = "GET", body?: unknown) =>
    request<T>(context.baseUrl, path, { token: context.token, method, body });
  const run = async (action: () => Promise<void>) => {
    if (busy) return;
    setBusy(true);
    setStatus("");
    try {
      await action();
    } catch (e) {
      if (alive.current) setStatus(userError(e));
    } finally {
      if (alive.current) setBusy(false);
    }
  };
  const load = async () => {
    let result: any = null;
    if (name === "privacy") {
      result = await api("/me/account/privacy");
      setPrivacy(result);
    } else if (name === "contacts" || name === "blocked")
      result = await api(
        name === "contacts" ? "/me/account/contacts" : "/me/account/blocks",
      );
    else if (name === "security") result = await api("/me/account/security");
    else if (name === "aliases") result = await api("/me/public-names");
    else if (name === "accounts") result = await accountRegistry.list();
    else if (name === "secret") {const selector=secretSelector.current;result = await secretChatLabels(await api("/me/account/secret-chats"),context.me.id,api,()=>alive.current&&foreground.current&&secretSelector.current===selector);}
    else if (name === "transport") result = await transportCapabilities();
    if (alive.current) setData(result);
  };
  useEffect(() => {
    alive.current = true;
    foreground.current=AppState.currentState==='active';
    const epoch=++selectorEpoch.current;
    secretSelector.current=new SecretPeerSelector(path=>api(path),()=>alive.current&&foreground.current&&epoch===selectorEpoch.current);
    void run(load);
    return () => {
      alive.current = false;
      selectorEpoch.current++;secretSelector.current?.reset();secretSelector.current=null;
      secretGeneration.current++;
      secretController.current?.close();secretController.current=null;
      secret.current?.destroy();
      secret.current = null;
      void protectSecretScreen(false).catch(()=>{});
    };
  }, [name, context.me.id, context.token, context.baseUrl, context.repository]);
  useEffect(() => {
    const sub = AppState.addEventListener("change", (state) => {
      foreground.current=state==='active';
      if (state !== "active") {
        clearSecret();
      }
    });
    let ticks=0,polling=false;
    const timer = setInterval(
      async () => {
        if(!alive.current||!foreground.current)return;
        setMessages(old=>old.filter(m=>m.expires_at>Date.now()));
        const controller=secretController.current;
        if(!controller){const store=secretStore.current;if(store)await pruneSecretVault(store).catch(()=>{});return;}
        if(controller)void controller.prune().then(()=>{if(secretController.current===controller&&controller.closed)clearSecret();}).catch(()=>{});
        if(controller&&++ticks%5===0&&!polling){polling=true;void controller.inbox().catch(error=>{if(secretController.current===controller&&/secret_device_mismatch/.test(String(error)))clearSecret();}).finally(()=>{polling=false;});}
      },
      1000,
    );
    return () => {
      sub.remove();
      clearInterval(timer);
    };
  }, []);
  const credentials = () => ({
    password: values.password || "",
    ...(/^\d{6}$/.test(values.code || "")
      ? { code: values.code }
      : { backup_code: values.code || "" }),
  });
  const text = (value: string, key?: string) => (
    <Text
      key={key || value}
      style={{ color: colors.text.primary, marginVertical: 7, fontSize: 16 }}
    >
      {value}
    </Text>
  );
  const field = (key: string, label: string, secure = false, multi = false, onChange?:()=>void) => (
    <View key={key} style={{ marginVertical: 7 }}>
      <Text style={{ color: colors.text.secondary }}>{label}</Text>
      <TextInput
        value={values[key] || ""}
        onChangeText={(v) => {set(key, v);onChange?.();}}
        secureTextEntry={secure}
        multiline={multi}
        autoCapitalize="none"
        placeholderTextColor={colors.text.gray2}
        style={{
          borderWidth: 1,
          borderColor: colors.divider,
          borderRadius: 9,
          padding: 11,
          color: colors.text.primary,
          backgroundColor: colors.windowBackgroundGray,
          minHeight: 44,
        }}
        accessibilityLabel={label}
      />
    </View>
  );
  const button = (
    label: string,
    action: () => Promise<void>,
    disabled = false,
    key = label,
  ) => (
    <Pressable
      key={key}
      disabled={disabled || busy}
      onPress={() => void run(action)}
      accessibilityRole="button"
      style={{
        padding: 13,
        marginVertical: 5,
        borderRadius: 9,
        backgroundColor: colors.windowBackgroundGray,
        opacity: disabled || busy ? 0.45 : 1,
      }}
    >
      <Text style={{ color: colors.text.blue, fontSize: 16 }}>{label}</Text>
    </Pressable>
  );
  const accountRows = () => (Array.isArray(data) ? (data as Row[]) : []);
  const beginSecret = async (c: Row) => {
    secretSelector.current?.reset();redrawSelector(n=>n+1);
    if (!hooks.deviceId) throw Error("Привязка устройства ещё не подключена");
    if(!alive.current||!foreground.current)throw Error('Откройте приложение для секретного чата');
    const owner=++secretGeneration.current,current=()=>alive.current&&foreground.current&&secretGeneration.current===owner;
    secretController.current?.close();secretController.current=null;secret.current?.destroy();secret.current=null;
    setMessages([]);setSecretChat(null);
    setValues(old=>Object.fromEntries(Object.entries(old).filter(([key])=>!key.startsWith('secret_')&&key!=='fingerprint')));
    await protectSecretScreen(true);
    if(!current())return;
    const device = await hooks.deviceId();
    if(!current())return;
    if(!context.repository)throw Error('Защищённое хранилище устройства ещё не подключено. Создание секретного чата недоступно.');
    const controller=await SecretChatController.open({chat:c,userId:context.me.id,deviceId:device,store:context.repository,Session:DeviceSecretSession,current,request:(path:string,options:{method?:string;body?:unknown}={})=>api(path,options.method,options.body)});
    if(!current()){controller.close();return;}
    const session=controller.session as DeviceSecretSession;
    secretController.current=controller;secret.current=session;
    setSecretChat(c);setMessages(controller.history());
    set("secret_public",session.publicKey);set("fingerprint",session.fingerprint||'');
  };
  const refreshSecret = async () => {
    const controller=secretController.current;
    if(!controller)throw Error('Откройте секретный чат');
    const owner=secretGeneration.current;
    try{await controller.refresh();}catch(error){if(secretController.current===controller&&/secret_device_mismatch/.test(String(error))){clearSecret();}throw error;}
    if(alive.current&&foreground.current&&secretGeneration.current===owner&&secretController.current===controller)setMessages(controller.history());
  };
  let body: React.ReactNode = null;
  if (name === "contacts" || name === "blocked")
    body = (
      <>
        {text(
          name === "contacts"
            ? uiText("Это ваши контакты на сервере. Телефонная книга не читается.")
            : uiText("Заблокированные пользователи не могут вам писать."),
        )}
        {accountRows().map((u) => (
          <View key={u.id}>
            {text(u.name || u.display_name || u.username || u.id)}
            {button(
              name === "contacts"
                ? (""+uiText("Удалить")+" ") + (u.username || u.id)
                : (""+uiText("Разблокировать")+" ") + (u.username || u.id),
              async () => {
                await api(
                  (name === "contacts"
                    ? "/me/account/contacts/"
                    : "/me/account/blocks/") + u.id,
                  "DELETE",
                );
                await load();
              },
            )}
          </View>
        ))}
        {name === "contacts" && (
          <>
            {field("query", uiText("Найти по имени"))}
            {button(uiText("Найти"), async () =>
              setSearch(
                await api<Row[]>(
                  "/users/search?q=" + encodeURIComponent(values.query || ""),
                ),
              ),
            )}
            {search.map((u) => (
              <View key={u.id}>
                {text("@" + u.username)}
                {button((""+uiText("Добавить")+" ") + u.username, async () => {
                  await api("/me/account/contacts/" + u.id, "PUT", {
                    name: u.display_name || u.username,
                  });
                  await load();
                })}
              </View>
            ))}
          </>
        )}
      </>
    );
  if (name === "privacy")
    body = (
      <>
        {(["last_seen", "write"] as const).map((k) => (
          <View key={k}>
            {text(
              k === "last_seen"
                ? uiText("Кто видит последнее посещение")
                : uiText("Кто может писать"),
            )}
            {[
              ["everyone", uiText("Все")],
              ["contacts", uiText("Мои контакты")],
              ["nobody", uiText("Никто")],
            ].map(([v, label]) =>
              button(
                (privacy[k] === v ? "✓ " : "") +
                  label +
                  " · " +
                  (k === "write" ? uiText("сообщения") : uiText("посещение")),
                async () => setPrivacy((old) => ({ ...old, [k]: v })),
              ),
            )}
          </View>
        ))}
        <View style={{ flexDirection: "row", alignItems: "center" }}>
          {text(uiText("Отметки прочтения"))}
          <Switch
            value={privacy.read_receipts !== false}
            onValueChange={(v) =>
              setPrivacy((old) => ({ ...old, read_receipts: v }))
            }
          />
        </View>
        {text(uiText("Отметки доступны, когда их разрешили оба участника."))}
        {button(uiText("Сохранить"), async () => {
          await api("/me/account/privacy", "PUT", privacy);
          setStatus(uiText("Сохранено"));
        })}
      </>
    );
  if (name === "security")
    body = (
      <>
        {text(data?.enabled ? uiText("TOTP включён") : uiText("TOTP выключен"))}
        {field("password", uiText("Пароль аккаунта"), true)}
        {field("code", uiText("Код TOTP или резервный код"))}
        {button(uiText("Настроить TOTP"), async () => {
          const result = await api(
            "/me/account/totp/setup",
            "POST",
            credentials(),
          );
          set("totp_secret", result.secret);
          setStatus(
            uiText("Добавьте секрет в приложение-аутентификатор и подтвердите кодом."),
          );
        })}
        {values.totp_secret && text((""+uiText("Секрет:")+" ") + values.totp_secret)}
        {button(uiText("Подтвердить TOTP"), async () => {
          const result = await api("/me/account/totp/enable", "POST", {
            code: values.code,
          });
          set("backup_codes", result.backup_codes.join("\n"));
          await load();
        })}
        {values.backup_codes &&
          text(
            (""+uiText("Сохраните резервные коды — показаны один раз:")+"\n") +
              values.backup_codes,
          )}
        {data?.enabled && (
          <>
            {button(uiText("Выключить TOTP"), async () => {
              await api("/me/account/totp/disable", "POST", credentials());
              await load();
            })}
            {button(
              data?.approval_required
                ? uiText("Выключить подтверждение устройств")
                : uiText("Подтверждать новые устройства"),
              async () => {
                let device = await import("expo-secure-store").then((s) =>
                  s.getItemAsync("67chat.login-device"),
                );
                if (!device) {
                  device = Array.from(
                    crypto.getRandomValues(new Uint8Array(32)),
                    (x) => x.toString(16).padStart(2, "0"),
                  ).join("");
                  await import("expo-secure-store").then((s) =>
                    s.setItemAsync("67chat.login-device", device!),
                  );
                }
                await api("/me/account/device-approval", "PUT", {
                  ...credentials(),
                  enabled: !data.approval_required,
                  device_key: device,
                  device_name: "Android",
                });
                await load();
              },
            )}
          </>
        )}
        {(data?.challenges || []).map((c: Row) => (
          <View key={c.id}>
            {text((""+uiText("Вход:")+" ") + (c.device_name || c.id))}
            {button((""+uiText("Подтвердить")+" ") + c.id, async () => {
              await api(
                "/me/account/challenges/" + c.id + "/approve",
                "POST",
                {},
              );
              await load();
            })}
          </View>
        ))}
        {text(uiText("История входов"))}
        {(data?.audit || []).map((a: Row) =>
          text(
            `${a.created_at}: ${a.device_name || uiText("Устройство")} · ${a.outcome} · ${a.ip || ""}`,
            a.id,
          ),
        )}
      </>
    );
  if (name === "aliases")
    body = (
      <>
        {(data?.public_names || []).map((n: Row) => (
          <View key={n.id}>
            {text(
              "@" + n.name + (n.kind === "primary_tag" ? (" "+uiText("· основной")+"") : ""),
            )}
            {n.kind !== "primary_tag" &&
              button(uiText("Сделать основным @") + n.name, async () => {
                await api("/me/public-names/" + n.id + "/promote", "POST", {});
                await load();
              })}
            {button(uiText("Удалить @") + n.name, async () => {
              await api("/me/public-names/" + n.id, "DELETE");
              await load();
            })}
          </View>
        ))}
        {text(
          uiText("Имя второго вашего аккаунта можно перенести объединением. Его вход и данные перейдут в текущий аккаунт."),
        )}
        {field("alias_username", uiText("Второй аккаунт"))}
        {field("alias_password", uiText("Пароль второго аккаунта"), true)}
        {field(
          "alias_factor",
          uiText("TOTP или резервный код второго аккаунта (если включён)"),
        )}
        {field("alias_confirm", uiText("Введите ОБЪЕДИНИТЬ"))}
        {button(uiText("Объединить аккаунты"), async () => {
          if (values.alias_confirm !== uiText("ОБЪЕДИНИТЬ"))
            throw Error("Введите ОБЪЕДИНИТЬ");
          await api("/me/public-names/merge", "POST", {
            secondary_username: values.alias_username,
            secondary_password: values.alias_password,
            ...(/^\d{6}$/.test(values.alias_factor || "")
              ? { secondary_totp: values.alias_factor }
              : { secondary_backup_code: values.alias_factor }),
          });
          await load();
        })}
      </>
    );
  if (name === "backup")
    body = (
      <>
        {text(
          uiText("Копия шифруется локально. Секретные чаты исключены. При переносе между вебом и Android сеансы восстанавливаются заново; старая история может быть недоступна."),
        )}
        {field("backup_password", uiText("Пароль копии (минимум 8 символов)"), true)}
        {field(
          "restore_password",
          uiText("Пароль аккаунта для нового сеанса восстановления"),
          true,
        )}
        {field(
          "restore_code",
          uiText("TOTP или резервный код аккаунта (если включён)"),
        )}
        {button(uiText("Экспортировать защищённую копию"), async () => {
          const keys = hooks.exportKeys
            ? await hooks.exportKeys()
            : await exportNativeBackupKeys(context.me.id);
          const repository = context.repository as unknown as {
            exportKeyState?: () => Promise<unknown>;
          };
          if (repository?.exportKeyState)
            keys.mobile_key_state = await repository.exportKeyState();
          const payload = await encryptBackup(
              keys,
              values.backup_password,
              { userId: context.me.id },
              mobileBackupCrypto,
            ),
            file = FileSystem.cacheDirectory + "67chat-encrypted-backup.json";
          await FileSystem.writeAsStringAsync(file, payload);
          try {
            await Sharing.shareAsync(file, {
              mimeType: "application/json",
              dialogTitle: uiText("Сохранить защищённую копию"),
            });
          } finally {
            await FileSystem.deleteAsync(file, { idempotent: true });
          }
        })}
        {button(uiText("Восстановить из файла"), async () => {
          const result = await DocumentPicker.getDocumentAsync({
            type: "application/json",
            copyToCacheDirectory: true,
          });
          if (result.canceled) return;
          const asset = result.assets[0];
          try {
            const info = await FileSystem.getInfoAsync(asset.uri);
            if (info.exists && info.size > 32 * 1024 * 1024)
              throw Error("Файл больше 32 МБ");
            const payload = await decryptBackup(
              await FileSystem.readAsStringAsync(asset.uri),
              values.backup_password,
              context.me.id,
              mobileBackupCrypto,
            );
            if (!hooks.restoreSession || !hooks.onRestoredSession)
              throw Error(
                "Обновите приложение: безопасный сеанс восстановления ещё не подключён",
              );
            const restored = await restoreAccountBackup({
              baseUrl: context.baseUrl,
              user: context.me,
              currentToken: context.token,
              currentDeviceId: await getMobileDeviceId(context.me.id),
              keys: payload.keys,
              credentials: {
                password: values.restore_password || "",
                ...(/^\d{6}$/.test(values.restore_code || "")
                  ? { totp: values.restore_code }
                  : { backup_code: values.restore_code }),
              },
              repository: context.repository as unknown as Parameters<
                typeof restoreAccountBackup
              >[0]["repository"],
              request: (path, options) =>
                request(context.baseUrl, path, options),
              restoreSession: hooks.restoreSession,
              onRestoredSession: hooks.onRestoredSession,
            });
            setStatus(restored.warning || uiText("Ключи восстановлены в новом сеансе"));
            await hooks.onRestoreFinished?.();
            await context.onRefresh();
          } finally {
            if (asset.uri.startsWith(FileSystem.cacheDirectory || "invalid:"))
              await FileSystem.deleteAsync(asset.uri, { idempotent: true });
          }
        })}
      </>
    );
  if (name === "accounts")
    body = (
      <>
        {text(
          uiText("Токены хранятся в защищённом хранилище Android. Смена закрывает сеанс, очереди и базу предыдущего аккаунта."),
        )}
        {accountRows().map((a) =>
          button(
            a.username + " · " + a.baseUrl,
            async () => {
              if (!hooks.switchAccount)
                throw Error("Переключение аккаунтов ещё не подключено");
              await hooks.switchAccount(a.id);
            },
            !hooks.switchAccount,
          ),
        )}
        {button(
          uiText("Добавить аккаунт"),
          async () => {
            if (!hooks.addAccount)
              throw Error("Добавление аккаунта ещё не подключено");
            await hooks.addAccount();
          },
          !hooks.addAccount,
        )}
      </>
    );
  if (name === "delete")
    body = (
      <>
        {text(
          uiText("Профиль, ключи, контакты и сеансы удаляются. Общие разговоры других участников сохраняются."),
        )}
        {field("password", uiText("Пароль"), true)}
        {field("code", uiText("TOTP или резервный код"))}
        {field("delete_confirm", uiText("Введите УДАЛИТЬ"))}
        {button(uiText("Удалить аккаунт"), async () => {
          if (values.delete_confirm !== uiText("УДАЛИТЬ"))
            throw Error("Введите УДАЛИТЬ");
          if (!hooks.onDeleted)
            throw Error("Очистка локального аккаунта ещё не подключена");
          await api("/me/account/delete", "POST", credentials());
          await hooks.onDeleted();
        })}
      </>
    );
  if (name === "qr")
    body = (
      <>
        {text(
          uiText("Отсканируйте запрос с нового устройства или вставьте его текст. Подтвердите вход, только если сами открыли этот запрос. Срок действия — две минуты."),
        )}
        {button(
          scanning ? uiText("Закрыть камеру") : uiText("Сканировать QR-код"),
          async () => {
            if (scanning) {
              setScanning(false);
              return;
            }
            const permission = await requestCameraPermission();
            if (!permission.granted)
              throw Error("Разрешите камеру для сканирования QR-кода");
            setScanning(true);
          },
        )}
        {scanning && cameraPermission?.granted && (
          <CameraView
            style={{ height: 260 }}
            barcodeScannerSettings={{ barcodeTypes: ["qr"] }}
            onBarcodeScanned={(event) => {
              try {
                const q = JSON.parse(event.data);
                if (q.v !== 1 || q.type !== "67chat-handoff") throw Error();
                set("qr", event.data);
                setScanning(false);
                setStatus(uiText("Запрос считан. Проверьте и подтвердите вход."));
              } catch {
                setStatus(uiText("Это не запрос входа 67chat"));
              }
            }}
          />
        )}
        {field("qr", uiText("JSON запроса 67chat-handoff"), false, true)}
        {button(uiText("Подтвердить вход"), async () => {
          const q = JSON.parse(values.qr || "");
          if (q.v !== 1 || q.type !== "67chat-handoff")
            throw Error("Неверный запрос");
          await api(
            "/me/account/handoff/" + encodeURIComponent(q.id) + "/approve",
            "POST",
            { approval: q.approval },
          );
          setStatus(uiText("Вход подтверждён"));
        })}
      </>
    );
  if (name === "transport")
    body = (
      <>
        {text((""+uiText("Текущий режим:")+" ") + currentTransport().mode)}
        {text(
          data?.tor || data?.proxy
            ? uiText("HTTP, WebSocket, вложения, медиа и обновления используют SOCKS5 с DNS через прокси. Смена режима перезапустит приложение; очередь отправки сохранится. Tor требует запущенный локальный SOCKS-сервис, например Orbot.")
            : uiText("Эта APK не содержит проверенный нативный прокси/Tor. Указание адреса само по себе не меняет маршрут. Прямой обход защищённого режима запрещён."),
        )}
        {field("proxy_host", uiText("IP прокси (например 127.0.0.1)"))}
        {field("proxy_port", uiText("Порт"))}
        {button(uiText("Прямое соединение"), async () => {
          await configureTransport({ mode: "direct" });
          await load();
        })}
        {button(
          uiText("Использовать SOCKS-прокси"),
          async () => {
            await configureTransport({
              mode: "proxy",
              host: values.proxy_host,
              port: Number(values.proxy_port),
            });
            await load();
          },
          !data?.proxy,
        )}
        {button(
          uiText("Использовать Tor"),
          async () => {
            await configureTransport({
              mode: "tor",
              host: values.proxy_host,
              port: Number(values.proxy_port),
            });
            await load();
          },
          !data?.tor,
        )}
        {text(
          uiText("В защищённом режиме звонки и Google Push отключены. При недоступном SOCKS прямого подключения не будет."),
        )}
      </>
    );
  if (name === "secret")
    body = (
      <>
        {text(
          uiText("Только два устройства. История зашифрована на этом устройстве и удаляется по таймеру. При уходе в фон экран и черновик очищаются. Экспорт запрещён. Сравните код безопасности вне чата."),
        )}
        {text(uiText("Это не Signal Double Ratchet: нет отдельного обновления ключа для каждого сообщения."))}
        {accountRows().map((c) =>
          button((""+uiText("Открыть")+" ") + c._label, () => beginSecret(c)),
        )}
        {field("secret_query", uiText("Имя пользователя собеседника"),false,false,()=>{secretSelector.current?.reset();redrawSelector(n=>n+1);})}
        {button(uiText('Найти собеседника'),async()=>{const selector=secretSelector.current;if(!selector)return;await selector.search(values.secret_query||'');redrawSelector(n=>n+1);if(!selector.users.length)setStatus(uiText('Пользователи не найдены'));})}
        {secretSelector.current?.users.map(user=>button(peerLabel(user),async()=>{const selector=secretSelector.current;if(!selector)return;const rows=await selector.selectUser(user.id);redrawSelector(n=>n+1);if(!rows.length)setStatus(uiText('Доступных устройств нет. Сначала откройте обычный чат и попросите собеседника войти на устройстве.'));},false,user.id))}
        {secretSelector.current?.peer&&text(uiText('Выберите устройство собеседника')+' · '+peerLabel(secretSelector.current.peer))}
        {secretSelector.current?.devices.map((device,index)=>button((secretSelector.current?.device===device?'✓ ':'')+deviceLabel(device,index),async()=>{secretSelector.current?.selectDevice(device.device_id);redrawSelector(n=>n+1);}))}
        {field("secret_ttl", uiText("Срок сообщения, секунд (30–86400)"))}
        {button(uiText("Создать секретный чат"), async () => {
          if (!hooks.deviceId) throw Error("Привязка устройства не подключена");
          const owner=secretGeneration.current,current=()=>alive.current&&foreground.current&&secretGeneration.current===owner;
          if(!current())return;
          await protectSecretScreen(true);if(!current())return;
          const deviceId=await hooks.deviceId();if(!current())return;
          const selector=secretSelector.current;if(!selector)throw Error('Выберите собеседника');
          const c = await api("/me/account/secret-chats", "POST", selector.payload(deviceId,values.secret_ttl||3600));
          if(!current())return;
          await beginSecret(c);
          await load();
        })}
        {secretChat && (
          <>
            {text((""+uiText("Публичный ключ этого устройства:")+" ") + values.secret_public)}
            {field("secret_peer_key", uiText("Публичный ключ собеседника"))}
            {button(uiText("Вычислить код безопасности"), async () => {
              if (!secret.current) throw Error("Чат закрыт");
              const controller=secretController.current;if(!controller)throw Error("Чат закрыт");
              set("fingerprint", await controller.accept(values.secret_peer_key));
            })}
            {values.fingerprint && text((""+uiText("Код:")+" ") + values.fingerprint)}
            {field("secret_verify", uiText("Введите сравниваемый код безопасности"))}
            {button(uiText("Подтвердить совпадение кода"), async () => {
              const controller=secretController.current;if(!controller)throw Error("Чат закрыт");await controller.verify(values.secret_verify);
              setStatus(uiText("Код подтверждён"));
            })}
            {messages.map((m, i) =>
              text(
                m.text + (" "+uiText("· до")+" ") + new Date(m.expires_at).toLocaleTimeString(uiLocale()),
                String(i),
              ),
            )}
            {field("secret_message", uiText("Секретное сообщение"), false, true)}
            {button(uiText("Отправить шифрованное сообщение"), async () => {
              if (!secret.current) throw Error("Чат закрыт");
              const controller=secretController.current;if(!controller)throw Error('Чат закрыт');
              await controller.send(values.secret_message);
              if(secretController.current!==controller)return;
              setMessages(controller.history());
              set("secret_message", "");
            })}
            {button(uiText("Получить и удалить с сервера"), refreshSecret)}
            {button(uiText("Завершить и удалить секретный чат"), async () => {
              if (!secret.current) return;
              await secretController.current?.remove();
              clearSecret();
              await load();
            })}
          </>
        )}
      </>
    );
  return (
    <ScrollView
      contentContainerStyle={{
        padding: 20,
        paddingBottom: 60,
        backgroundColor: colors.windowBackground,
        flexGrow: 1,
      }}
      keyboardShouldPersistTaps="handled"
    >
      <Text
        style={{
          color: colors.text.primary,
          fontSize: 22,
          fontWeight: "700",
          marginBottom: 16,
        }}
      >
        {uiText(TITLES[name] || uiText("Аккаунт"))}
      </Text>
      {body}
      <Text
        accessibilityLiveRegion="polite"
        style={{ color: colors.text.secondary, marginVertical: 10 }}
      >
        {busy ? uiText("Загрузка…") : status}
      </Text>
      {button(uiText("Закрыть"), async () => context.onClose())}
    </ScrollView>
  );
}
