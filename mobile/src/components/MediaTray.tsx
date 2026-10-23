import {t as uiText} from "../../../web/i18n.js";
import type { ProtectedUploader } from "../protectedUpload";
import { useEffect, useRef, useState } from "react";
import {
  ActivityIndicator,
  Alert,
  Image,
  Linking,
  Pressable,
  ScrollView,
  Text,
  TextInput,
  View,
} from "react-native";
import * as Picker from "expo-image-picker";
import * as DocumentPicker from "expo-document-picker";
import * as Files from "expo-file-system/legacy";
import { Video, ResizeMode } from "./Video";
import { CircleCamera } from "./CircleCamera";
import {PhotoEditor} from './PhotoEditor';
import {runMediaTask} from '../mediaLifecycle';
import {processVideo} from '../mediaProcessing';
import { useTheme } from "../ThemeProvider";
import { newId, type AccountRepository } from "../data/repository";
type Attachment = {
  original?:{uri:string;details?:import('../../../web/media-details.js').MediaDetails};
  quality?:'original'|480|720;
  details?:import('../../../web/media-details.js').MediaDetails;
  albumId?: string;
  caption?: string;
  id: string;
  uri: string;
  name: string;
  mime: string;
  kind: string;
  uploaded?: string;
  protectedMeta?: Record<string, unknown>;
};
export function MediaTray({
  uploader,
  initialCircle = false,
  baseUrl,
  token,
  roomId,
  repository,
  onSend,
  onSent,
}: {
  uploader: ProtectedUploader;
  initialCircle?: boolean;
  baseUrl: string;
  token: string;
  roomId: string;
  repository: AccountRepository | null;
  onSend: (
    caption: string,
    body: Record<string, unknown>,
    roomId: string,
  ) => Promise<void>;
  onSent: () => void;
}) {
  const { colors } = useTheme(),
    [items, setItems] = useState<Attachment[]>([]),
    [caption, setCaption] = useState(""),
    [progress, setProgress] = useState(0),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [circle, setCircle] = useState(initialCircle);
  const xhr = useRef<{abort():void} | null>(null),
    current = useRef(items);
  current.current = items;
  const stopped = useRef(false), generation = useRef(0), live = useRef(true);
  const [editingPhoto,setEditingPhoto]=useState<Attachment|null>(null);
  useEffect(() => {
    let active = true; live.current = true; const owner = ++generation.current;
    void repository
      ?.get<{
        items: Attachment[];
        caption: string;
      }>(`attachments:${roomId}`, { items: [], caption: "" })
      .then((value) => {
        if (active) {
          setItems(value.items);
          setCaption(value.caption);
        }
      });
    return () => {
      active = false; live.current = false; if (generation.current === owner) generation.current++;
      stopped.current = true;
      xhr.current?.abort();
    };
  }, [roomId]);
  const save = (next: Attachment[], text = caption) => {
    setItems(next);
    current.current = next;
    void repository?.set(`attachments:${roomId}`, {
      items: next,
      caption: text,
    });
  };
  const permissionError = () =>
    Alert.alert(uiText("Нет разрешения"), uiText("Разрешите доступ в настройках приложения"), [
      { text: uiText("Отмена") },
      { text: uiText("Настройки"), onPress: () => void Linking.openSettings() },
    ]);
  const add = async (values: Omit<Attachment, "id">[]) => {
    if(!repository || !live.current)return;
    const owner=generation.current;
    try {
      await runMediaTask(repository.userId,async task=>{
        const check=()=>{task.check();if(!live.current || generation.current!==owner)throw Error("Экран вложений закрыт");};
        const dir = `${Files.documentDirectory}draft-media/${repository.userId}/`,added:string[]=[];let saved=false;
        try{
          await Files.makeDirectoryAsync(dir, { intermediates: true });check();
          const additions=[];
          for(const value of values){const id=newId(),uri=dir+id+'-'+value.name.replace(/[^a-zA-Z0-9._-]/g,'_');added.push(uri);await Files.copyAsync({from:value.uri,to:uri});check();additions.push({...value,id,uri});}
          check();save([...current.current,...additions]);saved=true;
        }finally{if(!saved)await Promise.all(added.map(uri=>Files.deleteAsync(uri,{idempotent:true}).catch(()=>{})));}
      });
    }catch(e){if(live.current && generation.current===owner)setError(String(e));}
  };
  const gallery = async () => {
    try {
      if (!(await Picker.requestMediaLibraryPermissionsAsync()).granted) {
        permissionError();
        return;
      }
      const result = await Picker.launchImageLibraryAsync({
        mediaTypes: ["images", "videos"],
        allowsMultipleSelection: true,
        selectionLimit: 10,
        quality: 0.9,
      });
      if (!result.canceled)
        await add(
          result.assets.map((asset) => ({
            uri: asset.uri,
            name:
              asset.fileName ||
              `media.${asset.type === "video" ? "mp4" : "jpg"}`,
            mime:
              asset.mimeType ||
              `${asset.type === "video" ? "video/mp4" : "image/jpeg"}`,
            kind: asset.type === "video" ? "video" : "image",
            details:{width:asset.width,height:asset.height,duration_ms:asset.duration || undefined},
          })),
        );
    } catch (e) {
      setError(String(e));
    }
  };
  const videoQuality=async(item:Attachment)=>{
    if(!repository || !live.current)return;
    const quality=item.quality===undefined || item.quality==='original'?720:item.quality===720?480:'original';
    const original=item.original || {uri:item.uri,details:item.details};
    if(quality==='original'){save(current.current.map(value=>value.id===item.id?{...value,uri:original.uri,details:original.details,original,quality}:value));return;}
    await runMediaTask(repository.userId,async ticket=>{
    const owner=++generation.current;const valid=()=>{try{ticket.check();return live.current && !stopped.current && generation.current===owner;}catch{return false;}};
    setBusy(true);setError('');stopped.current=false;
    const task=processVideo(original.uri,quality,value=>{if(valid())setProgress(value);});xhr.current={abort:task.abort};const cancel=ticket.onCancel(task.abort);
    let output='',target='',published=false;
    try {
      const result=await task.promise;output=result.uri;if(!valid())return;
      target=`${Files.documentDirectory}draft-media/${repository.userId}/${newId()}-video.mp4`;
      await Files.copyAsync({from:result.uri,to:target});if(!valid())return;
      save(current.current.map(value=>value.id===item.id?{...value,uri:target,mime:'video/mp4',name:'video.mp4',details:{...original.details,width:result.width,height:result.height,duration_ms:result.duration_ms,orientation:result.orientation,rotation:result.rotation},original,quality}:value));published=true;
    }catch(value){if(valid())setError(String(value));}finally{
      cancel();
      if(output.startsWith(Files.cacheDirectory||'invalid:'))await Files.deleteAsync(output,{idempotent:true}).catch(()=>{});
      if(target && !published)await Files.deleteAsync(target,{idempotent:true}).catch(()=>{});
      if(live.current && generation.current===owner){setBusy(false);setProgress(0);xhr.current=null;}
    }
    });
  };
  const camera = async () => {
    try {
      if (!(await Picker.requestCameraPermissionsAsync()).granted) {
        permissionError();
        return;
      }
      const result = await Picker.launchCameraAsync({ quality: 0.9 });
      if (!result.canceled)
        await add(
          result.assets.map((asset) => ({
            uri: asset.uri,
            name: "camera.jpg",
            mime: "image/jpeg",
            kind: "image",
            details:{width:asset.width,height:asset.height},
          })),
        );
    } catch (e) {
      setError(String(e));
    }
  };
  const file = async () => {
    try {
      const result = await DocumentPicker.getDocumentAsync({
        multiple: true,
        copyToCacheDirectory: true,
      });
      if (!result.canceled)
        await add(
          result.assets.map((asset) => ({
            uri: asset.uri,
            name: asset.name,
            mime: asset.mimeType || "application/octet-stream",
            kind: "file",
          })),
        );
    } catch (e) {
      setError(String(e));
    }
  };
  const upload = (item: Attachment) => {
    const task = uploader(roomId, item, setProgress);
    xhr.current = task.request;
    return task.promise;
  };
  const send = async () => {
    setBusy(true);
    setError("");
    stopped.current = false;
    const album = newId();
    try {
      let remaining = current.current.map((item, index) => ({
        ...item,
        albumId: item.albumId || album,
        caption:
          item.caption === undefined
            ? index === 0
              ? caption
              : ""
            : item.caption,
      }));
      save(remaining);
      for (const item of [...remaining]) {
        if (stopped.current) break;
        const mediaId = item.uploaded;
        if (stopped.current) break;
        await onSend(
          item.caption || "",
          {
            type: item.kind === "video_note" ? "video_note" : "media",
            media_id: mediaId,
            __queue_id:item.id,
            ...(!mediaId?{__upload:{uri:item.uri,name:item.name,mime:item.mime,details:item.details}}:{}),
            text: item.caption || "",
            meta: { album_id: item.albumId, filename: item.name, ...item.protectedMeta },
          },
          roomId,
        );
        remaining = remaining.filter((x) => x.id !== item.id);
        save(remaining);
        // The durable outbox owns this file until delivery/cancellation.
      }
      if (!remaining.length) {
        setCaption("");
        await repository?.set(`attachments:${roomId}`, {
          items: [],
          caption: "",
        });
        onSent();
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
      setProgress(0);
    }
  };
  const button = (label: string, action: () => void) => (
    <Pressable
      onPress={action}
      disabled={busy}
      style={{ padding: 14, minHeight: 48 }}
    >
      <Text style={{ color: colors.text.blue }}>{label}</Text>
    </Pressable>
  );
  return (
    <View
      style={{
        maxHeight: "85%",
        backgroundColor: colors.windowBackground,
        paddingBottom: 20,
      }}
    >
      {editingPhoto && <PhotoEditor uri={editingPhoto.uri} onClose={()=>setEditingPhoto(null)} onApply={async (result,isCurrent)=>{
        if(!repository || stopped.current || !live.current)throw Error("Экран вложений закрыт");
        await runMediaTask(repository.userId,async task=>{
          const uri=`${Files.documentDirectory}draft-media/${repository.userId}/${newId()}-edited.jpg`;let saved=false;
          try{
            await Files.copyAsync({from:result.uri,to:uri});task.check();
            if(stopped.current || !live.current || isCurrent?.()===false)return;
            save(current.current.map(item=>item.id===editingPhoto.id?{...item,uri,mime:'image/jpeg',name:'photo.jpg',details:{width:result.width,height:result.height,orientation:1}}:item));saved=true;
          }finally{if(!saved)await Files.deleteAsync(uri,{idempotent:true}).catch(()=>{});}
        });
      }}/>}
      {!!error && (
        <Text style={{ padding: 12, color: colors.misc.error }}>{error}</Text>
      )}
      {circle ? (
        <CircleCamera
          userId={repository?.userId}
          onCancel={() => setCircle(false)}
          onRecorded={async (uri) => {
            setCircle(false);
            await add([
              {
                uri,
                name: "circle.mp4",
                mime: "video/mp4",
                kind: "video_note",
              },
            ]);
          }}
        />
      ) : (
        <>
          <View style={{ flexDirection: "row", flexWrap: "wrap" }}>
            {button(uiText("Галерея"), () => void gallery())}
            {button(uiText("Камера"), () => void camera())}
            {button(uiText("Файл"), () => void file())}
            {button(uiText("Видеокружок"), () => setCircle(true))}
          </View>
          <ScrollView horizontal style={{ minHeight: items.length ? 170 : 0 }}>
            {items.map((item) => (
              <View key={item.id} style={{ width: 140, margin: 8 }}>
                {item.mime.startsWith('image/') && button(uiText("Изменить фото"),()=>setEditingPhoto(item))}
                {item.mime.startsWith('video/') && button(uiText("Качество: {0}",item.quality || uiText("оригинал")),()=>void videoQuality(item))}
                {item.mime.startsWith("image/") ? (
                  <Image
                    source={{ uri: item.uri }}
                    style={{ width: 140, height: 120, borderRadius: 12 }}
                  />
                ) : item.mime.startsWith("video/") ? (
                  <Video
                    source={{ uri: item.uri }}
                    style={{ width: 140, height: 120 }}
                    resizeMode={ResizeMode.CONTAIN}
                    useNativeControls
                  />
                ) : (
                  <Text style={{ padding: 12, color: colors.text.primary }}>
                    {item.name}
                  </Text>
                )}
                <Pressable
                  onPress={() => {
                    save(current.current.filter((x) => x.id !== item.id));
                    void Files.deleteAsync(item.uri, { idempotent: true });
                  }}
                  disabled={busy}
                  style={{ minHeight: 48, padding: 10 }}
                >
                  <Text style={{ color: colors.misc.error }}>{uiText("Убрать")}</Text>
                </Pressable>
              </View>
            ))}
          </ScrollView>
          {!!items.length && (
            <>
              <TextInput
                value={caption}
                onChangeText={(value) => {
                  setCaption(value);
                  save(current.current, value);
                }}
                placeholder={uiText("Подпись")}
                placeholderTextColor={colors.text.secondary}
                editable={!busy}
                style={{ padding: 16, color: colors.text.primary }}
              />
              {busy ? (
                <>
                  <Text style={{ padding: 12, color: colors.text.secondary }}>
                    {uiText("Загрузка:")}{Math.round(progress * 100)}%
                  </Text>
                  <Pressable
                    onPress={() => {
                      stopped.current = true;
                      xhr.current?.abort();
                    }}
                    style={{ padding: 16, minHeight: 48 }}
                  >
                    <Text style={{ color: colors.misc.error }}>{uiText("Остановить")}</Text>
                  </Pressable>
                </>
              ) : (
                button(uiText("Отправить вложения"), () => void send())
              )}
            </>
          )}
        </>
      )}
    </View>
  );
}
