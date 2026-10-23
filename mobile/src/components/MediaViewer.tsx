import {t as uiText} from "../../../web/i18n.js";
import { useEffect, useRef, useState } from "react";
import { ActivityIndicator, Animated, Modal, PanResponder, Pressable, Text, View, useWindowDimensions } from "react-native";
import { ResizeMode, Video } from "./Video";
import * as MediaLibrary from 'expo-media-library';
import * as Files from 'expo-file-system/legacy';
import { VoicePlayer } from "./VoicePlayer";
import { useTheme } from "../ThemeProvider";
import { useMediaHeaders } from "../MediaSession";
import { resolveMediaSource } from "../decryptedMedia";
import { openMediaFile, openLocalMediaFile } from "../mediaFiles";
import { runMediaTask } from "../mediaLifecycle";
import { pauseAllMedia } from "../playbackOwnership";
import { userError } from "../errors";
import type { Message } from "../types";
export function MediaViewer({ message, messages, baseUrl, userId, decode, onChange, onClose }: {
  message: Message | null; messages: Message[]; baseUrl: string; userId: string; decode: (message: Message) => Promise<Message>; onChange: (message: Message) => void; onClose: () => void;
}) {
  const { colors } = useTheme(), headers = useMediaHeaders(), { width, height } = useWindowDimensions();
  const [source, setSource] = useState<{ uri: string; mime: string; name: string } | null>(null), [error, setError] = useState(''),[notice,setNotice]=useState(''),[saving,setSaving]=useState(false);
  const scale = useRef(new Animated.Value(1)).current, pan = useRef(new Animated.ValueXY()).current, zoom = useRef(1), baseline = useRef(1), distance = useRef(0);
  const [downloadProgress,setDownloadProgress]=useState(0);
  const generation=useRef(0),mounted=useRef(true);
  const close=()=>{generation.current++;void pauseAllMedia();onClose();};
  const items = messages.filter(m => m.media_id), index = items.findIndex(m => m.id === message?.id);
  const latest = useRef({ items, index, onChange }); latest.current = { items, index, onChange };
  const setZoom = (value: number) => { zoom.current = Math.max(1, Math.min(4, value)); scale.setValue(zoom.current); if (zoom.current === 1) pan.setValue({ x: 0, y: 0 }); };
  useEffect(() => {
    const owner=++generation.current;mounted.current=true;void pauseAllMedia();
    let current = true; setSaving(false);setSource(null); setError('');setNotice('');setDownloadProgress(0);setZoom(1);
    if (message) void resolveMediaSource(message, baseUrl, userId, headers, decode,value=>{if(current)setDownloadProgress(value);}).then(result => { if (current) setSource(result); }).catch(e => { if (current) setError(userError(e)); });
    return () => { current = false;mounted.current=false;if(generation.current===owner)generation.current++;void pauseAllMedia(); };
  }, [message?.id, userId, baseUrl]);
  const responder = useRef(PanResponder.create({
    onMoveShouldSetPanResponder: (event, g) => event.nativeEvent.touches.length === 2 || Math.abs(g.dx) > 35 || (zoom.current > 1 && Math.abs(g.dy) > 10),
    onPanResponderGrant: event => { baseline.current = zoom.current; const t = event.nativeEvent.touches; distance.current = t.length === 2 ? Math.hypot(t[0].pageX - t[1].pageX, t[0].pageY - t[1].pageY) : 0; },
    onPanResponderMove: (event, g) => { const t = event.nativeEvent.touches; if (t.length === 2 && distance.current) setZoom(baseline.current * Math.hypot(t[0].pageX - t[1].pageX, t[0].pageY - t[1].pageY) / distance.current); else if (zoom.current > 1) pan.setValue({ x: Math.max(-width, Math.min(width, g.dx)), y: Math.max(-height, Math.min(height, g.dy)) }); },
    onPanResponderRelease: (_, g) => { if (zoom.current === 1 && Math.abs(g.dx) > 70) { const p = latest.current, next = p.items[p.index + (g.dx < 0 ? 1 : -1)]; if (next) p.onChange(next); } },
  })).current;
  const action = (label: string, fn: () => void, disabled = false) => <Pressable accessibilityRole="button" accessibilityLabel={label} onPress={fn} disabled={disabled} style={{ padding: 12, minHeight: 48, opacity: disabled ? 0.3 : 1 }}><Text style={{ color: colors.text.blue }}>{label}</Text></Pressable>;
  const save=async()=>{
    if(!source||saving)return;
    const owner=generation.current,selected=source;const valid=()=>mounted.current && generation.current===owner;
    setSaving(true);setError('');setNotice('');let temporary='';
    try {
      await runMediaTask(userId,async task=>{
        const check=()=>{task.check();if(!valid())throw Error('Просмотр вложения закрыт');};
        const permission=await MediaLibrary.requestPermissionsAsync(true);check();
        if(!permission.granted)throw Error("Разрешите сохранение фото и видео в настройках телефона.");
        let uri=selected.uri;
        if(!uri.startsWith('file:')){
          const extension=selected.mime.startsWith('image/')?'jpg':'mp4';temporary=`${Files.cacheDirectory}gallery-${Date.now()}-${owner}.${extension}`;
          const download=Files.createDownloadResumable(uri,temporary,{headers}),remove=task.onCancel(()=>download.pauseAsync().catch(()=>{}));
          try{const result=await task.wait(download.downloadAsync());check();if(result?.status!==200)throw Error("Не удалось загрузить вложение");uri=result.uri;}finally{remove();}
        }
        check();await MediaLibrary.saveToLibraryAsync(uri);check();setNotice(uiText("Сохранено в галерею"));
      });
    }catch(value){if(valid())setError(userError(value));}finally{if(temporary)await Files.deleteAsync(temporary,{idempotent:true}).catch(()=>{});if(valid())setSaving(false);}
  };
  const fileAction=async(share=false)=>{
    if(!source)return;const owner=generation.current,isCurrent=()=>mounted.current && generation.current===owner;
    try{if(source.uri.startsWith('file:'))await openLocalMediaFile(source.uri,source.mime,userId,share,isCurrent);else await openMediaFile(source.uri,headers,source.mime,userId,share,isCurrent);}
    catch(value){if(mounted.current && generation.current===owner)setError(userError(value));}
  };
  return <Modal visible={!!message} animationType="fade" onRequestClose={close}><View style={{ flex: 1, paddingTop: 32, paddingBottom: 24, backgroundColor: colors.windowBackground }}>
    <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between" }}>{action(uiText("Закрыть"), close)}<Text style={{ color: colors.text.secondary }}>{index >= 0 ? `${index + 1} / ${items.length}` : uiText("Вложение")}</Text>{action(uiText("Поделиться"), () => void fileAction(true), !source)}</View>
    {source && /^(image|video)\//.test(source.mime) && action(saving?uiText("Сохраняем…"):uiText("Сохранить в галерею"),()=>void save(),saving)}
    {!!notice&&<Text accessibilityLiveRegion="polite" style={{padding:12,color:colors.text.blue}}>{notice}</Text>}
    <View style={{ flex: 1, overflow: "hidden", justifyContent: "center", alignItems: "center" }}>
      {!source && !error && <><ActivityIndicator color={colors.text.blue} /><Text style={{padding:12,color:colors.text.secondary}}>{downloadProgress?uiText("Загрузка {0}%",Math.round(downloadProgress*100)):uiText("Загрузка вложения…")}</Text></>}{!!error && <Text style={{ padding: 20, color: colors.misc.error }}>{error}</Text>}
      {source?.mime.startsWith("image/") && <Animated.View {...responder.panHandlers} style={{ width, height: height - 180, transform: [{ translateX: pan.x }, { translateY: pan.y }, { scale }] }}><Animated.Image source={{ uri: source.uri, headers }} accessibilityLabel={message?.text || uiText("Фотография")} resizeMode="contain" style={{ width: "100%", height: "100%" }} /></Animated.View>}
      {(source?.mime.startsWith("video/") || (source && message?.type === "video_note")) && <Video source={{ uri: source.uri, headers }} style={{ width, height: height - 190 }} useNativeControls resizeMode={ResizeMode.CONTAIN} />}
      {source && (message?.type === "voice" || source.mime.startsWith("audio/")) && <View style={{ width: width - 40 }}><VoicePlayer uri={source.uri} /></View>}
      {source && !/^(image|video|audio)\//.test(source.mime) && action(source.name || uiText("Открыть файл"), () => void fileAction())}
    </View>
    {!!message?.text && <Text numberOfLines={3} style={{ paddingHorizontal: 20, color: colors.text.primary }}>{message.text}</Text>}
    <View style={{ flexDirection: "row", justifyContent: "center", alignItems: "center" }}>{action(uiText("Назад"), () => onChange(items[index - 1]), index <= 0)}{source?.mime.startsWith("image/") && <>{action("−", () => setZoom(zoom.current - 0.5))}{action(uiText("Сброс"), () => setZoom(1))}{action("+", () => setZoom(zoom.current + 0.5))}</>}{action(uiText("Далее"), () => onChange(items[index + 1]), index < 0 || index >= items.length - 1)}</View>
  </View></Modal>;
}
