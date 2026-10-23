import {t as uiText} from "../../../web/i18n.js";
import { useEffect, useState } from "react";
import { Alert, FlatList, Pressable, Text, TextInput, View } from "react-native";
import { useTheme } from "../ThemeProvider";
import { mobileApi } from "../mobileApi";
import { roomTitle } from "../format";
import { userError } from "../errors";
import type { Room } from "../types";
type Folder = { id?: string; title: string; room_ids: string[];revision?:number;unread_count?:number };
export function FolderManager({ baseUrl, token, rooms, onChanged, onClose }: { baseUrl: string; token: string; rooms: Room[]; onChanged: () => Promise<void>; onClose: () => void }) {
  const { colors } = useTheme(), api = mobileApi(baseUrl, token);
  const [folders, setFolders] = useState<Folder[]>([]), [editing, setEditing] = useState<Folder | null>(null), [busy, setBusy] = useState(false), [error, setError] = useState("");
  useEffect(() => { void api.folders().then(setFolders).catch(e => setError(userError(e))); }, []);
  const action = async (fn: () => Promise<unknown>) => { if (busy) return; setBusy(true); setError(""); try { await fn(); setFolders(await api.folders()); await onChanged(); } catch (e) { setError(userError(e)); } finally { setBusy(false); } };
  const button = (label: string, fn: () => void) => <Pressable disabled={busy} accessibilityRole="button" onPress={fn} style={{ minHeight: 48, padding: 14 }}><Text style={{ color: colors.text.blue }}>{label}</Text></Pressable>;
  return <View style={{ flex: 1, backgroundColor: colors.windowBackground }}>
    <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>{button(uiText("Назад"), () => editing ? setEditing(null) : onClose())}<Text style={{ color: colors.text.primary, fontSize: 20 }}>{editing ? uiText("Папка") : uiText("Папки чатов")}</Text>{editing && button(uiText("Сохранить"), () => void action(async () => { await api.saveFolder(editing.title, editing.room_ids, editing.id); setEditing(null); }))}</View>
    {!!error && <Text style={{ padding: 14, color: colors.misc.error }}>{error}</Text>}
    {editing ? <><TextInput value={editing.title} placeholder={uiText("Название папки")} placeholderTextColor={colors.text.secondary} onChangeText={title => setEditing({ ...editing, title })} maxLength={40} style={{ padding: 16, margin: 12, borderRadius: 16, color: colors.text.primary, backgroundColor: colors.windowBackgroundGray }} accessibilityLabel={uiText("Название папки")} /><FlatList data={rooms.filter(r => !r.preferences?.hidden)} keyExtractor={r => r.id} renderItem={({ item }) => { const checked = editing.room_ids.includes(item.id); return <Pressable accessibilityRole="checkbox" accessibilityState={{ checked }} onPress={() => setEditing({ ...editing, room_ids: checked ? editing.room_ids.filter(id => id !== item.id) : [...editing.room_ids, item.id] })} style={{ padding: 18, minHeight: 56, flexDirection: "row", gap: 12 }}><Text style={{ color: colors.text.blue }}>{checked ? "✓" : "○"}</Text><Text style={{ color: colors.text.primary }}>{roomTitle(item)}</Text></Pressable>; }} /></> : <><FlatList data={folders} keyExtractor={f => f.id!} ListEmptyComponent={<Text style={{ padding: 20, color: colors.text.secondary }}>{uiText("Объедините чаты в папки: работа, друзья, семья.")}</Text>} renderItem={({ item }) => <View style={{ flexDirection: "row", alignItems: "center" }}><Pressable style={{ flex: 1, padding: 18, minHeight: 56 }} onPress={() => setEditing(item)}><Text style={{ color: colors.text.primary }}>{item.title}</Text><Text style={{ color: colors.text.secondary }}>{item.room_ids.length} {uiText("чатов")}</Text></Pressable>{button(uiText("Удалить"), () => Alert.alert(uiText("Удалить папку?"), uiText("Сами переписки сохранятся"), [{ text: uiText("Отмена") }, { text: uiText("Удалить"), onPress: () => void action(() => api.removeFolder(item.id!)) }]))}</View>} />{button(uiText("Создать папку"), () => setEditing({ title: "", room_ids: [] }))}</>}
  </View>;
}
