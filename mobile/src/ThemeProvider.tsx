import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { useColorScheme } from "react-native";
import { telegramColors } from "./theme";
import { telegramColors as night } from "./themes/telegramNight";
import { storage } from "./storage";
import {setLanguage as setUiLanguage} from '../../web/i18n.js';
import {setNativeUiLanguage} from './nativeUiLanguage';
type Widen<T> = {
  [K in keyof T]: T[K] extends string
    ? string
    : T[K] extends number
      ? number
      : T[K] extends object
        ? Widen<T[K]>
        : T[K];
};
export type Colors = Widen<typeof telegramColors>;
export type ThemeMode = "light" | "dark" | "system";
export type WallpaperMode = 'plain'|'dots'|'mesh';
export type AccentMode = 'emerald'|'blue'|'violet';
const accentColor:Record<AccentMode,string>={emerald:'#0FA37F',blue:'#3B82F6',violet:'#8B5CF6'};
const dark: Colors = {
  ...telegramColors,
  ...night,
  text: { ...telegramColors.text, ...night.text },
  chats: {
    ...telegramColors.chats,
    ...night.chats,
    searchBackground: "#232326",
    searchHint: "#9CA3AF",
  },
  chat: {
    ...telegramColors.chat,
    ...night.chat,
    replyBackgroundIn: "#FFFFFF0A",
    replyBackgroundOut: "#00000019",
    reactionChip: "#FFFFFF19",
    fileCard: "#FFFFFF0D",
  },
  composer: {
    ...telegramColors.composer,
    ...night.composer,
    inputBackground: "#181819",
  },
  misc: { ...telegramColors.misc, ...night.misc },
};
const Theme = createContext({
  colors: telegramColors as Colors,
  dark: false,
  mode: "light" as ThemeMode,
  setMode: (_mode: ThemeMode) => {},
  autoDownload: false,
  setAutoDownload: (_value: boolean) => {},
  fontSize: 16,
  setFontSize: (_size: number) => {},
  wallpaper: 'plain' as WallpaperMode,
  setWallpaper: (_value:WallpaperMode) => {},
  accent: 'emerald' as AccentMode,
  setAccent: (_value:AccentMode) => {},
  language: 'ru' as 'ru'|'en',
  setLanguage: (_value:'ru'|'en') => {},
});
export const useTheme = () => useContext(Theme);
export function ThemeProvider({ children }: { children: ReactNode }) {
  const system = useColorScheme(),
    [mode, setModeState] = useState<ThemeMode>("light"),
    [fontSize, setFontState] = useState(16),
    [autoDownload, setAutoState] = useState(false),
    [wallpaper,setWallpaperState]=useState<WallpaperMode>('plain'),
    [accent,setAccentState]=useState<AccentMode>('emerald'),
    [language,setLanguageState]=useState<'ru'|'en'>('ru');
  useEffect(() => {
    void storage.get("ui.theme").then((value) => {
      if (["light", "dark", "system"].includes(value || ""))
        setModeState(value as ThemeMode);
    });
    void storage.get("ui.font").then((value) => {
      if (value) setFontState(Math.max(14, Math.min(24, Number(value) || 16)));
    });
    void storage.get('ui.wallpaper').then(value=>{if(['plain','dots','mesh'].includes(value||''))setWallpaperState(value as WallpaperMode);});
    void storage.get('ui.accent').then(value=>{if(['emerald','blue','violet'].includes(value||''))setAccentState(value as AccentMode);});
    void storage.get('ui.language').then(value=>{if(value==='ru'||value==='en'){setUiLanguage(value);setLanguageState(value);}});
  }, []);
  useEffect(() => {
    void storage
      .get("media.autoDownload")
      .then((value) => setAutoState(value === "true"));
  }, []);
  const isDark = mode === "dark" || (mode === "system" && system === "dark");
  useEffect(()=>{setNativeUiLanguage(language);},[language]);
  const value = useMemo(
    () => ({
      colors: {...(isDark?dark:telegramColors),text:{...(isDark?dark:telegramColors).text,blue:accentColor[accent]},chats:{...(isDark?dark:telegramColors).chats,unreadCounter:accentColor[accent]}},
      dark: isDark,
      mode,
      fontSize,
      autoDownload,
      wallpaper,
      accent,
      language,
      setWallpaper(next:WallpaperMode){setWallpaperState(next);void storage.set('ui.wallpaper',next);},
      setAccent(next:AccentMode){setAccentState(next);void storage.set('ui.accent',next);},
      setLanguage(next:'ru'|'en'){setUiLanguage(next);setLanguageState(next);void storage.set('ui.language',next);},
      setAutoDownload(value: boolean) {
        setAutoState(value);
        void storage.set("media.autoDownload", String(value));
      },
      setMode(next: ThemeMode) {
        setModeState(next);
        void storage.set("ui.theme", next);
      },
      setFontSize(next: number) {
        setFontState(next);
        void storage.set("ui.font", String(next));
      },
    }),
    [mode, fontSize, isDark, autoDownload, wallpaper, accent, language],
  );
  return <Theme.Provider value={value}>{children}</Theme.Provider>;
}
