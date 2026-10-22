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
});
export const useTheme = () => useContext(Theme);
export function ThemeProvider({ children }: { children: ReactNode }) {
  const system = useColorScheme(),
    [mode, setModeState] = useState<ThemeMode>("light"),
    [fontSize, setFontState] = useState(16),
    [autoDownload, setAutoState] = useState(false);
  useEffect(() => {
    void storage.get("ui.theme").then((value) => {
      if (["light", "dark", "system"].includes(value || ""))
        setModeState(value as ThemeMode);
    });
    void storage.get("ui.font").then((value) => {
      if (value) setFontState(Math.max(14, Math.min(24, Number(value) || 16)));
    });
  }, []);
  useEffect(() => {
    void storage
      .get("media.autoDownload")
      .then((value) => setAutoState(value === "true"));
  }, []);
  const isDark = mode === "dark" || (mode === "system" && system === "dark");
  const value = useMemo(
    () => ({
      colors: isDark ? dark : telegramColors,
      dark: isDark,
      mode,
      fontSize,
      autoDownload,
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
    [mode, fontSize, isDark, autoDownload],
  );
  return <Theme.Provider value={value}>{children}</Theme.Provider>;
}
