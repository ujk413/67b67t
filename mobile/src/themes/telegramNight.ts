/**
 * Telegram Android — ночная тема.
 *
 * Значения не подобраны на глаз: они сняты из официального файла темы
 * /root/PrimeGram/TMessagesProj/src/main/assets/night.attheme и из дефолтов
 * ThemeColors.java. Это те же самые числа, которыми рисуется настоящая телега.
 */

// ─────────────────────────── цвета ───────────────────────────
// ВАЖНО: формат #RRGGBBAA — как ждёт React Native.
// Telegram в своих .attheme хранит #AARRGGBB (альфа первой), поэтому при
// копировании значений из PrimeGram байты надо переставлять, иначе RN
// прочитает #FF181819 как «красный FF, зелёный 18, синий 18, альфа 19» —
// то есть полупрозрачный красный, и весь интерфейс станет розовым.

export const telegramColors = {
  // окна и шапки
  windowBackground: "#181819", // фон списка чатов
  windowBackgroundGray: "#000000",
  actionBar: "#232326", // шапка
  actionBarIcon: "#FFFFFF",
  actionBarTitle: "#FFFFFF",
  actionBarSubtitle: "#F2F2F273",
  graySection: "#0B0B0C", // заголовки секций
  divider: "#000000",

  text: {
    primary: "#FFFFFF", // windowBackgroundWhiteBlackText
    secondary: "#FFFFFF6E", // windowBackgroundWhiteGrayText
    gray2: "#FDFDFF6E",
    gray3: "#868686",
    gray4: "#6A6A6A",
    gray5: "#7D8B99", // серо-голубой, тот самый
    blue: "#64B5EF", // акцент, ссылки
  },

  // список чатов
  chats: {
    name: "#F1F1F1",
    message: "#828282", // превью последнего сообщения
    date: "#787878",
    nameMessage: "#EBEBEB", // имя отправителя внутри превью
    unreadCounter: "#399EF3", // синий бейдж
    unreadCounterMuted: "#454545", // приглушённый (без звука)
    unreadCounterText: "#FFFFFF",
    pinnedIcon: "#646464",
    verified: "#46AAF7",
    selectedBackground: "#5F9DE32A", // подсветка нажатия
  },

  // чат
  chat: {
    wallpaper: "#0F0F10", // фон переписки
    inBubble: "#1F2123", // входящее
    outBubble: "#366CAF", // исходящее
    messageTextIn: "#FAFAFA",
    messageTextOut: "#FAFAFA",
    linkIn: "#79C4FC",
    linkOut: "#AEDFFF",
    inTime: "#7D7F81",
    outTime: "#94D3F6",
    inMenu: "#5D6364",
    outMenu: "#8EC4F3",
    serviceBackground: "#37373796", // плашка «сегодня» / системное сообщение
    serviceText: "#FFFFFF",
    inReplyLine: "#79C4FC",
    inReplyName: "#79C4FC",
    inReplyMessage: "#FFFFFF",
    outReplyLine: "#7ECEFF",
    outReplyName: "#9FD9FF",
    outReplyMessage: "#FFFFFF",
    sentCheck: "#C9F0FFE1", // галочка «отправлено»
    sentCheckRead: "#C9F0FFE1", // галочка «прочитано»
  },

  // панель ввода
  composer: {
    background: "#1E1E1F",
    text: "#FFFFFF",
    hint: "#FFFFFF64",
    icons: "#FFFFFF64",
    overlayText: "#64B5EF",
  },

  // аватары — телега красит по id собеседника, набор фиксированный
  avatar: {
    background: {
      red: "#FF845E",
      orange: "#FEBB5B",
      violet: "#B694F9",
      green: "#9AD164",
      cyan: "#5BCBE3",
      blue: "#5CAFFA",
      pink: "#FF8AAC",
    },
    nameInMessage: {
      red: "#E88878",
      orange: "#E8AB5A",
      violet: "#8C8DEA",
      green: "#7DD071",
      cyan: "#5BCBE3",
      blue: "#5CADEA",
      pink: "#E8749A",
    },
  },

  misc: {
    switchTrack: "#606060",
    switchTrackChecked: "#459CE3",
    progress: "#64B5FD",
    inLoader: "#5098E9",
    outLoader: "#FFFFFF",
  },
} as const;

// ────────────────────────── размеры ──────────────────────────
// dp, как в DialogCell.java и ChatMessageCell.java.

export const telegramMetrics = {
  // список чатов
  row: {
    height: 72,
    avatarSize: 56, // avatarRadius dp(28) * 2
    paddingH: 12,
    paddingV: 8,
    titleTop: 13, // отступ времени сверху, dp(13)
    timeRight: 15, // dp(15)
    textLeft: 16, // dp(16) от края аватара
  },

  // типографика списка
  font: {
    chatName: 17, // dp(17)
    chatMessage: 16, // dp(16)
    chatDate: 12,
    messageText: 16,
    messageTime: 12,
    messageName: 14, // имя отправителя в группе
    headerTitle: 18,
    headerSubtitle: 13,
  },

  // пузыри
  bubble: {
    radius: 12,
    tailWidth: 8,
    paddingH: 9,
    paddingV: 7,
    maxWidthRatio: 0.78, // телега не даёт пузырю больше ~78% ширины
    avatarSize: 32,
    avatarGap: 8,
    betweenMessages: 2, // внутри одной серии
    betweenGroups: 8, // между разными отправителями
  },

  // шапка
  header: { height: 56 },

  // панель ввода
  composer: {
    minHeight: 48,
    buttonSize: 42,
    paddingH: 8,
    radius: 20,
  },

  avatarPaletteSize: 7,
} as const;

/** Телега берёт цвет аватара из фиксированного набора по id собеседника. */
export function avatarColorFor(id: string): string {
  const palette = Object.values(telegramColors.avatar.background);
  let hash = 0;
  for (let i = 0; i < id.length; i++) {
    hash = (hash * 31 + id.charCodeAt(i)) >>> 0;
  }
  return palette[hash % palette.length];
}

/** Цвет имени отправителя в групповом чате. */
export function nameColorFor(id: string): string {
  const palette = Object.values(telegramColors.avatar.nameInMessage);
  let hash = 0;
  for (let i = 0; i < id.length; i++) {
    hash = (hash * 31 + id.charCodeAt(i)) >>> 0;
  }
  return palette[hash % palette.length];
}
