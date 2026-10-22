/**
 * Светлая тема в стиле современного мессенджера (на базе структуры Telegram Android).
 *
 * Белый фон, изумрудный акцент, серые вторичные тексты. Размеры (telegramMetrics)
 * остались от Android-версии Telegram.
 */

// ─────────────────────────── цвета ───────────────────────────
// ВАЖНО: формат #RRGGBBAA, как ждёт React Native (альфа последней).

export const telegramColors = {
  // окна и шапки
  windowBackground: "#FFFFFF", // фон списка чатов
  windowBackgroundGray: "#F2F4F7", // поля, плейсхолдеры медиа
  actionBar: "#FFFFFF", // шапка
  actionBarIcon: "#111827",
  actionBarTitle: "#0B1220",
  actionBarSubtitle: "#6B7280",
  graySection: "#F3F4F6", // заголовки секций
  divider: "#EEF0F3",

  text: {
    primary: "#0B1220",
    secondary: "#6B7280",
    gray2: "#9CA3AF",
    gray3: "#868E9A",
    gray4: "#A0A7B2",
    gray5: "#6B7280",
    blue: "#0FA37F", // акцент (имя ключа историческое): кнопки, ссылки
    onAccent: "#FFFFFF", // текст и иконки поверх акцентного фона
  },

  // список чатов
  chats: {
    name: "#0B1220",
    message: "#6B7280", // превью последнего сообщения
    date: "#9CA3AF",
    nameMessage: "#374151", // имя отправителя внутри превью
    unreadCounter: "#14B88F", // бейдж непрочитанных
    unreadCounterMuted: "#C4C9D1", // приглушённый (без звука)
    unreadCounterText: "#FFFFFF",
    pinnedIcon: "#9CA3AF",
    verified: "#14B88F",
    selectedBackground: "#14B88F1A", // подсветка нажатия
    searchBackground: "#F2F4F7", // поле поиска
    searchHint: "#8A93A2",
  },

  // чат
  chat: {
    wallpaper: "#F3F5F8", // фон переписки
    inBubble: "#FFFFFF", // входящее
    outBubble: "#DDF5EC", // исходящее
    messageTextIn: "#0B1220",
    messageTextOut: "#0B1220",
    linkIn: "#0FA37F",
    linkOut: "#0B7F61",
    inTime: "#9CA3AF",
    outTime: "#5E8F80",
    inMenu: "#9CA3AF",
    outMenu: "#5E8F80",
    serviceBackground: "#0B122033", // плашка «сегодня» / системное сообщение
    serviceText: "#FFFFFF",
    inReplyLine: "#14B88F",
    inReplyName: "#0FA37F",
    inReplyMessage: "#374151",
    outReplyLine: "#14B88F",
    outReplyName: "#0B7F61",
    outReplyMessage: "#374151",
    sentCheck: "#14B88F", // галочка «отправлено»
    sentCheckRead: "#14B88F", // галочка «прочитано»
    replyBackgroundIn: "#0B12200A",
    replyBackgroundOut: "#0B12200F",
    reactionChip: "#0B12200F",
    fileCard: "#0B12200A",
  },

  // панель ввода
  composer: {
    background: "#FFFFFF",
    inputBackground: "#F2F4F7",
    text: "#0B1220",
    hint: "#8A93A2",
    icons: "#6B7280",
    overlayText: "#0FA37F",
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
    switchTrack: "#C4C9D1",
    switchTrackChecked: "#14B88F",
    progress: "#14B88F",
    inLoader: "#14B88F",
    outLoader: "#0FA37F",
    error: "#E5484D",
    mediaOverlay: "#000000D9", // фон просмотра медиа и звонков
    mediaOverlayText: "#FFFFFF",
    mediaOverlayControl: "#FFFFFF1A",
    mediaOverlayControlBorder: "#FFFFFF14",
    backdrop: "#00000080", // затемнение под модальными окнами
    callAccept: "#4CD96433",
    callReject: "#FF4D6D33",
    callEnd: "#FF4D6DCC",
    callBackground: "#000000", // экран звонка
    callLocalVideo: "#1A1A1A",
    callControl: "#FFFFFF33",
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
    radius: 22,
    tailWidth: 8,
    paddingH: 11,
    paddingV: 6,
    maxWidthRatio: 0.84, // телега не даёт пузырю больше ~78% ширины
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
    buttonSize: 48,
    paddingH: 8,
    radius: 23,
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
