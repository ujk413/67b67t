/**
 * Telegram Android — ночная тема.
 *
 * Значения не подобраны на глаз: они сняты из официального файла темы
 * /root/PrimeGram/TMessagesProj/src/main/assets/night.attheme и из дефолтов
 * ThemeColors.java. Это те же самые числа, которыми рисуется настоящая телега.
 */

// ─────────────────────────── цвета ───────────────────────────
// Формат #AARRGGBB — как в .attheme, чтобы можно было сверять один в один.

export const telegramColors = {
  // окна и шапки
  windowBackground: "#FF181819", // фон списка чатов
  windowBackgroundGray: "#FF000000",
  actionBar: "#FF232326", // шапка
  actionBarIcon: "#FFFFFFFF",
  actionBarTitle: "#FFFFFFFF",
  actionBarSubtitle: "#73F2F2F2",
  graySection: "#FF0B0B0C", // заголовки секций
  divider: "#FF000000",

  text: {
    primary: "#FFFFFFFF", // windowBackgroundWhiteBlackText
    secondary: "#6EFFFFFF", // windowBackgroundWhiteGrayText
    gray2: "#6EFDFDFF",
    gray3: "#FF868686",
    gray4: "#FF6A6A6A",
    gray5: "#FF7D8B99", // серо-голубой, тот самый
    blue: "#FF64B5EF", // акцент, ссылки
  },

  // список чатов
  chats: {
    name: "#FFF1F1F1",
    message: "#FF828282", // превью последнего сообщения
    date: "#FF787878",
    nameMessage: "#FFEBEBEB", // имя отправителя внутри превью
    unreadCounter: "#FF399EF3", // синий бейдж
    unreadCounterMuted: "#FF454545", // приглушённый (без звука)
    unreadCounterText: "#FFFFFFFF",
    pinnedIcon: "#FF646464",
    verified: "#FF46AAF7",
    selectedBackground: "#2A5F9DE3", // подсветка нажатия
  },

  // чат
  chat: {
    wallpaper: "#FF0F0F10", // фон переписки
    inBubble: "#FF1F2123", // входящее
    outBubble: "#FF366CAF", // исходящее
    messageTextIn: "#FFFAFAFA",
    messageTextOut: "#FFFAFAFA",
    linkIn: "#FF79C4FC",
    linkOut: "#FFAEDFFF",
    inTime: "#FF7D7F81",
    outTime: "#FF94D3F6",
    inMenu: "#FF5D6364",
    outMenu: "#FF8EC4F3",
    serviceBackground: "#96373737", // плашка «сегодня» / системное сообщение
    serviceText: "#FFFFFFFF",
    inReplyLine: "#FF79C4FC",
    inReplyName: "#FF79C4FC",
    inReplyMessage: "#FFFFFFFF",
    outReplyLine: "#FF7ECEFF",
    outReplyName: "#FF9FD9FF",
    outReplyMessage: "#FFFFFFFF",
    sentCheck: "#E1C9F0FF", // галочка «отправлено»
    sentCheckRead: "#E1C9F0FF", // галочка «прочитано»
  },

  // панель ввода
  composer: {
    background: "#FF1E1E1F",
    text: "#FFFFFFFF",
    hint: "#64FFFFFF",
    icons: "#64FFFFFF",
    overlayText: "#FF64B5EF",
  },

  // аватары — телега красит по id собеседника, набор фиксированный
  avatar: {
    background: {
      red: "#FFFF845E",
      orange: "#FFFEBB5B",
      violet: "#FFB694F9",
      green: "#FF9AD164",
      cyan: "#FF5BCBE3",
      blue: "#FF5CAFFA",
      pink: "#FFFF8AAC",
    },
    nameInMessage: {
      red: "#FFE88878",
      orange: "#FFE8AB5A",
      violet: "#FF8C8DEA",
      green: "#FF7DD071",
      cyan: "#FF5BCBE3",
      blue: "#FF5CADEA",
      pink: "#FFE8749A",
    },
  },

  misc: {
    switchTrack: "#FF606060",
    switchTrackChecked: "#FF459CE3",
    progress: "#FF64B5FD",
    inLoader: "#FF5098E9",
    outLoader: "#FFFFFFFF",
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
