const fs = require("fs");

const file = process.argv[2];
if (!file) {
  console.error("Usage: node scripts/repair_js_strings.js <file>");
  process.exit(1);
}

const cp1251Map = new Map([
  ["Ђ", 0x80], ["Ѓ", 0x81], ["‚", 0x82], ["ѓ", 0x83], ["„", 0x84], ["…", 0x85], ["†", 0x86], ["‡", 0x87],
  ["€", 0x88], ["‰", 0x89], ["Љ", 0x8A], ["‹", 0x8B], ["Њ", 0x8C], ["Ќ", 0x8D], ["Ћ", 0x8E], ["Џ", 0x8F],
  ["ђ", 0x90], ["‘", 0x91], ["’", 0x92], ["“", 0x93], ["”", 0x94], ["•", 0x95], ["–", 0x96], ["—", 0x97],
  ["™", 0x99], ["љ", 0x9A], ["›", 0x9B], ["њ", 0x9C], ["ќ", 0x9D], ["ћ", 0x9E], ["џ", 0x9F],
  [" ", 0xA0], ["Ў", 0xA1], ["ў", 0xA2], ["Ј", 0xA3], ["¤", 0xA4], ["Ґ", 0xA5], ["¦", 0xA6], ["§", 0xA7],
  ["Ё", 0xA8], ["©", 0xA9], ["Є", 0xAA], ["«", 0xAB], ["¬", 0xAC], ["­", 0xAD], ["®", 0xAE], ["Ї", 0xAF],
  ["°", 0xB0], ["±", 0xB1], ["І", 0xB2], ["і", 0xB3], ["ґ", 0xB4], ["µ", 0xB5], ["¶", 0xB6], ["·", 0xB7],
  ["ё", 0xB8], ["№", 0xB9], ["є", 0xBA], ["»", 0xBB], ["ј", 0xBC], ["Ѕ", 0xBD], ["ѕ", 0xBE], ["ї", 0xBF]
]);

function toCp1251Buffer(text) {
  const bytes = [];
  for (const ch of text) {
    const code = ch.codePointAt(0);
    if (code <= 0x7F) {
      bytes.push(code);
      continue;
    }
    if (code >= 0x0410 && code <= 0x044F) {
      bytes.push(code <= 0x042F ? code - 0x350 : code - 0x360);
      continue;
    }
    if (cp1251Map.has(ch)) {
      bytes.push(cp1251Map.get(ch));
      continue;
    }
    return null;
  }
  return Buffer.from(bytes);
}

function fixMojibakeCp1251(text) {
  const buf = toCp1251Buffer(text);
  if (!buf) return text;
  return buf.toString("utf8");
}

function fixShiftedCp1251(text) {
  let out = "";
  for (const ch of text) {
    const code = ch.charCodeAt(0);
    if (code >= 0x10 && code <= 0x4F) out += String.fromCharCode(code + 0xB0);
    else out += ch;
  }
  return out;
}

function badScore(text) {
  let score = 0;
  for (let i = 0; i < text.length; i++) {
    const code = text.charCodeAt(i);
    if ((code >= 0 && code <= 8) || (code >= 11 && code <= 12) || (code >= 14 && code <= 31)) score += 5;
    if (code >= 0x10 && code <= 0x4F) score += 1;
    if (code === 0xFFFD) score += 6;
    if (i < text.length - 1) {
      const next = text.charCodeAt(i + 1);
      if ((code === 0x0420 || code === 0x0421 || code === 0x0440) && next >= 0x0400 && next <= 0x04FF) score += 3;
    }
  }
  score += (text.match(/\?{4,}/g) || []).length * 4;
  return score;
}

function goodScore(text) {
  let score = 0;
  for (const ch of text) {
    const code = ch.charCodeAt(0);
    if (code >= 0x0400 && code <= 0x04FF) score += 2;
    if (code > 0xFFFF) score += 2;
  }
  return score;
}

function chooseBest(text) {
  const mojibakeMarkers = [...text].filter((ch) => {
    const code = ch.charCodeAt(0);
    return code === 0x0420 || code === 0x0421 || code === 0x0440 || cp1251Map.has(ch);
  }).length;
  const shiftedChars = [...text].filter((ch) => {
    const code = ch.charCodeAt(0);
    return (code >= 0x10 && code <= 0x1F) || /[0-9:;<=>?@A-Z]/.test(ch);
  }).length;
  const hasLowerLatinWord = /[a-z]{3,}/.test(text);

  const candidates = [text];
  if (mojibakeMarkers >= 2) {
    candidates.push(fixMojibakeCp1251(text));
  }
  if (shiftedChars >= 4 && !hasLowerLatinWord) {
    candidates.push(fixShiftedCp1251(text));
  }
  if (mojibakeMarkers >= 2 && shiftedChars >= 4 && !hasLowerLatinWord) {
    candidates.push(fixMojibakeCp1251(fixShiftedCp1251(text)));
    candidates.push(fixShiftedCp1251(fixMojibakeCp1251(text)));
  }

  let best = text;
  let bestRank = goodScore(text) - badScore(text) * 3;
  for (const candidate of [...new Set(candidates)]) {
    const rank = goodScore(candidate) - badScore(candidate) * 3;
    if (rank > bestRank) {
      best = candidate;
      bestRank = rank;
    }
  }
  return best;
}

function repairText(text) {
  return text
    .replace(/[\u0400-\u04FF\u2018-\u201E\u2020-\u2022\u2030\u2039\u2122]+/g, (token) => chooseBest(token))
    .replace(/[\x10-\x1F0-9:;<=>?@A-Z]{4,}/g, (token) => chooseBest(token));
}

function escapeForQuote(text, quote) {
  return text
    .replace(/\\/g, "\\\\")
    .replace(/\r/g, "\\r")
    .replace(/\n/g, "\\n")
    .replace(new RegExp(`\\${quote}`, "g"), `\\${quote}`);
}

function escapeForTemplate(text) {
  return text
    .replace(/\\/g, "\\\\")
    .replace(/`/g, "\\`")
    .replace(/\$\{/g, "\\${");
}

function parseString(src, i, quote) {
  let raw = "";
  i++;
  while (i < src.length) {
    const ch = src[i];
    if (ch === "\\") {
      raw += ch;
      if (i + 1 < src.length) raw += src[i + 1];
      i += 2;
      continue;
    }
    if (ch === quote) {
      const fixed = repairText(raw);
      return { out: quote + (fixed === raw ? raw : escapeForQuote(fixed, quote)) + quote, i: i + 1 };
    }
    raw += ch;
    i++;
  }
  return { out: quote + raw, i };
}

function parseTemplate(src, i) {
  let out = "`";
  let chunk = "";
  i++;
  while (i < src.length) {
    const ch = src[i];
    if (ch === "`") {
      const fixed = repairText(chunk);
      out += fixed === chunk ? chunk : escapeForTemplate(fixed);
      out += "`";
      return { out, i: i + 1 };
    }
    if (ch === "\\" && i + 1 < src.length) {
      chunk += ch + src[i + 1];
      i += 2;
      continue;
    }
    if (ch === "$" && src[i + 1] === "{") {
      const fixed = repairText(chunk);
      out += fixed === chunk ? chunk : escapeForTemplate(fixed);
      chunk = "";
      out += "${";
      const expr = parseCode(src, i + 2, "}");
      out += expr.out + "}";
      i = expr.i + 1;
      continue;
    }
    chunk += ch;
    i++;
  }
  return { out, i };
}

function parseCode(src, i = 0, terminator = null) {
  let out = "";
  while (i < src.length) {
    const ch = src[i];
    if (terminator && ch === terminator) {
      return { out, i };
    }
    if (ch === "'" || ch === "\"") {
      const res = parseString(src, i, ch);
      out += res.out;
      i = res.i;
      continue;
    }
    if (ch === "`") {
      const res = parseTemplate(src, i);
      out += res.out;
      i = res.i;
      continue;
    }
    if (ch === "/" && src[i + 1] === "/") {
      const nextNl = src.indexOf("\n", i);
      if (nextNl === -1) {
        out += src.slice(i);
        return { out, i: src.length };
      }
      out += src.slice(i, nextNl);
      i = nextNl;
      continue;
    }
    if (ch === "/" && src[i + 1] === "*") {
      const end = src.indexOf("*/", i + 2);
      if (end === -1) {
        out += src.slice(i);
        return { out, i: src.length };
      }
      out += src.slice(i, end + 2);
      i = end + 2;
      continue;
    }
    out += ch;
    i++;
  }
  return { out, i };
}

const original = fs.readFileSync(file, "utf8");
const repaired = parseCode(original).out;
fs.writeFileSync(file, repaired, "utf8");
