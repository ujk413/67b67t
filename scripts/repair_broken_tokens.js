const fs = require("fs");

const file = process.argv[2];
if (!file) {
  console.error("Usage: node scripts/repair_broken_tokens.js <file>");
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
const cp1251ReverseMap = new Map([...cp1251Map.entries()].map(([char, byte]) => [byte, char]));

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

function fromCp1251Buffer(buf) {
  let out = "";
  for (const byte of buf) {
    if (byte <= 0x7F) out += String.fromCharCode(byte);
    else if (byte >= 0xC0 && byte <= 0xDF) out += String.fromCharCode(byte + 0x350);
    else if (byte >= 0xE0 && byte <= 0xFF) out += String.fromCharCode(byte + 0x360);
    else if (cp1251ReverseMap.has(byte)) out += cp1251ReverseMap.get(byte);
    else return null;
  }
  return out;
}

function fixMojibakeCp1251(text) {
  const buf = toCp1251Buffer(text);
  if (!buf) return text;
  return buf.toString("utf8");
}

function fixShiftedCp1251(text) {
  const bytes = [];
  for (const ch of text) {
    const code = ch.charCodeAt(0);
    if (code >= 0x10 && code <= 0x4F) bytes.push(code + 0xB0);
    else return text;
  }
  return fromCp1251Buffer(Buffer.from(bytes)) || text;
}

function badScore(text) {
  let score = 0;
  for (let i = 0; i < text.length; i++) {
    const code = text.charCodeAt(i);
    if ((code >= 0 && code <= 8) || (code >= 11 && code <= 12) || (code >= 14 && code <= 31)) score += 5;
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
    const code = ch.codePointAt(0);
    if (code >= 0x0400 && code <= 0x04FF) score += 2;
    if (code > 0xFFFF) score += 2;
  }
  return score;
}

function better(original, candidate) {
  return candidate !== original && (
    goodScore(candidate) > goodScore(original) + 1 ||
    badScore(candidate) < badScore(original)
  );
}

let text = fs.readFileSync(file, "utf8");

for (let i = 0; i < 3; i++) {
  text = text.replace(/[\x10-\x4F]{3,}/g, (token) => {
    if (/^[A-Z]{3,}$/.test(token)) return token;
    const fixed = fixShiftedCp1251(token);
    return better(token, fixed) ? fixed : token;
  });

  text = text.replace(/[\u0400-\u04FF\u2018-\u201E\u2020-\u2022\u2030\u2039\u2122]{2,}/g, (token) => {
    const fixed = fixMojibakeCp1251(token);
    return better(token, fixed) ? fixed : token;
  });
}

fs.writeFileSync(file, text, "utf8");
