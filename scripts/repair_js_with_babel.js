const fs = require("fs");
const path = require("path");

const file = process.argv[2];
if (!file) {
  console.error("Usage: node scripts/repair_js_with_babel.js <file>");
  process.exit(1);
}

const babelBase = path.join(__dirname, "..", "mobile", "node_modules", "@babel");
const parser = require(path.join(babelBase, "parser"));
const traverse = require(path.join(babelBase, "traverse")).default;
const generate = require(path.join(babelBase, "generator")).default;

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

function fromCp1251Buffer(buf) {
  let out = "";
  for (const byte of buf) {
    if (byte <= 0x7F) {
      out += String.fromCharCode(byte);
      continue;
    }
    if (byte >= 0xC0 && byte <= 0xDF) {
      out += String.fromCharCode(byte + 0x350);
      continue;
    }
    if (byte >= 0xE0 && byte <= 0xFF) {
      out += String.fromCharCode(byte + 0x360);
      continue;
    }
    if (cp1251ReverseMap.has(byte)) {
      out += cp1251ReverseMap.get(byte);
      continue;
    }
    return null;
  }
  return out;
}

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
  const bytes = [];
  for (const ch of text) {
    const code = ch.charCodeAt(0);
    if (code >= 0x10 && code <= 0x4F) bytes.push(code + 0xB0);
    else if (code <= 0x7F) bytes.push(code);
    else return text;
  }
  return fromCp1251Buffer(Buffer.from(bytes)) || text;
}

function badScore(text) {
  let score = 0;
  for (let i = 0; i < text.length; i++) {
    const code = text.charCodeAt(i);
    if ((code >= 0 && code <= 8) || (code >= 11 && code <= 12) || (code >= 14 && code <= 31)) score += 5;
    if (code >= 0x10 && code <= 0x1F) score += 3;
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
    const code = ch.codePointAt(0);
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

  let best = text;

  const maybeAdopt = (candidate) => {
    if (candidate === best) return;
    const better =
      goodScore(candidate) > goodScore(best) + 1 ||
      badScore(candidate) < badScore(best);
    if (better) best = candidate;
  };

  if (shiftedChars >= 4 && !hasLowerLatinWord) {
    maybeAdopt(fixShiftedCp1251(best));
  }
  if (mojibakeMarkers >= 2) {
    maybeAdopt(fixMojibakeCp1251(best));
  }
  if (shiftedChars >= 4 && mojibakeMarkers >= 2 && !hasLowerLatinWord) {
    maybeAdopt(fixMojibakeCp1251(fixShiftedCp1251(best)));
    maybeAdopt(fixShiftedCp1251(fixMojibakeCp1251(best)));
  }
  return best;
}

function repairText(text) {
  return text
    .replace(/[\u0400-\u04FF\u2018-\u201E\u2020-\u2022\u2030\u2039\u2122]+/g, (token) => chooseBest(token))
    .replace(/[\x10-\x4F]{4,}/g, (token) => chooseBest(token));
}

const original = fs.readFileSync(file, "utf8");
const ast = parser.parse(original, {
  sourceType: "module",
  plugins: [
    "optionalChaining",
    "nullishCoalescingOperator"
  ]
});

traverse(ast, {
  StringLiteral(path) {
    const next = repairText(path.node.value);
    if (next !== path.node.value) {
      path.node.value = next;
      delete path.node.extra;
    }
  },
  TemplateElement(path) {
    const cooked = path.node.value.cooked ?? path.node.value.raw;
    const next = repairText(cooked);
    if (next !== cooked) {
      path.node.value.cooked = next;
      path.node.value.raw = next
        .replace(/\\/g, "\\\\")
        .replace(/`/g, "\\`")
        .replace(/\$\{/g, "\\${");
    }
  }
});

const output = generate(ast, {
  retainLines: true,
  jsescOption: { minimal: true }
}, original);

fs.writeFileSync(file, output.code, "utf8");
