const fs = require("fs");

const file = process.argv[2];
if (!file) {
  console.error("Usage: node scripts/fix_mojibake.js <file>");
  process.exit(1);
}

const raw = fs.readFileSync(file, "utf8");

function scoreMojibake(text) {
  let score = 0;
  score += (text.match(/[РСр][\u0400-\u04FF]/g) || []).length * 2;
  score += (text.match(/�/g) || []).length * 4;
  score += (text.match(/\?{4,}/g) || []).length * 3;
  score += (text.match(/Ñ[\u0400-\u04FF]/g) || []).length * 2;
  return score;
}

function decodeCp1251AsUtf8(text) {
  const bytes = Buffer.from(text, "binary");
  return bytes.toString("utf8");
}

const fixed = raw
  .split(/\r?\n/)
  .map((line) => {
    const originalScore = scoreMojibake(line);
    if (!originalScore) return line;
    const candidate = decodeCp1251AsUtf8(line);
    const candidateScore = scoreMojibake(candidate);
    return candidateScore < originalScore ? candidate : line;
  })
  .join("\n");

fs.writeFileSync(file, fixed, "utf8");
