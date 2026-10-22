const fs = require("node:fs"),
  path = require("node:path");
const root = path.resolve(__dirname, ".."),
  sharp = require(path.join(root, "mobile/node_modules/sharp"));
(async () => {
  const sources = path.join(root, "mobile/assets/icons/source"),
    out = path.dirname(sources);
  const names = fs
    .readdirSync(sources)
    .filter((x) => x.endsWith(".png"))
    .map((x) => x.slice(0, -4))
    .sort();
  for (const name of names)
    for (const [suffix, size] of [
      ["", 24],
      ["@2x", 48],
      ["@3x", 72],
    ])
      await sharp(path.join(sources, name + ".png"))
        .resize(size, size)
        .png()
        .toFile(path.join(out, name + suffix + ".png"));
  const file = path.join(root, "mobile/src/components/Icon.tsx");
  let code = fs.readFileSync(file, "utf8");
  const start = code.indexOf("const icons"),
    end = code.indexOf("export type IconName");
  code =
    code.slice(0, start) +
    `const icons={${names.map((name) => `${JSON.stringify(name)}:require('../../assets/icons/${name}.png')`).join(",")}};\n` +
    code.slice(end);
  fs.writeFileSync(file, code);
  console.log(`Prepared ${names.length} generated icons at 1x/2x/3x`);
})().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
