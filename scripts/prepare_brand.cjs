// Packaging only: resize the imagegen artwork without redrawing it.
const sharp = require('../mobile/node_modules/sharp');
const fs = require('node:fs/promises');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
async function main() {
  const source = path.join(root, 'mobile/assets/brand/source.png');
  for (const [name, size] of [['app-1024.png',1024],['splash.png',512]]) {
    await sharp(source).resize(size,size).png().toFile(path.join(root,'mobile/assets/brand',name));
  }
  for (const size of [192,512]) {
    await sharp(source).resize(size,size).png().toFile(path.join(root,`web/icons/app-${size}.png`));
  }
  await sharp(path.join(root,'mobile/assets/icons/chats@3x.png')).resize(96,96).png().toFile(path.join(root,'web/icons/notification-96.png'));
}
main().catch(error => { console.error(error.message); process.exitCode=1; });
