const path = require('path');
const {getDefaultConfig} = require('expo/metro-config');
const config = getDefaultConfig(__dirname);
// The connection probe is shared with the no-build browser client.
config.watchFolders = [path.resolve(__dirname,'../web')];
module.exports = config;
