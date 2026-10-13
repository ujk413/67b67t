import AsyncStorage from "@react-native-async-storage/async-storage";

export const storage = {
  get: (key: string) => AsyncStorage.getItem(key),
  set: (key: string, value: string) => AsyncStorage.setItem(key, value),
  del: (key: string) => AsyncStorage.removeItem(key),
};

export const STORAGE_KEYS = {
  baseUrl: "mobile.baseUrl",
  token: "mobile.token",
};
