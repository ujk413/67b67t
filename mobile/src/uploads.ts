import { notifyUnauthorized } from "./api";
export function createUploadTask(
  baseUrl: string,
  token: string,
  kind: string,
  file: { uri: string; name: string; mime: string },
  onProgress: (progress: number) => void,
) {
  const request = new XMLHttpRequest();
  const promise = new Promise<string>((resolve, reject) => {
    request.open(
      "POST",
      `${baseUrl}/media/upload?kind=${encodeURIComponent(kind)}`,
    );
    request.setRequestHeader("Authorization", `Bearer ${token}`);
    request.timeout = 60000;
    request.upload.onprogress = (event) => {
      if (event.lengthComputable) onProgress(event.loaded / event.total);
    };
    request.onload = () => {
      try {
        const result = JSON.parse(request.responseText);
        if (request.status === 401) notifyUnauthorized("/media/upload", token);
        if (request.status < 200 || request.status >= 300)
          throw Object.assign(new Error(result.error || "Загрузка не удалась"),{status:request.status});
        resolve(result.id);
      } catch (error) {
        reject(error);
      }
    };
    request.onerror = () =>
      reject(new Error("Нет соединения. Вложения сохранены для повтора."));
    request.ontimeout = () =>
      reject(new Error("Сервер не ответил вовремя. Вложения сохранены."));
    request.onabort = () =>
      reject(new Error("Загрузка остановлена. Вложения сохранены."));
    const form = new FormData();
    form.append("file", {
      uri: file.uri,
      name: file.name,
      type: file.mime,
    } as never);
    request.send(form);
  });
  return { request, promise };
}
