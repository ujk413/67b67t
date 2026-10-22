export async function requestWithTimeout<T>(
  operation: (signal: AbortSignal) => Promise<T>,
  milliseconds = 15000,
): Promise<T> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout>;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(Object.assign(new Error('Нет ответа от сервера. Проверьте соединение и повторите.'), { code: 'ETIMEDOUT' }));
      controller.abort();
    }, milliseconds);
  });
  try {
    return await Promise.race([operation(controller.signal), deadline]);
  } finally {
    clearTimeout(timer!);
  }
}
