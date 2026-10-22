import { userError } from './errors.js';
export function createAuthFlow({ request, setUser, initialize, render, report, busy }) {
  let running = false;
  return async function authenticate(mode, credentials) {
    if (running) return;
    running = true; busy(true); report('');
    let created = false;
    try {
      const result = await request(`/auth/${mode}`, { method: 'POST', body: JSON.stringify(credentials) });
      if (!result?.user?.id) throw new Error('Не удалось подтвердить вход. Попробуйте войти с указанным именем и паролем.');
      created = mode === 'register';
      setUser(result.user);
      await render();
      // Key publication, push permission and chat refresh cannot turn success into registration failure.
      void initialize().catch(error => report(`Вход выполнен. Не удалось обновить данные: ${userError(error)}`, true));
    } catch (error) {
      report(created ? `Аккаунт создан. Войдите с указанным именем и паролем. ${userError(error)}` : userError(error));
    } finally { running = false; busy(false); }
  };
}
