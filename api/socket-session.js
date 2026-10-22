// Install listeners synchronously: native clients send auth immediately on open.
// Serialize frames so rooms.join cannot overtake the asynchronous DB auth query.
export function installSocketSession(socket, request, options) {
  let user = null, closed = false;
  const timer = setTimeout(() => reject(), options.timeoutMs || 10000);
  function reject() {
    if (closed) return;
    socket.send(JSON.stringify({ type: 'auth.error' }));
    socket.close(4401, 'Authentication required');
  }
  async function authenticate(req) {
    try {
      const result = await options.authenticate(req);
      if (closed || !result) return false;
      user = result;
      clearTimeout(timer);
      options.onAuthenticated(user);
      socket.send(JSON.stringify({ type: 'auth.ok', user: {
        id: user.id, username: user.username, display_name: user.display_name, avatar_id: user.avatar_id,
      } }));
      return true;
    } catch { return false; }
  }
  // A missing browser cookie is normal for a native client, not an expired session.
  let sequence = request.headers?.authorization || request.cookies?.token
    ? authenticate(request) : Promise.resolve();
  socket.on('message', raw => {
    sequence = sequence.then(async () => {
      if (closed) return;
      let message;
      try { message = JSON.parse(String(raw)); } catch { return; }
      if (message.type === 'auth') {
        if (user) return;
        const token = typeof message.token === 'string' ? message.token : '';
        const ok = await authenticate(token ? { headers: { authorization: 'Bearer ' + token }, cookies: {} } : request);
        if (!ok) reject();
        return;
      }
      if (!user) {
        socket.send(JSON.stringify({ type: 'auth.required' }));
        return;
      }
      await options.onMessage(message, user);
    }).catch(error => {
      options.onError?.(error);
      if (!closed) socket.send(JSON.stringify({ type: 'error', error: 'server_error' }));
    });
  });
  socket.on('close', () => {
    closed = true;
    clearTimeout(timer);
    options.onClose(user);
  });
  socket.on('error', error => options.onError?.(error));
}
