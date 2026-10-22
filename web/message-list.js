// Keep media DOM nodes and user selection alive when only a receipt/reaction changes.
const fingerprints = new WeakMap();
function patch(current, next) {
  if (current.nodeType !== next.nodeType || current.nodeName !== next.nodeName) { current.replaceWith(next); return; }
  if (current.nodeType === 3) { if (current.nodeValue !== next.nodeValue) current.nodeValue = next.nodeValue; return; }
  if (current.nodeType !== 1) return;
  for (const attr of [...current.attributes]) if (!next.hasAttribute(attr.name)) current.removeAttribute(attr.name);
  for (const attr of [...next.attributes]) if (current.getAttribute(attr.name) !== attr.value) current.setAttribute(attr.name, attr.value);
  if (['AUDIO', 'VIDEO', 'IMG'].includes(current.tagName) && current.getAttribute('src') === next.getAttribute('src')) return;
  const children = [...current.childNodes], targets = [...next.childNodes];
  for (let i = 0; i < Math.max(children.length, targets.length); i++) {
    if (!targets[i]) children[i]?.remove(); else if (!children[i]) current.append(targets[i]); else patch(children[i], targets[i]);
  }
}
export function reconcileMessages(box, html, stickToBottom = false) {
  const viewportTop = box.getBoundingClientRect().top;
  const anchor = [...box.children].find(n => n.id?.startsWith('msg-') && n.getBoundingClientRect().bottom > viewportTop);
  const anchorId = anchor?.id, anchorTop = anchor?.getBoundingClientRect().top;
  const template = document.createElement('template'); template.innerHTML = html;
  const desired = [...template.content.children], existing = new Map([...box.children].map((node, i) => [node.id || `day:${node.dataset.dayLabel || i}`, node]));
  let cursor = box.firstElementChild;
  for (let i = 0; i < desired.length; i++) {
    const next = desired[i], key = next.id || `day:${next.dataset.dayLabel || i}`, old = existing.get(key), fingerprint = next.outerHTML;
    const node = old || next;
    if (old) { existing.delete(key); if (fingerprints.get(old) !== fingerprint) patch(old, next); }
    fingerprints.set(node, fingerprint);
    if (node !== cursor) box.insertBefore(node, cursor);
    cursor = node.nextElementSibling;
  }
  for (const old of existing.values()) old.remove();
  if (stickToBottom) box.scrollTop = box.scrollHeight;
  else if (anchorId && anchorTop !== undefined) { const current = document.getElementById(anchorId); if (current) box.scrollTop += current.getBoundingClientRect().top - anchorTop; }
}
