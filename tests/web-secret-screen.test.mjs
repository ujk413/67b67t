import { test } from "node:test";
import assert from "node:assert/strict";
import { openAccountFeature } from "../web/account-features.js";
import { setLanguage, t } from "../web/i18n.js";
test("browser secret screen provides creation, capture warning and clears secret inputs on background", async () => {
  const original = {
      document: globalThis.document,
      setInterval: globalThis.setInterval,
      clearInterval: globalThis.clearInterval,
    },
    nodes = [],
    listeners = {};
  class Element {
    constructor(tag) {
      this.tag = tag;
      this.style = {};
      this.isConnected = true;
      this.listeners = {};
      this.children = [];
      nodes.push(this);
    }
    append(...children) {
      this.children.push(...children);
    }
    replaceChildren(...children) {
      this.children = children;
    }
    setAttribute() {}
    addEventListener(k, f) {
      this.listeners[k] = f;
    }
    removeEventListener() {}
    showModal() {}
    remove() {
      this.isConnected = false;
    }
    close() {
      this.listeners.close?.();
    }
  }
  globalThis.document = {
    hidden: false,
    createElement: (tag) => new Element(tag),
    body: new Element("body"),
    addEventListener: (key, fn) => {
      listeners[key] = fn;
    },
    removeEventListener: (key) => {
      delete listeners[key];
    },
  };
  let tick;
  globalThis.setInterval = (callback) => {
    tick = callback;
    return 1;
  };
  globalThis.clearInterval = () => {};
  try {
    for (const lang of ["ru", "en"]) {
      nodes.length = 0;
      setLanguage(lang);
      let vault = {
        v: 1,
        chats: { expired: { expiresAt: Date.now() - 1, messages: [] } },
      };
      const dialog = await openAccountFeature("secret", {
        me: { id: "alice" },
        request: async () => [],
        local: {
          get: async () => vault,
          set: async (_key, value) => {
            vault = value;
          },
          deviceId: async () => "a",
          isCurrent: () => true,
        },
      });
      assert.ok(
        nodes.some(
          (n) =>
            n.tag === "button" && n.textContent === t("Создать секретный чат"),
        ),
        "secret creation must be connected",
      );
      await tick();
      assert.deepEqual(
        vault.chats,
        {},
        "the secret list must prune expired state without an open chat",
      );
      assert.ok(
        nodes.some(
          (n) =>
            n.textContent ===
            t("Браузер не может запретить снимки и запись экрана."),
        ),
      );
      const fields = nodes.filter((n) => n.tag === "input");
      fields.forEach((n) => {
        n.value = "private draft";
      });
      globalThis.document.hidden = true;
      listeners.visibilitychange();
      assert.ok(fields.every((n) => n.value === ""));
      globalThis.document.hidden = false;
      dialog.close();
    }
  } finally {
    setLanguage("ru");
    Object.assign(globalThis, original);
  }
});

 test('secret chats are reachable from the rendered account settings in both UI languages',async()=>{const {renderMessengerShell}=await import('../web/views.js');try{for(const lang of ['ru','en']){setLanguage(lang);const view=renderMessengerShell({me:{id:'me',username:'owner'},rooms:[],roomMembers:[],settingsOpen:true,folders:[],messages:[]},'');assert.match(view,/openAccountTool\('secret'\)/);assert.ok(view.includes(t('Секретные чаты')));}}finally{setLanguage('ru');}});
