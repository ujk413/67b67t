import type { AccountRepository, Outgoing } from "./repository";
import type { Message } from "../types";
export class Outbox {
  private running = false;
  private stopped = false;
  private rerun = false;
  private cancelled = new Set<string>();
  constructor(
    private repository: AccountRepository,
    private send: (item: Outgoing) => Promise<Message>,
    private update: (item: Outgoing, message?: Message) => void,
  ) {}
  stop() {
    this.stopped = true;
  }
  async cancel(id: string) {
    this.cancelled.add(id);
    await this.repository.removeOutgoing(id);
  }
  async retry(id: string) {
    const item = (await this.repository.outgoing()).find((x) => x.id === id);
    if (item) {
      item.state = "queued";
      delete item.error;
      await this.repository.putOutgoing(item);
      this.update(item);
      await this.flush();
    }
  }
  async flush() {
    if (this.stopped) return;
    if (this.running) {
      this.rerun = true;
      return;
    }
    this.running = true;
    try {
      for (const item of await this.repository.outgoing()) {
        if (this.stopped) break;
        if (item.state === "failed" || this.cancelled.has(item.id)) continue;
        item.state = "sending";
        await this.repository.putOutgoing(item);
        this.update(item);
        try {
          const message = await this.send(item);
          if (this.stopped) break;
          await this.repository.saveMessages([message]);
          await this.repository.removeOutgoing(item.id);
          this.update(item, message);
        } catch (e) {
          if (this.stopped) break;
          const error = e as Error & { status?: number };
          const transient =
            /network|fetch|timeout|offline|соединен/i.test(
              error.message || "",
            ) ||
            (error.status || 0) >= 500 ||
            error.status === 429;
          item.state = transient ? "queued" : "failed";
          item.error = error.message || "Ошибка отправки";
          await this.repository.putOutgoing(item);
          this.update(item);
          if (transient) {
            this.rerun = false;
            break;
          }
        }
      }
    } finally {
      this.running = false;
      if (this.rerun && !this.stopped) {
        this.rerun = false;
        void this.flush();
      }
    }
  }
}
