import type { Bot } from "grammy";
import { buildTelegramThreadParams, type TelegramThreadSpec } from "./bot/helpers.js";

const TELEGRAM_STREAM_MAX_CHARS = 4096;
const DEFAULT_THROTTLE_MS = 1000;

export type TelegramDraftStream = {
  update: (text: string) => void;
  flush: () => Promise<void>;
  messageId: () => number | undefined;
  /**
   * Delete the stream preview message(s) and clean up state.
   * @param opts.keepCurrent - When true, preserve the current streamMessageId (e.g. it was
   *   finalized via in-place edit) but still delete any orphaned messages from prior turns.
   */
  clear: (opts?: { keepCurrent?: boolean }) => Promise<void>;
  stop: () => void;
  /** Reset internal state so the next update creates a new message instead of editing. */
  forceNewMessage: () => void;
};

export function createTelegramDraftStream(params: {
  api: Bot["api"];
  chatId: number;
  maxChars?: number;
  thread?: TelegramThreadSpec | null;
  replyToMessageId?: number;
  throttleMs?: number;
  log?: (message: string) => void;
  warn?: (message: string) => void;
}): TelegramDraftStream {
  const maxChars = Math.min(
    params.maxChars ?? TELEGRAM_STREAM_MAX_CHARS,
    TELEGRAM_STREAM_MAX_CHARS,
  );
  const throttleMs = Math.max(250, params.throttleMs ?? DEFAULT_THROTTLE_MS);
  const chatId = params.chatId;
  const threadParams = buildTelegramThreadParams(params.thread);
  const replyParams =
    params.replyToMessageId != null
      ? { ...threadParams, reply_to_message_id: params.replyToMessageId }
      : threadParams;

  let streamMessageId: number | undefined;
  let lastSentText = "";
  let lastSentAt = 0;
  let pendingText = "";
  let inFlightPromise: Promise<void> | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let stopped = false;
  // Tracks message IDs from previous assistant turns that were abandoned via forceNewMessage.
  // These must be deleted during clear() to avoid leaving orphaned partial messages in chat.
  const orphanedMessageIds: number[] = [];

  const sendOrEditStreamMessage = async (text: string) => {
    if (stopped) {
      return;
    }
    const trimmed = text.trimEnd();
    if (!trimmed) {
      return;
    }
    if (trimmed.length > maxChars) {
      // Telegram text messages/edits cap at 4096 chars.
      // Stop streaming once we exceed the cap to avoid repeated API failures.
      stopped = true;
      params.warn?.(
        `telegram stream preview stopped (text length ${trimmed.length} > ${maxChars})`,
      );
      return;
    }
    if (trimmed === lastSentText) {
      return;
    }
    lastSentText = trimmed;
    lastSentAt = Date.now();
    try {
      if (typeof streamMessageId === "number") {
        await params.api.editMessageText(chatId, streamMessageId, trimmed);
        return;
      }
      const sent = await params.api.sendMessage(chatId, trimmed, replyParams);
      const sentMessageId = sent?.message_id;
      if (typeof sentMessageId !== "number" || !Number.isFinite(sentMessageId)) {
        stopped = true;
        params.warn?.("telegram stream preview stopped (missing message id from sendMessage)");
        return;
      }
      streamMessageId = Math.trunc(sentMessageId);
    } catch (err) {
      stopped = true;
      params.warn?.(
        `telegram stream preview failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  };

  const flush = async () => {
    if (timer) {
      clearTimeout(timer);
      timer = undefined;
    }
    for (;;) {
      if (stopped) {
        return;
      }
      if (inFlightPromise) {
        await inFlightPromise;
        continue;
      }
      const text = pendingText;
      const trimmed = text.trim();
      if (!trimmed) {
        pendingText = "";
        return;
      }
      pendingText = "";
      const current = sendOrEditStreamMessage(text).finally(() => {
        if (inFlightPromise === current) {
          inFlightPromise = undefined;
        }
      });
      inFlightPromise = current;
      await current;
      if (!pendingText) {
        return;
      }
    }
  };

  const clear = async (opts?: { keepCurrent?: boolean }) => {
    if (timer) {
      clearTimeout(timer);
      timer = undefined;
    }
    pendingText = "";
    stopped = true;
    if (inFlightPromise) {
      await inFlightPromise;
    }
    // Collect all IDs to delete: current stream message (unless keepCurrent) + all orphaned IDs
    // from previous assistant turns that were abandoned via forceNewMessage().
    const toDelete: number[] = [...orphanedMessageIds];
    orphanedMessageIds.length = 0;
    const messageId = streamMessageId;
    streamMessageId = undefined;
    if (typeof messageId === "number" && !opts?.keepCurrent) {
      toDelete.push(messageId);
    }
    for (const id of toDelete) {
      try {
        await params.api.deleteMessage(chatId, id);
      } catch (err) {
        params.warn?.(
          `telegram stream preview cleanup failed: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }
  };

  const schedule = () => {
    if (timer) {
      return;
    }
    const delay = Math.max(0, throttleMs - (Date.now() - lastSentAt));
    timer = setTimeout(() => {
      void flush();
    }, delay);
  };

  const update = (text: string) => {
    if (stopped) {
      return;
    }
    pendingText = text;
    if (inFlightPromise) {
      schedule();
      return;
    }
    if (!timer && Date.now() - lastSentAt >= throttleMs) {
      void flush();
      return;
    }
    schedule();
  };

  const stop = () => {
    stopped = true;
    pendingText = "";
    if (timer) {
      clearTimeout(timer);
      timer = undefined;
    }
  };

  const forceNewMessage = () => {
    // Track the current message ID before clearing so clear() can delete it later.
    // Without this, the previous turn's partial-text message would be orphaned in chat.
    if (typeof streamMessageId === "number") {
      orphanedMessageIds.push(streamMessageId);
    }
    streamMessageId = undefined;
    lastSentText = "";
    pendingText = "";
  };

  params.log?.(`telegram stream preview ready (maxChars=${maxChars}, throttleMs=${throttleMs})`);

  return {
    update,
    flush,
    messageId: () => streamMessageId,
    clear,
    stop,
    forceNewMessage,
  };
}
