/**
 * 直播间会话 —— 单房间采集单元
 *
 * 通过 CDP 监听页面 WebSocket 帧（Network.webSocketFrameReceived），
 * 解码为 RawProtoMessage 后回调给上层，由管道负责标准化。
 * 本模块不关心业务事件协议，只负责"拿到帧并解码"。
 */

import type { Page, CDPSession } from 'playwright-core';
import { decodeFrame, type RawProtoMessage } from '../proto/douyin.proto.js';

export interface LiveSessionOptions {
  onMessage: (msg: RawProtoMessage, frame: { roomId: string }) => void;
  /** 弹幕通道建立（或断开后重新收到推送帧）时的回调 */
  onOpen?: () => void;
  /** 会话错误回调 */
  onError?: (err: Error) => void;
}

export type LiveSessionStatus = 'connecting' | 'live' | 'closed';

export class LiveSession {
  readonly roomId: string;
  readonly startedAt: number = Date.now();
  status: LiveSessionStatus = 'connecting';

  private page: Page;
  private cdp: CDPSession;
  private opts: LiveSessionOptions;
  private wsCount = 0;
  private frameCount = 0;
  private msgCount = 0;
  private stopped = false;
  /** 收到过抖音推送帧的 WebSocket（CDP requestId），只有它们关闭才算弹幕断开 */
  private pushSockets = new Set<string>();

  constructor(roomId: string, page: Page, cdp: CDPSession, opts: LiveSessionOptions) {
    this.roomId = roomId;
    this.page = page;
    this.cdp = cdp;
    this.opts = opts;
    this.attach();
  }

  private attach(): void {
    // CDP 的 webSocketFrameReceived 事件不含 url，无法按连接过滤；
    // 直接对全部帧尝试解码，非 PushFrame / 非法帧会被 decodeFrame 安全忽略（无副作用）。
    // 页面上还有别的 WebSocket，用 requestId 记下真正推送弹幕的那条连接。
    this.cdp.on('Network.webSocketFrameReceived', (params: any) => {
      this.wsCount++;
      const payloadData: string | undefined = params?.response?.payloadData;
      if (!payloadData) return;
      let buf: Uint8Array;
      try {
        buf = Buffer.from(payloadData, 'base64');
      } catch {
        return;
      }
      let decoded;
      try {
        decoded = decodeFrame(buf);
      } catch {
        return;
      }
      this.frameCount++;
      const isPush = decoded.frame.payloadType === 'msg' || decoded.frame.payloadType === 'push';
      if (isPush && params?.requestId) this.pushSockets.add(String(params.requestId));
      // 首次建立，或弹幕通道断开后又收到推送帧（抖音重连）→ 恢复为 live
      if (isPush && this.status !== 'live' && !this.stopped) {
        this.status = 'live';
        this.opts.onOpen?.();
      }
      for (const msg of decoded.messages) {
        this.msgCount++;
        this.opts.onMessage(msg, { roomId: this.roomId });
      }
    });
    this.cdp.on('Network.webSocketClosed', (params: any) => {
      // 页面上其它 WebSocket 关闭与弹幕无关；只有推送弹幕的连接全部关闭才算断开
      if (!this.pushSockets.delete(String(params?.requestId))) return;
      if (!this.stopped && this.pushSockets.size === 0) {
        this.status = 'closed';
        this.opts.onError?.(new Error(`房间 ${this.roomId} wss 连接已关闭`));
      }
    });
    this.page.on('close', () => {
      if (!this.stopped) {
        this.stopped = true;
        this.status = 'closed';
        this.opts.onError?.(new Error(`房间 ${this.roomId} 页面已关闭`));
      }
    });
  }

  /** 等待 wss 建立（最多 timeoutMs） */
  async waitForLive(timeoutMs = 15000): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    while (this.status !== 'live' && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 500));
    }
    return this.status === 'live';
  }

  stats() {
    return { wsCount: this.wsCount, frameCount: this.frameCount, msgCount: this.msgCount };
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.status = 'closed';
    try {
      await this.page.close();
    } catch {
      /* ignore */
    }
  }
}
