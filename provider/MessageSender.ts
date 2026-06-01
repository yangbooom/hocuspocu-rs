/* eslint-disable import/prefer-default-export */
import type { Encoder } from 'lib0/encoding';
import {
  createEncoder, toUint8Array, writeVarString, writeVarUint, writeVarUint8Array
} from 'lib0/encoding';
import { createDecoder, peekVarString } from 'lib0/decoding';

import { MessageType, type ConstructableOutgoingMessage } from './types';
import { HocusPocusWebSocket } from './HocuspocusProviderWebsocket';

function createFragmentStartMessage(documentName: string, uniqueFragmentId: string): Uint8Array {
  const encoder = createEncoder();
  writeVarString(encoder, documentName); // Document name to identify the document
  writeVarUint(encoder, MessageType.FragmentStart);
  writeVarString(encoder, uniqueFragmentId); // To identify this series of fragments
  // You might add metadata here, like total fragments or expected size
  return toUint8Array(encoder);
}

function createFragmentDataMessage(
  documentName: string,
  uniqueFragmentId: string,
  chunkIndex: number,
  chunkData: Uint8Array
): Uint8Array {
  const encoder = createEncoder();
  writeVarString(encoder, documentName);
  writeVarUint(encoder, MessageType.FragmentData);
  writeVarString(encoder, uniqueFragmentId);
  writeVarUint(encoder, chunkIndex); // To maintain order
  writeVarUint8Array(encoder, chunkData); // The actual fragment bytes
  return toUint8Array(encoder);
}

function createFragmentEndMessage(documentName: string, uniqueFragmentId: string) {
  const encoder = createEncoder();
  writeVarString(encoder, documentName);
  writeVarUint(encoder, MessageType.FragmentEnd);
  writeVarString(encoder, uniqueFragmentId);
  // You might add a checksum or hash here to verify integrity
  return toUint8Array(encoder);
}
export class MessageSender {
  encoder: Encoder;

  message: any;

  constructor(Message: ConstructableOutgoingMessage, args: any = {}) {
    this.message = new Message();
    this.encoder = this.message.get(args);
  }

  create() {
    return toUint8Array(this.encoder);
  }

  send(webSocket: HocusPocusWebSocket, args: { messageChunkSize?: number } = {}) {
    if (!args.messageChunkSize || args.messageChunkSize <= 0) {
      webSocket?.send(this.create());
      return;
    }
    const messageInUint8Array = this.create();

    if (messageInUint8Array.length <= args.messageChunkSize) {
      webSocket?.send(messageInUint8Array);
      return;
    }

    const decoder = createDecoder(messageInUint8Array);
    const documentName = peekVarString(decoder);
    const uniqueFragmentId = Math.random().toString(36).substring(2, 15);

    webSocket?.send(createFragmentStartMessage(documentName, uniqueFragmentId));

    for (let i = 0; i < messageInUint8Array.length; i += args.messageChunkSize) {
      const chunk = messageInUint8Array.slice(i, i + args.messageChunkSize);
      webSocket?.send(createFragmentDataMessage(
        documentName,
        uniqueFragmentId,
        Math.floor(i / args.messageChunkSize),
        chunk
      ));
    }
    webSocket?.send(createFragmentEndMessage(documentName, uniqueFragmentId));

    // webSocket?.send(this.create());
  }
}
