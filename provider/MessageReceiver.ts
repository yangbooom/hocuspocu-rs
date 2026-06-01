import { readAuthMessage } from '@hocuspocus/common';
import { readVarInt, readVarString } from 'lib0/decoding';
import * as awarenessProtocol from 'y-protocols/awareness';
import { messageYjsSyncStep2, readSyncMessage } from 'y-protocols/sync';
import type { CloseEvent } from 'ws';

import type { HocuspocusProvider } from './HocuspocusProvider';
import { IncomingMessage } from './IncomingMessage';
import { OutgoingMessage } from './OutgoingMessage';
import { MessageType } from './types';
import FragmentBuffer, { activeFragmentTransmissions } from './FragmentBuffer';

export class MessageReceiver {
  message: IncomingMessage;

  constructor(message: IncomingMessage) {
    this.message = message;
  }

  public apply(provider: HocuspocusProvider, emitSynced: boolean) {
    const { message } = this;
    const type = message.readVarUint();

    const emptyMessageLength = message.length();

    switch (type) {
      case MessageType.Sync:
        this.applySyncMessage(provider, emitSynced);
        break;

      case MessageType.Awareness:
        this.applyAwarenessMessage(provider);
        break;

      case MessageType.Auth:
        this.applyAuthMessage(provider);
        break;

      case MessageType.QueryAwareness:
        this.applyQueryAwarenessMessage(provider);
        break;

      case MessageType.Stateless:
        provider.receiveStateless(readVarString(message.decoder));
        break;

      case MessageType.SyncStatus:
        this.applySyncStatusMessage(provider, readVarInt(message.decoder) === 1);
        break;

      case MessageType.CLOSE:
        // eslint-disable-next-line no-case-declarations
        const event: CloseEvent = {
          code: 1000,
          reason: readVarString(message.decoder),
          // @ts-ignore
          target: provider.configuration.websocketProvider.webSocket!,
          type: 'close',
        };
        provider.onClose();
        provider.configuration.onClose({ event });
        provider.forwardClose(event);
        break;

      case MessageType.FragmentStart: {
        const uniqueFragmentId = message.readVarString();
        // Handle FragmentStart message
        if (activeFragmentTransmissions.has(uniqueFragmentId)) {
          console.warn(
            `Received FragmentStart for an already active fragment: ${uniqueFragmentId}`
          );
        }
        activeFragmentTransmissions.set(uniqueFragmentId, new FragmentBuffer(uniqueFragmentId));
        break;
      }
      case MessageType.FragmentData: {
        const uniqueFragmentId = message.readVarString();
        const chunkIndex = message.readVarUint();
        const chunkData = message.readVarUint8Array();
        // Handle FragmentData message
        const buffer = activeFragmentTransmissions.get(uniqueFragmentId);
        if (!buffer) {
          console.warn(
            `Received FragmentData for an unknown fragment: ${uniqueFragmentId}`
          );
          return;
        }
        buffer.addChunk(chunkIndex, chunkData);
        break;
      }
      case MessageType.FragmentEnd: {
        const uniqueFragmentId = message.readVarString();
        const buffer = activeFragmentTransmissions.get(uniqueFragmentId);
        if (!buffer) {
          console.warn(
            `Received FragmentEnd for an unknown fragment: ${uniqueFragmentId}`
          );
          return;
        }
        buffer.markEndReceived();
        if (buffer.isComplete()) {
          const combinedBytes = buffer.getCombinedBytes();
          activeFragmentTransmissions.delete(uniqueFragmentId);
          // Process the complete fragment data (recursive)
          const fragmentMessage = new IncomingMessage(combinedBytes);
          fragmentMessage.readVarString(); // Skip documentName
          const fragmentReceiver = new MessageReceiver(fragmentMessage);
          fragmentReceiver.apply(provider, emitSynced);
        }
        // Handle FragmentEnd message
        // This would typically involve combining buffered data and processing it
        break;
      }
      default:
        throw new Error(`Can’t apply message of unknown type: ${type}`);
    }

    // Reply
    if (message.length() > emptyMessageLength + 1) {
      // length of documentName (considered in emptyMessageLength plus length of yjs sync type, set in applySyncMessage)
      // @ts-ignore
      provider.send(OutgoingMessage, { encoder: message.encoder });
    }
  }

  private applySyncMessage(provider: HocuspocusProvider, emitSynced: boolean) {
    const { message } = this;

    message.writeVarUint(MessageType.Sync);
    // Apply update
    const syncMessageType = readSyncMessage(
      message.decoder,
      message.encoder,
      provider.document,
      provider
    );

    // Synced once we receive Step2
    if (emitSynced && syncMessageType === messageYjsSyncStep2) {
      provider.synced = true;
    }
  }

  applySyncStatusMessage(provider: HocuspocusProvider, applied: boolean) {
    if (applied) {
      provider.decrementUnsyncedChanges();
    }
  }

  private applyAwarenessMessage(provider: HocuspocusProvider) {
    if (!provider.awareness) return;

    const { message } = this;

    awarenessProtocol.applyAwarenessUpdate(
      provider.awareness,
      message.readVarUint8Array(),
      provider
    );
  }

  private applyAuthMessage(provider: HocuspocusProvider) {
    const { message } = this;

    readAuthMessage(
      message.decoder,
      provider.sendToken.bind(provider),
      provider.permissionDeniedHandler.bind(provider),
      provider.authenticatedHandler.bind(provider)
    );
  }

  private applyQueryAwarenessMessage(provider: HocuspocusProvider) {
    if (!provider.awareness) return;

    const { message } = this;

    message.writeVarUint(MessageType.Awareness);
    message.writeVarUint8Array(
      awarenessProtocol.encodeAwarenessUpdate(
        provider.awareness,
        Array.from(provider.awareness.getStates().keys())
      )
    );
  }
}
