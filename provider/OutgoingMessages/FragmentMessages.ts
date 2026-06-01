/* eslint-disable max-classes-per-file */
import { writeUint8Array, writeVarString, writeVarUint } from 'lib0/encoding';

import type { OutgoingMessageArguments } from '../types';
import { MessageType } from '../types';
import { OutgoingMessage } from '../OutgoingMessage';

export class FragmentStartMessage extends OutgoingMessage {
  type = MessageType.FragmentStart;

  description = 'A document update';

  get(args: Partial<OutgoingMessageArguments>) {
    writeVarString(this.encoder, args.documentName!);
    writeVarUint(this.encoder, this.type);
    writeVarString(this.encoder, args.uniqueFragmentId!);

    return this.encoder;
  }
}

export class FragmentDataMessage extends OutgoingMessage {
  type = MessageType.FragmentData;

  description = 'A document fragment data';

  get(args: Partial<OutgoingMessageArguments> & { chunkIndex: number; chunkData: Uint8Array }) {
    writeVarString(this.encoder, args.documentName!);
    writeVarUint(this.encoder, this.type);
    writeVarString(this.encoder, args.uniqueFragmentId!);
    writeVarUint(this.encoder, args.chunkIndex);
    writeUint8Array(this.encoder, args.chunkData);

    return this.encoder;
  }
}

export class FragmentEndMessage extends OutgoingMessage {
  type = MessageType.FragmentEnd;

  description = 'A document fragment end';

  get(args: Partial<OutgoingMessageArguments>) {
    writeVarString(this.encoder, args.documentName!);
    writeVarUint(this.encoder, this.type);
    writeVarString(this.encoder, args.uniqueFragmentId!);

    return this.encoder;
  }
}
