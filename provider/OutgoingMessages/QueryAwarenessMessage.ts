import * as encoding from 'lib0/encoding';

import type { OutgoingMessageArguments } from '../types';
import { MessageType } from '../types';
import { OutgoingMessage } from '../OutgoingMessage';

export class QueryAwarenessMessage extends OutgoingMessage {
  type = MessageType.QueryAwareness;

  description = 'Queries awareness states';

  get(args: Partial<OutgoingMessageArguments>) {
    encoding.writeVarString(this.encoder, args.documentName!);
    encoding.writeVarUint(this.encoder, this.type);

    return this.encoder;
  }
}
