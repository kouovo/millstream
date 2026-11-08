import { type Message, type Metadata, newMessage } from '@millstream/core';

/** Metadata key carrying the event name (same key as Watermill's CQRS marshalers). */
export const EVENT_NAME = 'name';

/** Event ↔ message. Swap it for protobuf / avro / a schema registry. */
export interface EventMarshaler {
  marshal(eventName: string, payload: unknown, opts: { uuid?: string; metadata?: Metadata }): Message;
  unmarshal(msg: Message): unknown;
  nameOf(msg: Message): string | undefined;
}

/**
 * JSON payload + `name` metadata. JSON has no Date/BigInt/Map: use schemas that coerce on
 * the way in (e.g. `z.coerce.date()`), so handlers get real types back.
 */
export const jsonEventMarshaler: EventMarshaler = {
  marshal: (eventName, payload, opts) =>
    newMessage(JSON.stringify(payload), { ...opts.metadata, [EVENT_NAME]: eventName }, opts.uuid),
  unmarshal: (msg) => JSON.parse(msg.payload.toString('utf8')),
  nameOf: (msg) => msg.metadata[EVENT_NAME],
};

/** Event name of a message produced by an EventBus (default marshaler). */
export function eventNameOf(msg: Message): string | undefined {
  return msg.metadata[EVENT_NAME];
}
