import { contextExecutor, txFromContext, withContext } from '@millstream/core';
import {
  HandlerNotFoundError,
  InvalidRequestError,
  Mediator,
  type MediatorMiddleware,
  type RequestInput,
  type RequestPayload,
  type RequestResult,
  defineCommand,
  defineQuery,
} from '@millstream/cqrs/mediator';
import { describe, expect, expectTypeOf, it } from 'vitest';
import { z } from 'zod';
import { EventBus, defineEvent } from '../src/index.js';

const CreateTodo = defineCommand(
  'todo.create',
  z.object({ title: z.string().min(1), due: z.coerce.date().optional() }),
).returns<{ id: string }>();
const CompleteTodo = defineCommand<{ id: string }>('todo.complete'); // types only, void result
const GetTodo = defineQuery('todo.get', z.object({ id: z.string() })).returns<{ id: string; title: string } | null>();
const CountTodos = defineQuery<void, number>('todo.count');

describe('definitions', () => {
  it('infers input / payload / result types', () => {
    expectTypeOf<RequestInput<typeof CreateTodo>>().toEqualTypeOf<{ title: string; due?: unknown }>();
    expectTypeOf<RequestPayload<typeof CreateTodo>>().toEqualTypeOf<{ title: string; due?: Date | undefined }>();
    expectTypeOf<RequestResult<typeof CreateTodo>>().toEqualTypeOf<{ id: string }>();
    expectTypeOf<RequestResult<typeof CompleteTodo>>().toEqualTypeOf<void>();
    expectTypeOf<RequestResult<typeof CountTodos>>().toEqualTypeOf<number>();
  });

  it('are frozen and carry their kind', () => {
    expect(CreateTodo.kind).toBe('command');
    expect(GetTodo.kind).toBe('query');
    expect(Object.isFrozen(CreateTodo)).toBe(true);
    expect(() => defineCommand(' ')).toThrow(/empty/);
  });
});

describe('Mediator', () => {
  it('executes commands and queries synchronously with typed results', async () => {
    const todos = new Map<string, string>();
    const m = new Mediator()
      .handle(CreateTodo, ({ title }) => {
        const id = `t${todos.size + 1}`;
        todos.set(id, title);
        return { id };
      })
      .handle(GetTodo, ({ id }) => (todos.has(id) ? { id, title: todos.get(id)! } : null))
      .handle(CountTodos, () => todos.size);

    const { id } = await m.execute(CreateTodo, { title: 'write tests' });
    expect(await m.query(GetTodo, { id })).toEqual({ id, title: 'write tests' });
    expect(await m.query(GetTodo, { id: 'nope' })).toBeNull();
    expect(await m.query(CountTodos, undefined)).toBe(1);

    // @ts-expect-error a query is not a command
    await expect(m.execute(GetTodo, { id })).rejects.toThrow(HandlerNotFoundError);
    // @ts-expect-error input type is checked
    await m.execute(CreateTodo, { title: 1 }).catch(() => {});
  });

  it('validates and transforms input before the handler and middlewares', async () => {
    const seen: unknown[] = [];
    let handled = 0;
    const m = new Mediator({ middlewares: [(ctx, next) => (seen.push(ctx.input), next())] }).handle(
      CreateTodo,
      ({ due }) => {
        handled++;
        expect(due).toBeInstanceOf(Date);
        return { id: 'x' };
      },
    );
    await m.execute(CreateTodo, { title: 'a', due: '2026-01-01T00:00:00Z' });
    expect(seen).toEqual([{ title: 'a', due: new Date('2026-01-01T00:00:00Z') }]);

    const err = await m.execute(CreateTodo, { title: '' }).catch((e) => e);
    expect(err).toBeInstanceOf(InvalidRequestError);
    expect(err.message).toMatch(/invalid "todo.create" command: title:/);
    expect(handled).toBe(1);
    expect(seen).toHaveLength(1);

    const raw = new Mediator({ validate: false }).handle(CreateTodo, ({ title }) => ({ id: title }));
    expect(await raw.execute(CreateTodo, { title: '' })).toEqual({ id: '' });
  });

  it('exactly one handler per name', async () => {
    const m = new Mediator().handle(CompleteTodo, () => {});
    expect(() => m.handle(CompleteTodo, () => {})).toThrow(/already has a handler/);
    expect(() => m.handle(defineQuery<{ id: string }, void>('todo.complete'), () => {})).toThrow(
      /registered as a command/,
    );
    expect(m.has(CompleteTodo)).toBe(true);
    expect(m.has(GetTodo)).toBe(false);
    await expect(m.query(GetTodo, { id: '1' })).rejects.toThrow(/no handler registered for query "todo.get"/);
  });

  it('middlewares wrap the handler, first one outermost, and see kind / name', async () => {
    const log: string[] = [];
    const mw =
      (tag: string): MediatorMiddleware =>
      async (ctx, next) => {
        log.push(`${tag}>${ctx.kind}:${ctx.name}`);
        const r = await next();
        log.push(`<${tag}`);
        return r;
      };
    const m = new Mediator({ middlewares: [mw('a'), mw('b')] }).handle(CountTodos, () => (log.push('handler'), 7));
    expect(await m.query(CountTodos, undefined)).toBe(7);
    expect(log).toEqual(['a>query:todo.count', 'b>query:todo.count', 'handler', '<b', '<a']);
  });

  it('handler errors propagate unchanged; aborted signals are rejected up front', async () => {
    const boom = new Error('boom');
    let calls = 0;
    const m = new Mediator().handle(CompleteTodo, async (_, ctx) => {
      calls++;
      expect(ctx.signal).toBeInstanceOf(AbortSignal);
      throw boom;
    });
    await expect(m.execute(CompleteTodo, { id: '1' }, { signal: new AbortController().signal })).rejects.toBe(boom);
    await expect(m.execute(CompleteTodo, { id: '1' }, { signal: AbortSignal.abort() })).rejects.toThrow();
    expect(calls).toBe(1);
  });

  it('a transaction middleware lets the command handler publish events in the same transaction', async () => {
    // Stand-in for withTransaction(db, …): puts a tx executor into the async context.
    const statements: string[] = [];
    const fakeTx = { query: async (text: string) => (statements.push(text), { rows: [], rowCount: 1 }) };
    const transaction: MediatorMiddleware = (ctx, next) =>
      ctx.kind === 'command' ? withContext({ tx: fakeTx }, next) : next();

    const TodoCreated = defineEvent<{ id: string }>('todo.created.v1');
    const exec = contextExecutor({ query: async () => ({ rows: [], rowCount: 0 }) });
    const bus = new EventBus({
      publisher: { publish: async (topic) => void (await exec.query(`INSERT INTO ${topic}`)), close: async () => {} },
    });
    const m = new Mediator({ middlewares: [transaction] }).handle(CreateTodo, async () => {
      expect(txFromContext()).toBe(fakeTx);
      await txFromContext()!.query('INSERT INTO todos');
      await bus.publish(TodoCreated, { id: 't1' });
      return { id: 't1' };
    });
    await m.execute(CreateTodo, { title: 'x' });
    expect(statements).toEqual(['INSERT INTO todos', 'INSERT INTO todo.created.v1']);
  });
});
