/*
 * Copyright (c) 2011-2021 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */
package io.vertx.core.net.impl.pool;

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.Future;
import io.vertx.core.http.ConnectionPoolTooBusyException;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.net.impl.clientconnection.ConnectResult;
import io.vertx.core.net.impl.clientconnection.Lease;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Predicate;

public class SimpleConnectionPool<C> implements ConnectionPool<C> {

  static class Slot<C> implements ConnectionEventListener {

    private final SimpleConnectionPool<C> pool;
    private final EventLoop eventLoop;
    private final Promise<C> result;
    private C connection;
    private int index;
    private int capacity;
    private int maxCapacity;
    private int weight;

    public Slot(SimpleConnectionPool<C> pool, EventLoop eventLoop, int index, int initialWeight) {
      this.pool = pool;
      this.eventLoop = eventLoop;
      this.connection = null;
      this.capacity = 0;
      this.index = index;
      this.weight = initialWeight;
      this.result = eventLoop.newPromise();
    }

    @Override
    public void remove() {
      pool.remove(this);
    }
  }

  static class Waiter<C> {

    final EventLoop eventLoop;
    final int weight;
    final FutureListener<Lease<C>> handler;

    Waiter(EventLoop eventLoop, final int weight, FutureListener<Lease<C>> handler) {
      this.eventLoop = eventLoop;
      this.weight = weight;
      this.handler = handler;
    }
  }

  private final Connector<C> connector;

  private final Slot<C>[] slots;
  private int size;
  private final Deque<Waiter<C>> waiters = new ArrayDeque<>();
  private final int maxWaiters;
  private final int maxWeight;
  private int weight;
  private boolean closed;
  private final Executor<SimpleConnectionPool<C>> sync;

  SimpleConnectionPool(Connector<C> connector, int maxSize, int maxWeight) {
    this(connector, maxSize, maxWeight, -1);
  }

  SimpleConnectionPool(Connector<C> connector, int maxSize, int maxWeight, int maxWaiters) {
    this.connector = connector;
    this.slots = new Slot[maxSize];
    this.size = 0;
    this.maxWaiters = maxWaiters;
    this.weight = 0;
    this.maxWeight = maxWeight;
    this.sync = new CombinerExecutor2<>(this);
  }

  private void execute(Executor.Action<SimpleConnectionPool<C>> action) {
    sync.submit(action);
  }

  public int size() {
      return size;
  }

  public void connect(Slot<C> slot, FutureListener<Lease<C>> handler) {
    connector.connect(slot.eventLoop, slot, res -> {
      if (res.isSuccess()) {
        execute(new ConnectSuccess<>(slot, res.getNow(), handler));
      } else {
        execute(new ConnectFailed<>(slot, res.cause(), handler));
      }
    });
  }

  private static class ConnectSuccess<C> implements Executor.Action<SimpleConnectionPool<C>> {

    private final Slot<C> slot;
    private final ConnectResult<C> result;
    private final FutureListener<Lease<C>> handler;

    private ConnectSuccess(Slot<C> slot, ConnectResult<C> result, FutureListener<Lease<C>> handler) {
      this.slot = slot;
      this.result = result;
      this.handler = handler;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      int initialWeight = slot.weight;
      slot.connection = result.connection();
      slot.maxCapacity = (int)result.concurrency();
      slot.weight = (int) result.weight();
      slot.capacity = slot.maxCapacity;
      pool.weight += (result.weight() - initialWeight);
      if (pool.closed) {
        return () -> {
          slot.eventLoop.<Lease<C>>newFailedFuture(new NoStackTraceThrowable("Closed")).addListener(handler);
          slot.result.setSuccess(slot.connection);
        };
      } else {
        int c = 1;
        LeaseImpl<C>[] extra;
        int m = Math.min(slot.capacity - 1, pool.waiters.size());
        if (m > 0) {
          c += m;
          extra = new LeaseImpl[m];
          for (int i = 0;i < m;i++) {
            extra[i] = new LeaseImpl<>(slot, pool.waiters.poll().handler);
          }
        } else {
          extra = null;
        }
        slot.capacity -= c;
        return () -> {
          new LeaseImpl<>(slot, handler).emit();
          if (extra != null) {
            for (LeaseImpl<C> lease : extra) {
              lease.emit();
            }
          }
          slot.result.setSuccess(slot.connection);
        };
      }
    }
  }

  private static class ConnectFailed<C> extends Remove<C> {

    private final Throwable cause;
    private final FutureListener<Lease<C>> handler;

    public ConnectFailed(Slot<C> removed, Throwable cause, FutureListener<Lease<C>> handler) {
      super(removed);
      this.cause = cause;
      this.handler = handler;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      Runnable res = super.execute(pool);
      return () -> {
        if (res != null) {
          res.run();
        }
        removed.eventLoop.<Lease<C>>newFailedFuture(cause).addListener(handler);
        removed.result.setFailure(cause);
      };
    }
  }

  private static class Remove<C> implements Executor.Action<SimpleConnectionPool<C>> {

    protected final Slot<C> removed;

    private Remove(Slot<C> removed) {
      this.removed = removed;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      int w = removed.weight;
      removed.capacity = 0;
      removed.maxCapacity = 0;
      removed.connection = null;
      removed.weight = 0;
      Waiter<C> waiter = pool.waiters.poll();
      if (waiter != null) {
        Slot<C> slot = new Slot<>(pool, waiter.eventLoop, removed.index, waiter.weight);
        pool.weight -= w;
        pool.weight += waiter.weight;
        pool.slots[removed.index] = slot;
        return () -> pool.connect(slot, waiter.handler);
      } else if (pool.size > 1) {
        Slot<C> tmp = pool.slots[pool.size - 1];
        tmp.index = removed.index;
        pool.slots[removed.index] = tmp;
        pool.slots[pool.size - 1] = null;
        pool.size--;
        pool.weight -= w;
        return null;
      } else {
        pool.slots[0] = null;
        pool.size--;
        pool.weight -= w;
        return null;
      }
    }
  }

  private void remove(Slot<C> removed) {
    execute(new Remove<>(removed));
  }

  private static class Evict<C> implements Executor.Action<SimpleConnectionPool<C>> {

    private final Predicate<C> predicate;
    private final Promise<List<C>> handler;

    public Evict(Predicate<C> predicate, Promise<List<C>> handler) {
      this.predicate = predicate;
      this.handler = handler;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      List<C> lst = new ArrayList<>();
      for (int i = pool.size - 1;i >= 0;i--) {
        Slot<C> slot = pool.slots[i];
        if (slot.connection != null && slot.capacity == slot.maxCapacity && predicate.test(slot.connection)) {
          lst.add(slot.connection);
          slot.capacity = 0;
          slot.maxCapacity = 0;
          slot.connection = null;
          if (i == pool.size - 1) {
            pool.slots[i] = null;
          } else {
            Slot<C> last = pool.slots[pool.size - 1];
            last.index = i;
            pool.slots[i] = last;
          }
          pool.weight -= slot.weight;
          pool.size--;
        }
      }
      return () -> handler.setSuccess(lst);
    }
  }

  @Override
  public void evict(Predicate<C> predicate, Promise<List<C>> handler) {
    execute(new Evict<>(predicate, handler));
  }

  private static class Acquire<C> implements Executor.Action<SimpleConnectionPool<C>> {

    private final EventLoop eventLoop;
    private final int weight;
    private final FutureListener<Lease<C>> handler;

    public Acquire(EventLoop eventLoop, int weight, FutureListener<Lease<C>> handler) {
      this.eventLoop = eventLoop;
      this.weight = weight;
      this.handler = handler;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      if (pool.closed) {
        return () -> eventLoop.<Lease<C>>newFailedFuture(new NoStackTraceThrowable("Closed")).addListener(handler);
      }

      // 1. Try reuse a existing connection with the same context
      for (int i = 0;i < pool.size;i++) {
        Slot<C> slot = pool.slots[i];
        if (slot != null && slot.eventLoop == eventLoop && slot.capacity > 0) {
          slot.capacity--;
          return () -> new LeaseImpl<>(slot, handler).emit();
        }
      }

      // 2. Try create connection
      if (pool.weight < pool.maxWeight) {
        pool.weight += weight;
        if (pool.size < pool.slots.length) {
          Slot<C> slot = new Slot<>(pool, eventLoop, pool.size, weight);
          pool.slots[pool.size++] = slot;
          return () -> pool.connect(slot, handler);
        } else {
          throw new IllegalStateException();
        }
      }

      // 3. Try use another context
      for (Slot<C> slot : pool.slots) {
        if (slot != null && slot.capacity > 0) {
          slot.capacity--;
          return () -> new LeaseImpl<>(slot, handler).emit();
        }
      }

      // 4. Fall in waiters list
      if (pool.maxWaiters == -1 || pool.waiters.size() < pool.maxWaiters) {
        pool.waiters.add(new Waiter<>(eventLoop, weight, handler));
        return null;
      } else {
        return () -> eventLoop.<Lease<C>>newFailedFuture(new ConnectionPoolTooBusyException("Connection pool reached max wait queue size of " + pool.maxWaiters)).addListener(handler);
      }
    }
  }

  public void acquire(EventLoop context, int weight, FutureListener<Lease<C>> handler) {
    execute(new Acquire<>(context, weight, handler));
  }

  static class LeaseImpl<C> implements Lease<C> {

    private final FutureListener<Lease<C>> handler;
    private final Slot<C> slot;
    private final C connection;
    private boolean recycled;

    public LeaseImpl(Slot<C> slot, FutureListener<Lease<C>> handler) {
      this.handler = handler;
      this.slot = slot;
      this.connection = slot.connection;
    }

    @Override
    public C get() {
      return connection;
    }

    @Override
    public void recycle() {
      slot.pool.recycle(this);
    }

    void emit() {
      slot.eventLoop.newSucceededFuture(new LeaseImpl<>(slot, handler)).addListener(handler);
    }
  }

  private static class Recycle<C> implements Executor.Action<SimpleConnectionPool<C>> {

    private final Slot<C> slot;

    public Recycle(Slot<C> slot) {
      this.slot = slot;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      if (slot.connection != null) {
        if (pool.waiters.size() > 0) {
          Waiter<C> waiter = pool.waiters.poll();
          return () -> new LeaseImpl<>(slot, waiter.handler).emit();
        } else {
          slot.capacity++;
          return null;
        }
      } else {
        return null;
      }
    }
  }

  private void recycle(LeaseImpl<C> lease) {
    if (lease.recycled) {
      throw new IllegalStateException("Attempt to recycle more than permitted");
    }
    lease.recycled = true;
    execute(new Recycle<>(lease.slot));
  }

  public int waiters() {
    return waiters.size();
  }

  public int weight() {
    return weight;
  }

  private static class Close<C> implements Executor.Action<SimpleConnectionPool<C>> {

    private final Promise<List<Future<C>>> handler;

    private Close(Promise<List<Future<C>>> handler) {
      this.handler = handler;
    }

    @Override
    public Runnable execute(SimpleConnectionPool<C> pool) {
      List<Future<C>> list;
      List<Waiter<C>> b;
      if (pool.closed) {
        throw new IllegalStateException();
      }
      pool.closed = true;
      b = new ArrayList<>(pool.waiters);
      pool.waiters.clear();
      list = new ArrayList<>();
      for (int i = 0;i < pool.size;i++) {
        list.add(pool.slots[i].result);
      }
      return () -> {
        b.forEach(w -> w.eventLoop.<Lease<C>>newFailedFuture(new NoStackTraceThrowable("Closed")).addListener(w.handler));
        // TODO
        handler.setSuccess(list);
      };
    }
  }

  @Override
  public void close(Promise<List<Future<C>>> handler) {
    execute(new Close<>(handler));
  }
}
