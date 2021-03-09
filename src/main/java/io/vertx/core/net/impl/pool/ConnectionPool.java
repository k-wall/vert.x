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
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.net.impl.clientconnection.Lease;

import java.util.List;
import java.util.function.Predicate;

/**
 * Connection pool.
 */
public interface ConnectionPool<C> {

  static <C> ConnectionPool<C> pool(Connector<C> connector, int maxSize, int maxWeight) {
    return new SimpleConnectionPool<>(connector, maxSize, maxWeight);
  }

  static <C> ConnectionPool<C> pool(Connector<C> connector, int maxSize, int maxWeight, int maxWaiters) {
    return new SimpleConnectionPool<>(connector, maxSize, maxWeight, maxWaiters);
  }

  /**
   * Acquire a connection from the pool.
   *
   * @param context the context
   * @param weight the weight
   * @param handler the callback handler with the result
   */
  void acquire(EventLoop context, int weight, FutureListener<Lease<C>> handler);

  /**
   * <p> Evict connections from the pool with a predicate, only unused connection are evicted.
   *
   * <p> The operation returns the list of connections that won't be managed anymore by the pool.
   *
   * @param predicate to determine whether a connection should be evicted
   * @param handler the callback handler with the result
   */
  void evict(Predicate<C> predicate, Promise<List<C>> handler);

  /**
   * Close the pool.
   *
   * <p> This will not close the connections, instead a list of connections to be closed is returned.
   *
   * @param handler the callback handler with the result
   */
  void close(Promise<List<Future<C>>> handler);

  /**
   * @return the number of managed connections
   */
  int size();

  /**
   * @return the number of waiters
   */
  int waiters();

  /**
   * @return the pool weight
   */
  int weight();

}
