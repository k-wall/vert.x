package io.vertx.core.net.impl.pool;

import io.netty.util.internal.PlatformDependent;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class NonBlockingSynchronization2<S> implements Synchronization<S> {

  private final Queue<Action<S>> q = PlatformDependent.newMpscQueue();
  private final AtomicInteger s = new AtomicInteger();
  private final S state;

  public NonBlockingSynchronization2(S state) {
    this.state = state;
  }

  @Override
  public void execute(Action<S> action) {
    q.add(action);
    if (s.get() != 0 || !s.compareAndSet(0, 1)) {
      return;
    }
    do {
      try {
        pollAndExecute();
      } finally {
        s.set(0);
      }
    } while (!q.isEmpty() && s.compareAndSet(0, 1));
  }

  private void pollAndExecute() {
    Action<S> a;
    while ((a = q.poll()) != null) {
      Runnable post = a.execute(state);
      if (post != null) {
        post.run();
      }
    }
  }
}
