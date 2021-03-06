package io.vertx.core.net.impl.pool;

/**
 * An executor of tasks acting on a given state in a serial fashion.
 */
public interface Executor<S> {

  /**
   * The action.
   */
  interface Action<S> {

    /**
     * Execute the action, the action should be side effect free and only update the {@code state}.
     *
     * Side effect actions should be executed in the returned post action.
     *
     * @param state the state to update
     * @return the post action to execute or {@code null} if nothing should happen
     */
    Runnable execute(S state);
  }

  /**
   * Submit an action.
   *
   * @param action the action
   */
  void submit(Action<S> action);

}
