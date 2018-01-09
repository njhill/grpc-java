/*
 * Copyright 2014, gRPC Authors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Preconditions;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executor ensuring that all {@link Runnable} tasks submitted are executed in order
 * using the provided {@link Executor}, and serially such that no two will ever be
 * running at the same time.
 */
// TODO(madongfly): figure out a way to not expose it or move it to transport package.
@SuppressWarnings("serial")
public final class SerializingExecutor extends AtomicBoolean implements Executor, Runnable {
  private static final Logger log =
      Logger.getLogger(SerializingExecutor.class.getName());

  private static final boolean STOPPED = false, RUNNING = true;
  
  /** Underlying executor that all submitted Runnable objects are run on. */
  private final Executor executor;

  /** A list of Runnables to be run in order. */
  private final Queue<Runnable> runQueue = new ConcurrentLinkedQueue<Runnable>();

  /**
   * Creates a SerializingExecutor, running tasks using {@code executor}.
   *
   * @param executor Executor in which tasks should be run. Must not be null.
   */
  public SerializingExecutor(Executor executor) {
    Preconditions.checkNotNull(executor, "'executor' must not be null.");
    this.executor = executor;
  }

  /**
   * Runs the given runnable strictly after all Runnables that were submitted
   * before it, and using the {@code executor} passed to the constructor.     .
   */
  @Override
  public void execute(Runnable r) {
    runQueue.add(checkNotNull(r, "'r' must not be null."));
    if (compareAndSet(STOPPED, RUNNING)) {
      boolean success = false;
      try {
        executor.execute(this);
        success = true;
      } finally {
        // It is possible that at this point that there are still tasks in
        // the queue, it would be nice to keep trying but the error may not
        // be recoverable.  So we update our state and propagate so that if
        // our caller deems it recoverable we won't be stuck.
        if (!success) {
          // This case can only be reached if 'this' was not currently running, and we failed to
          // reschedule.  The item should still be in the queue for removal.
          // ConcurrentLinkedQueue claims that null elements are not allowed, but seems to not
          // throw if the item to remove is null.  If removable is present in the queue twice,
          // the wrong one may be removed.  It doesn't seem possible for this case to exist today.
          // This is important to run in case of RejectedExectuionException, so that future calls
          // to execute don't succeed and accidentally run a previous runnable.
          runQueue.remove(r);
          set(STOPPED);
        }
      }
    }
  }

  @Override
  public void run() {
    Runnable r;
    while (true) {
      try {
        while ((r = runQueue.poll()) != null) {
          try {
            r.run();
          } catch (RuntimeException e) {
            // Log it and keep going.
            log.log(Level.SEVERE, "Exception while executing runnable " + r, e);
          }
        }
      } finally {
        set(STOPPED);
      }
      if (runQueue.isEmpty() || !compareAndSet(STOPPED, RUNNING)) {
        return;
      }
      // else we didn't enqueue anything but someone else did, continue
    }
  }
}
