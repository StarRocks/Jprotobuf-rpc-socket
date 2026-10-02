/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.baidu.jprotobuf.pbrpc.transport;

import java.util.concurrent.atomic.AtomicInteger;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorChooserFactory;

/**
 * Round-robin chooser that passes over event loops which are no longer running.
 *
 * <p>
 * Netty never replaces an event loop whose thread has died: NioEventLoop.run() rethrows any Error, an
 * OutOfMemoryError for instance, and the loop ends up terminated while the rest of its group keeps going. Netty's
 * default chooser still hands that dead loop out, and every connection registered on it fails at once. Skipping it
 * keeps new connections on the loops that still work; only when none is left does this fall back to plain round
 * robin and let the registration fail as it always would.
 * </p>
 */
public final class LiveEventExecutorChooserFactory implements EventExecutorChooserFactory {

    /** The shared instance; the factory itself holds no state. */
    public static final LiveEventExecutorChooserFactory INSTANCE = new LiveEventExecutorChooserFactory();

    private LiveEventExecutorChooserFactory() {
    }

    @Override
    public EventExecutorChooser newChooser(EventExecutor[] executors) {
        return new LiveChooser(executors);
    }

    /** Chooser over a fixed set of executors. */
    private static final class LiveChooser implements EventExecutorChooser {
        private final AtomicInteger idx = new AtomicInteger();
        private final EventExecutor[] executors;

        LiveChooser(EventExecutor[] executors) {
            this.executors = executors;
        }

        @Override
        public EventExecutor next() {
            int start = idx.getAndIncrement();
            for (int i = 0; i < executors.length; i++) {
                EventExecutor executor = executors[Math.floorMod(start + i, executors.length)];
                if (!executor.isShuttingDown()) {
                    return executor;
                }
            }
            return executors[Math.floorMod(start, executors.length)];
        }
    }
}
