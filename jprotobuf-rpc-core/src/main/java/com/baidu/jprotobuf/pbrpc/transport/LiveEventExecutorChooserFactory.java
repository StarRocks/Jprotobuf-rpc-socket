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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorChooserFactory;

/**
 * Round-robin chooser that replaces event loops whose thread has died.
 *
 * <p>
 * Netty never replaces an event loop whose thread has died: NioEventLoop.run() rethrows any Error, an
 * OutOfMemoryError for instance, and the loop ends up terminated while the rest of its group keeps going. Netty's
 * default chooser still hands that dead loop out, and every connection registered on it fails at once. Each OOM may
 * kill a few more loops, until none is left.
 * </p>
 *
 * <p>
 * This chooser hands out, for every slot of the group, either the group's own loop or, once that one has died, a
 * loop of a single-thread group made by the given factory, and makes a new one whenever the current one dies. New
 * connections thereby keep the group's full width however many loops die. The factory runs on the thread asking for a
 * loop and may fail, an OutOfMemoryError again; the chooser then hands out a live loop of another slot, or the dead
 * one when none is left, and tries again on a later call. Call {@link #shutdown()} before shutting the group down:
 * it stops replacing loops and shuts the replacements down.
 * </p>
 */
public final class LiveEventExecutorChooserFactory implements EventExecutorChooserFactory {

    /** Makes the single-thread group a dead loop is replaced with. */
    public interface ReplacementGroupFactory {
        /**
         * Creates a group with exactly one event loop, of the same kind as the group being chosen from.
         *
         * @return the new group
         */
        EventLoopGroup newGroup();
    }

    private static final Logger LOG = LoggerFactory.getLogger(LiveEventExecutorChooserFactory.class.getName());

    private final ReplacementGroupFactory replacementGroupFactory;
    private final List<LiveChooser> choosers = new CopyOnWriteArrayList<LiveChooser>();
    private volatile boolean shutdown;

    /**
     * @param replacementGroupFactory makes the group a dead loop is replaced with
     */
    public LiveEventExecutorChooserFactory(ReplacementGroupFactory replacementGroupFactory) {
        this.replacementGroupFactory = replacementGroupFactory;
    }

    @Override
    public EventExecutorChooser newChooser(EventExecutor[] executors) {
        LiveChooser chooser = new LiveChooser(executors);
        choosers.add(chooser);
        return chooser;
    }

    /**
     * Stops replacing dead loops and shuts down every replacement made so far. The group's own loops are left to the
     * group's own shutdown.
     */
    public void shutdown() {
        shutdown = true;
        for (LiveChooser chooser : choosers) {
            chooser.shutdownReplacements();
        }
    }

    /** Chooser over a fixed number of slots. */
    private final class LiveChooser implements EventExecutorChooser {
        private final AtomicInteger idx = new AtomicInteger();
        private final EventExecutor[] executors;
        private final AtomicReferenceArray<EventExecutor> slots;

        LiveChooser(EventExecutor[] executors) {
            this.executors = executors.clone();
            this.slots = new AtomicReferenceArray<EventExecutor>(executors);
        }

        @Override
        public EventExecutor next() {
            int slot = Math.floorMod(idx.getAndIncrement(), slots.length());
            EventExecutor executor = slots.get(slot);
            if (!executor.isShuttingDown()) {
                return executor;
            }
            if (!shutdown) {
                EventExecutor replacement = replace(slot, executor);
                if (replacement != null) {
                    return replacement;
                }
            }
            for (int i = 1; i < slots.length(); i++) {
                EventExecutor other = slots.get(Math.floorMod(slot + i, slots.length()));
                if (!other.isShuttingDown()) {
                    return other;
                }
            }
            return executor;
        }

        /**
         * Puts a new loop into the slot of a dead one, unless another thread got there first.
         *
         * @return the loop now in the slot if it is alive, otherwise null
         */
        private EventExecutor replace(int slot, EventExecutor dead) {
            EventExecutor loop;
            try {
                loop = replacementGroupFactory.newGroup().next();
            } catch (Throwable t) {
                LOG.warn("Failed to replace dead event loop " + slot + ", will try again", t);
                return null;
            }
            if (!slots.compareAndSet(slot, dead, loop)) {
                loop.parent().shutdownGracefully();
                EventExecutor current = slots.get(slot);
                return current.isShuttingDown() ? null : current;
            }
            LOG.warn("Replaced dead event loop " + slot + " (" + dead + ") with a new one");
            if (dead != executors[slot]) {
                // A replacement that died in turn; its single-thread group has nothing left to serve.
                dead.parent().shutdownGracefully();
            }
            if (shutdown) {
                // shutdown() may have run between the check in next() and the swap; it cannot see this loop.
                loop.parent().shutdownGracefully();
            }
            return loop;
        }

        private void shutdownReplacements() {
            for (int i = 0; i < slots.length(); i++) {
                EventExecutor executor = slots.get(i);
                if (executor != executors[i]) {
                    executor.parent().shutdownGracefully();
                }
            }
        }
    }
}
