/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util.concurrent;

import static io.netty.util.internal.ObjectUtil.checkPositive;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for {@link EventExecutorGroup} implementations that handles their tasks with multiple threads at
 * the same time.
 */
public abstract class MultithreadEventExecutorGroup extends AbstractEventExecutorGroup {

    //Reactor线程组中的Reactor集合
    private final EventExecutor[] children;
    private final Set<EventExecutor> readonlyChildren;

    // 关闭用到的变量
    private final AtomicInteger terminatedChildren = new AtomicInteger();

    //关闭future
    private final Promise<?> terminationFuture = new DefaultPromise(GlobalEventExecutor.INSTANCE);
    //从Reactor集合中选择一个特定的Reactor的绑定策略 用于channel注册绑定到一个固定的Reactor上
    private final EventExecutorChooserFactory.EventExecutorChooser chooser;

    /**
     * Create a new instance.
     *
     * @param nThreads          the number of threads that will be used by this instance.
     * @param threadFactory     the ThreadFactory to use, or {@code null} if the default should be used.
     * @param args              arguments which will passed to each {@link #newChild(Executor, Object...)} call
     */
    protected MultithreadEventExecutorGroup(int nThreads, ThreadFactory threadFactory, Object... args) {
        this(nThreads, threadFactory == null ? null : new ThreadPerTaskExecutor(threadFactory), args);
    }

    /**
     * Create a new instance.
     *
     * @param nThreads          the number of threads that will be used by this instance.
     * @param executor          the Executor to use, or {@code null} if the default should be used.
     * @param args              arguments which will passed to each {@link #newChild(Executor, Object...)} call
     */
    protected MultithreadEventExecutorGroup(int nThreads, Executor executor, Object... args) {
        this(nThreads, executor, DefaultEventExecutorChooserFactory.INSTANCE, args);
    }

    /**
     * Create a new instance.
     *
     * @param nThreads          the number of threads that will be used by this instance.
     * @param executor          the Executor to use, or {@code null} if the default should be used.
     * @param chooserFactory    the {@link EventExecutorChooserFactory} to use.
     * @param args              arguments which will passed to each {@link #newChild(Executor, Object...)} call
     *
     * args是
     * 1. selectorProvider
     * 2. selectStrategyFactory 就是 DefaultSelectStrategyFactory
     * 3. RejectedExecutionHandlers.reject()
     */
    protected MultithreadEventExecutorGroup(int nThreads, Executor executor,
                                            EventExecutorChooserFactory chooserFactory, Object... args) {
        /**
         * EventExecutorChooserFactory chooserFactory: 当客户端连接完成三次握手后，Main Reactor会创建客户端连接NioSocketChannel，并将其绑定到Sub Reactor Group中的一个固定Reactor，
         *                              那么具体要绑定到哪个具体的Sub Reactor上呢？
         *                              这个绑定策略就是由chooserFactory来创建的。默认为 DefaultEventExecutorChooserFactory
         */

        checkPositive(nThreads, "nThreads");

        if (executor == null) {
            // executor 用于创建Reactor线程
            executor = new ThreadPerTaskExecutor(newDefaultThreadFactory());
        }

        children = new EventExecutor[nThreads];

        //循环创建reactor group中的Reactor
        for (int i = 0; i < nThreads; i ++) {
            boolean success = false;
            try {
                /**
                 * Reactor线程组 NioEventLoopGroup 包含多个Reactor，存放于 private final EventExecutor[] children 数组中。
                 *
                 * 所以下面的事情就是创建 nThread 个Reactor，并存放于EventExecutor[] children字段中
                 */

                /**
                 * 创建reactor, 实现看 {@link io.netty.channel.nio.NioEventLoopGroup#newChild(Executor, Object...)}
                 * 重点看
                 */
                children[i] = newChild(executor, args);
                success = true;
            } catch (Exception e) {
                // TODO: Think about if this is a good exception type
                throw new IllegalStateException("failed to create a child event loop", e);
            } finally {
                if (!success) {
                    for (int j = 0; j < i; j ++) {
                        children[j].shutdownGracefully();
                    }

                    for (int j = 0; j < i; j ++) {
                        EventExecutor e = children[j];
                        try {
                            while (!e.isTerminated()) {
                                e.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
                            }
                        } catch (InterruptedException interrupted) {
                            // Let the caller handle the interruption.
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
        }
        // reactor group中的Reactor 创建完了

        /**
         * 这些Channel究竟应该注册到哪个Reactor上呢？这就需要一个绑定的策略来平均分配
         * 创建channel到Reactor的绑定策略
         *
         * 从Reactor集合中选择一个特定的Reactor的绑定策略 用于channel注册绑定到一个固定的Reactor上
         */
        chooser = chooserFactory.newChooser(children);

        // 创建Reactor关闭的回调函数terminationListener，在Reactor关闭时回调
        final FutureListener<Object> terminationListener = new FutureListener<Object>() {
            @Override
            public void operationComplete(Future<Object> future) throws Exception {
                // 通过AtomicInteger terminatedChildren变量记录已经关闭的Reactor个数，用来判断NioEventLoopGroup中的Reactor是否已经全部关闭。
                if (terminatedChildren.incrementAndGet() == children.length) {
                    //当所有Reactor关闭后 才认为是关闭成功
                    terminationFuture.setSuccess(null);
                }
            }
        };

        //为所有Reactor添加terminationListener
        for (EventExecutor e: children) {
            // 有创建就有启动，有启动就有关闭，这里会创建Reactor关闭的回调函数terminationListener，在Reactor关闭时回调
            // 看terminationListener回调的逻辑
            e.terminationFuture().addListener(terminationListener);
        }

        Set<EventExecutor> childrenSet = new LinkedHashSet<EventExecutor>(children.length);
        Collections.addAll(childrenSet, children);
        readonlyChildren = Collections.unmodifiableSet(childrenSet);
    }

    protected ThreadFactory newDefaultThreadFactory() {
        return new DefaultThreadFactory(getClass());
    }

    @Override
    public EventExecutor next() {
        return chooser.next();
    }

    @Override
    public Iterator<EventExecutor> iterator() {
        return readonlyChildren.iterator();
    }

    /**
     * Return the number of {@link EventExecutor} this implementation uses. This number is the maps
     * 1:1 to the threads it use.
     */
    public final int executorCount() {
        return children.length;
    }

    /**
     * Create a new EventExecutor which will later then accessible via the {@link #next()}  method. This method will be
     * called for each thread that will serve this {@link MultithreadEventExecutorGroup}.
     *
     */
    protected abstract EventExecutor newChild(Executor executor, Object... args) throws Exception;

    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
        for (EventExecutor l: children) {
            l.shutdownGracefully(quietPeriod, timeout, unit);
        }
        return terminationFuture();
    }

    @Override
    public Future<?> terminationFuture() {
        return terminationFuture;
    }

    @Override
    @Deprecated
    public void shutdown() {
        for (EventExecutor l: children) {
            l.shutdown();
        }
    }

    @Override
    public boolean isShuttingDown() {
        for (EventExecutor l: children) {
            if (!l.isShuttingDown()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isShutdown() {
        for (EventExecutor l: children) {
            if (!l.isShutdown()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isTerminated() {
        for (EventExecutor l: children) {
            if (!l.isTerminated()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        loop: for (EventExecutor l: children) {
            for (;;) {
                long timeLeft = deadline - System.nanoTime();
                if (timeLeft <= 0) {
                    break loop;
                }
                if (l.awaitTermination(timeLeft, TimeUnit.NANOSECONDS)) {
                    break;
                }
            }
        }
        return isTerminated();
    }
}
