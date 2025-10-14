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
package io.netty.channel.nio;

import io.netty.channel.Channel;
import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopTaskQueueFactory;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;

import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

/**
 * {@link MultithreadEventLoopGroup} implementations which is used for NIO {@link Selector} based {@link Channel}s.
 */
public class NioEventLoopGroup extends MultithreadEventLoopGroup {

    /**
     * Create a new instance using the default number of threads, the default {@link ThreadFactory} and
     * the {@link SelectorProvider} which is returned by {@link SelectorProvider#provider()}.
     */
    public NioEventLoopGroup() {
        this(0);
    }

    /**
     * Create a new instance using the specified number of threads, {@link ThreadFactory} and the
     * {@link SelectorProvider} which is returned by {@link SelectorProvider#provider()}.
     */
    public NioEventLoopGroup(int nThreads) {
        this(nThreads, (Executor) null);
    }

    /**
     * Create a new instance using the default number of threads, the given {@link ThreadFactory} and the
     * {@link SelectorProvider} which is returned by {@link SelectorProvider#provider()}.
     */
    public NioEventLoopGroup(ThreadFactory threadFactory) {
        //这里可以获得选择器提供器了
        this(0, threadFactory, SelectorProvider.provider());
    }

    /**
     * Create a new instance using the specified number of threads, the given {@link ThreadFactory} and the
     * {@link SelectorProvider} which is returned by {@link SelectorProvider#provider()}.
     */
    public NioEventLoopGroup(int nThreads, ThreadFactory threadFactory) {
        this(nThreads, threadFactory, SelectorProvider.provider());
    }

    public NioEventLoopGroup(int nThreads, Executor executor) {
        /**
         * Executor executor：负责启动Reactor线程进而Reactor才可以开始工作
         *
         * SelectorProvider.provider() 是 SelectorProvider, 是epoll还是kqueue就是这个来决定的
         * 这个SelectorProvider.provider()是jdk提供的
         * SelectorProvider 的加载方式有三种，优先级如下：
         * 1. 通过系统变量-D java.nio.channels.spi.SelectorProvider 指定 SelectorProvider 的自定义实现类全限定名。通过应用程序类加载器(Application Classloader)加载。
         * 2. 通过SPI方式加载。在工程目录META-INF/services下定义名为java.nio.channels.spi.SelectorProvider的SPI文件，文件中第一个定义的SelectorProvider实现类全限定名就会被加载。
         * 3. 如果以上两种方式均未被定义，那么就采用SelectorProvider系统默认实现sun.nio.ch.DefaultSelectorProvider。我当前使用的操作系统是MacOS，从源码中我们可以看到自动适配了KQueue实现。
         *
         * 不同操作系统中JDK对于DefaultSelectorProvider会有所不同，Linux内核版本2.6以上对应的Epoll，Linux内核版本2.6以下对应的Poll，MacOS对应的是KQueue。
         *
         * 还有最终netty会对Selector进行增强, 没有用jdk原生的那种, 所以会用到SelectorProvider的第三种加载方式
         */
        this(nThreads, executor, SelectorProvider.provider());
    }

    /**
     * Create a new instance using the specified number of threads, the given {@link ThreadFactory} and the given
     * {@link SelectorProvider}.
     */
    public NioEventLoopGroup(
            int nThreads, ThreadFactory threadFactory, final SelectorProvider selectorProvider) {
        this(nThreads, threadFactory, selectorProvider, DefaultSelectStrategyFactory.INSTANCE);
    }

    public NioEventLoopGroup(int nThreads, ThreadFactory threadFactory,
        final SelectorProvider selectorProvider, final SelectStrategyFactory selectStrategyFactory) {
        super(nThreads, threadFactory, selectorProvider, selectStrategyFactory, RejectedExecutionHandlers.reject());
    }

    public NioEventLoopGroup(
            int nThreads, Executor executor, final SelectorProvider selectorProvider) {
        /**
         * NioEventLoopGroup（Reactor线程组）里边有众多的Reactor，那么 这些Channel究竟应该注册到哪个Reactor上呢？这就需要一个绑定的策略来平均分配
         * 它主要用来创建Channel到Reactor的绑定策略。默认为DefaultEventExecutorChooserFactory.INSTANCE
         */
        this(nThreads, executor, selectorProvider, DefaultSelectStrategyFactory.INSTANCE);
    }

    /**
     * NioEventLoopGroup负责创建Reactor线程，在创建的时候会将executor传入
     *
     * selectStrategyFactory 就是 {@link DefaultSelectStrategyFactory}
     */
    public NioEventLoopGroup(int nThreads, Executor executor, final SelectorProvider selectorProvider,
                             final SelectStrategyFactory selectStrategyFactory) {
        /**
         * executor: Reactor线程组NioEventLoopGroup负责创建Reactor线程，在创建的时候会将executor传入。
         *
         * SelectorProvider selectorProvider: Reactor中的IO模型为IO多路复用模型，对应于JDK NIO中的实现为java.nio.channels.Selector（就是select,poll,epoll），
         * 每个Reactor中都包含一个Selector，用于轮询注册在该Reactor上的所有Channel上的IO事件。SelectorProvider就是用来创建Selector的
         *
         * SelectStrategyFactory selectStrategyFactory: Reactor最重要的事情就是轮询注册其上的Channel上的IO就绪事件，这里的SelectStrategyFactory用于指定轮询策略，默认为DefaultSelectStrategyFactory.INSTANCE。
         *
         * RejectedExecutionHandler: 当向Reactor添加异步任务添加失败时，采用的拒绝策略。Reactor的任务不只是监听IO活跃事件和IO任务的处理，还包括对异步任务的处理
         *
         * 委托给父类处理
         */
        super(nThreads, executor, selectorProvider, selectStrategyFactory, RejectedExecutionHandlers.reject());
    }

    public NioEventLoopGroup(int nThreads, Executor executor, EventExecutorChooserFactory chooserFactory,
                             final SelectorProvider selectorProvider,
                             final SelectStrategyFactory selectStrategyFactory) {
        super(nThreads, executor, chooserFactory, selectorProvider, selectStrategyFactory,
                RejectedExecutionHandlers.reject());
    }

    public NioEventLoopGroup(int nThreads, Executor executor, EventExecutorChooserFactory chooserFactory,
                             final SelectorProvider selectorProvider,
                             final SelectStrategyFactory selectStrategyFactory,
                             final RejectedExecutionHandler rejectedExecutionHandler) {
        super(nThreads, executor, chooserFactory, selectorProvider, selectStrategyFactory, rejectedExecutionHandler);
    }

    public NioEventLoopGroup(int nThreads, Executor executor, EventExecutorChooserFactory chooserFactory,
                             final SelectorProvider selectorProvider,
                             final SelectStrategyFactory selectStrategyFactory,
                             final RejectedExecutionHandler rejectedExecutionHandler,
                             final EventLoopTaskQueueFactory taskQueueFactory) {
        super(nThreads, executor, chooserFactory, selectorProvider, selectStrategyFactory,
                rejectedExecutionHandler, taskQueueFactory);
    }

    /**
     * @param nThreads the number of threads that will be used by this instance.
     * @param executor the Executor to use, or {@code null} if default one should be used.
     * @param chooserFactory the {@link EventExecutorChooserFactory} to use.
     * @param selectorProvider the {@link SelectorProvider} to use.
     * @param selectStrategyFactory the {@link SelectStrategyFactory} to use.
     * @param rejectedExecutionHandler the {@link RejectedExecutionHandler} to use.
     * @param taskQueueFactory the {@link EventLoopTaskQueueFactory} to use for
     *                         {@link SingleThreadEventLoop#execute(Runnable)},
     *                         or {@code null} if default one should be used.
     * @param tailTaskQueueFactory the {@link EventLoopTaskQueueFactory} to use for
     *                             {@link SingleThreadEventLoop#executeAfterEventLoopIteration(Runnable)},
     *                             or {@code null} if default one should be used.
     */
    public NioEventLoopGroup(int nThreads, Executor executor, EventExecutorChooserFactory chooserFactory,
                             SelectorProvider selectorProvider,
                             SelectStrategyFactory selectStrategyFactory,
                             RejectedExecutionHandler rejectedExecutionHandler,
                             EventLoopTaskQueueFactory taskQueueFactory,
                             EventLoopTaskQueueFactory tailTaskQueueFactory) {
        super(nThreads, executor, chooserFactory, selectorProvider, selectStrategyFactory,
                rejectedExecutionHandler, taskQueueFactory, tailTaskQueueFactory);
    }

    /**
     * Sets the percentage of the desired amount of time spent for I/O in the child event loops.  The default value is
     * {@code 50}, which means the event loop will try to spend the same amount of time for I/O as for non-I/O tasks.
     */
    public void setIoRatio(int ioRatio) {
        for (EventExecutor e: this) {
            ((NioEventLoop) e).setIoRatio(ioRatio);
        }
    }

    /**
     * Replaces the current {@link Selector}s of the child event loops with newly created {@link Selector}s to work
     * around the  infamous epoll 100% CPU bug.
     */
    public void rebuildSelectors() {
        for (EventExecutor e: this) {
            ((NioEventLoop) e).rebuildSelector();
        }
    }

    @Override
    protected EventLoop newChild(Executor executor, Object... args) throws Exception {
        SelectorProvider selectorProvider = (SelectorProvider) args[0];
        SelectStrategyFactory selectStrategyFactory = (SelectStrategyFactory) args[1];
        RejectedExecutionHandler rejectedExecutionHandler = (RejectedExecutionHandler) args[2];

        /**
         * EventLoopTaskQueueFactory queueFactory，
         *   前边提到Netty中的Reactor主要工作是轮询注册其上的所有Channel上的IO就绪事件，处理IO就绪事件。
         *   除了这些主要的工作外，Netty为了极致的压榨Reactor的性能，还会让它做一些异步任务的执行工作。
         *   既然要执行异步任务，那么Reactor中就需要一个队列来保存任务。
         *   这里的 EventLoopTaskQueueFactory 就是用来创建这样的一个队列来保存Reactor中待执行的异步任务。
         *
         *
         * 可以把Reactor理解成为一个单线程的线程池，类似于JDK中的SingleThreadExecutor，仅用一个线程来执行轮询IO就绪事件，处理IO就绪事件，执行异步任务。
         * 同时待执行的异步任务保存在Reactor里的taskQueue中。
         */
        EventLoopTaskQueueFactory taskQueueFactory = null;
        EventLoopTaskQueueFactory tailTaskQueueFactory = null;

        int argsLength = args.length;
        if (argsLength > 3) {
            taskQueueFactory = (EventLoopTaskQueueFactory) args[3];
        }
        if (argsLength > 4) {
            tailTaskQueueFactory = (EventLoopTaskQueueFactory) args[4];
        }

        /**
         * 构造方法, 重点, 但是他的参数也要看下
         * 开始创建 Reactor
         *
         * NioEventLoop 这也就是 EventExecutor
         */
        return new NioEventLoop(this, executor, selectorProvider,
                selectStrategyFactory.newSelectStrategy(),
                rejectedExecutionHandler, taskQueueFactory, tailTaskQueueFactory);
    }
}
