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

import io.netty.channel.*;
import io.netty.util.IntSupplier;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.ReflectionUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;

import java.nio.channels.spi.SelectorProvider;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link SingleThreadEventLoop} implementation which register the {@link Channel}'s to a
 * {@link Selector} and so does the multi-plexing of these in the event loop.
 *
 */
public final class NioEventLoop extends SingleThreadEventLoop {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(NioEventLoop.class);

    private static final int CLEANUP_INTERVAL = 256; // XXX Hard-coded value, but won't need customization.

    //Selector优化开关 默认开启 为了遍历的效率 会对Selector中的SelectedKeys进行数据结构优化
    private static final boolean DISABLE_KEY_SET_OPTIMIZATION =
            SystemPropertyUtil.getBoolean("io.netty.noKeySetOptimization", false);

    private static final int MIN_PREMATURE_SELECTOR_RETURNS = 3;
    private static final int SELECTOR_AUTO_REBUILD_THRESHOLD;

    private final IntSupplier selectNowSupplier = new IntSupplier() {
        @Override
        public int get() throws Exception {
            //非阻塞, 选择操作更新了其就绪操作集的键数（可能为零）
            return selectNow();
        }
    };

    // Workaround for JDK NIO bug.
    //
    // See:
    // - https://bugs.openjdk.java.net/browse/JDK-6427854 for first few dev (unreleased) builds of JDK 7
    // - https://bugs.openjdk.java.net/browse/JDK-6527572 for JDK prior to 5.0u15-rev and 6u10
    // - https://github.com/netty/netty/issues/203
    static {
        if (PlatformDependent.javaVersion() < 7) {
            final String key = "sun.nio.ch.bugLevel";
            final String bugLevel = SystemPropertyUtil.get(key);
            if (bugLevel == null) {
                try {
                    AccessController.doPrivileged(new PrivilegedAction<Void>() {
                        @Override
                        public Void run() {
                            System.setProperty(key, "");
                            return null;
                        }
                    });
                } catch (final SecurityException e) {
                    logger.debug("Unable to get/set System Property: " + key, e);
                }
            }
        }

        int selectorAutoRebuildThreshold = SystemPropertyUtil.getInt("io.netty.selectorAutoRebuildThreshold", 512);
        if (selectorAutoRebuildThreshold < MIN_PREMATURE_SELECTOR_RETURNS) {
            selectorAutoRebuildThreshold = 0;
        }

        SELECTOR_AUTO_REBUILD_THRESHOLD = selectorAutoRebuildThreshold;

        if (logger.isDebugEnabled()) {
            logger.debug("-Dio.netty.noKeySetOptimization: {}", DISABLE_KEY_SET_OPTIMIZATION);
            logger.debug("-Dio.netty.selectorAutoRebuildThreshold: {}", SELECTOR_AUTO_REBUILD_THRESHOLD);
        }
    }

    /**
     * The NIO {@link Selector}.
     */
    private Selector selector;
    private Selector unwrappedSelector;

    /**
     * 会通过反射替换selector对象中的selectedKeySet保存就绪的selectKey
     * 该字段为持有selector对象selectedKeys的引用，当IO事件就绪时，直接从这里获取
     */
    private SelectedSelectionKeySet selectedKeys;

    //用于创建JDK NIO Selector,ServerSocketChannel
    private final SelectorProvider provider;

    private static final long AWAKE = -1L;
    private static final long NONE = Long.MAX_VALUE;

    // nextWakeupNanos is:
    //    AWAKE            when EL is awake
    //    NONE             when EL is waiting with no wakeup scheduled
    //    other value T    when EL is waiting with wakeup scheduled at time T
    private final AtomicLong nextWakeupNanos = new AtomicLong(AWAKE);

    //Selector轮询策略 决定什么时候轮询，什么时候处理IO事件，什么时候执行异步任务
    private final SelectStrategy selectStrategy;

    private volatile int ioRatio = 50;
    private int cancelledKeys;
    private boolean needsToSelectAgain;

    /**
     * 可以把Reactor理解成为一个单线程的线程池，类似于JDK中的SingleThreadExecutor，仅用一个线程来执行轮询IO就绪事件，处理IO就绪事件，执行异步任务
     *
     * 完整的Reactor架构, Reactor 里面是这4个
     +----------------------------+
     |        NioEventLoop        |
     |                            |
     |  +----------------------+  |
     |  |       Reactor        |  |
     |  |  +---------------+   |  |
     |  |  |     thread    |   |  |
     |  |  +---------------+   |  |
     |  |  +---------------+   |  |
     |  |  |    selector   |   |  |
     |  |  +---------------+   |  |
     |  |  +---------------+   |  |
     |  |  |   taskQueue   |   |  |
     |  |  +---------------+   |  |
     |  |  +---------------+   |  |
     |  |  |   tailQueue   |   |  |
     |  |  +---------------+   |  |
     |  +----------------------+  |
     |                            |
     +----------------------------+
     */
    NioEventLoop(NioEventLoopGroup parent, Executor executor, SelectorProvider selectorProvider,
                 SelectStrategy strategy, RejectedExecutionHandler rejectedExecutionHandler,
                 EventLoopTaskQueueFactory taskQueueFactory, EventLoopTaskQueueFactory tailTaskQueueFactory) {
        /**
         * super 里面 SingleThreadEventLoop 初始化 也是重点
         *
         * Reactor负责执行的异步任务分为三类：
         *  普通任务：这是Netty最主要执行的异步任务，存放在普通任务队列taskQueue中。在NioEventLoop构造函数中创建。
         *  定时任务： 存放在优先级队列中。
         *  尾部任务： 存放于尾部任务队列tailTasks中，尾部任务一般不常用，在普通任务执行完后 Reactor线程会执行尾部任务。
         *            使用场景：比如对Netty 的运行状态做一些统计数据，例如任务循环的耗时、占用物理内存的大小等等都可以向尾部队列添加一个收尾任务完成统计数据的实时更新。
         */
        super(parent, executor, false,
                /**
                 * Reactor中任务队列的创建过程, newTaskQueue 重点
                 *      Reactor除了需要监听IO就绪事件以及处理IO就绪事件外，还需要执行一些异步任务，
                 *      当外部线程向Reactor提交异步任务后，Reactor就需要一个队列来保存这些异步任务，等待Reactor线程执行
                 */
                newTaskQueue(taskQueueFactory), newTaskQueue(tailTaskQueueFactory),
                rejectedExecutionHandler);
        this.provider = ObjectUtil.checkNotNull(selectorProvider, "selectorProvider");
        this.selectStrategy = ObjectUtil.checkNotNull(strategy, "selectStrategy");

        /**
         * 重点
         * NioEventLoop类中用于创建IO多路复用的Selector，并对创建出来的JDK NIO 原生的Selector进行性能优化
         */
        final SelectorTuple selectorTuple = openSelector();

        //通过用 SelectedSelectionKeySet 装饰后的 unwrappedSelector
        this.selector = selectorTuple.selector;

        //Netty优化过的JDK NIO远程Selector
        this.unwrappedSelector = selectorTuple.unwrappedSelector;
    }

    private static Queue<Runnable> newTaskQueue(
            EventLoopTaskQueueFactory queueFactory) {
        if (queueFactory == null) {
            // 重点
            return newTaskQueue0(DEFAULT_MAX_PENDING_TASKS);
        }
        return queueFactory.newTaskQueue(DEFAULT_MAX_PENDING_TASKS);
    }

    private static final class SelectorTuple {
        final Selector unwrappedSelector;
        final Selector selector;

        SelectorTuple(Selector unwrappedSelector) {
            this.unwrappedSelector = unwrappedSelector;
            this.selector = unwrappedSelector;
        }

        SelectorTuple(Selector unwrappedSelector, Selector selector) {
            this.unwrappedSelector = unwrappedSelector;
            this.selector = selector;
        }
    }

    // openSelector是NioEventLoop类中用于创建IO多路复用的Selector，并对创建出来的JDK NIO 原生的Selector进行性能优化
    private SelectorTuple openSelector() {
        final Selector unwrappedSelector;
        try {
            /**
             * provider 是 SelectorProvider
             * SelectorProvider 会根据操作系统的不同选择JDK在不同操作系统版本下的对应Selector的实现。Linux下会选择Epoll，Mac下会选择Kqueue。
             */
            unwrappedSelector = provider.openSelector();
        } catch (IOException e) {
            throw new ChannelException("failed to open a new selector", e);
        }

        /**
         * DISABLE_KEY_SET_OPTIMIZATION: 可以看下这个属性的上面赋值, Selector优化开关 默认开启 为了遍历的效率 会对Selector中的SelectedKeys进行数据结构优化
         * 如果优化开关DISABLE_KEY_SET_OPTIMIZATION是关闭的，那么直接返回JDK NIO原生的Selector
         */
        if (DISABLE_KEY_SET_OPTIMIZATION) { // 默认false 不会进去
            //JDK NIO原生Selector ，Selector优化开关 默认开启需要对Selector进行优化
            return new SelectorTuple(unwrappedSelector);
        }

        Object maybeSelectorImplClass = AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    /************************下面为Netty对JDK NIO原生的Selector的优化过程************************/

                    /**
                     * 获取JDK NIO原生Selector的抽象实现类 sun.nio.ch.SelectorImpl
                     * JDK NIO原生Selector的实现均继承于该抽象类。用于判断由SelectorProvider创建出来的Selector是否为JDK默认实现（SelectorProvider第三种加载方式）。
                     * 因为SelectorProvider可以是自定义加载，所以它创建出来的Selector并不一定是JDK NIO 原生的。
                     */
                    return Class.forName(
                            "sun.nio.ch.SelectorImpl",
                            false,
                            PlatformDependent.getSystemClassLoader());

                /**
                 * sun.nio.ch.SelectorImpl 讲解

                 public abstract class SelectorImpl extends AbstractSelector {

                     // The set of keys with data ready for an operation
                     // IO就绪的SelectionKey（里面包裹着channel）, Selector会将自己监听到的IO就绪的Channel放到selectedKeys中
                     // 这里的SelectionKey暂且可以理解为Channel在 Selector 中的表示，封装IO就绪Socket的信息。其实SelectionKey里包含的信息不止是Channel还有很多IO相关的信息
                     // SelectionKey 在Channel注册到Selector中后生成。
                     protected Set<SelectionKey> selectedKeys;

                     // The set of keys registered with this Selector
                     // 注册在该Selector上的所有SelectionKey（里面包裹着channel）
                     protected HashSet<SelectionKey> keys;

                     // Public views of the key sets
                     //用于向调用线程返回的keys，不可变
                     private Set<SelectionKey> publicKeys; // Immutable

                     // publicSelectedKeys 相当于是selectedKeys的视图
                     //当有IO就绪的SelectionKey时，向调用线程返回。只可删除其中元素，不可增加
                     private Set<SelectionKey> publicSelectedKeys; // Removal allowed, but not addition

                     protected SelectorImpl(SelectorProvider sp) {
                         super(sp);
                         keys = new HashSet<SelectionKey>();
                         selectedKeys = new HashSet<SelectionKey>();
                         if (Util.atBugLevel("1.4")) {
                             publicKeys = keys;
                             publicSelectedKeys = selectedKeys;
                         } else {
                             //不可变
                             publicKeys = Collections.unmodifiableSet(keys);
                             //只可删除其中元素，不可增加
                             publicSelectedKeys = Util.ungrowableSet(selectedKeys);
                         }
                     }
                 }

                 *  //IO就绪的SelectionKey（里面包裹着channel）, Set<SelectionKey> selectedKeys 类似于Epoll提到的就绪队列eventpoll->rdllist，Selector这里可以理解为Epoll, Selector会将自己监听到的IO就绪的Channel放到selectedKeys中
                 *   protected Set<SelectionKey> selectedKeys;
                 *
                 * 这里的SelectionKey暂且可以理解为Channel在Selector中的表示，封装IO就绪Socket的信息。其实SelectionKey里包含的信息不止是Channel还有很多IO相关的信息
                 * SelectionKey 在Channel注册到Selector中后生成。
                 *      Set<SelectionKey> publicSelectedKeys 相当于是selectedKeys的视图，用于向外部线程返回IO就绪的SelectionKey。这个集合在外部线程中只能做删除操作不可增加元素，并且不是线程安全的。
                 *      Set<SelectionKey> publicKeys相当于keys的不可变视图，用于向外部线程返回所有注册在该Selector上的SelectionKey
                 *
                 * 这里需要重点关注抽象类sun.nio.ch.SelectorImpl中的selectedKeys和publicSelectedKeys这两个字段，注意它们的类型都是HashSet，一会优化的就是这里！！！！
                 */
                } catch (Throwable cause) {
                    return cause;
                }
            }
        });

        /**
         * 进行对 jdk 的sun.nio.ch.SelectorImpl 优化
         *
         * 判断是否可以对Selector进行优化，这里主要针对JDK NIO原生Selector的实现类进行优化，因为SelectorProvider可以加载的是自定义Selector实现
         * 如果SelectorProvider创建的Selector不是JDK原生sun.nio.ch.SelectorImpl的实现类，那么无法进行优化，直接返回
         */
        if (!(maybeSelectorImplClass instanceof Class) ||
            // ensure the current selector implementation is what we can instrument.
            !((Class<?>) maybeSelectorImplClass).isAssignableFrom(unwrappedSelector.getClass())) {
            if (maybeSelectorImplClass instanceof Throwable) {
                Throwable t = (Throwable) maybeSelectorImplClass;
                logger.trace("failed to instrument a special java.util.Set into: {}", unwrappedSelector, t);
            }
            return new SelectorTuple(unwrappedSelector);
        }

        final Class<?> selectorImplClass = (Class<?>) maybeSelectorImplClass;
        /**
         * 重点
         * 创建SelectedSelectionKeySet通过反射替换掉sun.nio.ch.SelectorImpl类中selectedKeys和publicSelectedKeys的默认HashSet实现。
         * 为什么要用SelectedSelectionKeySet替换掉原来的HashSet呢？？
         * 因为这里涉及到对HashSet类型的sun.nio.ch.SelectorImpl#selectedKeys集合的两种操作：
         *      插入操作：
         *          通过前边对sun.nio.ch.SelectorImpl类中字段的介绍我们知道，在Selector监听到IO就绪的SelectionKey后，会将IO就绪的SelectionKey插入sun.nio.ch.SelectorImpl#selectedKeys集合中，这时Reactor线程会从java.nio.channels.Selector#select(long)阻塞调用中返回（类似epoll_wait）。
         *      遍历操作：
         *          Reactor线程返回后，会从Selector中获取IO就绪的SelectionKey集合（也就是sun.nio.ch.SelectorImpl#selectedKeys），Reactor线程遍历selectedKeys,获取IO就绪的SocketChannel，并处理SocketChannel上的IO事件。
         *
         * 我们都知道HashSet底层数据结构是一个哈希表，由于Hash冲突这种情况的存在，所以导致对哈希表进行插入和遍历操作的性能不如对数组进行插入和遍历操作的性能好。
         * 还有一个重要原因是，数组可以利用CPU缓存的优势来提高遍历的效率
         *
         * 所以Netty为了优化对sun.nio.ch.SelectorImpl#selectedKeys集合的插入，遍历性能，自己用数组这种数据结构实现了SelectedSelectionKeySet，用它来替换原来的HashSet实现。
         *
         * 优化点, 主要是这3个方面
         * 1. 初始化SelectionKey[] keys数组大小为1024，当数组容量不够时，扩容为原来的两倍大小。
         * 2. 通过数组尾部指针size，在向数组插入元素的时候可以直接定位到插入位置keys[size++]。操作一步到位，不用像哈希表那样还需要解决Hash冲突。
         * 3. 对数组的遍历操作也是如丝般顺滑，CPU直接可以在缓存行中遍历读取数组元素无需访问内存。比HashSet的迭代器java.util.HashMap.KeyIterator 遍历方式性能不知高到哪里去了。
         *
         * 那之前jdk用hashset, netty用数组, 去重怎么办? 自己处理完把这个SelectionKey移除. 不移除的话,因为 key 还在集合里，下次 select 又返回了，就空转了
         */
        final SelectedSelectionKeySet selectedKeySet = new SelectedSelectionKeySet();
        // 上面对象创建好了, 那怎么替换呢? 在下面, 反射调用替换的

        // Netty通过反射的方式用 SelectedSelectionKeySet 替换掉sun.nio.ch.SelectorImpl#selectedKeys，sun.nio.ch.SelectorImpl#publicSelectedKeys这两个集合中原来HashSet的实现
        Object maybeException = AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    // 反射获取sun.nio.ch.SelectorImpl类中selectedKeys和publicSelectedKeys
                    Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
                    Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");

                    // Java9版本以上通过sun.misc.Unsafe设置字段值的方式
                    if (PlatformDependent.javaVersion() >= 9 && PlatformDependent.hasUnsafe()) {
                        // Let us try to use sun.misc.Unsafe to replace the SelectionKeySet.
                        // This allows us to also do this in Java9+ without any extra flags.
                        long selectedKeysFieldOffset = PlatformDependent.objectFieldOffset(selectedKeysField);
                        long publicSelectedKeysFieldOffset =
                                PlatformDependent.objectFieldOffset(publicSelectedKeysField);

                        if (selectedKeysFieldOffset != -1 && publicSelectedKeysFieldOffset != -1) {
                            /**
                             * 这2个set替换为数组, 这边替换是替换同一个数组对象
                             * 本来在jdk源码里面这俩就是同一个set 只不过public那个只是另外一个的不可变视图
                             *
                             * SelectionKey 是给 jdk 内部用的，而 publicSelectedKeys 是给我们用户看的，所以这两个集合必须全部替换。如果只替换 SelectionKey，用户调用 selectedKeys() 就是一个空的，啥也看不到。
                             */
                            PlatformDependent.putObject(
                                    unwrappedSelector, selectedKeysFieldOffset, selectedKeySet);
                            PlatformDependent.putObject(
                                    unwrappedSelector, publicSelectedKeysFieldOffset, selectedKeySet);
                            return null;
                        }
                        // We could not retrieve the offset, lets try reflection as last-resort.
                    }

                    // 通过反射的方式用 SelectedSelectionKeySet 替换掉hashSet实现的sun.nio.ch.SelectorImpl#selectedKeys，sun.nio.ch.SelectorImpl#publicSelectedKeys
                    Throwable cause = ReflectionUtil.trySetAccessible(selectedKeysField, true);
                    if (cause != null) {
                        return cause;
                    }
                    cause = ReflectionUtil.trySetAccessible(publicSelectedKeysField, true);
                    if (cause != null) {
                        return cause;
                    }

                    //Java8反射替换字段
                    selectedKeysField.set(unwrappedSelector, selectedKeySet);
                    publicSelectedKeysField.set(unwrappedSelector, selectedKeySet);
                    return null;
                } catch (NoSuchFieldException e) {
                    return e;
                } catch (IllegalAccessException e) {
                    return e;
                }
            }
        });

        if (maybeException instanceof Exception) {
            selectedKeys = null;
            Exception e = (Exception) maybeException;
            logger.trace("failed to instrument a special java.util.Set into: {}", unwrappedSelector, e);
            return new SelectorTuple(unwrappedSelector);
        }
        /**
         * 将与sun.nio.ch.SelectorImpl类中selectedKeys和publicSelectedKeys关联好的Netty优化实现SelectedSelectionKeySet，
         * 设置到io.netty.channel.nio.NioEventLoop#selectedKeys字段中保存
         * 后续Reactor线程就会直接从io.netty.channel.nio.NioEventLoop#selectedKeys中获取IO就绪的SocketChannel
         */
        selectedKeys = selectedKeySet;
        logger.trace("instrumented a special java.util.Set into: {}", unwrappedSelector);

        /**
         * 用SelectorTuple封装unwrappedSelector和wrappedSelector返回给NioEventLoop构造函数。到此Reactor中的Selector就创建完毕了
         *
         * 所谓的unwrappedSelector是指被Netty优化过的JDK NIO原生Selector。
         * 所谓的wrappedSelector就是用SelectedSelectionKeySetSelector装饰类将unwrappedSelector和与sun.nio.ch.SelectorImpl类关联好的Netty优化实现SelectedSelectionKeySet封装装饰起来。
         */
        return new SelectorTuple(unwrappedSelector,
                /**
                 * 可以看下SelectedSelectionKeySetSelector
                 * wrappedSelector会将所有对Selector的操作全部代理给unwrappedSelector，并在发起轮询IO事件的相关操作中，重置SelectedSelectionKeySet清空上一次的轮询结果。
                 */
                new SelectedSelectionKeySetSelector(unwrappedSelector, selectedKeySet));
        // 到这里Reactor的核心Selector就创建好了
    }

    /**
     * Returns the {@link SelectorProvider} used by this {@link NioEventLoop} to obtain the {@link Selector}.
     */
    public SelectorProvider selectorProvider() {
        return provider;
    }

    @Override
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        return newTaskQueue0(maxPendingTasks);
    }

    private static Queue<Runnable> newTaskQueue0(int maxPendingTasks) {
        // This event loop never calls takeTask()
        /**
         * 根据 DEFAULT_MAX_PENDING_TASKS 变量的设定，来决定创建无界任务队列还是有界任务队列
         * Reactor内的异步任务队列的类型为 MpscQueue,它是由JCTools提供的一个高性能无锁队列，从命名前缀Mpsc可以看出，它适用于多生产者单消费者的场景，它支持多个生产者线程安全的访问队列，
         * 同一时刻只允许一个消费者线程读取队列中的元素。
         *
         * Netty中的Reactor可以线程安全的处理注册其上的多个 SocketChannel 上的IO数据，保证Reactor线程安全的核心原因正是因为这个MpscQueue，
         * 它可以支持多个业务线程在处理完业务逻辑后，线程安全的向 MpscQueue 添加异步写任务，然后由单个Reactor线程来执行这些写任务。
         * 既然是单线程执行，那肯定是线程安全的了。
         *
         * MpscQueue 存的比如是 channelHandlerContext.writeAndFlush(buffer) 这个任务
         * 就是你给 reactor 提交的所有异步或者定时任务，都会放在这个 mpsc 中
         */
        return maxPendingTasks == Integer.MAX_VALUE ? PlatformDependent.<Runnable>newMpscQueue()
                : PlatformDependent.<Runnable>newMpscQueue(maxPendingTasks);
    }

    /**
     * Registers an arbitrary {@link SelectableChannel}, not necessarily created by Netty, to the {@link Selector}
     * of this event loop.  Once the specified {@link SelectableChannel} is registered, the specified {@code task} will
     * be executed by this event loop when the {@link SelectableChannel} is ready.
     */
    public void register(final SelectableChannel ch, final int interestOps, final NioTask<?> task) {
        ObjectUtil.checkNotNull(ch, "ch");
        if (interestOps == 0) {
            throw new IllegalArgumentException("interestOps must be non-zero.");
        }
        if ((interestOps & ~ch.validOps()) != 0) {
            throw new IllegalArgumentException(
                    "invalid interestOps: " + interestOps + "(validOps: " + ch.validOps() + ')');
        }
        ObjectUtil.checkNotNull(task, "task");

        if (isShutdown()) {
            throw new IllegalStateException("event loop shut down");
        }

        if (inEventLoop()) {
            register0(ch, interestOps, task);
        } else {
            try {
                // Offload to the EventLoop as otherwise java.nio.channels.spi.AbstractSelectableChannel.register
                // may block for a long time while trying to obtain an internal lock that may be hold while selecting.
                submit(new Runnable() {
                    @Override
                    public void run() {
                        register0(ch, interestOps, task);
                    }
                }).sync();
            } catch (InterruptedException ignore) {
                // Even if interrupted we did schedule it so just mark the Thread as interrupted.
                Thread.currentThread().interrupt();
            }
        }
    }

    private void register0(SelectableChannel ch, int interestOps, NioTask<?> task) {
        try {
            ch.register(unwrappedSelector, interestOps, task);
        } catch (Exception e) {
            throw new EventLoopException("failed to register a channel", e);
        }
    }

    /**
     * Returns the percentage of the desired amount of time spent for I/O in the event loop.
     */
    public int getIoRatio() {
        return ioRatio;
    }

    /**
     * Sets the percentage of the desired amount of time spent for I/O in the event loop. Value range from 1-100.
     * The default value is {@code 50}, which means the event loop will try to spend the same amount of time for I/O
     * as for non-I/O tasks. The lower the number the more time can be spent on non-I/O tasks. If value set to
     * {@code 100}, this feature will be disabled and event loop will not attempt to balance I/O and non-I/O tasks.
     */
    public void setIoRatio(int ioRatio) {
        if (ioRatio <= 0 || ioRatio > 100) {
            throw new IllegalArgumentException("ioRatio: " + ioRatio + " (expected: 0 < ioRatio <= 100)");
        }
        this.ioRatio = ioRatio;
    }

    /**
     * Replaces the current {@link Selector} of this event loop with newly created {@link Selector}s to work
     * around the infamous epoll 100% CPU bug.
     */
    public void rebuildSelector() {
        if (!inEventLoop()) {
            execute(new Runnable() {
                @Override
                public void run() {
                    rebuildSelector0();
                }
            });
            return;
        }
        rebuildSelector0();
    }

    @Override
    public int registeredChannels() {
        return selector.keys().size() - cancelledKeys;
    }

    @Override
    public Iterator<Channel> registeredChannelsIterator() {
        assert inEventLoop();
        final Set<SelectionKey> keys = selector.keys();
        if (keys.isEmpty()) {
            return ChannelsReadOnlyIterator.empty();
        }
        return new Iterator<Channel>() {
            final Iterator<SelectionKey> selectionKeyIterator =
                    ObjectUtil.checkNotNull(keys, "selectionKeys")
                            .iterator();
            Channel next;
            boolean isDone;

            @Override
            public boolean hasNext() {
                if (isDone) {
                    return false;
                }
                Channel cur = next;
                if (cur == null) {
                    cur = next = nextOrDone();
                    return cur != null;
                }
                return true;
            }

            @Override
            public Channel next() {
                if (isDone) {
                    throw new NoSuchElementException();
                }
                Channel cur = next;
                if (cur == null) {
                    cur = nextOrDone();
                    if (cur == null) {
                        throw new NoSuchElementException();
                    }
                }
                next = nextOrDone();
                return cur;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove");
            }

            private Channel nextOrDone() {
                Iterator<SelectionKey> it = selectionKeyIterator;
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    if (key.isValid()) {
                        Object attachment = key.attachment();
                        if (attachment instanceof AbstractNioChannel) {
                            return (AbstractNioChannel) attachment;
                        }
                    }
                }
                isDone = true;
                return null;
            }
        };
    }

    private void rebuildSelector0() {
        final Selector oldSelector = selector;
        final SelectorTuple newSelectorTuple;

        if (oldSelector == null) {
            return;
        }

        try {
            newSelectorTuple = openSelector();
        } catch (Exception e) {
            logger.warn("Failed to create a new Selector.", e);
            return;
        }

        // Register all channels to the new Selector.
        int nChannels = 0;
        for (SelectionKey key: oldSelector.keys()) {
            Object a = key.attachment();
            try {
                if (!key.isValid() || key.channel().keyFor(newSelectorTuple.unwrappedSelector) != null) {
                    continue;
                }

                int interestOps = key.interestOps();
                key.cancel();
                SelectionKey newKey = key.channel().register(newSelectorTuple.unwrappedSelector, interestOps, a);
                if (a instanceof AbstractNioChannel) {
                    // Update SelectionKey
                    ((AbstractNioChannel) a).selectionKey = newKey;
                }
                nChannels ++;
            } catch (Exception e) {
                logger.warn("Failed to re-register a Channel to the new Selector.", e);
                if (a instanceof AbstractNioChannel) {
                    AbstractNioChannel ch = (AbstractNioChannel) a;
                    ch.unsafe().close(ch.unsafe().voidPromise());
                } else {
                    @SuppressWarnings("unchecked")
                    NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                    invokeChannelUnregistered(task, key, e);
                }
            }
        }

        selector = newSelectorTuple.selector;
        unwrappedSelector = newSelectorTuple.unwrappedSelector;

        try {
            // time to close the old selector as everything else is registered to the new one
            oldSelector.close();
        } catch (Throwable t) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to close the old Selector.", t);
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("Migrated " + nChannels + " channel(s) to the new Selector.");
        }
    }

    /**
     * ┌─────────────────────────────┐
     * │ Reactor线程工作流程起点      │
     * └─────────────┬───────────────┘
     *               │
     *               ▼
     *     ┌─────────────────────────┐
     *     │ 检查是否有异步任务需要执行 │
     *     └───────┬─────────┬───────┘
     *             │Yes      │No
     *             ▼         ▼
     * ┌────────────────┐   ┌────────────────────┐
     * │ 非阻塞轮询Selector │   │ 检查是否有定时任务   │
     * └────────┬───────┘   └─────────┬──────────┘
     *          │                      │
     *          ▼                      ▼
     *     ┌─────────────┐       ┌─────────────────────┐
     *     │   是否有IO事件? │       │ No: selector阻塞直到 │
     *     └──────┬──────┘       │     满足唤醒条件      │
     *            │Yes            └─────────┬───────────┘
     *            ▼                        │
     *  ┌───────────────────┐              │
     *  │ 处理IO就绪事件        │◄───────────┘
     *  └──────────┬────────┘
     *             │
     *             ▼
     *     ┌─────────────────┐
     *     │ 执行异步任务       │
     *     └─────────────────┘
     *
     * ────────────────────────────────────────────────
     * 说明：
     * 1. 如果此时有IO事件 → IO事件和异步任务一起执行
     * 2. 如果没有IO事件   → 不会阻塞，马上执行异步任务
     * 3. 定时任务会在Selector上注册deadline，或等待其他就绪事件
     *
     * Netty框架中的异步任务分为三类：
     * 存放在普通任务队列taskQueue中的普通异步任务。
     * 存放在尾部队列tailTasks中的用于执行统计任务等收尾动作的尾部任务。
     * 还有一种就是这里即将提到的定时任务。存放在Reactor中的定时任务队列scheduledTaskQueue中。
     *
     */
    @Override
    protected void run() {
        /**
         * jdk 空轮询的bug: https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6670302
         * 记录轮询次数 用于解决JDK epoll的空轮训bug
         */
        int selectCnt = 0;
        for (;;) {
            try {
                //轮询结果
                int strategy;
                try {
                    /**
                     * {@link DefaultSelectStrategy#calculateStrategy(IntSupplier, boolean)}
                     * 根据轮询策略获取轮询结果 这里的hasTasks()主要检查的是普通队列和尾部队列中是否有异步任务等待执行
                     * selectSupplier 就是 {@link NioEventLoop#selectNowSupplier} 即 selectNow() - 非阻塞轮询, 立即检查是否有 I/O 事件，不阻塞线程
                     *
                     * 1. 有任务待处理 (hasTasks = true)：
                     *     - 执行 selectSupplier.get() 即 selectNow() - 非阻塞轮询
                     *     - 立即检查是否有 I/O 事件，不阻塞线程
                     *   2. 无任务待处理 (hasTasks = false)：
                     *     - 返回 SelectStrategy.SELECT - 阻塞轮询
                     *     - 让线程阻塞等待 I/O 事件
                     *
                     * 这里需要注意的是netty只会自动注册OP_READ事件，而OP_WRITE事件是在当Socket写入缓冲区以满无法继续写入发送数据时由用户自己注册。
                     *
                     * 如果Reactor中有异步任务需要执行，那么Reactor线程需要立即执行，不能阻塞在Selector上。
                     * 在返回前需要再顺带调用selectNow()非阻塞查看一下当前是否有IO就绪事件发生。如果有，那么正好可以和异步任务一起被处理，如果没有，则及时地处理异步任务。
                     *
                     * 这里Netty要表达的语义是：
                     * 首先Reactor线程需要优先保证IO就绪事件的处理，然后在保证异步任务的及时执行。
                     * 如果当前没有IO就绪事件但是有异步任务需要执行时，Reactor线程就要去及时执行异步任务而不是继续阻塞在Selector上等待IO就绪事件。
                     */
                    strategy = selectStrategy.calculateStrategy(selectNowSupplier, hasTasks());
                    switch (strategy) {
                    case SelectStrategy.CONTINUE: // 重新开启一轮IO轮询
                        continue;

                    case SelectStrategy.BUSY_WAIT: // Reactor线程进行自旋轮询，由于NIO 不支持自旋操作，所以这里直接跳到SelectStrategy.SELECT策略。
                        // NIO不支持自旋（BUSY_WAIT）
                        // fall-through to SELECT since the busy-wait is not supported with NIO

                    case SelectStrategy.SELECT: // 此时没有任何异步任务需要执行，Reactor线程可以安心的阻塞在Selector上等待IO就绪事件的来临
                        // 核心逻辑是有任务需要执行，则Reactor线程立马执行异步任务，如果没有异步任务执行，则进行轮询IO事件

                        /**
                         *  获取下一个定时任务的截止时间
                         *  从定时任务队列中取出即将快要执行的定时任务deadline
                         */
                        long curDeadlineNanos = nextScheduledTaskDeadlineNanos();
                        if (curDeadlineNanos == -1L) {
                            // 没有定时任务，可以无限阻塞
                            curDeadlineNanos = NONE; // nothing on the calendar
                        }

                        /**
                         * 设置预期的唤醒时间
                         * 最早执行定时任务的deadline作为 select的阻塞时间，意思是到了定时任务的执行时间
                         * 不管有无IO就绪事件，必须唤醒selector，从而使reactor线程执行定时任务
                         */
                        nextWakeupNanos.set(curDeadlineNanos);
                        try {
                            // 如果没有立即要执行的任务，才进行 select 阻塞
                            if (!hasTasks()) {
                                /**
                                 * 根据 deadline 决定阻塞时间
                                 * 再次检查普通任务队列中是否有异步任务
                                 * 没有的话开始select阻塞轮询IO就绪事件
                                 */
                                strategy = select(curDeadlineNanos);
                            }
                        } finally {
                            // This update is just to help block unnecessary selector wakeups
                            // so use of lazySet is ok (no race condition)
                            /**
                             * 执行到这里说明Reactor已经从Selector上被唤醒了
                             * 设置Reactor的状态为苏醒状态AWAKE
                             * lazySet优化不必要的volatile操作，不使用内存屏障，不保证写操作的可见性（单线程不需要保证）
                             */
                            nextWakeupNanos.lazySet(AWAKE);
                        }
                        // fall through
                    default:
                    }
                } catch (IOException e) {
                    // If we receive an IOException here its because the Selector is messed up. Let's rebuild
                    // the selector and retry. https://github.com/netty/netty/issues/8566
                    rebuildSelector0();
                    selectCnt = 0;
                    handleLoopException(e);
                    continue;
                }

                // 执行到这里说明满足了唤醒条件，Reactor线程从selector上被唤醒开始处理IO就绪事件和执行异步任务

                /**
                 * Reactor线程需要保证及时的执行异步任务，只要有异步任务提交，就需要退出轮询。
                 * 有IO事件就优先处理IO事件，然后处理异步任务
                 */

                selectCnt++;
                cancelledKeys = 0;
                //主要用于从IO就绪的SelectedKeys集合中剔除已经失效的selectKey
                needsToSelectAgain = false;
                //调整Reactor线程执行IO事件和执行异步任务的CPU时间比例 默认50，表示执行IO事件和异步任务的时间比例是一比一
                final int ioRatio = this.ioRatio;
                /**
                 * 这里主要处理IO就绪事件，以及执行异步任务
                 * 需要优先处理IO就绪事件，然后根据ioRatio设置的处理IO事件CPU用时与异步任务CPU用时比例，
                 * 来决定执行多长时间的异步任务
                 */

                boolean ranTasks;
                if (ioRatio == 100) {
                    try {
                        if (strategy > 0) {
                            processSelectedKeys();
                        }
                    } finally {
                        // Ensure we always run tasks.
                        ranTasks = runAllTasks();
                    }
                } else if (strategy > 0) {
                    final long ioStartTime = System.nanoTime();
                    try {
                        processSelectedKeys();
                    } finally {
                        // Ensure we always run tasks.
                        final long ioTime = System.nanoTime() - ioStartTime;
                        ranTasks = runAllTasks(ioTime * (100 - ioRatio) / ioRatio);
                    }
                } else {
                    ranTasks = runAllTasks(0); // This will run the minimum number of tasks
                }

                //判断是否触发JDK Epoll BUG 触发空轮询
                if (ranTasks || strategy > 0) {
                    if (selectCnt > MIN_PREMATURE_SELECTOR_RETURNS && logger.isDebugEnabled()) {
                        logger.debug("Selector.select() returned prematurely {} times in a row for Selector {}.",
                                selectCnt - 1, selector);
                    }
                    selectCnt = 0;
                } else if (unexpectedSelectorWakeup(selectCnt)) { // Unexpected wakeup (unusual case)

                    //既没有IO就绪事件，也没有异步任务，Reactor线程从Selector上被异常唤醒 触发JDK Epoll空轮训BUG
                    //重新构建Selector,selectCnt归零
                    selectCnt = 0;
                }
            } catch (CancelledKeyException e) {
                // Harmless exception - log anyway
                if (logger.isDebugEnabled()) {
                    logger.debug(CancelledKeyException.class.getSimpleName() + " raised by a Selector {} - JDK bug?",
                            selector, e);
                }
            } catch (Error e) {
                throw e;
            } catch (Throwable t) {
                handleLoopException(t);
            } finally {
                // Always handle shutdown even if the loop processing threw an exception.
                try {
                    if (isShuttingDown()) {
                        closeAll();
                        if (confirmShutdown()) {
                            return;
                        }
                    }
                } catch (Error e) {
                    throw e;
                } catch (Throwable t) {
                    handleLoopException(t);
                }
            }
        }
    }

    // returns true if selectCnt should be reset
    private boolean unexpectedSelectorWakeup(int selectCnt) {
        if (Thread.interrupted()) {
            // Thread was interrupted so reset selected keys and break so we not run into a busy loop.
            // As this is most likely a bug in the handler of the user or it's client library we will
            // also log it.
            //
            // See https://github.com/netty/netty/issues/2426
            if (logger.isDebugEnabled()) {
                logger.debug("Selector.select() returned prematurely because " +
                        "Thread.currentThread().interrupt() was called. Use " +
                        "NioEventLoop.shutdownGracefully() to shutdown the NioEventLoop.");
            }
            return true;
        }
        if (SELECTOR_AUTO_REBUILD_THRESHOLD > 0 &&
                selectCnt >= SELECTOR_AUTO_REBUILD_THRESHOLD) {
            // The selector returned prematurely many times in a row.
            // Rebuild the selector to work around the problem.
            logger.warn("Selector.select() returned prematurely {} times in a row; rebuilding Selector {}.",
                    selectCnt, selector);
            rebuildSelector();
            return true;
        }
        return false;
    }

    private static void handleLoopException(Throwable t) {
        logger.warn("Unexpected exception in the selector loop.", t);

        // Prevent possible consecutive immediate failures that lead to
        // excessive CPU consumption.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }

    private void processSelectedKeys() {
        if (selectedKeys != null) {
            processSelectedKeysOptimized();
        } else {
            processSelectedKeysPlain(selector.selectedKeys());
        }
    }

    @Override
    protected void cleanup() {
        try {
            selector.close();
        } catch (IOException e) {
            logger.warn("Failed to close a selector.", e);
        }
    }

    void cancel(SelectionKey key) {
        key.cancel();
        cancelledKeys ++;
        if (cancelledKeys >= CLEANUP_INTERVAL) {
            cancelledKeys = 0;
            needsToSelectAgain = true;
        }
    }

    private void processSelectedKeysPlain(Set<SelectionKey> selectedKeys) {
        // check if the set is empty and if so just return to not create garbage by
        // creating a new Iterator every time even if there is nothing to process.
        // See https://github.com/netty/netty/issues/597
        if (selectedKeys.isEmpty()) {
            return;
        }

        Iterator<SelectionKey> i = selectedKeys.iterator();
        for (;;) {
            final SelectionKey k = i.next();
            final Object a = k.attachment();
            i.remove();

            if (a instanceof AbstractNioChannel) {
                processSelectedKey(k, (AbstractNioChannel) a);
            } else {
                @SuppressWarnings("unchecked")
                NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                processSelectedKey(k, task);
            }

            if (!i.hasNext()) {
                break;
            }

            if (needsToSelectAgain) {
                selectAgain();
                selectedKeys = selector.selectedKeys();

                // Create the iterator again to avoid ConcurrentModificationException
                if (selectedKeys.isEmpty()) {
                    break;
                } else {
                    i = selectedKeys.iterator();
                }
            }
        }
    }

    private void processSelectedKeysOptimized() {
        for (int i = 0; i < selectedKeys.size; ++i) {
            final SelectionKey k = selectedKeys.keys[i];
            // null out entry in the array to allow to have it GC'ed once the Channel close
            // See https://github.com/netty/netty/issues/2363
            selectedKeys.keys[i] = null;

            final Object a = k.attachment();

            if (a instanceof AbstractNioChannel) {
                processSelectedKey(k, (AbstractNioChannel) a);
            } else {
                @SuppressWarnings("unchecked")
                NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                processSelectedKey(k, task);
            }

            if (needsToSelectAgain) {
                // null out entries in the array to allow to have it GC'ed once the Channel close
                // See https://github.com/netty/netty/issues/2363
                selectedKeys.reset(i + 1);

                selectAgain();
                i = -1;
            }
        }
    }

    private void processSelectedKey(SelectionKey k, AbstractNioChannel ch) {
        final AbstractNioChannel.NioUnsafe unsafe = ch.unsafe();
        if (!k.isValid()) {
            final EventLoop eventLoop;
            try {
                eventLoop = ch.eventLoop();
            } catch (Throwable ignored) {
                // If the channel implementation throws an exception because there is no event loop, we ignore this
                // because we are only trying to determine if ch is registered to this event loop and thus has authority
                // to close ch.
                return;
            }
            // Only close ch if ch is still registered to this EventLoop. ch could have deregistered from the event loop
            // and thus the SelectionKey could be cancelled as part of the deregistration process, but the channel is
            // still healthy and should not be closed.
            // See https://github.com/netty/netty/issues/5125
            if (eventLoop == this) {
                // close the channel if the key is not valid anymore
                unsafe.close(unsafe.voidPromise());
            }
            return;
        }

        try {
            int readyOps = k.readyOps();
            // We first need to call finishConnect() before try to trigger a read(...) or write(...) as otherwise
            // the NIO JDK channel implementation may throw a NotYetConnectedException.
            if ((readyOps & SelectionKey.OP_CONNECT) != 0) {
                // remove OP_CONNECT as otherwise Selector.select(..) will always return without blocking
                // See https://github.com/netty/netty/issues/924
                int ops = k.interestOps();
                ops &= ~SelectionKey.OP_CONNECT;
                k.interestOps(ops);

                unsafe.finishConnect();
            }

            // Process OP_WRITE first as we may be able to write some queued buffers and so free memory.
            if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                // Call forceFlush which will also take care of clear the OP_WRITE once there is nothing left to write
               unsafe.forceFlush();
            }

            // Also check for readOps of 0 to workaround possible JDK bug which may otherwise lead
            // to a spin loop
            if ((readyOps & (SelectionKey.OP_READ | SelectionKey.OP_ACCEPT)) != 0 || readyOps == 0) {
                unsafe.read();
            }
        } catch (CancelledKeyException ignored) {
            unsafe.close(unsafe.voidPromise());
        }
    }

    private static void processSelectedKey(SelectionKey k, NioTask<SelectableChannel> task) {
        int state = 0;
        try {
            task.channelReady(k.channel(), k);
            state = 1;
        } catch (Exception e) {
            k.cancel();
            invokeChannelUnregistered(task, k, e);
            state = 2;
        } finally {
            switch (state) {
            case 0:
                k.cancel();
                invokeChannelUnregistered(task, k, null);
                break;
            case 1:
                if (!k.isValid()) { // Cancelled by channelReady()
                    invokeChannelUnregistered(task, k, null);
                }
                break;
            default:
                 break;
            }
        }
    }

    private void closeAll() {
        selectAgain();
        Set<SelectionKey> keys = selector.keys();
        Collection<AbstractNioChannel> channels = new ArrayList<AbstractNioChannel>(keys.size());
        for (SelectionKey k: keys) {
            Object a = k.attachment();
            if (a instanceof AbstractNioChannel) {
                channels.add((AbstractNioChannel) a);
            } else {
                k.cancel();
                @SuppressWarnings("unchecked")
                NioTask<SelectableChannel> task = (NioTask<SelectableChannel>) a;
                invokeChannelUnregistered(task, k, null);
            }
        }

        for (AbstractNioChannel ch: channels) {
            ch.unsafe().close(ch.unsafe().voidPromise());
        }
    }

    private static void invokeChannelUnregistered(NioTask<SelectableChannel> task, SelectionKey k, Throwable cause) {
        try {
            task.channelUnregistered(k.channel(), cause);
        } catch (Exception e) {
            logger.warn("Unexpected exception while running NioTask.channelUnregistered()", e);
        }
    }

    @Override
    protected void wakeup(boolean inEventLoop) {
        if (!inEventLoop && nextWakeupNanos.getAndSet(AWAKE) != AWAKE) {
            selector.wakeup();
        }
    }

    @Override
    protected boolean beforeScheduledTaskSubmitted(long deadlineNanos) {
        // Note this is also correct for the nextWakeupNanos == -1 (AWAKE) case
        return deadlineNanos < nextWakeupNanos.get();
    }

    @Override
    protected boolean afterScheduledTaskSubmitted(long deadlineNanos) {
        // Note this is also correct for the nextWakeupNanos == -1 (AWAKE) case
        return deadlineNanos < nextWakeupNanos.get();
    }

    Selector unwrappedSelector() {
        return unwrappedSelector;
    }

    int selectNow() throws IOException {
        return selector.selectNow();
    }

    private int select(long deadlineNanos) throws IOException {
        if (deadlineNanos == NONE) {
            return selector.select();
        }
        // Timeout will only be 0 if deadline is within 5 microsecs
        long timeoutMillis = deadlineToDelayNanos(deadlineNanos + 995000L) / 1000000L;
        return timeoutMillis <= 0 ? selector.selectNow() : selector.select(timeoutMillis);
    }

    private void selectAgain() {
        needsToSelectAgain = false;
        try {
            selector.selectNow();
        } catch (Throwable t) {
            logger.warn("Failed to update SelectionKeys.", t);
        }
    }
}
