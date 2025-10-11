/*
 * Copyright 2015 The Netty Project
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
package io.netty.channel;

import static io.netty.util.internal.ObjectUtil.checkPositive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.UncheckedBooleanSupplier;

/**
 * Default implementation of {@link MaxMessagesRecvByteBufAllocator} which respects {@link ChannelConfig#isAutoRead()}
 * and also prevents overflow.
 */
public abstract class DefaultMaxMessagesRecvByteBufAllocator implements MaxMessagesRecvByteBufAllocator {
    private final boolean ignoreBytesRead;
    private volatile int maxMessagesPerRead;
    private volatile boolean respectMaybeMoreData = true;

    public DefaultMaxMessagesRecvByteBufAllocator() {
        this(1);
    }

    public DefaultMaxMessagesRecvByteBufAllocator(int maxMessagesPerRead) {
        this(maxMessagesPerRead, false);
    }

    DefaultMaxMessagesRecvByteBufAllocator(int maxMessagesPerRead, boolean ignoreBytesRead) {
        this.ignoreBytesRead = ignoreBytesRead;
        maxMessagesPerRead(maxMessagesPerRead);
    }

    @Override
    public int maxMessagesPerRead() {
        return maxMessagesPerRead;
    }

    @Override
    public MaxMessagesRecvByteBufAllocator maxMessagesPerRead(int maxMessagesPerRead) {
        checkPositive(maxMessagesPerRead, "maxMessagesPerRead");
        this.maxMessagesPerRead = maxMessagesPerRead;
        return this;
    }

    /**
     * Determine if future instances of {@link #newHandle()} will stop reading if we think there is no more data.
     * @param respectMaybeMoreData
     * <ul>
     *     <li>{@code true} to stop reading if we think there is no more data. This may save a system call to read from
     *          the socket, but if data has arrived in a racy fashion we may give up our {@link #maxMessagesPerRead()}
     *          quantum and have to wait for the selector to notify us of more data.</li>
     *     <li>{@code false} to keep reading (up to {@link #maxMessagesPerRead()}) or until there is no data when we
     *          attempt to read.</li>
     * </ul>
     * @return {@code this}.
     */
    public DefaultMaxMessagesRecvByteBufAllocator respectMaybeMoreData(boolean respectMaybeMoreData) {
        this.respectMaybeMoreData = respectMaybeMoreData;
        return this;
    }

    /**
     * Get if future instances of {@link #newHandle()} will stop reading if we think there is no more data.
     * @return
     * <ul>
     *     <li>{@code true} to stop reading if we think there is no more data. This may save a system call to read from
     *          the socket, but if data has arrived in a racy fashion we may give up our {@link #maxMessagesPerRead()}
     *          quantum and have to wait for the selector to notify us of more data.</li>
     *     <li>{@code false} to keep reading (up to {@link #maxMessagesPerRead()}) or until there is no data when we
     *          attempt to read.</li>
     * </ul>
     */
    public final boolean respectMaybeMoreData() {
        return respectMaybeMoreData;
    }

    /**
     * Focuses on enforcing the maximum messages per read condition for {@link #continueReading()}.
     */
    public abstract class MaxMessageHandle implements ExtendedHandle {

        /**
         * Netty 会在一个 read loop 中不停的读取 socket 中的数据直到数据被读取完毕或者读满 16 次，结束 read loop 停止读取。
         * ByteBuf 越大那么 Netty 读取的次数就越少，ByteBuf 越小那么 Netty 读取的次数就越多，所以需要一种机制将 ByteBuf 的容量控制在一个合理的范围内。
         */


        private ChannelConfig config;

        //用于控制每次read loop里最大可以循环读取的次数，默认为16次
        //可在启动配置类ServerBootstrap中通过ChannelOption.MAX_MESSAGES_PER_READ选项设置。
        private int maxMessagePerRead;

        //本次事件轮询总共读取的message数,这里指的是接收连接的数量
        //用于统计read loop中总共接收的连接个数，NioSocketChannel中表示读取数据的次数
        //每次read loop循环后会调用allocHandle.incMessagesRead增加记录接收到的连接个数
        private int totalMessages;

        //本次事件轮询总共读取的字节数
        //用于统计在read loop中总共接收到客户端连接上的数据大小
        private int totalBytesRead;

        //表示本次read loop 尝试读取多少字节，byteBuffer剩余可写的字节数
        private int attemptedBytesRead;

        //本次read loop读取到的字节数
        private int lastBytesRead;

        private final boolean respectMaybeMoreData = DefaultMaxMessagesRecvByteBufAllocator.this.respectMaybeMoreData;
        private final UncheckedBooleanSupplier defaultMaybeMoreSupplier = new UncheckedBooleanSupplier() {
            @Override
            public boolean get() {
                return attemptedBytesRead == lastBytesRead;
            }
        };

        /**
         * Only {@link ChannelConfig#getMaxMessagesPerRead()} is used.
         *
         * 每次在使用allocHandle前需要调用allocHandle.reset(config);重置里边的统计指标
         */
        @Override
        public void reset(ChannelConfig config) {
            this.config = config;
            //默认每次最多读取16次
            maxMessagePerRead = maxMessagesPerRead();
            totalMessages = totalBytesRead = 0;
        }

        @Override
        public ByteBuf allocate(ByteBufAllocator alloc) {
            /**
             * 默认初始化大小为2048，这个容量由guess()方法决定
             *
             */
            return alloc.ioBuffer(guess());
        }

        @Override
        public final void incMessagesRead(int amt) {
            // totalMessages：用于统计read loop中总共接收的连接个数，每次read loop循环后会调用allocHandle.incMessagesRead增加记录接收到的连接个数
            totalMessages += amt;
        }

        /**
         * lastBytesRead < 0：表示客户端主动发起了连接关闭流程，Netty开始连接关闭处理流程
         *
         * lastBytesRead = 0：表示当前NioSocketChannel上的数据已经全部读取完毕，没有数据可读了。本次OP_READ事件圆满处理完毕，可以开开心心的退出read loop。
         *
         * 当lastBytesRead > 0：表示在本次read loop中从NioSocketChannel中读取到了数据，会在NioSocketChannel的pipeline中触发ChannelRead事件。进而在pipeline中负责IO处理的ChannelHandler中响应，处理网络请求。
         */
        @Override
        public void lastBytesRead(int bytes) {
            lastBytesRead = bytes;
            if (bytes > 0) {
                /**
                 * totalBytesRead：用于统计在read loop中总共接收到客户端连接上的数据大小，这个字段主要用于sub reactor在接收客户端NioSocketChannel上的网络数据用的，
                 * 如果是main reactor接收客户端连接，所以这里并不会用到这个字段。
                 * 这个字段会在sub reactor每次读取完NioSocketChannel上的网络数据时增加记录。
                 */
                totalBytesRead += bytes;
            }
        }

        @Override
        public final int lastBytesRead() {
            return lastBytesRead;
        }

        @Override
        public boolean continueReading() {
            return continueReading(defaultMaybeMoreSupplier);
        }

        @Override
        public boolean continueReading(UncheckedBooleanSupplier maybeMoreDataSupplier) {
            /**
             * 是否继续进行read loop需要同时满足以下几个条件：
             *
             * totalMessages < maxMessagePerRead 当前读取次数是否已经超过16次，如果超过，就退出do(...)while()循环。进行下一轮OP_READ事件的轮询。
             * 因为每个Sub Reactor管理了多个NioSocketChannel，不能在一个NioSocketChannel上占用太多时间，要将机会均匀地分配给Sub Reactor所管理的所有NioSocketChannel。
             *
             * totalBytesRead > 0 本次OP_READ事件处理是否读取到了数据，如果已经没有数据可读了，那么就直接退出read loop。
             *
             * !respectMaybeMoreData || maybeMoreDataSupplier.get() 这个条件比较复杂，它其实就是通过respectMaybeMoreData字段来控制NioSocketChannel中可能还有数据可读的情况下该如何处理。
             *
             * maybeMoreDataSupplier.get()：true表示本次读取从NioSocketChannel中读取数据，ByteBuffer满载而归。
             * 说明可能NioSocketChannel中还有数据没读完。false表示ByteBuffer还没有装满，说明NioSocketChannel中已经没有数据可读了。
             *
             *
             * respectMaybeMoreData = true表示要对可能还有更多数据进行处理的这种情况要respect认真对待,
             * 如果本次循环读取到的数据已经装满ByteBuffer，表示后面可能还有数据，那么就要进行读取。
             * 如果ByteBuffer还没装满表示已经没有数据可读了那么就退出循环。
             *
             * respectMaybeMoreData = false表示对可能还有更多数据的这种情况不认真对待 not respect。
             * 不管本次循环读取数据ByteBuffer是否满载而归，都要继续进行读取，直到读取不到数据在退出循环，属于无脑读取。
             *
             * 同时满足以上三个条件，那么read loop继续进行。继续从NioSocketChannel中读取数据，直到读取不到或者不满足三个条件中的任意一个为止
             */
            return config.isAutoRead() &&
                   (!respectMaybeMoreData || maybeMoreDataSupplier.get()) &&
                   totalMessages < maxMessagePerRead && (ignoreBytesRead || totalBytesRead > 0);
        }

        @Override
        public void readComplete() {
        }

        @Override
        public int attemptedBytesRead() {
            return attemptedBytesRead;
        }

        @Override
        public void attemptedBytesRead(int bytes) {
            attemptedBytesRead = bytes;
        }

        protected final int totalBytesRead() {
            return totalBytesRead < 0 ? Integer.MAX_VALUE : totalBytesRead;
        }
    }
}
