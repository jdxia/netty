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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.IOException;
import java.net.PortUnreachableException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link AbstractNioChannel} base class for {@link Channel}s that operate on messages.
 *
 * AbstractNioMessageChannel类主要是对NioServerSocketChannel底层读写行为的封装和定义，比如accept接收客户端连接
 *
 * 服务端NioServerSocketChannel主要负责处理OP_ACCEPT事件，创建用于通信的客户端NioSocketChannel。这时候客户端与服务端还没开始通信，
 * 所以Main Reactor线程从NioServerSocketChannel的读取对象为Message。这里的Message指的就是底层的SocketChannel客户端连接。
 */
public abstract class AbstractNioMessageChannel extends AbstractNioChannel {
    boolean inputShutdown;

    /**
     * @see AbstractNioChannel#AbstractNioChannel(Channel, SelectableChannel, int)
     */
    protected AbstractNioMessageChannel(Channel parent, SelectableChannel ch, int readInterestOp) {
        super(parent, ch, readInterestOp);
    }

    @Override
    protected AbstractNioUnsafe newUnsafe() {
        return new NioMessageUnsafe();
    }

    @Override
    protected void doBeginRead() throws Exception {
        if (inputShutdown) {
            return;
        }
        super.doBeginRead();
    }

    protected boolean continueReading(RecvByteBufAllocator.Handle allocHandle) {
        return allocHandle.continueReading();
    }

    private final class NioMessageUnsafe extends AbstractNioUnsafe {

        //存放连接建立后，创建的客户端SocketChannel
        private final List<Object> readBuf = new ArrayList<Object>();

        /**
         * main reactor线程是在一个do...while{...}循环read loop中不断的调用JDK NIO serverSocketChannel.accept()方法来接收完成三次握手的客户端连接NioSocketChannel的，
         * 并将接收到的客户端连接NioSocketChannel临时保存在List<Object> readBuf集合中，后续会服务端NioServerSocketChannel的pipeline中通过ChannelRead事件来传递，
         * 最终会在ServerBootstrapAcceptor这个ChannelHandler中被处理初始化，并将其注册到Sub Reactor Group中。
         * 这里的read loop循环会被限定只能读取16次，当main reactor从NioServerSocketChannel中读取客户端连接NioSocketChannel的次数达到16次之后，
         * 无论此时是否还有客户端连接都不能在继续读取了
         *
         */
        @Override
        public void read() {
            //必须在Main Reactor线程中执行
            assert eventLoop().inEventLoop();

            //注意下面的config和pipeline都是服务端ServerSocketChannel中的
            final ChannelConfig config = config();
            final ChannelPipeline pipeline = pipeline();

            /**
             * 创建接收数据Buffer分配器（用于分配容量大小合适的byteBuffer用来容纳接收数据）
             * 在接收连接的场景中，这里的allocHandle只是用于控制read loop的循环读取创建连接的次数。
             */
            final RecvByteBufAllocator.Handle allocHandle = unsafe().recvBufAllocHandle();
            allocHandle.reset(config);

            boolean closed = false;
            Throwable exception = null;
            try {
                try {
                    do {

                        /**
                         *  {@link NioServerSocketChannel#doReadMessages(List)}
                         * 底层调用NioServerSocketChannel->doReadMessages 创建客户端SocketChannel
                         * 接收完成三次握手的客户端连接，底层会调用到JDK NIO ServerSocketChannel的accept方法
                         *
                         * localRead正常情况下都会返回1，当localRead <= 0时意味着已经没有新的客户端连接可以接收了
                         *
                         * 对于服务端NioServerSocketChannel来说，它上边的IO数据就是客户端的连接，它的长度和类型都是固定的，
                         * 所以在接收客户端连接的时候并不需要这样的一个ByteBuffer来接收，我们会将接收到的客户端连接存放在List<Object> readBuf集合中
                         */
                        int localRead = doReadMessages(readBuf);

                        //已无新的连接可接收则退出read loop
                        if (localRead == 0) {
                            break;
                        }
                        if (localRead < 0) {
                            closed = true;
                            break;
                        }

                        //统计在当前事件循环中已经读取到得Message数量（创建连接的个数）
                        allocHandle.incMessagesRead(localRead);
                    } while (continueReading(allocHandle)); //判断是否已经读满16次
                } catch (Throwable t) {
                    exception = t;
                }

                // 获取接收到的客户端连接
                int size = readBuf.size();
                for (int i = 0; i < size; i ++) {
                    readPending = false;

                    /**
                     * 在NioServerSocketChannel对应的pipeline中传播ChannelRead事件
                     *
                     * 最终pipeline中的ChannelHandler(ServerBootstrapAcceptor)会响应ChannelRead事件，并在相应回调函数中初始化客户端NioSocketChannel，
                     * {@link ServerBootstrap.ServerBootstrapAcceptor#channelRead(ChannelHandlerContext, Object)}
                     * 并将其注册到Sub Reactor Group中。
                     * 此后客户端NioSocketChannel绑定到的sub reactor就开始监听处理客户端连接上的读写事件了
                     *
                     * ServerBootstrapAcceptor主要的作用就是初始化客户端NioSocketChannel，并将客户端NioSocketChannel注册到Sub Reactor Group中，并监听OP_READ事件
                     *
                     */
                    pipeline.fireChannelRead(readBuf.get(i));
                }

                //清除本次accept 创建的客户端SocketChannel集合
                readBuf.clear();
                allocHandle.readComplete();

                /**
                 * ChannelRead事件：一次循环读取一次数据，就触发一次ChannelRead事件。
                 * 本次最多读取在read loop循环开始分配的DirectByteBuffer容量大小。这个容量会动态调整。
                 *
                 * ChannelReadComplete事件：当读取不到数据或者不满足continueReading的任意一个条件就会退出read loop，
                 * 这时就会触发ChannelReadComplete事件。表示本次OP_READ事件处理完毕。
                 *
                 * 这里需要特别注意下触发ChannelReadComplete事件并不代表NioSocketChannel中的数据已经读取完了，
                 * 只能说明本次OP_READ事件处理完毕。因为有可能是客户端发送的数据太多，Netty读了16次还没读完，
                 * 那就只能等到下次OP_READ事件到来的时候在进行读取了。
                 */

                //触发readComplete事件传播
                pipeline.fireChannelReadComplete();

                if (exception != null) {
                    closed = closeOnReadError(exception);

                    pipeline.fireExceptionCaught(exception);
                }

                if (closed) {
                    inputShutdown = true;
                    if (isOpen()) {
                        close(voidPromise());
                    }
                }
            } finally {
                // Check if there is a readPending which was not processed yet.
                // This could be for two reasons:
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelRead(...) method
                // * The user called Channel.read() or ChannelHandlerContext.read() in channelReadComplete(...) method
                //
                // See https://github.com/netty/netty/issues/2254
                if (!readPending && !config.isAutoRead()) {
                    removeReadOp();
                }
            }
        }
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        final SelectionKey key = selectionKey();
        final int interestOps = key.interestOps();

        int maxMessagesPerWrite = maxMessagesPerWrite();
        while (maxMessagesPerWrite > 0) {
            Object msg = in.current();
            if (msg == null) {
                break;
            }
            try {
                boolean done = false;
                for (int i = config().getWriteSpinCount() - 1; i >= 0; i--) {
                    if (doWriteMessage(msg, in)) {
                        done = true;
                        break;
                    }
                }

                if (done) {
                    maxMessagesPerWrite--;
                    in.remove();
                } else {
                    break;
                }
            } catch (Exception e) {
                if (continueOnWriteError()) {
                    maxMessagesPerWrite--;
                    in.remove(e);
                } else {
                    throw e;
                }
            }
        }
        if (in.isEmpty()) {
            // Wrote all messages.
            if ((interestOps & SelectionKey.OP_WRITE) != 0) {
                key.interestOps(interestOps & ~SelectionKey.OP_WRITE);
            }
        } else {
            // Did not write all messages.
            if ((interestOps & SelectionKey.OP_WRITE) == 0) {
                key.interestOps(interestOps | SelectionKey.OP_WRITE);
            }
        }
    }

    /**
     * Returns {@code true} if we should continue the write loop on a write error.
     */
    protected boolean continueOnWriteError() {
        return false;
    }

    protected boolean closeOnReadError(Throwable cause) {
        if (!isActive()) {
            // If the channel is not active anymore for whatever reason we should not try to continue reading.
            return true;
        }
        if (cause instanceof PortUnreachableException) {
            return false;
        }
        if (cause instanceof IOException) {
            // ServerChannel should not be closed even on IOException because it can often continue
            // accepting incoming connections. (e.g. too many open files)
            return !(this instanceof ServerChannel);
        }
        return true;
    }

    /**
     * Read messages into the given array and return the amount which was read.
     */
    protected abstract int doReadMessages(List<Object> buf) throws Exception;

    /**
     * Write a message to the underlying {@link java.nio.channels.Channel}.
     *
     * @return {@code true} if and only if the message has been written
     */
    protected abstract boolean doWriteMessage(Object msg, ChannelOutboundBuffer in) throws Exception;
}
