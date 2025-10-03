package io.netty.example.demo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.example.demo.handler.server.http.MyHttpServerHandler;
import io.netty.example.demo.handler.server.im.GroupChatServerHandler;
import io.netty.example.demo.handler.server.serverHandler.NettyServerHandler;
import io.netty.example.demo.handler.server.serverHandler.NettyServerHandler2;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.TimeUnit;

import static jdk.nashorn.internal.objects.NativeFunction.bind;

public class NettyServer {

    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);

    public static void main(String[] args) throws InterruptedException {
        /**
         * nio 3个核心部分: channel(通道), buffer(缓冲区), selector(选择器)
         *
         * java nio里面的 selector类似epoll, socketChannel类似socket资源描述符, SelectionKey就是监听的事件,
         * 服务器监听一个端口会出现serverSocketChanel , 然后有网络连接后会是socketChannel, 后续就是通过这个socketChannel和客户端通信,
         * SelectionKey 类似 某个 channel 在 selector 上的注册结果, 类似 epoll_event
         *
         * netty对jdk原生的selector做了优化,把JDK基于 HashSet 的 selectedKeys/publicSelectedKeys 改造成 Netty 自己的“数组实现”SelectedSelectionKeySet，
         * 并用一个包装 Selector（SelectedSelectionKeySetSelector）在每次 select 前“重置”该数组。这样可以显著降低遍历和插入的开销、减少 GC，而且还配合了对 JDK epoll 100% CPU bug 的重建 Selector 方案与“少唤醒/少阻塞”的 select 策略，整体提升吞吐与稳定性。
         *
         *
         * JDK的 NIO 默认实现是水平触发，Netty 是边缘触发(默认)和水平触发可切换
         * 水平触发 (LT) : 当某个资源（如 socket）就绪后，只要你还没有完全处理完（例如 socket 的缓冲区还有数据没读完），Selector 就会一直返回这个事件, 容错性高
         * 边缘触发 (ET) : 当某个资源（如 socket）就绪后，只要你还没有完全处理完（例如 socket 的缓冲区还有数据没读完），Selector 就不会返回这个事件
         *
         * netty 里面有 nioEventLoop (thread, selector, taskQueue, tailQueue) 他是一个线程, 里面会注册 很多 nioSocketChannel (看是那个group)
         *
         * netty将NettyNioServerSocketChannel中包装的JDK NIO ServerSocketChannel注册到Reactor中的JDK NIO Selector上,
         * 并且将Netty自定义的NioServerSocketChannel 附着在SelectionKey的att属性上，完成Netty自定义Channel与JDK NIO Channel的关系绑定。
         *
         * netty里面一个 channel有自己的pipeline
         *
         * ChannelPipeline (管道)
         *    ↓ 事件传递
         * [HandlerA] <-> [HandlerContextA]
         * [HandlerB] <-> [HandlerContextB]
         * [HandlerC] <-> [HandlerContextC]
         *
         * pipeline其实是一个ChannelHandlerContext类型的双向链表。头结点HeadContext,尾结点TailContext, ChannelHandlerContext中包装着ChannelHandler
         *
         * 注意:
         * channel.write(...) 从 Tail 开始走整个出站链，经过所有出站处理器
         * ctx.write(...) 从当前 Context 向前（靠近 Head）继续走出站链，只会经过当前节点之前的出站处理器
         *
         * 以下三个条件中只要满足任意一个条件，Reactor线程就会被从Selector上唤醒：
         * 当Selector轮询到有IO活跃事件发生时 (socket)。
         * 当Reactor线程需要执行的定时任务到达任务执行时间deadline时。
         * 当有异步任务提交给Reactor时，Reactor线程需要从Selector上被唤醒，这样才能及时的去执行异步任务。
         */

        /**
         * 创建bossGroup线程组: 处理网络事件--连接事件
         *
         * 首先要创建一个Socket用于listen和bind端口地址，我们把这个叫做监听Socket,这里对应的就是 NioServerSocketChannel.class
         * 当客户端连接完成三次握手，系统调用accept函数会基于监听Socket创建出来一个新的Socket专门用于与客户端之间的网络通信我们称为客户端连接Socket,这里对应的就是 NioSocketChannel.class
         *
         * netty有两种Channel类型：
         * 一种是服务端用于监听绑定端口地址的NioServerSocketChannel,
         * 一种是用于客户端通信的NioSocketChannel。
         * 每种Channel类型实例都会对应一个PipeLine用于编排对应channel实例上的IO事件处理逻辑。
         * PipeLine中组织的就是ChannelHandler用于编写特定的IO处理逻辑
         */
        printEnv();
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);

        /**
         * 创建workerGroup线程组: 处理网络事件--读写事件
         * Reactor线程的个数可以通过系统参数 -D io.netty.eventLoopThreads指定。默认的Reactor的个数为CPU核数 * 2
         */
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            // 创建服务端启动助手
            final ServerBootstrap serverBootstrap = new ServerBootstrap();
            final AttributeKey<Object> clientKey = AttributeKey.newInstance("clientKey");

            // 设置bossGroup线程组和workerGroup线程组
            serverBootstrap.group(bossGroup, workerGroup)
                    /**
                     * 设置服务端通道实现为NIO, 主ReactorGroup中的MainReactor管理的Channel类型为NioServerSocketChannel
                     * NioServerSocketChannel 就是对JDK NIO中ServerSocketChannel的封装
                     * 里面有 ReflectiveChannelFactory
                     *
                     * NioServerSocketChannel 主要用来监听端口，接收客户端连接，为客户端创建初始化NioSocketChannel，
                     * 然后采用round-robin轮询的方式从ReactorGroup中选择一个SubReactor与该客户端NioSocketChannel进行绑定。
                     */
                    .channel(NioServerSocketChannel.class)

                    // 为服务端 channel(NioServerSocketChannel) 指定自定义属性
                    .attr(AttributeKey.newInstance("serverName"), "nettyService")
                    // 为每一条链指定自定义属性
                    .childAttr(clientKey, "clientValue")

                    /**
                     * 相关的底层Socket选项，netty全部枚举在 {@link ChannelOption}
                     */

                    // 参数设置, 设置主Reactor中channel的option选项
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                    // 设置服务端ServerSocketChannel中的SocketOption, 可连接队列的大小
                    .option(ChannelOption.SO_BACKLOG, 1024)

                    /**
                     * ServerBootstrap启动类方法带有child前缀的均是设置客户端NioSocketChannel属性的
                     */
                    .childOption(ChannelOption.SO_KEEPALIVE, Boolean.TRUE)
                    .childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE)

                    /**
                     * 不管是服务端用到的NioServerSocketChannel还是客户端用到的NioSocketChannel，
                     * 每个Channel实例都会有一个Pipeline，Pipeline中有多个ChannelHandler用于编排处理对应Channel上感兴趣的IO事件。
                     */

                    /**
                     * 显式添加
                     * 设置主Reactor中Channel->pipLine->handler
                     * 服务端NioServerSocketChannel PipeLine中的ChannelHandler
                     *
                     * 如果需要添加多个ChannelHandler，则可以通过ChannelInitializer向pipeline中进行添加。
                     *
                     * 隐式添加: ServerBootstrapAcceptor 是由Netty框架在启动的时候负责添加，用户无需关心。
                     */
                    .handler(new LoggingHandler(LogLevel.TRACE))

                    /**
                     * 创建一个通道初始化对象
                     * NioSocketChannel 是对JDK NIO SocketChannel 封装
                     * 从ReactorGroup中的SubReactor管理的Channel类型为NioSocketChannel，它是netty中定义客户端连接的一个模型，每个连接对应一个
                     *
                     * 不管是服务端用到的NioServerSocketChannel还是客户端用到的NioSocketChannel，
                     * 每个Channel实例都会有一个Pipeline，Pipeline中有多个ChannelHandler用于编排处理对应Channel上感兴趣的IO事件
                     *
                     * ChannelInitializer是一种特殊的ChannelHandler，用于初始化pipeline。适用于向pipeline中添加多个ChannelHandler的场景。
                     */
                    .childHandler(new ChannelInitializer<NioSocketChannel>() {

                        // 设置从Reactor中注册channel的pipeline
                        @Override
                        protected void initChannel(NioSocketChannel ch) throws Exception {
                            log.info("clientKey: {}", ch.attr(clientKey).get());
                            /**
                             * 事件在pipeline中的传播具有方向性：
                             * inbound事件从HeadContext开始逐个向后传播直到TailContext。
                             * outbound事件则是反向传播，从TailContext开始反向向前传播直到HeadContext。
                             *
                             * inbound事件只能被pipeline中的ChannelInboundHandler响应处理
                             * outbound事件只能被pipeline中的ChannelOutboundHandler响应处理
                             *
                             * ChannelInboundHandlerAdapter 入站的
                             * ChannelOutboundHandlerAdapter 出站的
                             * ChannelDuplexHandler  入站和出站的
                             */

                            System.out.println("客户的socketChannel hashCode=" + ch.hashCode());

                            // 向pipeline中添加自定义业务处理handler, 添加的数量不受限制
                            ch.pipeline().addLast("logHandler", new LoggingHandler(LogLevel.TRACE));

                            /**
                             * 说明 : netty 提供的处理空闲状态的
                             * 1. readerIdleTime: 读空闲, 表示多长时间没有读, 就会发送一个心跳检测包检测是否连接
                             * 2. writerIdleTime: 写空闲,表示多长时间没有写, 就会发送一个心跳检测包是否连接
                             * 3. allIdleTime: 读写空闲,表示多长时间没有读写, 就会发送一个心跳检测包检测是否连接
                             * 如果你把某个时间设为 0，就表示禁用该方向的空闲检测（即不监测读空闲 / 写空闲 / 全空闲）
                             *
                             * 当某个空闲状态被触发时，IdleStateHandler 会在 pipeline 中 触发一个特殊事件 —— IdleStateEvent，
                             *  通过 ctx.fireUserEventTriggered(...) 通知下游 handler。下游的 handler 可以重写 userEventTriggered(...) 来捕获这个事件并做处理
                             */
                            ch.pipeline().addLast("idleState", new IdleStateHandler(3, 5, 7, TimeUnit.SECONDS));

                            // 测试普通的 channelHandler
//                            testServerHandler(ch);

                            // 测试http Handler
//                            testHttpHandler(ch);

                            // 测试im handler
                            testImHandler(ch);
                        }
                    });

            // 启动服务端并绑定端口

            int port = 9999;
            // 这里 有 注册ServerSocketChannel到main reactor上
            ChannelFuture future = serverBootstrap.bind("0.0.0.0", port);

            future.addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future future) throws Exception {
                    if (future.isSuccess()) {
                        log.info("端口[ {} ]绑定成功!", port);
                        return;
                    }

                    log.error("端口[ {} ]绑定失败! 端口+1 重新绑定", port);
                    bind(serverBootstrap, port + 1);
                }
            });

            log.info("服务端启动成功 ==========================> ");
            /**
             * 关闭通道(并不是真正意义上关闭,而是监听通道关闭的状态)和关闭连接池
             * 阻塞在这里，开始监听accept事件
             */
            future.channel().closeFuture().sync();
        } finally {
            /**
             * 优雅关闭主从Reactor线程组里的所有Reactor线程
             */
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }

    private static void testImHandler(NioSocketChannel ch) {
        ch.pipeline().addLast("decoder", new StringDecoder());
        ch.pipeline().addLast("encoder", new StringEncoder());

        // 加入自己业务处理Handler
        ch.pipeline().addLast("groupChatServerHandler", new GroupChatServerHandler());
    }

    private static void testHttpHandler(NioSocketChannel ch) {
        // netty提供的http的编解码器
        ch.pipeline().addLast("httpCodec", new HttpServerCodec());

        // http自定义处理器
        ch.pipeline().addLast("httpHandler", new MyHttpServerHandler());
    }

    private static void testServerHandler(NioSocketChannel ch) {
        ch.pipeline().addLast("nettyServerHandler", new NettyServerHandler());
        ch.pipeline().addLast("nettyServerHandler2", new NettyServerHandler2());
    }

    private static void printEnv() {
        System.out.println("===> netty检测的可用核数" + NettyRuntime.availableProcessors());
    }

}
