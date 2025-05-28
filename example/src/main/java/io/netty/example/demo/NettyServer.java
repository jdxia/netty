package io.netty.example.demo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static jdk.nashorn.internal.objects.NativeFunction.bind;

public class NettyServer {

    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);

    public static void main(String[] args) throws InterruptedException {
        /**
         * JDK的 NIO 默认实现是水平触发，Netty 是边缘触发(默认)和水平触发可切换
         * Netty 实现的垃圾回收更少、性能更好。
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
                    // 设置服务端ServerSocketChannel中的SocketOption
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
                             * inbound事件只能被pipeline中的ChannelInboundHandler响应处理outbound事件只能被pipeline中的ChannelOutboundHandler响应处理
                             */


                            // 向pipeline中添加自定义业务处理handler, 添加的数量不受限制
                            ch.pipeline().addLast(new LoggingHandler(LogLevel.TRACE));

                            ch.pipeline().addLast(new NettyServerHandler());
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

}
