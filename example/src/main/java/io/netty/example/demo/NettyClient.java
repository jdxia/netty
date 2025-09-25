package io.netty.example.demo;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioChannelOption;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyClient {

    public static void main(String[] args) throws InterruptedException {
        //1. 创建线程组
        EventLoopGroup group = new NioEventLoopGroup();
        //2. 创建客户端启动助手, 客户端用的不是 serverBootStrap 而是 Bootstrap
        Bootstrap bootstrap = new Bootstrap();
        //3. 设置线程组
        bootstrap.group(group)
                .channel(NioSocketChannel.class)//4. 设置客户端通道实现为NIO
                .attr(AttributeKey.valueOf("clientName"), "nettyClient")
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new LoggingHandler(LogLevel.INFO))
                .handler(new ChannelInitializer<SocketChannel>() { //5. 创建一个通道初始化对象
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        //6. 向pipeline中添加自定义业务处理handler
                        ch.pipeline().addLast(new LoggingHandler(LogLevel.TRACE));
                        ch.pipeline().addLast(new NettyClientHandler());
                    }
                });
        //7. 启动客户端,等待连接服务端
        ChannelFuture channelFuture = bootstrap.connect("127.0.0.1", 9999);
        channelFuture.addListener(future -> {
            if (future.isSuccess()) {
                log.info("连接成功!");
            } else {
                log.error("连接失败!");
                // 重新连接
            }
        });

        //8. 等待关闭通道和关闭连接池
        channelFuture.channel().closeFuture().sync();
        group.shutdownGracefully();
    }
}
