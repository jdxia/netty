package io.netty.example.demo;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;

public class NettyServerHandler2 implements ChannelInboundHandler {

    /**
     * 当 ChannelHandler 从实际的上下文中移除并且不再处理事件时调用。
     * 这个方法用于释放资源或进行一些清理操作。
     */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Handler added: " + ctx.handler());
    }

    /**
     * 当 ChannelHandler 被添加到实际的上下文中并且准备处理事件时调用。
     * 这个方法用于进行一些初始化操作，例如分配资源。
     */
    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Handler removed: " + ctx.handler());
    }

    /**
     * 当 Channel 被注册到它的 EventLoop 时调用。
     * 这个方法的作用是当 Channel 已经注册到其 EventLoop 并准备好处理 I/O 操作时进行一些初始化操作。
     */
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Channel registered: " + ctx.channel());

    }

    /**
     * 当 Channel 从它的 EventLoop 注销时调用。
     * 这个方法用于处理 Channel 注销后的清理工作，通常是关闭资源或释放占用的资源。
     */
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Channel unregistered: " + ctx.channel());

    }

    /**
     * 当 Channel 激活并且已经连接时调用。
     * 这个方法表示通道已经准备好发送和接收数据，可以在这里进行一些启动操作。
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Channel is active: " + ctx.channel());

    }

    /**
     * 当 Channel 变为非活动状态并且不再连接时调用。
     * 这个方法通常在 Channel 关闭时调用，用于处理关闭连接后的操作。
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Channel is inactive: " + ctx.channel());
    }

    /**
     * 当从 Channel 读取到数据时调用。
     * 这个方法用于处理读取到的数据，通常是解码、处理消息或将消息转发到下一个处理器
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println("Message received: " + msg);
        ctx.fireChannelRead(msg); // 将消息传递给下一个处理器
    }

    /**
     * 当读操作完成时调用。
     * 这个方法表示当前批次的读取操作已经结束，可以在这里进行一些收尾工作或触发下一个操作。
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Read complete");
        ctx.flush(); // 刷新缓冲区，发送数据
    }

    /**
     * 当接收到用户自定义事件时调用。
     * 这个方法用于处理用户自定义事件，例如心跳检测等。
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        System.out.println("User event triggered: " + evt);

    }

    /**
     * 当 Channel 的可写状态发生变化时调用。
     * 这个方法用于处理 Channel 的流量控制，例如当缓冲区变为可写或不可写时进行相应的操作。
     */
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {

        if (ctx.channel().isWritable()) {
            System.out.println("Channel is writable");
        } else {
            System.out.println("Channel is not writable");
        }
    }

    /**
     * 当处理过程中发生异常时调用。
     * 这个方法用于捕获并处理异常，通常是记录日志并关闭 Channel。
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        System.err.println("Exception caught: " + cause);
        ctx.close(); // 关闭通道
    }
}
