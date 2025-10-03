package io.netty.example.demo.handler.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import java.nio.charset.StandardCharsets;

public class IdleServerHandler extends ChannelInboundHandlerAdapter {


    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        super.userEventTriggered(ctx, evt);
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            switch (e.state()) {
                case READER_IDLE:
                    // 读空闲 —— 客户端长时间未发送数据
                    // 可以选择关闭连接或发送心跳检测客户端是否在线
                    System.out.println("READER_IDLE - 客户端长时间未发送数据，连接可能已断开");
                    // ctx.close(); // 如果需要关闭连接，取消注释
                    break;

                case WRITER_IDLE:
                    // 写空闲 —— 服务器长时间未发送数据
                    // 发送心跳包保持连接活跃
                    System.out.println("WRITER_IDLE - 发送心跳包");
                    break;

                case ALL_IDLE:
                    // 全空闲 —— 读写都空闲
                    // 可以发送心跳或关闭长时间无活动的连接
                    System.out.println("ALL_IDLE - 连接长时间无活动");
                    // ctx.writeAndFlush(HEARTBEAT_MSG.retainedDuplicate());
                    break;

                default:
                    // 其他状态的处理
                    break;
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("连接断开: " + ctx.channel().remoteAddress());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.err.println("发生异常: " + cause.getMessage());
        ctx.close();
    }
}
