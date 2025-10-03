package io.netty.example.demo.handler.server.im;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.text.SimpleDateFormat;

public class GroupChatServerHandler extends SimpleChannelInboundHandler<String> {

    // 定义一个channel组, 管理所有的channel
    private static final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 表示连接建立, 一旦连接, 第一个被执行, 将当前的channel加入到channelGroup中
     * @param ctx
     * @throws Exception
     */
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        super.handlerAdded(ctx);
        Channel channel = ctx.channel();
        //将当前客户聊天信息推送给其他在线客户
        channelGroup.forEach(ch -> {
            if (ch != channel) {
                ch.writeAndFlush("[客户端]" + channel.remoteAddress() + "加入聊天" + sdf.format(new java.util.Date()) + "\n");
            }
        });
        channelGroup.add(channel);
        System.out.println("当前在线人数:" + channelGroup.size());
        System.out.println("当前客户端: " + channel.remoteAddress());
    }

    /**
     * channel处于活动状态, 提示上线
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        Channel channel = ctx.channel();
        //向其他客户端发送上线通知
        channelGroup.forEach(ch -> {
            if (ch != channel) {
                ch.writeAndFlush("[客户端]" + channel.remoteAddress() + "上线了" + sdf.format(new java.util.Date()) + "\n");
            }
        });
    }

    /**
     * channel处于不活动状态, 提示下线
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        Channel channel = ctx.channel();
        //向其他客户端发送下线通知
        channelGroup.forEach(ch -> {
            if (ch != channel) {
                ch.writeAndFlush("[客户端]" + channel.remoteAddress() + "下线了" + sdf.format(new java.util.Date()) + "\n");
            }
        });
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        Channel channel = ctx.channel();
        //向其他客户端发送离开通知
        channelGroup.forEach(ch -> {
            if (ch != channel) {
                ch.writeAndFlush("[客户端]" + channel.remoteAddress() + "离开聊天" + sdf.format(new java.util.Date()) + "\n");
            }
        });
        channelGroup.remove(ctx.channel());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        // 获取当前channel
        Channel channel = ctx.channel();

        channelGroup.forEach(ch -> {
            // 不是当前channel, 才发送
            if (channel != ch) {
                ch.writeAndFlush("[客户]" + channel.remoteAddress() + "发送了消息:" + msg + "\n");
            } else {
                ch.writeAndFlush("[自己]发送了消息:" + msg + "\n");
            }
        });

    }

    // 异常
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        Channel channel = ctx.channel();
        //向其他客户端通知该客户端异常断开
        channelGroup.forEach(ch -> {
            if (ch != channel) {
                ch.writeAndFlush("[系统]" + channel.remoteAddress() + "异常断开连接" + sdf.format(new java.util.Date()) + "\n");
            }
        });
        ctx.close();
        channelGroup.remove(channel);
        System.err.println("客户端异常断开: " + channel.remoteAddress());
//        cause.printStackTrace();
    }
}
