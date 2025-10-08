package io.netty.example.demo.handler.server.ws;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.time.LocalDateTime;

/**
 * 这里的TextWebSocketFrame 类型表示一个文本帧(frame)
 */
public class MyTextWsFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame msg) throws Exception {
        System.out.println("服务器收到消息: " + msg.text());

        // 回复消息
        ctx.channel().writeAndFlush(new TextWebSocketFrame("服务器时间: " + LocalDateTime.now() + " " + msg.text()));
    }

    // web客户端连接后被调用, 触发方法
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        // longText是唯一, shortText不是唯一
        System.out.println("handlerAdded被调用: " + ctx.channel().id().asLongText());
        System.out.println("handlerAdded被调用: " + ctx.channel().id().asShortText());
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        System.out.println("handlerRemoved被调用, 断开连接: "+ ctx.channel().id().asLongText());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println("异常发生: " + cause.getMessage());
        ctx.close();
    }
}
