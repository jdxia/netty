package io.netty.example.demo;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

// 这个类很像 reactor 模式里的processor线程，负责读区请求然后返回响应
@Slf4j
public class NettyServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        log.info("NettyServerHandler handlerAdded");
        super.handlerAdded(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // 第一步，获取客户端请求的内容
        ByteBuf buffer = (ByteBuf) msg;
        byte[] requestBytes = new byte[buffer.readableBytes()];
        buffer.readBytes(requestBytes);

        String request = new String(requestBytes, StandardCharsets.UTF_8);
        log.info("收到请求: {}", request);

        //第二步，向客户端返回信息
        String response = "收到请求后返回响应";
        ByteBuf responseBuffer = Unpooled.copiedBuffer(response.getBytes());
        ctx.write(responseBuffer);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // 真正的发送
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("NettyServerHandler error", cause);
        ctx.close();
    }

    @Override
    // 只要channel打通了，就会执行
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Server is Active......");
    }
}
