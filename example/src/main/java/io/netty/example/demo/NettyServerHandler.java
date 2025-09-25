package io.netty.example.demo;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
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

    // 只要channel打通了，就会执行
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("Server is Active......");

        String response = "hello server";
        ByteBuf responseBuffer = Unpooled.copiedBuffer(response, CharsetUtil.UTF_8);

        // 写到缓冲区但是没刷
        ctx.writeAndFlush(responseBuffer);
    }

    /**
     * ChannelHandlerContext ctx 上下文对象, 含有 管道 pipeline, 通道 channel, 连接的地址
     * Object msg 客户端发送的数据
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        /**
         * 从ctx里面可以知道他是 inBound 入栈的还是出栈的
         * 也能拿到是那个eventLoop
         * ctx既有 channel也有pipeLine
         */
        Channel channel = ctx.channel();
        // channel 和 pipeLine是互相持有的
        ChannelPipeline pipeline = channel.pipeline();

        // 第一步，获取客户端请求的内容
        ByteBuf buffer = (ByteBuf) msg;
        byte[] requestBytes = null;
        try {
            requestBytes = new byte[buffer.readableBytes()];
            buffer.readBytes(requestBytes);
        } finally {
            ReferenceCountUtil.safeRelease(buffer); // 关键：释放引用计数
        }

        String request = new String(requestBytes, StandardCharsets.UTF_8);
        log.info("客户端地址: {}, 收到请求: {}", ctx.channel().remoteAddress(), request);

        //第二步，向客户端返回信息
        String response = "收到请求后返回响应";
        ByteBuf responseBuffer = Unpooled.copiedBuffer(response, CharsetUtil.UTF_8);

        // 写到缓冲区但是没刷
        ctx.write(responseBuffer);
    }

    // 数据读取完毕
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // 真正的发送
        ctx.flush();
    }

    // 处理异常,一般要关闭通道
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("NettyServerHandler error", cause);
        ctx.close();
    }
}
