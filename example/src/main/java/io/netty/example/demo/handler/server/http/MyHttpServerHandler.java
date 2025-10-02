package io.netty.example.demo.handler.server.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * SimpleChannelInboundHandler 是 ChannelInboundHandlerAdapter 子类
 */
public class MyHttpServerHandler extends SimpleChannelInboundHandler<HttpObject> {

    // 读取客户端数据, http://127.0.0.1:9999/
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {

        // 这个就是 MyHttpServerHandler
        ChannelHandler handler = ctx.handler();

        // http请求
        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;
            System.out.println("pipeline类型: " +  ctx.pipeline().hashCode() + ", handler hash: " + this.hashCode() + ", msg类型: " + msg.getClass());
            System.out.println("客户端地址: " + ctx.channel().remoteAddress());

            URI uri = new URI(req.uri());
            if ("/favicon.ico".equals(uri.getPath())) {
                System.out.println("请求了 favicon.ico, 不做响应");
                return;
            }

            // 回复信息给浏览器[HTTP 协议]
            // 注意：内容包含中文，使用 UTF-8 编码以避免乱码
            ByteBuf content = Unpooled.copiedBuffer("hello 我是服务器", StandardCharsets.UTF_8);

            // 构造一个 HTTP 响应
            DefaultFullHttpResponse defaultFullHttpResponse = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);

            // Content-Length 必须使用字节数（readableBytes），否则浏览器可能截断或挂起
            defaultFullHttpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

            // 告诉客户端正文是文本且按 UTF-8 解析：text/plain; charset=UTF-8
            // 这里按你的要求使用 HttpHeaderValues.TEXT_PLAIN 与 HttpHeaderValues.CHARSET 常量
            String contentType = HttpHeaderValues.TEXT_PLAIN + "; " + HttpHeaderValues.CHARSET + "=" + StandardCharsets.UTF_8.name();
            defaultFullHttpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);

            // 将数据写入到 channelPipeline中当前channelHandler的下一个 channelHandler开始处理
            ctx.writeAndFlush(defaultFullHttpResponse);
        }

    }
}
