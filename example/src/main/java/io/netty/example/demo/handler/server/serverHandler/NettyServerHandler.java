package io.netty.example.demo.handler.server.serverHandler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoop;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

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
        // 检查任务队列里面有没有任务, taskQueue 和 scheduleTaskQueue 里面可以看
        NioEventLoop eventLoop = (NioEventLoop) channel.eventLoop();

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

        /**
         * Netty 中有两个触发 write 事件传播的方法，它们的传播处理逻辑都是一样的，只不过它们在 pipeline 中的传播起点是不同的。
         *
         * channelHandlerContext.write() 方法会从当前 ChannelHandler 开始在 pipeline 中向前传播 write 事件直到 HeadContext。
         *
         * channelHandlerContext.channel().write() 方法则会从 pipeline 的尾结点 TailContext 开始在 pipeline 中向前传播 write 事件直到 HeadContext 。
         */

        /**
         * 写到缓冲区但是没刷
         * {@link AbstractChannelHandlerContext#write(Object)}
         */
        ctx.write(responseBuffer);


        // 第三步, 自定义异步任务, 提交到 taskQueue
        ctx.channel().eventLoop().execute(() -> {
            try {
                Thread.sleep(3000);
                System.out.println("=======> async execute taskQueue");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // 第四步, 自定义定时任务, 提交到 scheduleTaskQueue
        ctx.channel().eventLoop().schedule(() -> {
            try {
                Thread.sleep(3000);
                System.out.println("=======> async execute scheduleTaskQueue");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 5, TimeUnit.SECONDS);

    }

    // 数据读取完毕
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // 真正的发送
        ctx.flush();
    }

    /**
     * 处理异常,一般要关闭通道
     * <p>
     * 当异步事件在 pipeline 传播的过程中发生异常时，异步事件就会停止在 pipeline 中传播。所以我们在日常开发中，需要对写操作异常情况进行处理。
     * <p>
     * 其中 inbound 类异步事件发生异常时，会触发exceptionCaught事件传播。
     * exceptionCaught 事件本身也是一种 inbound 事件，传播方向会从当前发生异常的 ChannelHandler 开始一直向后传播直到 TailContext。
     * <p>
     * 而 outbound 类异步事件发生异常时，则不会触发exceptionCaught事件传播。
     * 一般只是通知相关 ChannelFuture。
     * 但如果是 flush 事件在传播过程中发生异常，则会触发当前发生异常的 ChannelHandler 中 exceptionCaught 事件回调。
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("NettyServerHandler error", cause);
        ctx.close();
    }
}
