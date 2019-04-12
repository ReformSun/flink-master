package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;

public class EchoClientHandlerTest extends SimpleChannelInboundHandler {
	@Override
	protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
		System.out.println("Client received");
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		ctx.writeAndFlush(Unpooled.copiedBuffer("Netty rocks!", //2
			CharsetUtil.UTF_8));
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		super.channelRead(ctx, msg);
	}
}
