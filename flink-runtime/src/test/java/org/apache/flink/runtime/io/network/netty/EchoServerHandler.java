package org.apache.flink.runtime.io.network.netty;


import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;


public class EchoServerHandler extends ChannelInboundHandlerAdapter {
	@Override
	public void channelRead(ChannelHandlerContext ctx,
							Object msg) {
		ByteBuf in = (ByteBuf) msg;
		System.out.println("Server received: ");        //2
		ctx.write(in);                            //3
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//		ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)//4
//			.addListener(ChannelFutureListener.CLOSE);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx,
								Throwable cause) {
		cause.printStackTrace();                //5
		ctx.close();                            //6
	}
}