package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;

public class EchoClientHandlerTest extends SimpleChannelInboundHandler {
	private NettyBufferPool nettyBufferPool;

	public EchoClientHandlerTest(NettyBufferPool nettyBufferPool) {
		this.nettyBufferPool = nettyBufferPool;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
		System.out.println("Client received");
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
//		ctx.writeAndFlush(Unpooled.copiedBuffer("Netty rocks!", //2
//			CharsetUtil.UTF_8));
		ResultPartitionID resultPartitionID = new ResultPartitionID();
		InputChannelID inputChannelID = new InputChannelID();
		NettyMessage.PartitionRequest partitionRequest = new NettyMessage.PartitionRequest(resultPartitionID,1,inputChannelID,1);
		ctx.writeAndFlush(partitionRequest.write(nettyBufferPool));
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		super.channelRead(ctx, msg);
	}
}
