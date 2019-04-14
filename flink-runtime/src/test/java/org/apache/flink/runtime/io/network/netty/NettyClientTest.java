package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.util.NetUtils;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.apache.flink.runtime.io.network.netty.PartitionRequestClientHandlerTest.createRemoteInputChannel;
import static org.apache.flink.runtime.io.network.netty.PartitionRequestClientHandlerTest.createSingleInputGate;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** 
* NettyClient Tester. 
* 
* @author <Authors name> 
* @since <pre>四月 12, 2019</pre> 
* @version 1.0 
*/ 
public class NettyClientTest { 
	NettyClient nettyClient;
@Before
public void before() throws Exception {
	int numberOfSlots = 2;
	NettyConfig config = new NettyConfig(
		InetAddress.getLocalHost(),
		8889,
		1024,
		numberOfSlots,
		new Configuration());
	nettyClient = new NettyClient(config);
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: init(final NettyProtocol protocol, NettyBufferPool nettyBufferPool) 
* 
*/ 
@Test
public void testInit() throws Exception {
	NettyBufferPool bufferPool = new NettyBufferPool(2);
	ChannelHandler[] channelHandlers = new ChannelHandler[]{new EchoClientHandlerTest(bufferPool)};
	NettyProtocol nettyProtocol = mock(NettyProtocol.class);
	when(nettyProtocol.getClientChannelHandlers()).thenReturn(channelHandlers);
	nettyClient.init(nettyProtocol,bufferPool);
	InetSocketAddress serverSocketAddress = new InetSocketAddress(InetAddress.getLocalHost(),8889);
	ChannelFuture channelFuture = nettyClient.connect(serverSocketAddress);
	channelFuture.sync().channel().closeFuture().sync();
}

@Test
public void testMethod1() throws Exception {
	NettyBufferPool bufferPool = new NettyBufferPool(2);
	ResultPartitionProvider partitionProvider = mock(ResultPartitionProvider.class);
	TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);
	NettyProtocol nettyProtocol = new NettyProtocol(partitionProvider,taskEventDispatcher,false);
	nettyClient.init(nettyProtocol,bufferPool);

	PartitionRequestClientFactory partitionRequestClientFactory = new PartitionRequestClientFactory(nettyClient);
	InetSocketAddress serverSocketAddress = new InetSocketAddress(InetAddress.getLocalHost(),8889);
	ConnectionID connectionID = new ConnectionID(serverSocketAddress,1);
	PartitionRequestClient partitionRequestClient = partitionRequestClientFactory.createPartitionRequestClient(connectionID);

	ResultPartitionID resultPartitionID = new ResultPartitionID();
	int subpartitionIndex = 1;
	final NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, 32);
	final SingleInputGate inputGate = createSingleInputGate();
	final RemoteInputChannel inputChannel = createRemoteInputChannel(inputGate, partitionRequestClient, 1, 2);

	ChannelFuture channelFuture = partitionRequestClient.requestSubpartition(resultPartitionID,subpartitionIndex,inputChannel,0);
	channelFuture.sync().channel().closeFuture().sync();
}

@Test
public void testMethod2(){

}

} 
