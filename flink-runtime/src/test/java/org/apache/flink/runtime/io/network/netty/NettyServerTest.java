package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.util.NetUtils;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;

/** 
* NettyServer Tester. 
* 
* @author <Authors name> 
* @since <pre>四月 12, 2019</pre> 
* @version 1.0 
*/ 
public class NettyServerTest {
	NettyServer nettyServer;
@Before
public void before() throws Exception {
	int numberOfSlots = 2;
	NettyConfig config = new NettyConfig(
		InetAddress.getLocalHost(),
		8889,
		1024,
		numberOfSlots,
		new Configuration());
	nettyServer = new NettyServer(config);
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

	ChannelHandler[] channelHandlers = new ChannelHandler[]{new EchoServerHandlerTest()};
	NettyProtocol nettyProtocol = mock(NettyProtocol.class);
	when(nettyProtocol.getServerChannelHandlers()).thenReturn(channelHandlers);
	nettyServer.init(nettyProtocol,bufferPool);
	Thread.sleep(100000);
}

@Test
public void testMethod1() throws IOException, InterruptedException {
	NettyBufferPool bufferPool = new NettyBufferPool(2);
	ResultPartitionProvider partitionProvider = mock(ResultPartitionProvider.class);
	TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);
	NettyProtocol nettyProtocol = new NettyProtocol(partitionProvider,taskEventDispatcher,false);
	nettyServer.init(nettyProtocol,bufferPool);
	Thread.sleep(100000);

}

@Test
public void testMethod2() throws Exception{
	NettyBufferPool bufferPool = new NettyBufferPool(2);

	ResultPartitionProvider partitionProvider = mock(ResultPartitionProvider.class);
	TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);
	PartitionRequestQueue queueOfPartitionQueues = new PartitionRequestQueue();
	PartitionRequestServerHandler serverHandler = new PartitionRequestServerHandler(
		partitionProvider, taskEventDispatcher, queueOfPartitionQueues, false);

	ChannelHandler[] channelHandlers = new ChannelHandler[]{new NettyMessage.NettyMessageEncoder()
		,new NettyMessage.NettyMessageDecoder(false),serverHandler,queueOfPartitionQueues};
	NettyProtocol nettyProtocol = mock(NettyProtocol.class);
	when(nettyProtocol.getServerChannelHandlers()).thenReturn(channelHandlers);
	nettyServer.init(nettyProtocol,bufferPool);
	Thread.sleep(100000);
}

} 
