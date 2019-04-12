package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.util.NetUtils;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
		NetUtils.getAvailablePort(),
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
	ChannelHandler[] channelHandlers = new ChannelHandler[]{new EchoServerHandler()};
	NettyProtocol nettyProtocol = mock(NettyProtocol.class);
	when(nettyProtocol.getServerChannelHandlers()).thenReturn(channelHandlers);
	nettyServer.init(nettyProtocol,bufferPool);

} 

} 
