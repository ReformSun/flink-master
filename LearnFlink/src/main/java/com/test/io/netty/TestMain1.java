package com.test.io.netty;

/**
 *
 *  * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p> When deploying such a program in parallel, the intermediate result will be partitioned over its
 * producing parallel subtasks; each of these partitions is furthermore partitioned into one or more
 * subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * {@link org.apache.flink.runtime.io.network.netty.NettyClient}
 * {@link org.apache.flink.runtime.io.network.netty.PartitionRequestClientHandler}
 *
 * {@link org.apache.flink.runtime.io.network.netty.PartitionRequestClient}
 * 分区请求客户端负责远程分区的请求 它是通过{@link org.apache.flink.runtime.io.network.netty.PartitionRequestClientFactory}
 * 创建的，通过{@link org.apache.flink.runtime.io.network.ConnectionID}这个参数创建，通过它可以知道需要连接的地址，可以通过
 * {@link org.apache.flink.runtime.io.network.netty.NettyClient}连接到服务端，并返回通道{@link org.apache.flink.shaded.netty4.io.netty.channel.Channel}
 *
 * 通过{@link org.apache.flink.runtime.io.network.netty.PartitionRequestClient}的requestSubpartition方法可以与中间结果的子分区建立联系，通过
 * {@link org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel}存储通道返回的结果，存储到队列中
 *
 * 每个{@link org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate}存储多个{@link org.apache.flink.runtime.io.network.partition.consumer.InputChannel}
 * 和网络层进行信息交换
 *
 * 每一个RemoteInputChannel都有一个InputChannelID{@link org.apache.flink.runtime.io.network.partition.consumer.InputChannelID} 当当前的RemoteInputChannel与对应的子分区建立联系时，
 * 子分区就会拥有InputChannelID了。当发送客户端回去到消息时，会通过在NettyMessage.BufferResponse{@link org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse}中的receiverId
 * 找到对应的RemoteInputChannel然后把获取的信息给RemoteInputChannel，他会存储到队列中，供{@link org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate}消费，
 * 每一个InputGate可以有多个RemoteInputChannel，每个RemoteInputChannel也可以和多个子分区相关联，但是每个RemoteInputChannel只能有一个InputGate
 *
 * 每个RemoteInputChannel只能连接一个连接地址通过{@link org.apache.flink.runtime.io.network.ConnectionID}，
 *
 * 每个子任务可以有多个InputGate，
 * 每个输入网关，因为有有个输入通道，所以可以和多个子任务进行信息交互，具体的交互流程封装到了输入通道类中了
 *
 */
public class TestMain1 {
}
