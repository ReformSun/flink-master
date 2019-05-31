## kafka 和 flink结合实现数据不丢失关键知识点
### kafka 中的几个关键知识点
1. 
### flink中kafka消费者重要的几个特性
1. OffsetCommitMode 偏移量提交模式
   1. DISABLED         禁止偏移量提交
   2. ON_CHECKPOINTS   当检查点完成后进行偏移量提交
   3. KAFKA_PERIODIC   周期性的提交偏移量
2. StartupMode 开始时的模式
   1. GROUP_OFFSETS      从指定的消费组中已提交的偏移量开始消费
   2. EARLIEST           从最早的偏移量开始消费
   3. LATEST             从最近的偏移量开始消费
   4. TIMESTAMP          从每个分区的用户提供的时间戳开始消费
   5. SPECIFIC_OFFSETS   从每个分区的用户提供的偏移量开始消费
   
* 对于kafka的提交逻辑是：当设置了  DISABLED 和 ON_CHECKPOINTS模式时，就关闭提交模式，自己手动提交，当未设置提交模式时，使用
kafka自己默认的提交模式周期性提交模式，每5秒钟提交
* 对于开始模式:默认设置为GROUP_OFFSETS 这种模式可以从已经被提交到broker中的偏移量位置开始消费,但是虽然已经设了开始模式
但是没有开始分区设置，就会被堵塞在unassignedPartitionsQueue.getBatchBlocking()位置，知道有还没有被分配的分区才行
但是在FlinkKafkaConsumerBase的run方法后面会调用partitionDiscoverer.discoverPartitions()方法获取kafka中的分区情况
，调用addDiscoveredPartitions方法增加到Fetcher中并生成新的newPartitionStates分区状态加到unassignedPartitionsQueue
未被分配的分区状态中，这个生成的未被分区状态设置的开始模式是EARLIEST_OFFSET，从最新的偏移量开始消费，上面这种情况是本地没有
状态值信息时的状况，如果通过安全点开始，就会从状态后端的安全点信息中保存的偏移量开始消费




### kafka多分区 针对 flink多source任务的分配方式
```$xslt
        kafkaConsumer.partitionsFor(topic) 通过调用kafka接口获取topic对应的分区元数据，
        然后得到每个分区对应的值，PartitionInfo 包含每个分区的索引值等其他信息
        然后通过flink子任务数和分区索引值得到每个子任务对应的分区
        for (int i = 0; i < 5 ; i++) {
            int start = ((2 * 31) & 0x7FFFFFFF) % 3;
            System.out.println((start + i) % 3);
        }
```
