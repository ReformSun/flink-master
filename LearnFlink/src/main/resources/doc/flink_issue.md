### 腾讯在使用flink中遇到的问题
[腾讯文章](https://mp.weixin.qq.com/s/zIp_14_hgRRa0sKCW4Vejw)
1. flink在出现错误时，无法快速定位到错误位置 flink web ui没有暴露关键指标
2. jobManager 优化failover
3. 检查点失败处理
#### 使用时需注意的问题
1. 实现普通的RichSourceFunction和SourceFunction接口的source平行度只能设为1，只有实现ParallelSourceFunction接口
才能设置多平行度
2. 想使用kafka实现数据的不丢失，一定要开启检查点模式，并设置kafka的偏移量提交模式为检查点结束提交的模式，开始的模式最好设置指定时间，或
指定偏移量，不然会被认为是组模式，组模式会被转成从最新位置消费，所以一定要设检查点，并且从安全点位置开始
