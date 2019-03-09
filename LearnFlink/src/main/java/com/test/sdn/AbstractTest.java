package com.test.sdn;

/**
 * noc数据结构
 * [tid=traceId]
 * [pathhash=pathhash]
 * [sid=spanid]
 * [pid=parentid]
 * [tm=timestamp]
 *[cpathhash=cpathhash]
 * [st=status]
 * [du=duration]
 *
 * tid:业务唯一TraceID流水号
 * pathhash:服务实例的唯一标识
 * sid:被调用者的spanid
 * pid:发起调用者的spanid
 * tm:时间戳,已经存在
 * cpathhash:调用者服务实例的唯一标识
 * st:成功或失败说明
 * du:持续时间
 *
 * 用于衡量一台服务器每秒能够响应的查询次数QPS,实现需要工作量
 * 记录客户端从发出request到收到response的耗时T1。
 * 记录服务端从收到request到返回response的耗时T2。
 * 通过T1-T2大概计算出网络上的传输耗时，并且也知道rpc总耗时T1，以及服务端的处理耗时T2。
 * lid:光口号
 * 业务数据,根据光口号查找所有相关的调用链
 */
public class AbstractTest {

}
