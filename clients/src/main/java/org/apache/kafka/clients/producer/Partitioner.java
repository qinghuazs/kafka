/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;

import java.io.Closeable;

/**
 * Kafka生产者的分区器接口
 * <br/>
 * 分区器负责决定消息应该被发送到主题的哪个分区。实现此接口可以自定义分区策略，例如：
 * - 基于消息键的哈希值进行分区
 * - 实现消息的轮询分发
 * - 根据消息内容特征进行分区
 * <br/>
 * 要启用分区器的监控功能，需实现 {@link org.apache.kafka.common.metrics.Monitorable} 接口。
 * 系统会自动为所有注册的监控指标添加以下标签：
 * - <code>config</code>: 设置为 <code>partitioner.class</code>
 * - <code>class</code>: 设置为具体的分区器类名
 * <br/>
 * 监控指标可用于观察分区器的性能和行为，如分区分布情况、处理时延等。
 */
public interface Partitioner extends Configurable, Closeable {

    /**
     * 为给定的消息记录计算目标分区号
     *
     * @param topic 目标主题名称
     * @param key 用于分区计算的消息键对象，如果消息没有指定键则为null
     * @param keyBytes 序列化后的消息键字节数组，如果消息没有指定键则为null。
     *                 当需要对键进行哈希计算时，建议使用此参数而不是key对象，可以避免重复序列化
     * @param value 消息的值对象，可以用于基于消息内容的分区策略，如果不需要则可以忽略
     * @param valueBytes 序列化后的消息值字节数组，如果不需要则可以忽略
     * @param cluster 当前的集群元数据，包含了主题分区分配等信息。
     *                可用于获取主题的分区数量、分区leader位置等信息
     * @return 返回目标分区号，必须是一个大于等于0且小于主题总分区数的整数
     */
    int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster);

    /**
     * 当分区器关闭时调用此方法
     * <br/>
     * 可以在此方法中执行必要的清理工作，如：
     * - 释放资源
     * - 保存状态
     * - 关闭连接等
     */
    void close();
}
