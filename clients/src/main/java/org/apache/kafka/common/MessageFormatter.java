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
package org.apache.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Closeable;
import java.io.PrintStream;
import java.util.Map;

/**
 * 消息格式化器接口。
 * 用于定义如何解析和格式化Consumer读取的记录以供显示。
 * kafka-console-consumer工具通过--formatter参数内置支持此接口。
 *
 * Kafka提供了多个实现来显示内部主题的记录，例如：
 * - __consumer_offsets：消费者偏移量主题
 * - __transaction_state：事务状态主题
 * - MirrorMaker2主题：用于集群镜像的主题
 */
public interface MessageFormatter extends Configurable, Closeable {

    /**
     * 配置消息格式化器
     * @param configs 用于配置格式化器的参数映射
     */
    default void configure(Map<String, ?> configs) {}

    /**
     * 解析并格式化一条记录用于显示
     * @param consumerRecord 需要格式化的消费者记录
     * @param output 用于输出格式化结果的打印流
     */
    void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output);

    /**
     * 关闭格式化器，释放相关资源
     */
    default void close() {}
}
