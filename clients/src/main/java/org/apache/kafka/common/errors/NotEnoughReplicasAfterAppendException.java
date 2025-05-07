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
package org.apache.kafka.common.errors;

/**
 * 当分区的同步副本（ISR）数量低于配置的最小同步副本数（min.insync.replicas）时抛出此异常。
 * 特别的是，这个异常发生在消息已经被追加到日志之后才发现ISR数量不足。
 * 
 * 应用场景：
 * 1. 数据可靠性保证：确保写入操作在足够多的副本同步后才算成功
 * 2. 高可用性维护：防止在副本同步不足时继续写入，可能导致数据丢失
 * 3. 生产者写入确认：当acks=all时，需要所有ISR副本确认才视为写入成功
 * 
 * 设计考虑：
 * 1. 继承自RetriableException表明这是一个可重试的异常
 * 2. 由于消息已经写入，重试可能导致消息重复
 * 3. 这种情况通常发生在副本失效或网络分区时
 * 4. 与NotEnoughReplicasException的区别是发现时机不同
 * 
 * 注意事项：
 * 1. 生产者重试可能会导致消息重复，因为原始消息已经写入
 * 2. 应用程序需要考虑消息重复的处理机制
 * 3. 建议监控ISR大小，及时发现副本同步问题
 */
public class NotEnoughReplicasAfterAppendException extends RetriableException {
    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息创建异常实例
     * @param message 描述异常的详细信息，通常包含当前ISR数量和所需最小ISR数量
     */
    public NotEnoughReplicasAfterAppendException(String message) {
        super(message);
    }

}
