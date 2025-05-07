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
 * 合格的Leader节点不可用异常
 * 
 * 该异常在Kafka集群中，当需要选举新的Leader但找不到合格的候选者时抛出。
 * 主要出现在以下场景：
 * 1. 所有潜在的Leader候选者都不可用（可能离线或不健康）
 * 2. 候选者不满足最小ISR要求
 * 3. 候选者不满足必要的同步要求
 * 
 * 该异常继承自InvalidMetadataException，表明这是一个元数据相关的问题。
 * 当该异常发生时，通常需要：
 * - 等待更多的副本追赶上Leader
 * - 检查集群健康状态
 * - 可能需要手动干预来恢复服务
 * 
 * 这个异常的出现通常表明集群处于不健康状态，
 * 需要运维人员介入检查并解决底层问题。
 */
public class EligibleLeadersNotAvailableException extends InvalidMetadataException {

    /**
     * 使用指定的错误消息构造合格Leader不可用异常
     * 
     * @param message 描述为什么没有合格Leader的错误消息
     */
    public EligibleLeadersNotAvailableException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因构造合格Leader不可用异常
     * 
     * @param message 描述为什么没有合格Leader的错误消息
     * @param cause 导致该异常的原始异常
     */
    public EligibleLeadersNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
