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
 * 当Broker使用过期的epoch值进行操作时抛出此异常。
 * 
 * 在Kafka集群中，每个Broker都有一个epoch值，用于：
 * 1. 标识Broker的生命周期
 * 2. 防止"脑裂"问题
 * 3. 确保集群操作的顺序性
 * 
 * 此异常通常出现在以下场景：
 * - Broker重启后使用旧的epoch值进行操作
 * - 网络分区恢复后，使用旧epoch的Broker尝试加入集群
 * - 控制器选举后，旧控制器使用过期的epoch执行操作
 * 
 * 处理建议：
 * - 确保Broker使用最新的epoch值
 * - 在必要时重新加入集群以获取新的epoch
 * - 检查集群的网络连接状态
 */
public class StaleBrokerEpochException extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息构造异常
     * @param message 描述Broker epoch过期原因的错误消息
     */
    public StaleBrokerEpochException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因构造异常
     * @param message 描述Broker epoch过期原因的错误消息
     * @param cause 导致此异常的原始异常
     */
    public StaleBrokerEpochException(String message, Throwable cause) {
        super(message, cause);
    }

}
