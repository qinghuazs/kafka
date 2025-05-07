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
 * 表示获取会话的Epoch值无效的异常
 *
 * 此异常继承自RetriableException（可重试异常），当消费者使用的fetch会话的epoch值与服务器端不匹配时抛出。
 * Kafka的增量获取（Incremental Fetch）特性使用会话机制来优化fetch请求，每个会话都有一个epoch值用于版本控制。
 *
 * 触发场景：
 * 1. 消费者使用了过期的fetch会话epoch
 * 2. broker重启后，之前的fetch会话已失效
 * 3. 由于网络分区等原因，消费者和broker的会话状态不同步
 * 4. fetch会话超时，服务器端已清理相关状态
 *
 * 影响：
 * 1. 增量获取请求失败
 * 2. 消费者需要创建新的fetch会话
 * 3. 可能导致短暂的消费延迟
 *
 * 处理机制：
 * 1. 作为RetriableException的子类，表明此错误是临时性的
 * 2. 消费者应该重新初始化fetch会话
 * 3. 使用新的epoch值重试fetch操作
 */
public class InvalidFetchSessionEpochException extends RetriableException {
    
    // 序列化版本号
    private static final long serialVersionUID = 1L;

    /**
     * 创建一个无参数的InvalidFetchSessionEpochException实例
     */
    public InvalidFetchSessionEpochException() {
    }

    /**
     * 使用指定的错误消息创建InvalidFetchSessionEpochException实例
     *
     * @param message 详细描述异常原因的错误消息
     */
    public InvalidFetchSessionEpochException(String message) {
        super(message);
    }
}
