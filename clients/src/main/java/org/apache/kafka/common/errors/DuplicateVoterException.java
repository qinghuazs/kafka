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
 * 重复投票者异常
 * 
 * 该异常在Kafka的Leader选举过程中，当发现重复的投票者时抛出。
 * 主要应用场景：
 * 1. Kafka Controller进行Leader选举时的投票者验证
 * 2. 确保每个Broker在选举过程中只能投票一次
 * 
 * 重复投票者可能导致的问题：
 * - 选举结果不公平
 * - 投票统计错误
 * - 选举过程不合规
 * 
 * 该异常的处理通常涉及：
 * - 重新验证投票者列表
 * - 清理重复的投票记录
 * - 可能需要重新发起选举
 */
public class DuplicateVoterException extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息构造重复投票者异常
     * 
     * @param message 描述重复投票者问题的错误消息
     */
    public DuplicateVoterException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因构造重复投票者异常
     * 
     * @param message 描述重复投票者问题的错误消息
     * @param cause 导致该异常的原始异常
     */
    public DuplicateVoterException(String message, Throwable cause) {
        super(message, cause);
    }
}
