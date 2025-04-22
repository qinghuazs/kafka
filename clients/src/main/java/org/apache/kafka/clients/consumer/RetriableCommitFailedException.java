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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.errors.RetriableException;

/**
 * 可重试的提交失败异常类，用于处理Kafka消费者在提交偏移量时遇到的临时性错误。
 * 该异常继承自RetriableException，表明这类错误是可以通过重试来解决的。
 * 
 * 应用场景：
 * 1. 网络临时故障导致提交偏移量请求失败
 * 2. Kafka集群负载过高，暂时无法处理提交请求
 * 3. 集群进行Leader选举或分区重平衡时的临时不可用
 * 
 * 当消费者捕获到此异常时，应该实现重试机制，在一定时间后重新尝试提交偏移量。
 */
public class RetriableCommitFailedException extends RetriableException {

    /**
     * 序列化版本号，用于类的序列化和反序列化过程
     */
    private static final long serialVersionUID = 1L;

    /**
     * 使用原始异常构造RetriableCommitFailedException
     * 
     * @param t 导致提交失败的原始异常
     */
    public RetriableCommitFailedException(Throwable t) {
        super("Offset commit failed with a retriable exception. You should retry committing " +
                "the latest consumed offsets.", t);
    }

    /**
     * 使用自定义错误消息构造RetriableCommitFailedException
     * 
     * @param message 描述提交失败原因的错误消息
     */
    public RetriableCommitFailedException(String message) {
        super(message);
    }

    /**
     * 使用自定义错误消息和原始异常构造RetriableCommitFailedException
     * 
     * @param message 描述提交失败原因的错误消息
     * @param t 导致提交失败的原始异常
     */
    public RetriableCommitFailedException(String message, Throwable t) {
        super(message, t);
    }
}
