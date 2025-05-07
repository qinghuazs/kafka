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
 * 当发送到分区领导者的生产请求包含不匹配的生产者Epoch时抛出此异常。
 * 
 * 应用场景：
 * 1. 事务性生产者在发送消息时，其Epoch值与服务器端记录的不一致
 * 2. 多个生产者实例使用了相同的事务ID导致Epoch冲突
 * 3. 生产者在长时间空闲后恢复生产，原Epoch已失效
 * 
 * 设计考虑：
 * - Epoch用于维护生产者会话的唯一性和有序性
 * - 该异常表明生产者状态已过期，需要重新初始化
 * - 遇到此异常时，用户应通过调用KafkaProducer#abortTransaction来中止当前事务
 * - 中止事务后，生产者会自动发送initPidRequest并在内部重新初始化
 * 
 * @see org.apache.kafka.clients.producer.KafkaProducer#abortTransaction()
 */
public class InvalidProducerEpochException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidProducerEpochException(String message) {
        super(message);
    }
}
