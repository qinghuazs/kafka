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
 * 序列号重复异常
 * 
 * 该异常在Kafka事务处理过程中，当检测到重复的序列号时抛出。
 * 主要用于以下场景：
 * 1. 生产者在事务中发送消息时，使用了已经使用过的序列号
 * 2. 确保消息的顺序性和唯一性，防止重复处理
 * 
 * 序列号是Kafka用于维护消息顺序和幂等性的重要机制，当出现重复时，
 * 说明可能存在以下问题：
 * - 生产者重试导致的重复发送
 * - 事务状态不一致
 * - 序列号生成逻辑出现问题
 */
public class DuplicateSequenceException extends ApiException {

    /**
     * 使用指定的错误消息构造序列号重复异常
     * 
     * @param message 描述序列号重复原因的错误消息
     */
    public DuplicateSequenceException(String message) {
        super(message);
    }
}
