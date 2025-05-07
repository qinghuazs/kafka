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
 * 当请求的主题分区存在不稳定的偏移量时抛出此异常。
 * 
 * 应用场景：
 * 1. 消费者组再平衡过程中的偏移量提交
 * 2. 分区迁移或副本同步过程中的偏移量不一致
 * 3. 消费者组成员变更导致的偏移量状态不稳定
 * 
 * 设计考虑：
 * - 继承自RetriableException，表明这是一个可重试的临时性错误
 * - 用于防止在不稳定状态下提交偏移量，确保数据消费的准确性
 * - 通过重试机制等待系统状态恢复稳定
 */
public class UnstableOffsetCommitException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public UnstableOffsetCommitException(String message) {
        super(message);
    }
}
