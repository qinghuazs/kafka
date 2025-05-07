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
 * 当尝试访问已经被移动到分层存储中的消息偏移量时抛出此异常。
 * 
 * 应用场景：
 * 1. 消费者尝试获取的消息已被移动到冷存储层
 * 2. 访问的偏移量对应的消息不在活跃存储层中
 * 3. 消息已根据存储策略迁移到不同的存储层级
 * 
 * 设计考虑：
 * - 支持Kafka的分层存储架构，区分热数据和冷数据的访问
 * - 提示客户端需要通过特定的方式访问历史数据
 * - 优化存储资源利用，降低热存储的压力
 */
public class OffsetMovedToTieredStorageException extends ApiException {

    private static final long serialVersionUID = 1L;

    public OffsetMovedToTieredStorageException(String message) {
        super(message);
    }

    public OffsetMovedToTieredStorageException(String message, Throwable cause) {
        super(message, cause);
    }

}
