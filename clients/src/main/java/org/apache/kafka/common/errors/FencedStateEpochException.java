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
 * 共享状态Epoch隔离异常。当请求中的共享组状态Epoch值与协调者维护的状态Epoch不匹配时抛出此异常。
 * 
 * 应用场景：
 * 1. 在Kafka的共享状态管理中，使用Epoch机制确保状态更新的一致性
 * 2. 协调者通过状态Epoch来追踪共享状态的版本，防止并发更新冲突
 * 
 * 触发条件：
 * 1. 客户端使用过期的状态Epoch尝试更新共享状态
 * 2. 多个客户端同时尝试更新相同的共享状态
 * 
 * 处理机制：
 * 1. 客户端需要重新获取最新的状态信息和Epoch值
 * 2. 使用新的Epoch值重试状态更新操作
 * 
 * 设计考虑：
 * 1. 保证分布式环境下共享状态的一致性
 * 2. 避免并发更新导致的状态不一致问题
 */
public class FencedStateEpochException extends ApiException {
    // 序列化版本号
    private static final long serialVersionUID = 1L;

    /**
     * 创建一个新的共享状态Epoch隔离异常
     * @param message 异常描述信息
     */
    public FencedStateEpochException(String message) {
        super(message);
    }
}
