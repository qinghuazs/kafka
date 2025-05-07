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

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * 消费者组成员Epoch隔离异常。当消费者组成员的Epoch值小于当前组协调者记录的Epoch值时抛出此异常。
 * 
 * 应用场景：
 * 1. 在消费者组的成员管理中，每个成员都有一个Epoch值，用于标识其加入组的世代
 * 2. 当成员重新加入组或被移除时，Epoch值会更新，确保旧成员无法继续参与组操作
 * 
 * 触发条件：
 * 1. 消费者组成员使用过期的Epoch值发送请求
 * 2. 成员在被移除后仍尝试执行组操作
 * 
 * 处理机制：
 * 1. 成员需要重新加入组，获取新的Epoch值
 * 2. 通过重新平衡过程确保组成员状态的一致性
 * 
 * 设计考虑：
 * 1. 维护消费者组成员的正确性，避免已过期成员的干扰
 * 2. 配合消费者组的成员管理机制，保证组操作的有序性
 */
@InterfaceStability.Evolving
public class FencedMemberEpochException extends ApiException {
    /**
     * 创建一个新的消费者组成员Epoch隔离异常
     * @param message 异常描述信息
     */
    public FencedMemberEpochException(String message) {
        super(message);
    }
}
