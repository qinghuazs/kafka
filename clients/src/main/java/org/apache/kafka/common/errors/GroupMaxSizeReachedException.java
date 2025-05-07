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
 * 当消费者组达到其配置的最大容量且无法容纳更多成员时抛出此异常。
 * 
 * 应用场景：
 * 1. 新的消费者尝试加入一个已达到最大成员数量的消费者组
 * 2. 用于实现消费者组的规模控制和资源管理
 * 3. 防止单个消费者组过度扩展导致的性能问题
 * 
 * 设计考虑：
 * 1. 通过配置限制消费者组大小，确保系统稳定性
 * 2. 提供清晰的错误信息，便于运维人员进行容量规划
 * 3. 作为消费者组弹性伸缩的边界控制机制
 */
public class GroupMaxSizeReachedException extends ApiException {
    /**
     * 序列化版本ID
     */
    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     * @param message 异常描述信息，通常包含消费者组ID和当前配置的最大容量信息
     */
    public GroupMaxSizeReachedException(String message) {
        super(message);
    }
}
