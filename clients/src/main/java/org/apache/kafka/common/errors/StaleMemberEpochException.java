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
 * 此异常用于新的消费者组协议(KIP-848)上下文中，当收到的成员epoch值与当前成员epoch不匹配时，
 * 在OffsetCommit（提交偏移量）和Fetch（获取消息）API中返回此错误。
 * 
 * 消费者组成员的epoch用于：
 * 1. 标识消费者组成员的版本
 * 2. 确保消费者组操作的顺序性
 * 3. 防止过期的消费者操作影响组的状态
 * 
 * 此异常通常出现在以下场景：
 * - 消费者重平衡后使用旧的epoch提交偏移量
 * - 消费者使用过期的epoch尝试获取消息
 * - 网络延迟导致请求携带的epoch过期
 * 
 * 处理建议：
 * - 确保使用最新的成员epoch进行操作
 * - 在收到此异常后重新加入消费者组
 * - 避免长时间的消费者暂停操作
 */
@InterfaceStability.Evolving
public class StaleMemberEpochException extends ApiException {
    /**
     * 使用指定的错误消息构造异常
     * @param message 描述成员epoch过期原因的错误消息
     */
    public StaleMemberEpochException(String message) {
        super(message);
    }
}
