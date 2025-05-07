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

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.Optional;
import java.util.Set;

/**
 * Listener for getting notified of membership state changes.
 */
/**
 * 成员状态监听器接口
 * 用于接收成员资格状态变更的通知
 * 实现此接口的类可以监听和响应消费者组成员的状态变化
 */
public interface MemberStateListener {

    /**
     * 当成员的epoch发生变化时调用此方法
     * 变化可能来自以下两种情况：
     * 1. 从代理接收到新的epoch值
     * 2. 当成员不再是组的一部分时（被隔离、离开组或失败）epoch被清除
     *
     * 应用场景：
     * - 监控成员的生命周期状态
     * - 跟踪成员在组中的活跃状态
     * - 处理成员被隔离或离组的情况
     * - 维护成员状态的一致性
     *
     * @param memberEpoch 从代理接收的新成员epoch。如果成员不再是组的一部分，则为空
     * @param memberId 当前成员ID。在进程终止之前不会改变
     */
    void onMemberEpochUpdated(Optional<Integer> memberEpoch, String memberId);

    /**
     * 当组成员分配的分区集发生变化时调用此回调方法
     * 分区分配可能因以下情况发生变化：
     * 1. 组协调器进行分区重分配
     * 2. 成员取消订阅
     * 3. 成员离开组
     *
     * 应用场景：
     * - 处理分区分配变更
     * - 更新本地分区状态
     * - 调整消费策略
     * - 重置消费位置
     *
     * 设计考虑：
     * - 使用default方法提供空实现，使接口向后兼容
     * - 允许实现类选择性地处理分区变更
     * - 确保分区集合不为null，但可以为空
     *
     * @param partitions 新的分区分配，可以为空，但不能为null
     */
    default void onGroupAssignmentUpdated(Set<TopicPartition> partitions) {
        // 默认实现为空，允许实现类选择性地覆盖此方法
    }
}
