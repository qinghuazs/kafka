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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * MemberState枚举定义了Kafka消费者组成员的所有可能状态
 * 这些状态反映了成员在消费者组生命周期中的不同阶段
 */
public enum MemberState {

    /**
     * 未订阅状态
     * 成员有组ID，但未订阅任何主题以接收自动分配
     * 当成员从未订阅或已取消所有主题的订阅时，将处于此状态
     * 在此状态下，成员可以提交偏移量，但不会成为消费者组的活动成员（不发送心跳）
     */
    UNSUBSCRIBED,

    /**
     * 加入状态
     * 成员正在尝试加入消费者组
     * 在此状态下，成员将以固定间隔发送epoch为0的心跳请求
     * 直到收到epoch > 0的响应或遇到致命失败
     * 当成员首次调用subscribe尝试加入组，或被隔离后尝试重新加入时，会转换到此状态
     */
    JOINING,

    /**
     * 协调状态
     * 成员已收到新的目标分配（可能分配或撤销了分区），正在处理中
     * 在此状态下，成员将继续按间隔发送心跳，并协调分配：
     * - 必要时提交偏移量
     * - 调用用户的onPartitionsAssigned或onPartitionsRevoked回调
     * - 使新分配生效
     * 注意：在此状态下，成员可能正在解析目标分配的元数据
     * 或者如果主题名称已解析，则触发提交/回调
     */
    RECONCILING,

    /**
     * 确认状态
     * 成员已完成协调接收到的分配，并保持此状态直到发送下一个心跳请求以向服务器确认分配
     * 此状态表明必须立即发送下一个心跳请求，而不是等待心跳间隔过期
     * 注意：一旦确认发送完成，如果还有待协调的分配，成员可能会回到RECONCILING状态：
     * - 等待元数据的分配
     * - 元数据已解析的分配
     * - 从代理接收的新分配
     */
    ACKNOWLEDGING,

    /**
     * 稳定状态
     * 成员在组中处于活动状态，并已处理所有收到的分配
     * 在此状态下，成员将按间隔发送心跳
     */
    STABLE,

    /**
     * 隔离状态
     * 当成员收到UNKNOWN_MEMBER_ID或FENCED_MEMBER_EPOCH错误时，转换到此状态
     * 表明成员已被排除在组外
     * 在此状态下：
     * - 成员将停止发送心跳
     * - 通过调用用户的onPartitionsLost回调放弃其分区
     * - 然后转换到JOINING状态以作为新成员重新加入组
     */
    FENCED,

    /**
     * 准备离开状态
     * 成员在发送心跳离开组之前转换到此状态
     * 在此状态下：
     * - 成员将继续发送心跳，同时通过调用用户回调释放其分配
     * - 当回调完成时，成员将转换到LEAVING状态以发送心跳离开组
     * 注意：如果因轮询计时器过期而离开，成员在此状态下不执行任何回调
     * 直接转换到LEAVING然后到STALE状态
     */
    PREPARE_LEAVING,

    /**
     * 离开状态
     * 成员已提交偏移量并释放其分配
     * 保持此状态直到发送下一个epoch为-1或-2的心跳请求以有效离开组
     * 此状态表明必须立即发送下一个心跳请求，而不是等待心跳间隔过期
     */
    LEAVING,

    /**
     * 致命状态
     * 成员在心跳响应中收到不可恢复的错误时进入此状态
     * 这是一个不可恢复的状态：
     * - 成员不会向代理发送任何请求
     * - 不能执行任何其他状态转换
     */
    FATAL,

    /**
     * 过期状态
     * 当轮询计时器过期时成员转换到此状态，表明在max.poll.interval.ms内没有调用consumer.poll
     * 在此状态下：
     * - 成员将发送心跳离开组
     * - 调用onPartitionsLost回调
     * - 清除其分配
     * 成员只能在下一次应用程序轮询事件时转出此状态
     * 然后转换到JOINING状态以重新加入组
     */
    STALE;

    // 有效的状态转换定义
    static {
        // 稳定状态可以从加入、确认和协调状态转换而来
        STABLE.previousValidStates = Arrays.asList(JOINING, ACKNOWLEDGING, RECONCILING);

        // 协调状态可以从稳定、加入、确认和协调状态转换而来
        RECONCILING.previousValidStates = Arrays.asList(STABLE, JOINING, ACKNOWLEDGING, RECONCILING);

        // 确认状态只能从协调状态转换而来
        ACKNOWLEDGING.previousValidStates = Collections.singletonList(RECONCILING);

        // 致命状态可以从多个状态转换而来
        FATAL.previousValidStates = Arrays.asList(JOINING, STABLE, RECONCILING, ACKNOWLEDGING,
                PREPARE_LEAVING, LEAVING, UNSUBSCRIBED);

        // 隔离状态可以从多个状态转换而来
        FENCED.previousValidStates = Arrays.asList(JOINING, STABLE, RECONCILING, ACKNOWLEDGING,
                PREPARE_LEAVING, LEAVING);

        // 加入状态可以从隔离、未订阅和过期状态转换而来
        JOINING.previousValidStates = Arrays.asList(FENCED, UNSUBSCRIBED, STALE);

        // 准备离开状态可以从多个状态转换而来
        PREPARE_LEAVING.previousValidStates = Arrays.asList(JOINING, STABLE, RECONCILING,
                ACKNOWLEDGING, UNSUBSCRIBED);

        // 离开状态只能从准备离开状态转换而来
        LEAVING.previousValidStates = Collections.singletonList(PREPARE_LEAVING);

        // 未订阅状态可以从准备离开、离开和隔离状态转换而来
        UNSUBSCRIBED.previousValidStates = Arrays.asList(PREPARE_LEAVING, LEAVING, FENCED);

        // 过期状态只能从离开状态转换而来
        STALE.previousValidStates = Collections.singletonList(LEAVING);
    }

    /**
     * 存储当前状态的有效前置状态列表
     */
    private List<MemberState> previousValidStates;

    /**
     * 构造函数
     * 初始化前置状态列表为空列表
     */
    MemberState() {
        this.previousValidStates = new ArrayList<>();
    }

    /**
     * 获取当前状态的有效前置状态列表
     *
     * @return 有效的前置状态列表
     */
    public List<MemberState> getPreviousValidStates() {
        return this.previousValidStates;
    }

    /**
     * 检查成员是否可以处理新的分配
     * 当成员是组的一部分并打算继续留在组中时，预期返回true
     * （例如，当成员准备离开组时返回false）
     *
     * @return 如果成员可以处理新分配则返回true
     */
    public boolean canHandleNewAssignment() {
        // 检查当前状态是否在RECONCILING状态的有效前置状态列表中
        return MemberState.RECONCILING.getPreviousValidStates().contains(this);
    }
}
