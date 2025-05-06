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
package org.apache.kafka.clients.admin;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

/**
 * 描述Kafka集群中单个消费者组成员的详细信息。
 * 该类用于管理和跟踪消费者组成员的状态，包括成员标识、主机信息、分区分配等关键属性。
 * 支持两种类型的消费者组：
 * 1. 经典消费者组（Classic Consumer Group）
 * 2. 新版消费者组（Consumer Group）
 */
public class MemberDescription {
    /**
     * 消费者组成员的唯一标识符
     * 由Kafka集群在成员加入组时自动生成
     */
    private final String memberId;
    
    /**
     * 消费者组成员的实例ID
     * 用于支持静态成员资格，允许消费者在重启后重新加入组时保持相同的分区分配
     */
    private final Optional<String> groupInstanceId;
    
    /**
     * 客户端ID
     * 由客户端应用程序指定，用于在日志和监控中标识特定的消费者实例
     */
    private final String clientId;
    
    /**
     * 运行消费者的主机地址
     * 包含主机名或IP地址，用于定位消费者实例的物理位置
     */
    private final String host;
    
    /**
     * 当前分区分配信息
     * 包含分配给该成员的主题分区列表
     */
    private final MemberAssignment assignment;
    
    /**
     * 目标分区分配信息
     * 仅在新版消费者组中使用，表示计划要分配给该成员的分区
     */
    private final Optional<MemberAssignment> targetAssignment;
    
    /**
     * 成员的世代编号
     * 在新版消费者组中用于跟踪成员的状态变更，为空表示经典消费者组
     */
    private final Optional<Integer> memberEpoch;
    
    /**
     * 是否升级到新版消费者组协议的标志
     * true表示使用新协议，false表示使用旧协议，空值表示未知或经典消费者组
     */
    private final Optional<Boolean> upgraded;

    /**
     * 创建一个新的消费者组成员描述实例
     * 
     * @param memberId 成员ID，如果为null则使用空字符串
     * @param groupInstanceId 静态成员ID，用于支持静态成员资格
     * @param clientId 客户端ID，如果为null则使用空字符串
     * @param host 主机地址，如果为null则使用空字符串
     * @param assignment 当前分区分配信息，如果为null则创建空分配
     * @param targetAssignment 目标分区分配信息，仅用于新版消费者组
     * @param memberEpoch 成员世代编号，仅用于新版消费者组
     * @param upgraded 是否使用新版消费者组协议的标志
     */
    public MemberDescription(
        String memberId,
        Optional<String> groupInstanceId,
        String clientId,
        String host,
        MemberAssignment assignment,
        Optional<MemberAssignment> targetAssignment,
        Optional<Integer> memberEpoch,
        Optional<Boolean> upgraded
    ) {
        // 处理null值，确保字段不为null
        this.memberId = memberId == null ? "" : memberId;
        this.groupInstanceId = groupInstanceId;
        this.clientId = clientId == null ? "" : clientId;
        this.host = host == null ? "" : host;
        // 如果分配为null，创建一个空的分配集合
        this.assignment = assignment == null ?
            new MemberAssignment(Collections.emptySet()) : assignment;
        this.targetAssignment = targetAssignment;
        this.memberEpoch = memberEpoch;
        this.upgraded = upgraded;
    }

    /**
     * @deprecated 自4.0版本起已废弃。请使用 {@link #MemberDescription(String, Optional, String, String, MemberAssignment, Optional, Optional, Optional)} 替代。
     * 
     * 创建消费者组成员描述实例的旧版构造函数
     * 该构造函数不支持成员世代和协议升级标志
     */
    @Deprecated
    public MemberDescription(
        String memberId,
        Optional<String> groupInstanceId,
        String clientId,
        String host,
        MemberAssignment assignment,
        Optional<MemberAssignment> targetAssignment
    ) {
        // 调用完整的构造函数，将新增字段设置为空
        this(
            memberId,
            groupInstanceId,
            clientId,
            host,
            assignment,
            targetAssignment,
            Optional.empty(),
            Optional.empty()
        );
    }

    /**
     * @deprecated 自4.0版本起已废弃。请使用 {@link #MemberDescription(String, Optional, String, String, MemberAssignment, Optional, Optional, Optional)} 替代。
     * 
     * 创建消费者组成员描述实例的旧版构造函数
     * 该构造函数不支持目标分配、成员世代和协议升级标志
     */
    @Deprecated
    public MemberDescription(
        String memberId,
        Optional<String> groupInstanceId,
        String clientId,
        String host,
        MemberAssignment assignment
    ) {
        // 调用包含目标分配的构造函数，将目标分配设置为空
        this(
            memberId,
            groupInstanceId,
            clientId,
            host,
            assignment,
            Optional.empty()
        );
    }

    /**
     * @deprecated 自4.0版本起已废弃。请使用 {@link #MemberDescription(String, Optional, String, String, MemberAssignment, Optional, Optional, Optional)} 替代。
     * 
     * 创建消费者组成员描述实例的最简单构造函数
     * 该构造函数不支持静态成员ID、目标分配、成员世代和协议升级标志
     */
    @Deprecated
    public MemberDescription(String memberId,
                             String clientId,
                             String host,
                             MemberAssignment assignment) {
        // 调用包含静态成员ID的构造函数，将静态成员ID设置为空
        this(memberId, Optional.empty(), clientId, host, assignment);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberDescription that = (MemberDescription) o;
        return memberId.equals(that.memberId) &&
            groupInstanceId.equals(that.groupInstanceId) &&
            clientId.equals(that.clientId) &&
            host.equals(that.host) &&
            assignment.equals(that.assignment) &&
            targetAssignment.equals(that.targetAssignment) &&
            memberEpoch.equals(that.memberEpoch) &&
            upgraded.equals(that.upgraded);
    }

    @Override
    public int hashCode() {
        return Objects.hash(memberId, groupInstanceId, clientId, host, assignment, targetAssignment, memberEpoch, upgraded);
    }

    /**
     * 获取消费者组成员的ID
     * 该ID由Kafka集群在成员加入组时自动生成，用于唯一标识组内的每个成员
     * 应用场景：
     * 1. 在消费者组再平衡时识别成员身份
     * 2. 在监控和日志中追踪特定成员的行为
     * 3. 在分区分配过程中确定分配关系
     * 
     * @return 返回成员ID字符串，如果原始ID为null则返回空字符串
     */
    public String consumerId() {
        return memberId;
    }

    /**
     * 获取消费者组成员的实例ID
     * 实例ID用于支持静态成员资格特性，使消费者在重启后能够保持原有的分区分配
     * 应用场景：
     * 1. 支持有状态的消费者应用，减少不必要的再平衡
     * 2. 在容器环境中维持稳定的分区分配关系
     * 3. 实现消费者的快速故障恢复
     * 
     * @return 返回Optional包装的实例ID，如果未设置则返回空Optional
     */
    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }

    /**
     * 获取消费者的客户端ID
     * 客户端ID由应用程序在创建消费者时指定，用于在日志和监控中标识消费者实例
     * 应用场景：
     * 1. 在日志中识别和追踪特定的消费者实例
     * 2. 在监控系统中展示消费者的状态和指标
     * 3. 在调试和故障排除时定位问题
     * 
     * @return 返回客户端ID字符串，如果原始ID为null则返回空字符串
     */
    public String clientId() {
        return clientId;
    }

    /**
     * 获取运行消费者的主机地址
     * 包含主机名或IP地址，用于定位消费者实例的物理位置
     * 应用场景：
     * 1. 在分布式环境中定位消费者实例
     * 2. 进行负载均衡和容量规划
     * 3. 监控特定主机上的消费者行为
     * 
     * @return 返回主机地址字符串，如果原始地址为null则返回空字符串
     */
    public String host() {
        return host;
    }

    /**
     * 获取消费者当前的分区分配信息
     * 包含分配给该成员的所有主题分区，适用于经典和新版消费者组
     * 应用场景：
     * 1. 查看消费者负责处理的分区列表
     * 2. 监控分区分配的均衡性
     * 3. 诊断消费延迟问题
     * 
     * @return 返回当前的分区分配对象，如果未分配则返回空集合的分配对象
     */
    public MemberAssignment assignment() {
        return assignment;
    }

    /**
     * 获取消费者的目标分区分配信息
     * 仅在新版消费者组中使用，表示计划要分配给该成员的分区
     * 应用场景：
     * 1. 在渐进式再平衡过程中预览即将生效的分配方案
     * 2. 评估分区迁移的影响
     * 3. 优化再平衡的性能
     * 
     * @return 返回Optional包装的目标分配对象，如果是经典消费者组则返回空Optional
     */
    public Optional<MemberAssignment> targetAssignment() {
        return targetAssignment;
    }

    /**
     * 获取消费者成员的世代编号
     * 世代编号用于在新版消费者组中跟踪成员的状态变更
     * 应用场景：
     * 1. 确保分区分配的一致性
     * 2. 检测成员状态的变化
     * 3. 协调组成员之间的操作
     * 
     * @return 返回Optional包装的世代编号，对于新版消费者组返回整数值，对于经典消费者组返回空Optional
     */
    public Optional<Integer> memberEpoch() {
        return memberEpoch;
    }

    /**
     * 获取消费者是否已升级到新版协议的标志
     * 用于标识新版消费者组中的成员是否使用新协议
     * 应用场景：
     * 1. 在混合部署环境中识别协议版本
     * 2. 管理协议升级过程
     * 3. 确保组内协议的兼容性
     * 
     * @return 返回Optional包装的布尔值：
     *         - true表示使用新协议
     *         - false表示使用旧协议
     *         - 空Optional表示未知或经典消费者组
     */
    public Optional<Boolean> upgraded() {
        return upgraded;
    }

    @Override
    public String toString() {
        return "(memberId=" + memberId +
            ", groupInstanceId=" + groupInstanceId.orElse("null") +
            ", clientId=" + clientId +
            ", host=" + host +
            ", assignment=" + assignment +
            ", targetAssignment=" + targetAssignment +
            ", memberEpoch=" + memberEpoch.orElse(null) +
            ", upgraded=" + upgraded.orElse(null) +
            ")";
    }
}
