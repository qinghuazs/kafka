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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.Objects;

/**
 * 描述Kafka集群中单个共享消费者组成员的详细信息。
 * 
 * 该类在共享消费者组管理中发挥关键作用：
 * 1. 记录和跟踪每个成员的基本信息（ID、客户端ID、主机等）
 * 2. 维护成员的分区分配状态，支持共享消费者组的分区管理
 * 3. 通过成员世代（epoch）跟踪成员状态的变更历史
 * 4. 配合ShareGroupDescription提供完整的共享消费者组视图
 */
@InterfaceStability.Evolving
public class ShareMemberDescription {
    /**
     * 共享消费者组成员的唯一标识符
     * - 在组内具有唯一性，用于区分不同的成员
     * - 由Kafka自动生成，格式通常为：consumerId-随机UUID
     */
    private final String memberId;

    /**
     * 客户端应用程序的标识符
     * - 由客户端在创建消费者时指定
     * - 用于跟踪和调试目的，帮助识别具体的客户端应用
     */
    private final String clientId;

    /**
     * 运行该成员的主机信息
     * - 通常包含主机名或IP地址
     * - 用于定位成员的物理位置，便于问题排查
     */
    private final String host;

    /**
     * 该成员当前的分区分配信息
     * - 包含分配给该成员的所有主题分区
     * - 在共享消费者组的再平衡过程中会更新
     */
    private final ShareMemberAssignment assignment;

    /**
     * 成员的世代号
     * - 用于跟踪成员状态的变更历史
     * - 每次成员加入、离开或更新状态时递增
     * - 协助检测成员状态的不一致性
     */
    private final int memberEpoch;

    /**
     * 创建ShareMemberDescription实例
     * 
     * @param memberId 成员ID，如果为null则使用空字符串
     * @param clientId 客户端ID，如果为null则使用空字符串
     * @param host 主机信息，如果为null则使用空字符串
     * @param assignment 分区分配信息，如果为null则创建空的分配
     * @param memberEpoch 成员世代号
     */
    public ShareMemberDescription(
        String memberId,
        String clientId,
        String host,
        ShareMemberAssignment assignment,
        int memberEpoch
    ) {
        // 处理null值，确保字段不为null
        this.memberId = memberId == null ? "" : memberId;
        this.clientId = clientId == null ? "" : clientId;
        this.host = host == null ? "" : host;
        // 如果assignment为null，创建一个空的分配对象
        this.assignment = assignment == null ?
            new ShareMemberAssignment(Collections.emptySet()) : assignment;
        this.memberEpoch = memberEpoch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShareMemberDescription that = (ShareMemberDescription) o;
        return memberId.equals(that.memberId) &&
            clientId.equals(that.clientId) &&
            host.equals(that.host) &&
            assignment.equals(that.assignment) &&
            memberEpoch == that.memberEpoch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(memberId, clientId, host, assignment, memberEpoch);
    }

    /**
     * 获取成员的消费者ID
     * 
     * @return 返回成员的唯一标识符，该ID在共享消费者组内唯一
     */
    public String consumerId() {
        return memberId;
    }

    /**
     * 获取成员的客户端ID
     * 
     * @return 返回客户端应用程序的标识符，用于日志和监控
     */
    public String clientId() {
        return clientId;
    }

    /**
     * 获取成员所在的主机信息
     * 
     * @return 返回运行该成员的主机名或IP地址
     */
    public String host() {
        return host;
    }

    /**
     * 获取成员的分区分配信息
     * 
     * @return 返回当前分配给该成员的主题分区集合
     */
    public ShareMemberAssignment assignment() {
        return assignment;
    }

    /**
     * 获取成员的世代号
     * 
     * @return 返回当前的成员世代号，用于跟踪状态变更
     */
    public int memberEpoch() {
        return memberEpoch;
    }

    @Override
    public String toString() {
        return "(memberId=" + memberId +
            ", clientId=" + clientId +
            ", host=" + host +
            ", assignment=" + assignment +
            ", memberEpoch=" + memberEpoch +
            ")";
    }
}
