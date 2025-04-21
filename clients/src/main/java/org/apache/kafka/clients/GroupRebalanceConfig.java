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
package org.apache.kafka.clients;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.requests.JoinGroupRequest;

import java.util.Locale;
import java.util.Optional;

/**
 * 用于提取和管理Kafka消费者组重平衡相关的配置类。
 * 该类负责处理会话超时、重平衡超时、心跳间隔等关键配置参数，
 * 并根据不同的协议类型(消费者、连接器、共享)采用相应的配置策略。
 */
public class GroupRebalanceConfig {

    /**
     * 定义支持的协议类型枚举
     * CONSUMER: 消费者协议，用于普通的消费者组
     * CONNECT: 连接器协议，用于Kafka Connect框架
     * SHARE: 共享协议，用于资源共享场景
     */
    public enum ProtocolType {
        CONSUMER,
        CONNECT,
        SHARE;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    // 会话超时时间(毫秒)，如果消费者在该时间内没有发送心跳，将被认为已死亡
    public final int sessionTimeoutMs;
    // 重平衡超时时间(毫秒)，表示重平衡操作必须在该时间内完成
    public final int rebalanceTimeoutMs;
    // 心跳发送间隔(毫秒)，消费者定期向协调者发送心跳的时间间隔
    public final int heartbeatIntervalMs;
    // 消费者组ID，用于标识消费者所属的组
    public final String groupId;
    // 消费者实例ID，用于静态成员机制，仅在消费者协议中支持
    public final Optional<String> groupInstanceId;
    // 重试回退时间(毫秒)，操作失败后的初始等待时间
    public final long retryBackoffMs;
    // 最大重试回退时间(毫秒)，重试等待时间的上限
    public final long retryBackoffMaxMs;
    // 关闭时是否主动离开组，仅在消费者协议中可配置
    public final boolean leaveGroupOnClose;

    /**
     * 主构造函数，根据配置对象和协议类型初始化所有配置参数
     * @param config 包含所有配置项的配置对象
     * @param protocolType 协议类型，决定特定配置的处理逻辑
     */
    public GroupRebalanceConfig(AbstractConfig config, ProtocolType protocolType) {
        // 设置会话超时时间
        this.sessionTimeoutMs = config.getInt(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG);

        // 根据不同的协议类型设置重平衡超时时间
        // 消费者和共享协议使用MAX_POLL_INTERVAL_MS_CONFIG
        // 连接器使用REBALANCE_TIMEOUT_MS_CONFIG
        if ((protocolType == ProtocolType.CONSUMER) || (protocolType == ProtocolType.SHARE)) {
            this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        } else {
            this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG);
        }

        // 设置心跳间隔时间
        this.heartbeatIntervalMs = config.getInt(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG);
        // 设置消费者组ID
        this.groupId = config.getString(CommonClientConfigs.GROUP_ID_CONFIG);

        // 处理静态成员配置，仅在消费者协议中支持
        if (protocolType == ProtocolType.CONSUMER) {
            String groupInstanceId = config.getString(CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG);
            if (groupInstanceId != null) {
                // 验证实例ID的有效性
                JoinGroupRequest.validateGroupInstanceId(groupInstanceId);
                this.groupInstanceId = Optional.of(groupInstanceId);
            } else {
                this.groupInstanceId = Optional.empty();
            }
        } else {
            this.groupInstanceId = Optional.empty();
        }

        // 设置重试相关的配置
        this.retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);
        this.retryBackoffMaxMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MAX_MS_CONFIG);

        // 设置关闭时的组离开行为，仅消费者协议可配置
        if (protocolType == ProtocolType.CONSUMER) {
            this.leaveGroupOnClose = config.getBoolean("internal.leave.group.on.close");
        } else {
            this.leaveGroupOnClose = true;
        }
    }

    /**
     * 用于测试目的的构造函数，允许直接设置所有配置参数
     * 
     * @param sessionTimeoutMs 会话超时时间(毫秒)，用于测试成员存活检测机制
     * @param rebalanceTimeoutMs 重平衡超时时间(毫秒)，用于测试重平衡过程的时间限制
     * @param heartbeatIntervalMs 心跳间隔时间(毫秒)，用于测试心跳机制
     * @param groupId 消费者组ID，用于测试组成员身份识别
     * @param groupInstanceId 消费者实例ID，用于测试静态成员机制
     * @param retryBackoffMs 重试回退时间(毫秒)，用于测试失败重试机制
     * @param retryBackoffMaxMs 最大重试回退时间(毫秒)，用于测试重试时间上限
     * @param leaveGroupOnClose 是否在关闭时离开组，用于测试组成员优雅退出
     */
    public GroupRebalanceConfig(final int sessionTimeoutMs,
                                final int rebalanceTimeoutMs,
                                final int heartbeatIntervalMs,
                                String groupId,
                                Optional<String> groupInstanceId,
                                long retryBackoffMs,
                                long retryBackoffMaxMs,
                                boolean leaveGroupOnClose) {
        // 初始化会话超时时间
        this.sessionTimeoutMs = sessionTimeoutMs;
        // 初始化重平衡超时时间
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        // 初始化心跳间隔时间
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        // 初始化消费者组ID
        this.groupId = groupId;
        // 初始化消费者实例ID（静态成员ID）
        this.groupInstanceId = groupInstanceId;
        // 初始化重试回退时间
        this.retryBackoffMs = retryBackoffMs;
        // 初始化最大重试回退时间
        this.retryBackoffMaxMs = retryBackoffMaxMs;
        // 初始化关闭时的组离开行为
        this.leaveGroupOnClose = leaveGroupOnClose;
    }
}
