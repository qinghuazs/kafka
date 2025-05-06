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

import java.util.Locale;
import java.util.Objects;

/**
 * Raft仲裁投票者的网络端点。
 * 
 * 该类用于表示Kafka集群中参与Raft共识协议的投票者节点的网络连接信息。
 * 在Kafka的KRaft（Kafka Raft）模式中，这些端点用于：
 * 1. 维护集群中控制器之间的通信
 * 2. 参与领导者选举过程
 * 3. 处理元数据的复制和同步
 */
@InterfaceStability.Stable
public class RaftVoterEndpoint {
    /**
     * 投票者节点的人类可读名称，必须是大写字母，例如：CONTROLLER_1, BROKER_2 等
     */
    private final String name;

    /**
     * 投票者节点的DNS主机名或IP地址
     */
    private final String host;

    /**
     * 投票者节点的网络端口号
     */
    private final int port;

    /**
     * 验证输入字符串是否符合要求：非空、无前后空格、全大写
     * 
     * @param input 需要验证的输入字符串
     * @return 验证通过的字符串
     * @throws IllegalArgumentException 当输入不符合要求时抛出异常
     */
    static String requireNonNullAllCapsNonEmpty(String input) {
        // 检查输入是否为null
        if (input == null) {
            throw new IllegalArgumentException("Null argument not allowed.");
        }
        // 检查是否有前导或尾随空格
        if (!input.trim().equals(input)) {
            throw new IllegalArgumentException("Leading or trailing whitespace is not allowed.");
        }
        // 检查是否为空字符串
        if (input.isEmpty()) {
            throw new IllegalArgumentException("Empty string is not allowed.");
        }
        // 检查是否全部为大写字母
        if (!input.toUpperCase(Locale.ROOT).equals(input)) {
            throw new IllegalArgumentException("String must be UPPERCASE.");
        }
        return input;
    }

    /**
     * 创建一个Raft投票者端点实例
     *
     * @param name 端点的人类可读名称，必须是大写字母，例如：CONTROLLER
     * @param host 端点的DNS主机名或IP地址
     * @param port 端点的网络端口号
     * @throws IllegalArgumentException 当name不符合要求（非空、无空格、全大写）时抛出异常
     * @throws NullPointerException 当host为null时抛出异常
     */
    public RaftVoterEndpoint(
        String name,
        String host,
        int port
    ) {
        // 验证并设置节点名称
        this.name = requireNonNullAllCapsNonEmpty(name);
        // 验证并设置主机名
        this.host = Objects.requireNonNull(host);
        // 设置端口号
        this.port = port;
    }

    /**
     * 获取投票者节点的名称
     * @return 节点名称（大写字母格式）
     */
    public String name() {
        return name;
    }

    /**
     * 获取投票者节点的主机名或IP地址
     * @return 主机名或IP地址
     */
    public String host() {
        return host;
    }

    /**
     * 获取投票者节点的端口号
     * @return 端口号
     */
    public int port() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!o.getClass().equals(getClass()))) return false;
        RaftVoterEndpoint other = (RaftVoterEndpoint) o;
        return name.equals(other.name) &&
            host.equals(other.host) &&
            port == other.port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, host, port);
    }

    @Override
    public String toString() {
        // enclose IPv6 hosts in square brackets for readability
        String hostString = host.contains(":") ? "[" + host + "]" : host;
        return name + "://" + hostString + ":" + port;
    }
}
