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
package org.apache.kafka.common;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Objects;
import java.util.Optional;

/**
 * 表示一个Broker端点。
 * 端点包含了Broker的监听器名称、安全协议、主机名和端口等信息，
 * 用于客户端与Broker建立连接。
 */
@InterfaceStability.Evolving
public class Endpoint {

    // 监听器名称，用于标识特定的监听器配置
    private final String listenerName;
    // 安全协议类型，如PLAINTEXT、SSL、SASL等
    private final SecurityProtocol securityProtocol;
    // Broker的主机名或IP地址
    private final String host;
    // Broker监听的端口号
    private final int port;

    /**
     * 创建一个新的端点实例
     * @param listenerName 监听器名称
     * @param securityProtocol 安全协议
     * @param host 主机名
     * @param port 端口号
     */
    public Endpoint(String listenerName, SecurityProtocol securityProtocol, String host, int port) {
        this.listenerName = listenerName;
        this.securityProtocol = securityProtocol;
        this.host = host;
        this.port = port;
    }

    /**
     * 返回端点的监听器名称。
     * 对于提供给broker插件的端点，该值不为空；
     * 但在客户端使用时可能为空。
     */
    public Optional<String> listenerName() {
        return Optional.ofNullable(listenerName);
    }

    /**
     * 返回端点使用的安全协议
     */
    public SecurityProtocol securityProtocol() {
        return securityProtocol;
    }

    /**
     * 返回端点的已配置主机名
     */
    public String host() {
        return host;
    }

    /**
     * 返回监听器绑定的端口号
     */
    public int port() {
        return port;
    }

    /**
     * 比较两个端点是否相等
     * 当所有字段(监听器名称、安全协议、主机名、端口)都相等时返回true
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Endpoint)) {
            return false;
        }

        Endpoint that = (Endpoint) o;
        return Objects.equals(this.listenerName, that.listenerName) &&
            Objects.equals(this.securityProtocol, that.securityProtocol) &&
            Objects.equals(this.host, that.host) &&
            this.port == that.port;

    }

    /**
     * 生成端点的哈希码
     * 使用所有字段计算哈希值
     */
    @Override
    public int hashCode() {
        return Objects.hash(listenerName, securityProtocol, host, port);
    }

    /**
     * 返回端点的字符串表示
     * 包含所有字段的值
     */
    @Override
    public String toString() {
        return "Endpoint(" +
            "listenerName='" + listenerName + "'" +
            ", securityProtocol=" + securityProtocol +
            ", host='" + host + "'" +
            ", port=" + port +
            ")";
    }
}
