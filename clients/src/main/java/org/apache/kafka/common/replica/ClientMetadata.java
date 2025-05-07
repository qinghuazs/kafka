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
package org.apache.kafka.common.replica;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.net.InetAddress;
import java.util.Objects;

/**
 * 用于确定首选副本所需的所有客户端元数据的持有者。
 * 该接口封装了客户端的关键信息，包括机架ID、客户端ID、网络地址等，
 * 这些信息用于副本选择器进行智能的副本选择决策。
 */
public interface ClientMetadata {

    /**
     * 客户端发送的机架ID
     * 用于机架感知的副本选择，帮助实现就近读取策略
     * @return 客户端所在的机架ID，如果客户端未指定则可能为null
     */
    String rackId();

    /**
     * 客户端发送的客户端ID
     * 用于唯一标识客户端，便于跟踪和调试
     * @return 客户端的唯一标识符
     */
    String clientId();

    /**
     * 客户端的网络地址
     * 用于网络相关的决策和监控
     * @return 客户端的IP地址
     */
    InetAddress clientAddress();

    /**
     * 客户端的安全主体
     * 用于安全认证和授权决策
     * @return 客户端的Kafka安全主体
     */
    KafkaPrincipal principal();

    /**
     * 客户端的监听器名称
     * 用于多监听器场景下的连接管理
     * @return 客户端连接使用的监听器名称
     */
    String listenerName();


    class DefaultClientMetadata implements ClientMetadata {
        private final String rackId;
        private final String clientId;
        private final InetAddress clientAddress;
        private final KafkaPrincipal principal;
        private final String listenerName;

        public DefaultClientMetadata(String rackId, String clientId, InetAddress clientAddress,
                                     KafkaPrincipal principal, String listenerName) {
            this.rackId = rackId;
            this.clientId = clientId;
            this.clientAddress = clientAddress;
            this.principal = principal;
            this.listenerName = listenerName;
        }

        @Override
        public String rackId() {
            return rackId;
        }

        @Override
        public String clientId() {
            return clientId;
        }

        @Override
        public InetAddress clientAddress() {
            return clientAddress;
        }

        @Override
        public KafkaPrincipal principal() {
            return principal;
        }

        @Override
        public String listenerName() {
            return listenerName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DefaultClientMetadata that = (DefaultClientMetadata) o;
            return Objects.equals(rackId, that.rackId) &&
                    Objects.equals(clientId, that.clientId) &&
                    Objects.equals(clientAddress, that.clientAddress) &&
                    Objects.equals(principal, that.principal) &&
                    Objects.equals(listenerName, that.listenerName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rackId, clientId, clientAddress, principal, listenerName);
        }

        @Override
        public String toString() {
            return "DefaultClientMetadata{" +
                    "rackId='" + rackId + '\'' +
                    ", clientId='" + clientId + '\'' +
                    ", clientAddress=" + clientAddress +
                    ", principal=" + principal +
                    ", listenerName='" + listenerName + '\'' +
                    '}';
        }
    }
}
