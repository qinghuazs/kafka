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

/**
 * 节点连接状态枚举类
 *
 * 连接状态的生命周期：
 * 1. DISCONNECTED（未连接）：初始状态，表示连接尚未建立
 * 2. CONNECTING（连接中）：正在尝试建立连接
 * 3. CHECKING_API_VERSIONS（API版本检查中）：连接已建立，正在进行API版本兼容性检查
 *    - 如果版本检查失败，连接将关闭并返回到DISCONNECTED状态
 * 4. READY（就绪）：连接完全建立，可以发送请求
 * 5. AUTHENTICATION_FAILED（认证失败）：连接因认证错误而失败
 *    - 这是一个终止状态，需要重新初始化连接才能继续
 */
public enum ConnectionState {
    // 未连接状态：表示连接尚未建立或已断开
    DISCONNECTED,
    // 连接中状态：正在尝试建立TCP连接
    CONNECTING,
    // API版本检查状态：正在验证客户端和服务器的API版本兼容性
    CHECKING_API_VERSIONS,
    // 就绪状态：连接已完全建立，可以正常通信
    READY,
    // 认证失败状态：由于认证问题导致连接失败
    AUTHENTICATION_FAILED;

    /**
     * 检查连接是否处于断开状态
     * @return 如果连接处于AUTHENTICATION_FAILED或DISCONNECTED状态则返回true
     */
    public boolean isDisconnected() {
        return this == AUTHENTICATION_FAILED || this == DISCONNECTED;
    }

    /**
     * 检查连接是否已建立
     * @return 如果连接处于CHECKING_API_VERSIONS或READY状态则返回true
     */
    public boolean isConnected() {
        return this == CHECKING_API_VERSIONS || this == READY;
    }
}
