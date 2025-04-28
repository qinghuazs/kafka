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

package org.apache.kafka.common.network;

import java.util.Objects;

/**
 * Kafka客户端信息类，用于存储和管理客户端软件的名称和版本信息。
 * 该类在Kafka网络通信中扮演重要角色，主要用于：
 * 1. 客户端版本兼容性检查
 * 2. 客户端监控和统计
 * 3. 问题诊断和调试
 */
public class ClientInformation {
    /**
     * 表示未知的软件名称或版本的常量值
     * 当客户端未提供软件名称或版本时使用此默认值
     */
    public static final String UNKNOWN_NAME_OR_VERSION = "unknown";

    /**
     * 预定义的空客户端信息实例
     * 用于表示未知客户端的场景，软件名称和版本都设置为unknown
     */
    public static final ClientInformation EMPTY = new ClientInformation(UNKNOWN_NAME_OR_VERSION, UNKNOWN_NAME_OR_VERSION);

    /**
     * 客户端软件名称
     * 例如："apache-kafka-java", "kafka-python"等
     */
    private final String softwareName;

    /**
     * 客户端软件版本
     * 例如："2.8.0", "3.0.0"等
     */
    private final String softwareVersion;

    /**
     * 创建ClientInformation实例
     * 
     * @param softwareName 客户端软件名称，如果为空则使用UNKNOWN_NAME_OR_VERSION
     * @param softwareVersion 客户端软件版本，如果为空则使用UNKNOWN_NAME_OR_VERSION
     */
    public ClientInformation(String softwareName, String softwareVersion) {
        // 如果软件名称为空，则使用默认的未知值
        this.softwareName = softwareName.isEmpty() ? UNKNOWN_NAME_OR_VERSION : softwareName;
        // 如果软件版本为空，则使用默认的未知值
        this.softwareVersion = softwareVersion.isEmpty() ? UNKNOWN_NAME_OR_VERSION : softwareVersion;
    }

    /**
     * 获取客户端软件名称
     * 
     * @return 返回软件名称，如果未设置则返回"unknown"
     */
    public String softwareName() {
        return this.softwareName;
    }

    /**
     * 获取客户端软件版本
     * 
     * @return 返回软件版本，如果未设置则返回"unknown"
     */
    public String softwareVersion() {
        return this.softwareVersion;
    }

    @Override
    public String toString() {
        return "ClientInformation(softwareName=" + softwareName +
            ", softwareVersion=" + softwareVersion + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(softwareName, softwareVersion);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof ClientInformation)) {
            return false;
        }
        ClientInformation other = (ClientInformation) o;
        return other.softwareName.equals(softwareName) &&
            other.softwareVersion.equals(softwareVersion);
    }
}
