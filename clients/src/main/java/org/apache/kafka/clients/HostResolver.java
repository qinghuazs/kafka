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

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Kafka客户端的主机名解析器接口，用于将主机名解析为IP地址。
 * 该接口定义了DNS解析的核心功能，使Kafka客户端能够灵活地实现不同的主机名解析策略。
 * 通过实现此接口，可以自定义主机名解析的行为，例如：
 * - 支持自定义DNS服务器
 * - 实现DNS缓存机制
 * - 提供故障转移和负载均衡功能
 */
public interface HostResolver {

    /**
     * 将指定的主机名解析为对应的IP地址数组
     * 
     * @param host 需要解析的主机名字符串，可以是域名（如"kafka.apache.org"）或IP地址字符串
     * @return 返回与该主机名关联的所有IP地址数组，支持同一主机名对应多个IP地址的情况
     * @throws UnknownHostException 当无法解析主机名时抛出此异常，可能的原因包括：
     *         - DNS服务器无响应
     *         - 主机名不存在
     *         - 网络连接问题
     */
    InetAddress[] resolve(String host) throws UnknownHostException;
}
