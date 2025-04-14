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

import java.util.Locale;

/**
 * Kafka客户端DNS查找策略的枚举类。
 * 定义了两种DNS解析方式：使用所有DNS IP地址或仅解析规范引导服务器。
 */
public enum ClientDnsLookup {
    /**
     * 使用所有可用的DNS IP地址。
     * 当配置为此选项时，客户端将使用DNS查询返回的所有IP地址来连接服务器。
     */
    USE_ALL_DNS_IPS("use_all_dns_ips"),

    /**
     * 仅解析规范引导服务器。
     * 当配置为此选项时，客户端将只使用引导服务器的规范主机名对应的IP地址。
     */
    RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY("resolve_canonical_bootstrap_servers_only");

    /**
     * 存储枚举值对应的配置字符串
     */
    private final String clientDnsLookup;

    /**
     * 构造函数
     * @param clientDnsLookup DNS查找策略的配置字符串
     */
    ClientDnsLookup(String clientDnsLookup) {
        this.clientDnsLookup = clientDnsLookup;
    }

    /**
     * 重写toString方法，返回DNS查找策略的配置字符串
     * @return 返回DNS查找策略的配置字符串
     */
    @Override
    public String toString() {
        return clientDnsLookup;
    }

    /**
     * 根据配置字符串获取对应的DNS查找策略枚举值
     * @param config 配置字符串
     * @return 返回对应的ClientDnsLookup枚举值
     */
    public static ClientDnsLookup forConfig(String config) {
        // 将配置字符串转换为大写并返回对应的枚举值
        return ClientDnsLookup.valueOf(config.toUpperCase(Locale.ROOT));
    }
}
