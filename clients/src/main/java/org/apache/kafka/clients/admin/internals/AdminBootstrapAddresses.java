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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Kafka管理客户端的引导地址配置类
 * 该类用于管理Kafka集群的引导地址，支持两种模式：
 * 1. 普通的bootstrap servers模式：用于连接任意的broker节点
 * 2. bootstrap controllers模式：专门用于连接controller节点
 */
public final class AdminBootstrapAddresses {
    /**
     * 标识是否使用bootstrap controllers模式
     * true表示使用controller节点作为引导地址
     * false表示使用普通broker节点作为引导地址
     */
    private final boolean usingBootstrapControllers;

    /**
     * 存储解析后的引导服务器地址列表
     * 可能是broker地址列表或controller地址列表，取决于usingBootstrapControllers的值
     */
    private final List<InetSocketAddress> addresses;

    /**
     * 构造函数
     * @param usingBootstrapControllers 是否使用bootstrap controllers模式
     * @param addresses 解析后的地址列表
     */
    AdminBootstrapAddresses(
        boolean usingBootstrapControllers,
        List<InetSocketAddress> addresses
    ) {
        this.usingBootstrapControllers = usingBootstrapControllers;
        this.addresses = addresses;
    }

    /**
     * 获取当前是否使用bootstrap controllers模式
     * @return true表示使用controller节点，false表示使用普通broker节点
     */
    public boolean usingBootstrapControllers() {
        return usingBootstrapControllers;
    }

    /**
     * 获取解析后的地址列表
     * @return 引导服务器的地址列表
     */
    public List<InetSocketAddress> addresses() {
        return addresses;
    }

    /**
     * 从配置中创建AdminBootstrapAddresses实例
     * 该方法会处理bootstrap.servers和bootstrap.controllers两个配置项
     * 这两个配置项互斥，必须设置其中之一，但不能同时设置
     *
     * @param config Kafka客户端配置对象
     * @return 根据配置创建的AdminBootstrapAddresses实例
     * @throws ConfigException 当配置无效时抛出异常
     */
    public static AdminBootstrapAddresses fromConfig(AbstractConfig config) {
        // 获取bootstrap.servers配置项的值，如果未配置则使用空列表
        List<String> bootstrapServers = config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrapServers == null) {
            bootstrapServers = Collections.emptyList();
        }

        // 获取bootstrap.controllers配置项的值，如果未配置则使用空列表
        List<String> controllerServers = config.getList(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG);
        if (controllerServers == null) {
            controllerServers = Collections.emptyList();
        }

        // 获取客户端DNS查找配置，用于地址解析
        String clientDnsLookupConfig = config.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG);

        // 如果bootstrap.servers未配置
        if (bootstrapServers.isEmpty()) {
            // 如果bootstrap.controllers也未配置，抛出异常
            if (controllerServers.isEmpty()) {
                throw new ConfigException("You must set either " +
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + " or " +
                        AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG);
            } else {
                // 使用bootstrap.controllers模式，解析并验证controller地址
                return new AdminBootstrapAddresses(true,
                    ClientUtils.parseAndValidateAddresses(controllerServers, clientDnsLookupConfig));
            }
        } else {
            // 如果bootstrap.controllers未配置，使用bootstrap.servers模式
            if (controllerServers.isEmpty()) {
                return new AdminBootstrapAddresses(false,
                    ClientUtils.parseAndValidateAddresses(bootstrapServers, clientDnsLookupConfig));
            } else {
                // 如果两个配置都设置了，抛出异常
                throw new ConfigException("You cannot set both " +
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + " and " +
                        AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG);
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(usingBootstrapControllers, addresses);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!o.getClass().equals(AdminBootstrapAddresses.class))) return false;
        AdminBootstrapAddresses other = (AdminBootstrapAddresses) o;
        return usingBootstrapControllers == other.usingBootstrapControllers &&
            addresses.equals(other.addresses);
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("AdminBootstrapAddresses");
        bld.append("(usingBoostrapControllers=").append(usingBootstrapControllers);
        bld.append(", addresses=[");
        String prefix = "";
        for (InetSocketAddress address : addresses) {
            bld.append(prefix).append(address);
            prefix = ", ";
        }
        bld.append("])");
        return bld.toString();
    }
}
