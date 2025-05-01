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

/**
 * 用于配置{@link Admin#describeCluster()}操作的选项类。
 *
 * 此类提供了查询Kafka集群信息时的配置选项，包括：
 * - 是否包含授权操作信息
 * - 是否包含已隔离（fenced）的broker信息
 * - 超时设置
 * 
 * 应用场景：
 * 1. 集群健康状态监控
 * 2. 权限管理和审计
 * 3. 运维故障排查
 * 
 * 注意：该API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeClusterOptions extends AbstractOptions<DescribeClusterOptions> {

    /**
     * 是否在响应中包含授权操作信息
     */
    private boolean includeAuthorizedOperations;

    /**
     * 是否在响应中包含已隔离的broker信息
     */
    private boolean includeFencedBrokers;

    /**
     * 设置操作的超时时间（毫秒）。
     * 如果设置为null，则使用AdminClient的默认API超时时间。
     *
     * @param timeoutMs 超时时间，单位为毫秒
     * @return 当前对象，支持链式调用
     */
    // 此方法保留是为了保持与0.11版本的二进制兼容性
    public DescribeClusterOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    /**
     * 设置是否在响应中包含授权操作信息。
     *
     * @param includeAuthorizedOperations 是否包含授权操作信息
     * @return 当前对象，支持链式调用
     */
    public DescribeClusterOptions includeAuthorizedOperations(boolean includeAuthorizedOperations) {
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        return this;
    }

    /**
     * 设置是否在响应中包含已隔离的broker信息。
     *
     * @param includeFencedBrokers 是否包含已隔离的broker信息
     * @return 当前对象，支持链式调用
     */
    public DescribeClusterOptions includeFencedBrokers(boolean includeFencedBrokers) {
        this.includeFencedBrokers = includeFencedBrokers;
        return this;
    }

    /**
     * 获取是否包含授权操作信息的设置。
     * 
     * 注意：某些较旧版本的broker即使请求了此信息也可能无法提供。
     * 
     * @return 如果为true，表示响应中将包含授权操作信息
     */
    public boolean includeAuthorizedOperations() {
        return includeAuthorizedOperations;
    }

    /**
     * 获取是否包含已隔离broker信息的设置。
     * 
     * 注意：某些较旧版本的broker即使请求了此信息也可能无法提供。
     * 
     * @return 如果为true，表示响应中将包含已隔离的broker信息
     */
    public boolean includeFencedBrokers() {
        return includeFencedBrokers;
    }
}
