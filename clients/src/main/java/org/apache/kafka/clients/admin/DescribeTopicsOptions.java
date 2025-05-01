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

import java.util.Collection;

/**
 * 用于 {@link Admin#describeTopics(Collection)} 的配置选项类。
 * 该类用于描述Kafka主题的信息，包括分区信息、授权操作等。
 *
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeTopicsOptions extends AbstractOptions<DescribeTopicsOptions> {

    // 控制是否在响应中包含已授权的操作列表
    private boolean includeAuthorizedOperations;
    // 控制每个响应中返回的最大分区数量，默认为2000
    private int partitionSizeLimitPerResponse = 2000;

    /**
     * 设置操作的超时时间（毫秒）
     * 如果为null，则使用AdminClient的默认API超时时间
     * 
     * 注：此方法保留是为了保持与0.11版本的二进制兼容性
     */
    public DescribeTopicsOptions timeoutMs(Integer timeoutMs) {
        // 设置超时时间
        this.timeoutMs = timeoutMs;
        // 返回当前实例以支持方法链式调用
        return this;
    }

    /**
     * 设置是否在响应中包含已授权的操作列表
     * 
     * @param includeAuthorizedOperations 如果为true，响应将包含该主题已被授权的操作列表
     * @return 当前DescribeTopicsOptions实例，支持链式调用
     */
    public DescribeTopicsOptions includeAuthorizedOperations(boolean includeAuthorizedOperations) {
        // 设置是否包含已授权操作的标志
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        // 返回当前实例以支持方法链式调用
        return this;
    }

    /**
     * 设置每个响应中返回的最大分区数量
     * 注意：如果设置的值大于服务器端的max.request.partition.size.limit配置，该设置将不会生效
     * 
     * @param partitionSizeLimitPerResponse 每个响应中返回的最大分区数量
     * @return 当前DescribeTopicsOptions实例，支持链式调用
     */
    public DescribeTopicsOptions partitionSizeLimitPerResponse(int partitionSizeLimitPerResponse) {
        // 设置每个响应的分区数量限制
        this.partitionSizeLimitPerResponse = partitionSizeLimitPerResponse;
        // 返回当前实例以支持方法链式调用
        return this;
    }

    /**
     * 获取是否包含已授权操作的设置
     * 
     * @return 如果为true，表示响应中将包含已授权的操作列表；否则不包含
     */
    public boolean includeAuthorizedOperations() {
        // 返回是否包含已授权操作的标志值
        return includeAuthorizedOperations;
    }

    /**
     * 获取每个响应中返回的最大分区数量
     * 
     * @return 返回每个响应中允许的最大分区数量
     */
    public int partitionSizeLimitPerResponse() {
        // 返回分区数量限制值
        return partitionSizeLimitPerResponse;
    }
}
