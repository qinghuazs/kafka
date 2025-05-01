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
 * 用于配置{@link Admin#describeClassicGroups(Collection, DescribeClassicGroupsOptions)}操作的选项类。
 * 该类用于查询Kafka经典消费者组的详细信息。
 * <p>
 * 该类的API仍在演进中，详见{@link Admin}。
 * 
 * 应用场景：
 * 1. 消费者组监控：查询消费者组的状态和配置信息
 * 2. 运维管理：检查消费者组的成员分布和分区分配
 * 3. 权限审计：获取消费者组的授权操作信息
 */
@InterfaceStability.Evolving
public class DescribeClassicGroupsOptions extends AbstractOptions<DescribeClassicGroupsOptions> {
    /**
     * 是否包含已授权操作的标志
     * 当设置为true时，查询结果将包含该消费者组被授权的操作列表
     */
    private boolean includeAuthorizedOperations;

    /**
     * 设置是否在查询结果中包含已授权的操作信息
     * 
     * 实现说明：
     * - 通过链式调用方式设置参数
     * - 当启用时，可以获取消费者组的权限信息
     * - 有助于进行权限审计和安全管理
     * 
     * @param includeAuthorizedOperations 是否包含授权操作信息的标志
     * @return 返回当前对象实例，支持链式调用
     */
    public DescribeClassicGroupsOptions includeAuthorizedOperations(boolean includeAuthorizedOperations) {
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        return this;
    }

    /**
     * 获取是否包含已授权操作的配置值
     * 
     * @return 如果设置为true，表示查询结果将包含授权操作信息
     */
    public boolean includeAuthorizedOperations() {
        return includeAuthorizedOperations;
    }
}
