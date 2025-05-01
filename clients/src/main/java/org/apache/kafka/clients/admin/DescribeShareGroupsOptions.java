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
 * 用于 {@link Admin#describeShareGroups(Collection, DescribeShareGroupsOptions)} 的配置选项类。
 * 该类用于描述Kafka共享组的信息，包括组成员、授权操作等。
 * <p>
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeShareGroupsOptions extends AbstractOptions<DescribeShareGroupsOptions> {
    // 控制是否在响应中包含已授权的操作列表
    private boolean includeAuthorizedOperations;

    /**
     * 设置是否在响应中包含已授权的操作列表
     * 
     * @param includeAuthorizedOperations 如果为true，响应将包含该共享组已被授权的操作列表
     * @return 当前DescribeShareGroupsOptions实例，支持链式调用
     */
    public DescribeShareGroupsOptions includeAuthorizedOperations(boolean includeAuthorizedOperations) {
        // 设置是否包含已授权操作的标志
        this.includeAuthorizedOperations = includeAuthorizedOperations;
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
}