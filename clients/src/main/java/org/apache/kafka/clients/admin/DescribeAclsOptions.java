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

import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * 用于配置{@link Admin#describeAcls(AclBindingFilter)}操作的选项类。
 * ACL（访问控制列表）查询操作用于获取Kafka集群中的访问控制规则信息。
 * 
 * 该类的API仍在演进中，详见{@link Admin}。
 * 
 * 应用场景：
 * 1. 安全审计：查询特定资源（如Topic）的访问控制规则
 * 2. 权限管理：验证用户或客户端的权限配置是否正确
 * 3. 运维监控：监控和检查集群的安全策略设置
 */
@InterfaceStability.Evolving
public class DescribeAclsOptions extends AbstractOptions<DescribeAclsOptions> {

    /**
     * 设置此操作的超时时间（以毫秒为单位）
     * 如果设置为{@code null}，将使用AdminClient的默认API超时时间
     * 
     * 实现说明：
     * - 通过链式调用方式设置超时参数
     * - 超时设置对于控制ACL查询操作的执行时间很重要，特别是在大规模集群中
     * - 该方法保留是为了保持与0.11版本的二进制兼容性
     */
    // 此方法保留是为了保持与0.11版本的二进制兼容性
    public DescribeAclsOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

}
