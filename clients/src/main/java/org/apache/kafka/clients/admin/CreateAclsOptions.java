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
 * 用于 {@link Admin#createAcls(Collection)} 操作的配置选项类。
 * 
 * 该类用于配置创建ACL（访问控制列表）操作的参数。ACL是Kafka的一个重要安全特性，
 * 用于控制客户端对Kafka资源（如Topic、Group、Cluster等）的访问权限。
 * 
 * 应用场景：
 * 1. 为新的用户或客户端配置资源访问权限
 * 2. 在多租户环境中实现细粒度的权限控制
 * 3. 为不同的生产者/消费者分配不同的操作权限
 * 
 * 注意：该API仍在演进中，详见 {@link Admin} 文档。
 */
@InterfaceStability.Evolving
public class CreateAclsOptions extends AbstractOptions<CreateAclsOptions> {

    /**
     * 设置此操作的超时时间（以毫秒为单位）。
     * 
     * 实现细节：
     * 1. 如果设置为null，将使用AdminClient的默认API超时时间
     * 2. 超时设置对于控制ACL创建操作的执行时间很重要，特别是在大规模集群中
     * 3. 该方法支持链式调用，返回this对象以便继续配置其他选项
     * 
     * @param timeoutMs 超时时间（毫秒），如果为null则使用默认超时时间
     * @return 当前CreateAclsOptions实例，支持方法链式调用
     */
    // 该方法保留是为了保持与0.11版本的二进制兼容性
    public CreateAclsOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

}
