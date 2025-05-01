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
 * 用于{@link Admin#deleteAcls(Collection)}调用的选项类。
 * 
 * 此类用于配置删除ACL（访问控制列表）操作的相关参数。ACL是Kafka中用于实现细粒度权限控制的机制，
 * 可以控制用户对特定资源（如Topic、Group等）的访问权限。通过此类可以自定义删除ACL操作的行为。
 * 
 * 应用场景：
 * 1. 当需要批量删除某些资源的访问控制规则时
 * 2. 在权限清理或权限重组时使用
 * 3. 用于撤销之前授予的访问权限
 * 
 * 注意：该类的API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class DeleteAclsOptions extends AbstractOptions<DeleteAclsOptions> {

    /**
     * 设置此操作的超时时间（以毫秒为单位）。
     * 
     * @param timeoutMs 操作超时时间，如果为{@code null}则使用AdminClient的默认API超时时间
     * @return 当前DeleteAclsOptions实例，用于支持方法链式调用
     * 
     * 实现说明：
     * 1. 将传入的超时时间值赋给timeoutMs字段
     * 2. 返回this以支持链式调用
     * 3. 超时时间决定了删除ACL操作的最长等待时间
     */
    // 此方法保留是为了保持与0.11版本的二进制兼容性
    public DeleteAclsOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

}
