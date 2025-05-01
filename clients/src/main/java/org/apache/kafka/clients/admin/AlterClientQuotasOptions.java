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
 * {@link Admin#alterClientQuotas(Collection, AlterClientQuotasOptions)}方法的选项类
 *
 * 该类用于配置修改客户端配额操作的行为。继承自AbstractOptions，提供了基本的超时设置功能，
 * 并增加了验证模式选项，允许在不实际修改配置的情况下验证修改请求的有效性。
 *
 * 注意：这个类的API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class AlterClientQuotasOptions extends AbstractOptions<AlterClientQuotasOptions> {

    /**
     * 是否仅验证请求而不实际修改配置
     * 默认为false，表示会实际执行修改操作
     */
    private boolean validateOnly = false;

    /**
     * 获取是否仅验证请求而不修改配置
     *
     * @return 如果为true，表示仅验证请求；如果为false，表示会实际执行修改
     */
    public boolean validateOnly() {
        return this.validateOnly;
    }

    /**
     * 设置是否仅验证请求而不修改配置
     *
     * @param validateOnly 如果设置为true，则只验证请求的有效性而不实际修改配置；
     *                    如果设置为false，则会实际执行修改操作
     * @return 返回当前对象实例，支持方法链式调用
     */
    public AlterClientQuotasOptions validateOnly(boolean validateOnly) {
        // 设置验证模式标志
        this.validateOnly = validateOnly;
        // 返回当前实例，支持链式调用
        return this;
    }
}
