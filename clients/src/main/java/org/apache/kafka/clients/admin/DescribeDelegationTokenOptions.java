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
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.List;

/**
 * 用于 {@link Admin#describeDelegationToken(DescribeDelegationTokenOptions)} 的配置选项类。
 * 
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeDelegationTokenOptions extends AbstractOptions<DescribeDelegationTokenOptions> {
    // 存储要描述其委托令牌的所有者列表
    private List<KafkaPrincipal> owners;

    /**
     * 设置要描述其委托令牌的所有者列表。
     * 如果owners参数为null，将返回所有用户拥有的令牌以及用户具有Describe权限的令牌。
     * 
     * @param owners 要描述其委托令牌的所有者列表
     * @return 当前DescribeDelegationTokenOptions实例，支持链式调用
     */
    public DescribeDelegationTokenOptions owners(List<KafkaPrincipal> owners) {
        // 设置owners字段的值
        this.owners = owners;
        // 返回当前实例以支持方法链式调用
        return this;
    }

    /**
     * 获取要描述其委托令牌的所有者列表
     * 
     * @return 返回配置的所有者列表，如果未配置则返回null
     */
    public List<KafkaPrincipal> owners() {
        // 返回owners字段的值
        return owners;
    }
}
