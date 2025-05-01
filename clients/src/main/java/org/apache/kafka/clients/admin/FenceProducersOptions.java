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
 * 用于 {@link Admin#fenceProducers(Collection, FenceProducersOptions)} 的配置选项类。
 * 该类用于配置生产者隔离操作的参数，包括超时设置等。
 *
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class FenceProducersOptions extends AbstractOptions<FenceProducersOptions> {
    // 继承自AbstractOptions以获取通用的选项处理功能，如超时设置
    // 目前该类没有额外的配置选项，但未来可能会添加更多选项

    @Override
    public String toString() {
        // 返回包含超时时间的字符串表示
        return "FenceProducersOptions{" +
                "timeoutMs=" + timeoutMs +
                '}';
    }
}
