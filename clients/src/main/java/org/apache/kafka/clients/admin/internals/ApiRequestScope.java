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
package org.apache.kafka.clients.admin.internals;

import java.util.OptionalInt;

/**
 * This interface is used by {@link AdminApiDriver} to bridge the gap
 * to the internal `NodeProvider` defined in
 * {@link org.apache.kafka.clients.admin.KafkaAdminClient}. However, a
 * request scope is more than just a target broker specification. It also
 * provides a way to group key lookups according to different batching
 * mechanics. See {@link AdminApiLookupStrategy#lookupScope(Object)} for
 * more detail.
 *
 * 该接口由{@link AdminApiDriver}使用，用于连接内部定义在{@link org.apache.kafka.clients.admin.KafkaAdminClient}
 * 中的`NodeProvider`。然而，请求范围不仅仅是目标代理的规范，它还提供了一种根据不同批处理机制对键查找进行分组的方式。
 * 更多详细信息请参见{@link AdminApiLookupStrategy#lookupScope(Object)}。
 *
 * 应用场景：
 * 1. 在Kafka管理客户端中，用于确定请求应该发送到哪个broker
 * 2. 支持批量处理请求，提高请求处理效率
 * 3. 实现请求路由和负载均衡策略
 */
public interface ApiRequestScope {

    /**
     * Get the target broker ID that a request is intended for or
     * empty if the request can be sent to any broker.
     *
     * Note that if the destination broker ID is present in the
     * {@link ApiRequestScope} returned by {@link AdminApiLookupStrategy#lookupScope(Object)},
     * then no lookup will be attempted.
     *
     * @return optional broker ID
     *
     * 获取请求目标broker的ID，如果请求可以发送到任意broker则返回空。
     *
     * 注意：如果在{@link AdminApiLookupStrategy#lookupScope(Object)}返回的{@link ApiRequestScope}中
     * 存在目标broker ID，则不会尝试进行查找。
     *
     * 实现说明：
     * 1. 默认实现返回OptionalInt.empty()，表示请求可以发送到任意broker
     * 2. 子类可以重写此方法以指定特定的目标broker
     * 3. 返回类型使用OptionalInt以优雅处理可能不存在目标broker的情况
     *
     * @return 可选的broker ID
     */
    default OptionalInt destinationBrokerId() {
        return OptionalInt.empty();
    }

}
