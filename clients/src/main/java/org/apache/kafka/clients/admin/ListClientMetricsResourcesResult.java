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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Collection;

/**
 * Admin#listClientMetricsResources()调用的结果类。
 * <p>
 * 该类的API仍在演进中，详细信息请参见Admin接口的说明。
 */
@InterfaceStability.Evolving
public class ListClientMetricsResourcesResult {
    /**
     * 用于异步获取客户端指标资源列表的Future对象
     * 包含了ClientMetricsResourceListing集合的异步结果
     */
    private final KafkaFuture<Collection<ClientMetricsResourceListing>> future;

    /**
     * 构造函数，初始化结果对象
     * 
     * @param future 包含客户端指标资源列表的KafkaFuture对象
     */
    ListClientMetricsResourcesResult(KafkaFuture<Collection<ClientMetricsResourceListing>> future) {
        // 初始化future字段
        this.future = future;
    }

    /**
     * 返回一个Future对象，该对象要么产生一个异常，要么产生完整的客户端指标列表。
     * 
     * 如果发生失败，Future将只返回首个发生的异常。
     * 
     * @return 返回包含客户端指标资源列表的KafkaFuture对象
     */
    public KafkaFuture<Collection<ClientMetricsResourceListing>> all() {
        // 创建新的KafkaFutureImpl实例用于返回结果
        final KafkaFutureImpl<Collection<ClientMetricsResourceListing>> result = new KafkaFutureImpl<>();
        
        // 为原始future添加完成回调
        future.whenComplete((listings, throwable) -> {
            // 如果存在异常，则使用该异常完成result
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else {
                // 否则，使用获取到的listings完成result
                result.complete(listings);
            }
        });
        
        // 返回结果Future
        return result;
    }
}
