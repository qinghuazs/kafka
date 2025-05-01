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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * {@link KafkaAdminClient#describeShareGroups(Collection, DescribeShareGroupsOptions)} 调用的结果类。
 * <p>
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeShareGroupsResult {

    // 存储每个共享组ID对应的描述信息Future的映射
    // 使用KafkaFuture而不是CompletableFuture是为了提供更好的异常处理和类型安全
    private final Map<String, KafkaFuture<ShareGroupDescription>> futures;

    /**
     * 构造函数，初始化共享组描述结果
     * 
     * @param futures 包含每个共享组ID对应的描述信息Future的映射
     */
    public DescribeShareGroupsResult(final Map<String, KafkaFuture<ShareGroupDescription>> futures) {
        // 初始化futures字段，存储每个共享组的异步描述结果
        this.futures = futures;
    }

    /**
     * 返回从共享组ID到其描述信息Future的映射
     * 
     * @return 返回一个新的HashMap，包含所有共享组的描述信息Future
     */
    public Map<String, KafkaFuture<ShareGroupDescription>> describedGroups() {
        // 返回futures的副本以防止外部修改
        return new HashMap<>(futures);
    }

    /**
     * 返回一个Future，当所有描述操作都成功完成时，该Future将包含所有ShareGroupDescription对象
     * 
     * @return 包含所有共享组描述信息的Future
     */
    public KafkaFuture<Map<String, ShareGroupDescription>> all() {
        // 使用KafkaFuture.allOf等待所有Future完成
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).thenApply(
            nil -> {
                // 创建一个新的HashMap来存储所有描述结果
                Map<String, ShareGroupDescription> descriptions = new HashMap<>(futures.size());
                // 遍历所有Future，获取结果并存入map
                futures.forEach((key, future) -> {
                    try {
                        // 获取Future的结果并放入descriptions map
                        descriptions.put(key, future.get());
                    } catch (InterruptedException | ExecutionException e) {
                        // 这种情况理论上不会发生，因为KafkaFuture.allOf已经确保所有Future都成功完成
                        throw new RuntimeException(e);
                    }
                });
                // 返回包含所有共享组描述信息的map
                return descriptions;
            });
    }
}
