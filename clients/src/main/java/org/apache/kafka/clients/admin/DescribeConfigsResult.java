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
import org.apache.kafka.common.config.ConfigResource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * KafkaAdminClient#describeConfigs(Collection) 调用的结果类。
 * 该类用于异步获取Kafka配置资源的描述信息。
 * 
 * 应用场景：
 * 1. 当需要查询多个Kafka资源（如Topic、Broker等）的配置信息时
 * 2. 在需要批量获取配置并进行验证或更新时
 * 3. 系统启动时进行配置检查和验证
 * 
 * 该类的API仍在演进中，详见 {@link Admin}
 */
@InterfaceStability.Evolving
public class DescribeConfigsResult {

    /**
     * 存储配置资源查询的异步结果映射
     * Key: 配置资源对象（如Topic、Broker等）
     * Value: 对应资源的配置信息的Future对象
     */
    private final Map<ConfigResource, KafkaFuture<Config>> futures;

    /**
     * 构造函数
     * @param futures 配置资源到其对应Future的映射，每个Future包含该资源的配置信息
     */
    protected DescribeConfigsResult(Map<ConfigResource, KafkaFuture<Config>> futures) {
        this.futures = futures;
    }

    /**
     * 获取所有配置资源的Future映射
     * 
     * 使用场景：
     * 1. 需要分别处理每个资源的配置结果时
     * 2. 对不同资源的配置进行独立的异步处理
     * 
     * @return 返回一个Map，key为配置资源，value为包含该资源配置信息的Future
     */
    public Map<ConfigResource, KafkaFuture<Config>> values() {
        return futures;
    }

    /**
     * 获取所有配置描述的组合Future
     * 
     * 实现细节：
     * 1. 使用KafkaFuture.allOf等待所有Future完成
     * 2. 通过thenApply转换结果格式
     * 
     * 使用场景：
     * 1. 需要等待所有配置查询都完成后统一处理
     * 2. 批量验证多个资源的配置信息
     * 
     * @return 返回一个Future，其结果包含所有配置资源及其配置信息的映射
     */
    public KafkaFuture<Map<ConfigResource, Config>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).
                thenApply(v -> {
                    // 创建结果Map，预设容量以优化性能
                    Map<ConfigResource, Config> configs = new HashMap<>(futures.size());
                    // 遍历所有Future，获取其结果并存入结果Map
                    for (Map.Entry<ConfigResource, KafkaFuture<Config>> entry : futures.entrySet()) {
                        try {
                            // 获取每个Future的结果并添加到结果Map中
                            configs.put(entry.getKey(), entry.getValue().get());
                        } catch (InterruptedException | ExecutionException e) {
                            // 此处代码理论上不可达，因为KafkaFuture.allOf已确保所有Future成功完成
                            // 如果出现异常，说明存在并发问题或系统异常
                            throw new RuntimeException(e);
                        }
                    }
                    return configs;
                });
    }
}
