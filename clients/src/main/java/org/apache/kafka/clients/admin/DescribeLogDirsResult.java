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
 * {@link Admin#describeLogDirs(Collection)} 调用的结果类。
 * 该类用于获取Kafka broker的日志目录信息。
 *
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeLogDirsResult {
    // 存储每个broker的日志目录描述信息的Future映射
    // key: broker ID
    // value: 该broker的日志目录描述信息Future，其结果是一个从目录路径到目录描述的映射
    private final Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> futures;

    /**
     * 构造函数，初始化日志目录描述结果
     * 
     * @param futures broker ID到其日志目录描述信息Future的映射
     */
    DescribeLogDirsResult(Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> futures) {
        // 初始化futures字段，存储每个broker的异步日志目录描述结果
        this.futures = futures;
    }

    /**
     * 返回从broker ID到其日志目录描述信息Future的映射。
     * Future的结果是一个从broker日志目录路径到该目录描述的映射。
     * 
     * @return broker ID到日志目录描述信息Future的映射
     */
    public Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions() {
        // 返回futures映射，允许调用者分别获取每个broker的日志目录信息
        return futures;
    }

    /**
     * 返回一个Future，只有当所有broker都成功响应时才会完成。
     * Future的结果是一个从broker ID到该broker的日志目录描述映射的映射，
     * 其中日志目录描述是从目录路径到目录描述的映射。
     * 
     * @return 包含所有broker日志目录描述信息的Future
     */
    public KafkaFuture<Map<Integer, Map<String, LogDirDescription>>> allDescriptions() {
        // 使用KafkaFuture.allOf等待所有Future完成
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).
            thenApply(v -> {
                // 创建一个新的HashMap来存储所有broker的日志目录描述
                Map<Integer, Map<String, LogDirDescription>> descriptions = new HashMap<>(futures.size());
                // 遍历所有Future条目
                for (Map.Entry<Integer, KafkaFuture<Map<String, LogDirDescription>>> entry : futures.entrySet()) {
                    try {
                        // 获取每个Future的结果并存入descriptions映射
                        descriptions.put(entry.getKey(), entry.getValue().get());
                    } catch (InterruptedException | ExecutionException e) {
                        // 这种情况理论上不会发生，因为KafkaFuture.allOf已经确保所有Future都成功完成
                        throw new RuntimeException(e);
                    }
                }
                // 返回包含所有broker日志目录描述的映射
                return descriptions;
            });
    }
}
