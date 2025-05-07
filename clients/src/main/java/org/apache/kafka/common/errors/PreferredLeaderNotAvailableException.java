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
package org.apache.kafka.common.errors;

/**
 * 首选Leader副本不可用异常
 * 
 * 当分区的首选Leader副本（通常是第一个分配的副本）不可用时抛出此异常。这种情况可能发生在：
 * 1. Leader副本所在的Broker发生故障
 * 2. 网络分区导致Leader副本暂时不可访问
 * 3. 正在进行Leader选举或分区重分配
 * 
 * 影响：
 * - 可能导致写入性能下降，因为请求可能被路由到非优选的副本
 * - 如果配置了首选Leader选举，系统会尝试将Leader角色恢复到首选副本
 * 
 * 处理建议：
 * - 检查Broker的健康状态
 * - 等待自动Leader选举完成
 * - 考虑手动触发Leader选举（如果必要）
 */
public class PreferredLeaderNotAvailableException extends InvalidMetadataException {

    public PreferredLeaderNotAvailableException(String message) {
        super(message);
    }

    public PreferredLeaderNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
