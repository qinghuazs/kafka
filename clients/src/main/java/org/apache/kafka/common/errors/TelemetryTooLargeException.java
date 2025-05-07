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
 * 遥测数据过大异常
 * 
 * 该异常表示Kafka遥测指标数据的大小超过了允许的最大限制。
 * 在以下情况下可能会抛出此异常：
 * 1. 收集的监控指标数量过多
 * 2. 单个指标的数据量过大
 * 3. 批量上报的遥测数据超过了服务端的接收限制
 * 
 * 处理建议：
 * - 检查遥测配置，适当调整采集频率
 * - 优化指标收集策略，只收集必要的指标
 * - 考虑使用采样或聚合来减少数据量
 * - 适当调整批量上报的大小限制
 */
public class TelemetryTooLargeException extends ApiException {

    public TelemetryTooLargeException(String message) {
        super(message);
    }
}
