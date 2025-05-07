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
 * 当创建主题的请求不满足为该主题配置的策略时抛出此异常。
 * 
 * 应用场景：
 * 1. 创建的主题配置违反了预定义的命名规则
 * 2. 分区数量或副本因子不符合集群策略要求
 * 3. 主题的配置参数（如保留策略、清理策略）超出允许范围
 * 4. 请求的资源配置超出了租户或用户的配额限制
 * 
 * 设计考虑：
 * - 确保主题创建符合企业规范和最佳实践
 * - 实现多租户环境下的资源管控
 * - 防止错误配置导致的集群性能问题
 */
public class PolicyViolationException extends ApiException {

    public PolicyViolationException(String message) {
        super(message);
    }

    public PolicyViolationException(String message, Throwable cause) {
        super(message, cause);
    }
}
