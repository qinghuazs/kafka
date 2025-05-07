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
 * 重新引导需求异常
 * 
 * 该异常表示Kafka客户端需要重新进行引导（bootstrap）操作。在以下情况下可能抛出此异常：
 * 1. 客户端的元数据严重过期或无效
 * 2. 集群配置发生重大变更，需要重新建立连接
 * 3. 客户端与集群之间的连接状态异常，需要重新初始化
 * 
 * 处理建议：
 * - 重新创建客户端实例
 * - 重新配置并连接到bootstrap服务器
 * - 刷新客户端的元数据缓存
 */
public class RebootstrapRequiredException extends ApiException {
    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     *
     * @param message 异常消息，描述需要重新引导的具体原因
     */
    public RebootstrapRequiredException(String message) {
        super(message);
    }

    /**
     * 构造函数
     *
     * @param message 异常消息，描述需要重新引导的具体原因
     * @param cause 导致此异常的原始异常
     */
    public RebootstrapRequiredException(String message, Throwable cause) {
        super(message, cause);
    }
}
