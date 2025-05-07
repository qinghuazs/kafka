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
 * 当协调器正在加载元数据时抛出此异常。
 * 
 * 应用场景：
 * 1. 组协调器（Group Coordinator）上下文：
 *    - 当协调器正在加载消费者组的元数据时（例如，在组元数据主题分区发生领导者变更后）
 *    - 在消费者组重新平衡过程中，如果协调器正在加载状态
 * 
 * 2. 事务协调器（Transaction Coordinator）上下文：
 *    - 当存在具有相同事务ID的待处理事务请求时
 *    - 当事务缓存正在从事务日志中被填充时
 * 
 * 设计考虑：
 * 1. 继承自RetriableException，表明这是一个可重试的异常
 * 2. 用于处理协调器临时不可用的情况
 * 3. 提供明确的错误信息，帮助诊断协调器状态
 * 4. 允许客户端在协调器加载完成后重试操作
 */
public class CoordinatorLoadInProgressException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public CoordinatorLoadInProgressException(String message) {
        super(message);
    }

    public CoordinatorLoadInProgressException(String message, Throwable cause) {
        super(message, cause);
    }

}
