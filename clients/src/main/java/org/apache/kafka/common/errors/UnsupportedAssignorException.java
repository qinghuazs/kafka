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

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * 当使用了不支持的分区分配器类型时抛出此异常。
 * 
 * 应用场景：
 * 1. 消费者组配置了系统不支持的分区分配策略
 * 2. 新版本客户端使用了旧版本协调器不支持的分配器
 * 3. 消费者组成员之间的分配器配置不一致
 * 
 * 设计考虑：
 * - 接口稳定性标记为Evolving，表明API可能在未来版本中变化
 * - 帮助用户快速发现分区分配配置问题
 * - 确保消费者组内所有成员使用兼容的分配策略
 */
@InterfaceStability.Evolving
public class UnsupportedAssignorException extends ApiException {
    public UnsupportedAssignorException(String message) {
        super(message);
    }
}
