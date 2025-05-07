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
 * 副本不合格异常
 * 
 * 该异常在以下场景中抛出：
 * 1. 当一个副本不满足成为Leader副本的条件时
 * 2. 当副本落后于Leader太多，不能作为ISR（In-Sync Replicas）成员时
 * 3. 当副本所在的broker不满足特定的配置要求时
 * 
 * 副本合格性检查的作用：
 * - 确保选择合适的副本作为Leader
 * - 维护数据的一致性和可用性
 * - 保证副本同步的质量
 */
public class IneligibleReplicaException extends ApiException {
    /**
     * 使用指定的错误消息构造异常
     * 
     * @param message 描述副本不合格问题的详细信息
     */
    public IneligibleReplicaException(String message) {
        super(message);
    }
}
