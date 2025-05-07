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
 * 当Topic分区的副本分配方案无效时抛出此异常。
 * 
 * 应用场景：
 * 1. 创建Topic时指定了无效的副本分配方案
 * 2. 当副本分配违反了基本规则，如：
 *    - 副本数量不足或过多
 *    - 同一分区的多个副本被分配到同一个Broker
 *    - 分配的Broker ID不存在或无效
 * 3. 在分区重分配过程中指定了不合理的目标分配方案
 * 
 * 设计考虑：
 * - 确保副本分配的合理性和可用性
 * - 防止因错误的副本分配导致数据可靠性降低
 * - 维护集群的负载均衡
 */
public class InvalidReplicaAssignmentException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidReplicaAssignmentException(String message) {
        super(message);
    }

    public InvalidReplicaAssignmentException(String message, Throwable cause) {
        super(message, cause);
    }

}
