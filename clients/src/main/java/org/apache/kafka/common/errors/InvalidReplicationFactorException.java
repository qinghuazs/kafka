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
 * 当Topic的复制因子设置无效时抛出此异常。
 * 
 * 应用场景：
 * 1. 创建Topic时指定了不合理的复制因子，例如：
 *    - 复制因子大于可用的Broker数量
 *    - 复制因子小于1或超过系统允许的最大值
 * 2. 在Topic配置更新时设置了无效的复制因子
 * 3. 在分区重分配时违反了复制因子约束
 * 
 * 设计考虑：
 * - 确保数据的可靠性和持久性
 * - 防止因复制因子设置不当导致系统资源浪费
 * - 维护集群的数据冗余度在合理范围内
 */
public class InvalidReplicationFactorException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidReplicationFactorException(String message) {
        super(message);
    }

    public InvalidReplicationFactorException(String message, Throwable cause) {
        super(message, cause);
    }

}
