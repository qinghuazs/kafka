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
 * Leader不可用异常
 * 
 * 当指定分区没有可用的Leader副本时抛出此异常。这种情况通常有两个原因：
 * 1. 正在进行Leader选举过程
 * 2. 该分区的所有副本都处于离线状态
 * 
 * 应用场景：
 * 1. 分区Leader选举：在Leader发生切换时通知客户端
 * 2. 故障转移：当原Leader节点失效，新Leader尚未选出时
 * 3. 集群维护：在进行计划内的Leader迁移时
 * 
 * 设计考虑：
 * 1. 继承自InvalidMetadataException，表明客户端需要更新元数据
 * 2. 提供序列化支持，确保在分布式环境中的异常传递
 * 3. 包含详细的错误信息，帮助定位Leader不可用的具体原因
 */
public class LeaderNotAvailableException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    public LeaderNotAvailableException(String message) {
        super(message);
    }

    public LeaderNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

}
