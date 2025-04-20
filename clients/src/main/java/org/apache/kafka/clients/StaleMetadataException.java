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
package org.apache.kafka.clients;

import org.apache.kafka.common.errors.InvalidMetadataException;

/**
 * 当前元数据无法使用时抛出此异常。这个异常通常用作触发元数据更新的机制，
 * 在重试其他操作之前会先更新元数据。
 * 
 * 该异常继承自InvalidMetadataException，表示客户端的元数据已过期或无效。
 * 在Kafka集群中，当节点发生变化（如broker上下线、topic分区重分配等）时，
 * 客户端的元数据可能会过期，此时需要重新获取最新的元数据信息。
 * 
 * 注意：这不是一个公共API。
 */
public class StaleMetadataException extends InvalidMetadataException {
    // 序列化版本号，用于序列化/反序列化过程中的版本控制
    private static final long serialVersionUID = 1L;

    /**
     * 创建一个无参数的StaleMetadataException实例
     */
    public StaleMetadataException() {}

    /**
     * 创建一个带有错误信息的StaleMetadataException实例
     * @param message 描述元数据过期原因的错误信息
     */
    public StaleMetadataException(String message) {
        super(message);
    }
}
