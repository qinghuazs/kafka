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
 * 表示客户端的元数据可能已过期的异常
 *
 * 此异常是一个抽象基类，继承自RetriableException（可重试异常），用于表示由于元数据不一致导致的错误。
 * 在Kafka中，元数据包含了集群的拓扑信息，如分区的leader副本位置、broker列表等。当这些信息与实际不符时，
 * 客户端的请求可能会失败。
 *
 * 应用场景：
 * 1. 集群发生变化（如broker上下线、分区重分配）后，客户端尚未感知到最新状态
 * 2. 客户端请求发送到了错误的broker（如旧的leader已经变更）
 * 3. 网络分区导致客户端无法获取最新的元数据
 *
 * 处理机制：
 * 1. 作为RetriableException的子类，表明这类错误是临时性的，可以通过重试来解决
 * 2. 客户端应该在捕获到此异常时刷新元数据，然后重试操作
 * 3. 子类包括NetworkException（网络异常）和UnknownTopicIdException（未知主题ID异常）等
 */
public abstract class InvalidMetadataException extends RetriableException {

    // 序列化版本号
    private static final long serialVersionUID = 1L;

    /**
     * 创建一个无参数的InvalidMetadataException实例
     */
    protected InvalidMetadataException() {
        super();
    }

    /**
     * 使用指定的错误消息创建InvalidMetadataException实例
     *
     * @param message 详细描述异常原因的错误消息
     */
    protected InvalidMetadataException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因创建InvalidMetadataException实例
     *
     * @param message 详细描述异常原因的错误消息
     * @param cause 导致此异常的原始异常
     */
    protected InvalidMetadataException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 使用指定的原因创建InvalidMetadataException实例
     *
     * @param cause 导致此异常的原始异常
     */
    protected InvalidMetadataException(Throwable cause) {
        super(cause);
    }

}
