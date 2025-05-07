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
 * 集群ID不一致异常
 * 
 * 该异常在以下场景中抛出：
 * 1. 当Kafka集群中的不同节点具有不同的集群ID时
 * 2. 当一个broker尝试加入具有不同集群ID的集群时
 * 3. 当客户端连接到与其之前连接的集群具有不同集群ID的新集群时
 * 
 * 集群ID是Kafka集群的唯一标识符，用于：
 * - 防止不同集群之间的意外连接
 * - 确保数据复制和同步在正确的集群内进行
 * - 在灾难恢复和迁移场景中维护集群一致性
 */
public class InconsistentClusterIdException extends ApiException {

    /**
     * 使用指定的错误消息构造异常
     * 
     * @param message 描述集群ID不一致问题的详细信息
     */
    public InconsistentClusterIdException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因构造异常
     * 
     * @param message 描述集群ID不一致问题的详细信息
     * @param throwable 导致此异常的原始异常
     */
    public InconsistentClusterIdException(String message, Throwable throwable) {
        super(message, throwable);
    }
}