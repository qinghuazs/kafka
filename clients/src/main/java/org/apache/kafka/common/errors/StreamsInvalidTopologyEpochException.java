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
 * 当Kafka Streams应用程序使用过期的拓扑epoch值进行操作时抛出此异常。
 * 
 * 在Kafka Streams中，拓扑epoch用于：
 * 1. 标识拓扑结构的版本
 * 2. 跟踪拓扑变更
 * 3. 确保处理任务使用正确的拓扑版本
 * 
 * 此异常通常出现在以下场景：
 * - 应用程序重启后使用旧的拓扑epoch
 * - 拓扑更新后，部分任务仍使用旧版本
 * - 多个实例间的拓扑版本不一致
 * 
 * 处理建议：
 * - 确保所有实例使用最新的拓扑版本
 * - 在拓扑更新后重新分配任务
 * - 验证应用程序的部署状态
 */
public class StreamsInvalidTopologyEpochException extends ApiException {
    /**
     * 使用指定的错误消息构造异常
     * @param message 描述拓扑epoch无效原因的错误消息
     */
    public StreamsInvalidTopologyEpochException(String message) {
        super(message);
    }
}
