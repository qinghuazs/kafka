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
 * 当尝试与一个不存在或已失效的Kafka控制器ID进行通信时抛出此异常。
 * 在Kafka集群中，控制器是一个特殊的broker，负责管理分区领导者的选举和其他管理任务。
 * 每个控制器都有一个唯一的ID，当客户端或其他broker使用了错误的或已过期的控制器ID时，会触发此异常。
 * 
 * 应用场景：
 * 1. 当broker尝试与已经不再是控制器的节点通信时
 * 2. 在控制器发生切换后，使用了旧的控制器ID
 * 3. 集群元数据更新期间，临时出现控制器ID不匹配的情况
 */
public class UnknownControllerIdException extends ApiException {
    /**
     * 构造一个UnknownControllerIdException异常实例
     * 
     * @param message 异常描述信息，通常包含无效的控制器ID和具体的错误原因
     */
    public UnknownControllerIdException(String message) {
        super(message);
    }
}
