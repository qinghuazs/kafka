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
 * 当尝试访问或操作一个不存在的Kafka快照时抛出此异常。
 * 
 * 在Kafka中，快照机制用于：
 * 1. 存储消费者组的偏移量信息
 * 2. 保存流处理应用的状态
 * 3. 记录集群元数据的某个时间点的状态
 * 
 * 此异常通常出现在以下场景：
 * - 尝试从某个快照恢复状态，但该快照已被清理
 * - 请求访问的快照ID无效或已过期
 * - 快照文件损坏或被意外删除
 */
public class SnapshotNotFoundException extends ApiException {

    private static final long serialVersionUID = 1;

    /**
     * 使用指定的错误消息构造异常
     * @param s 描述快照访问失败原因的错误消息
     */
    public SnapshotNotFoundException(String s) {
        super(s);
    }

    /**
     * 使用指定的错误消息和原因构造异常
     * @param message 描述快照访问失败原因的错误消息
     * @param cause 导致此异常的原始异常
     */
    public SnapshotNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

}
