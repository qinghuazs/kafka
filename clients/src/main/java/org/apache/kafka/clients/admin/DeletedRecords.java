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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * 表示已删除记录的相关信息
 * 
 * 这个类用于在Kafka中追踪消息删除操作的结果。当执行删除操作时，Kafka会更新分区的低水位标记（low watermark）。
 * 低水位标记表示在这个偏移量之前的所有消息都已被删除或过期。这对于以下场景很重要：
 * 1. 消息清理：帮助Kafka确定哪些消息可以被物理删除
 * 2. 消费者位移管理：防止消费者访问已删除的消息
 * 3. 存储空间回收：协助Kafka进行存储空间的回收
 * 
 * 该API仍在演进中，我们可能在次要版本中破坏兼容性（如有必要）。
 */
@InterfaceStability.Evolving
public class DeletedRecords {

    /**
     * 表示主题分区的低水位标记
     * 这个值标识了已删除消息的边界：所有小于此偏移量的消息都已被删除
     */
    private final long lowWatermark;

    /**
     * 创建DeletedRecords实例
     * 
     * @param lowWatermark 执行删除操作的主题分区的低水位标记。
     *                    这个值表示所有小于此偏移量的消息都已被标记为可删除
     */
    public DeletedRecords(long lowWatermark) {
        this.lowWatermark = lowWatermark;
    }

    /**
     * 获取执行删除操作的主题分区的低水位标记
     * 
     * @return 低水位标记值，表示小于此偏移量的所有消息都已被删除
     */
    public long lowWatermark() {
        return lowWatermark;
    }
}
