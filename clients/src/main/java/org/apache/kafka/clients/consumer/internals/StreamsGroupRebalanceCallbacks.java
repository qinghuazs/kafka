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
package org.apache.kafka.clients.consumer.internals;

import java.util.Optional;
import java.util.Set;

/**
 * Kafka Streams中流组重平衡事件的回调接口
 * 
 * 应用场景：
 * 1. 处理流线程任务的撤销
 * 2. 处理流线程任务的分配
 * 3. 处理流线程任务的完全丢失
 * 4. 提供重平衡事件的异常处理机制
 */
public interface StreamsGroupRebalanceCallbacks {

    /**
     * 当任务从流线程中被撤销时调用
     * 
     * 使用场景：
     * - 在重平衡过程中，当任务需要从当前流线程中移除时
     * - 在流应用关闭时，需要清理任务资源
     * - 在任务迁移过程中，需要保存任务状态
     *
     * @param tasks 要被撤销的任务集合，包含任务ID信息
     * @return 回调过程中抛出的异常（如果有），使用Optional包装
     */
    Optional<Exception> onTasksRevoked(final Set<StreamsRebalanceData.TaskId> tasks);

    /**
     * 当任务被分配给流线程时调用
     * 
     * 使用场景：
     * - 在重平衡完成后，接收新分配的任务
     * - 在流应用启动时，初始化任务
     * - 在任务重新分配后，恢复任务状态
     *
     * @param assignment 任务分配信息，包含分配给该线程的所有任务
     * @return 回调过程中抛出的异常（如果有），使用Optional包装
     */
    Optional<Exception> onTasksAssigned(final StreamsRebalanceData.Assignment assignment);

    /**
     * 当流线程失去所有已分配的任务时调用
     * 
     * 使用场景：
     * - 在发生严重错误时，需要清理所有任务资源
     * - 在流线程关闭时，需要进行最终清理
     * - 在重平衡导致任务完全重新分配时
     *
     * @return 回调过程中抛出的异常（如果有），使用Optional包装
     */
    Optional<Exception> onAllTasksLost();
}
