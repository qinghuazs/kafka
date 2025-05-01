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

import java.util.Map;

/**
 * 用于修改消费者组偏移量的选项类，为{@link AdminClient#alterConsumerGroupOffsets(String, Map, AlterConsumerGroupOffsetsOptions)}调用提供配置。
 * 
 * 此类主要用于以下场景：
 * 1. 手动调整消费者组的消费位置，例如在需要重新消费某些消息时
 * 2. 恢复消费者组到特定的历史消费状态
 * 3. 在消费者组迁移或故障恢复时，重置消费位置
 * 
 * 该类继承自AbstractOptions，提供了一个通用的选项配置框架。通过此类，用户可以：
 * - 精确控制消费者组的偏移量
 * - 实现消息的重新消费
 * - 管理消费者组的消费进度
 * 
 * 注意：该API仍在演进中，详见{@link AdminClient}。
 * 
 * @see AdminClient#alterConsumerGroupOffsets(String, Map, AlterConsumerGroupOffsetsOptions)
 * @see AbstractOptions
 */
@InterfaceStability.Evolving
public class AlterConsumerGroupOffsetsOptions extends AbstractOptions<AlterConsumerGroupOffsetsOptions> {
}
