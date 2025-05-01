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

import java.util.Set;

/**
 * 用于配置删除消费者组位移操作的选项类。
 * 
 * 该类用于{@link Admin#deleteConsumerGroupOffsets(String, Set)}方法调用时的配置选项。
 * 主要应用场景包括：
 * 1. 在需要重置消费者组位移时，删除指定主题分区的位移信息
 * 2. 在消费者组迁移或重组时，清理特定分区的消费位移
 * 3. 处理消费者组位移异常时，手动删除问题分区的位移记录
 * 
 * 继承自AbstractOptions，提供了通用的选项配置功能。
 * 通过该类可以灵活地控制位移删除操作的行为，确保消费者组的位移管理更加可控和安全。
 * 
 * 注意：该API仍在演进中，详见{@link Admin}。
 * 
 * @see Admin#deleteConsumerGroupOffsets(String, Set) 删除消费者组位移的管理接口
 * @see AbstractOptions 抽象选项基类
 */
@InterfaceStability.Evolving
public class DeleteConsumerGroupOffsetsOptions extends AbstractOptions<DeleteConsumerGroupOffsetsOptions> {

}
