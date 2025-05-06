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
 * Options for {@link Admin#listShareGroupOffsets(Map, ListShareGroupOffsetsOptions)}.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 *
 * 用于配置{@link Admin#listShareGroupOffsets(Map, ListShareGroupOffsetsOptions)}方法的选项类。
 * 该类用于在查询共享消费者组（Share Group）的偏移量信息时提供额外的配置选项。
 * 
 * 共享消费者组是Kafka中的一个特性，允许多个消费者组共享相同的偏移量提交，
 * 这在某些场景下非常有用，比如：
 * 1. 多个应用需要从相同的偏移量位置读取数据
 * 2. 实现消息的备份或镜像处理
 * 3. 支持消费者组之间的故障转移
 * 
 * 目前该类继承自AbstractOptions，获得了超时设置等基本选项。
 * 由于API仍在演进中，未来可能会添加更多的配置选项，如：
 * - 批量查询的大小限制
 * - 是否包含已过期的偏移量
 * - 查询结果的排序方式
 * 
 * 请参见{@link Admin}了解更多API演进的详细信息。
 */
@InterfaceStability.Evolving
public class ListShareGroupOffsetsOptions extends AbstractOptions<ListShareGroupOffsetsOptions> {
}
