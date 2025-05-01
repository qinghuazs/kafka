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
 * 用于Kafka分区重分配操作的配置选项类。
 * 该类用于{@link AdminClient#alterPartitionReassignments(Map, AlterPartitionReassignmentsOptions)}方法，
 * 支持管理员在以下场景下进行分区重分配：
 * 
 * 1. 负载均衡：当集群中的broker负载不均衡时，可以通过重分配将分区迁移到负载较轻的broker上
 * 2. 扩容场景：向集群添加新的broker后，需要将现有分区重新分配到新broker上
 * 3. 缩容场景：从集群移除broker前，需要将待移除broker上的分区重新分配到其他broker上
 * 4. 故障恢复：当某个broker发生故障时，可以通过重分配将其上的分区迁移到健康的broker上
 * 
 * 该类继承自AbstractOptions，提供了一个通用的配置框架，允许在未来版本中添加更多的配置选项。
 * 
 * 注意：该API仍在演进中，详情请参考{@link AdminClient}。
 */
@InterfaceStability.Evolving
public class AlterPartitionReassignmentsOptions extends AbstractOptions<AlterPartitionReassignmentsOptions> {
}
