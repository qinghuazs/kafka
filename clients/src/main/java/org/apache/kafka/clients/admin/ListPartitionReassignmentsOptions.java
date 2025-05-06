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
 * Options for {@link AdminClient#listPartitionReassignments(ListPartitionReassignmentsOptions)}
 * 用于{@link AdminClient#listPartitionReassignments(ListPartitionReassignmentsOptions)}方法的配置选项类
 *
 * The API of this class is evolving. See {@link AdminClient} for details.
 * 该API仍在演进中，详情请参见{@link AdminClient}
 *
 * 该类用于查询Kafka集群中正在进行的分区重分配操作的状态信息，主要应用场景包括：
 * 1. 监控分区重分配进度：在进行大规模的分区重分配操作时，可以通过该接口实时监控重分配的进度
 * 2. 故障诊断：当分区重分配操作出现异常时，可以通过该接口查看具体哪些分区的重分配出现问题
 * 3. 运维管理：集群管理员可以使用该接口来验证重分配操作是否按预期执行
 *
 * 设计说明：
 * 1. 继承自AbstractOptions，复用了通用的选项处理框架
 * 2. 使用@InterfaceStability.Evolving注解标识API的稳定性状态
 * 3. 目前没有额外的配置参数，但预留了扩展空间，未来可能会添加：
 *    - 超时设置
 *    - 特定主题或分区的过滤条件
 *    - 详细程度控制等
 */
@InterfaceStability.Evolving
public class ListPartitionReassignmentsOptions extends AbstractOptions<ListPartitionReassignmentsOptions> {
}
