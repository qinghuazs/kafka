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
 * 用于配置修改副本日志目录操作的选项类。
 * 
 * 该类用于{@link Admin#alterReplicaLogDirs(Map, AlterReplicaLogDirsOptions)}方法，
 * 支持管理员修改Kafka分区副本的日志存储目录。主要应用场景包括：
 * 1. 数据迁移：将特定分区的副本数据从一个目录迁移到另一个目录，用于存储容量管理
 * 2. 存储均衡：通过调整不同副本的存储位置，实现跨目录的存储负载均衡
 * 3. 硬件管理：在新增或替换存储设备时，重新分配副本的存储位置
 * 
 * 继承自AbstractOptions，提供了通用的选项配置功能。
 * 该API仍在演进中，参见{@link Admin}了解详细信息。
 */
@InterfaceStability.Evolving
public class AlterReplicaLogDirsOptions extends AbstractOptions<AlterReplicaLogDirsOptions> {

}
