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

/**
 * 用于 {@link Admin#describeMetadataQuorum(DescribeMetadataQuorumOptions)} 的配置选项类。
 * 该类用于描述Kafka集群中的元数据仲裁组信息，包括仲裁组成员、投票配置和状态等。
 */
public class DescribeMetadataQuorumOptions extends AbstractOptions<DescribeMetadataQuorumOptions> {
    // 目前该类没有额外的配置选项
    // 继承自AbstractOptions以获取通用的选项处理功能
    // 未来可能会添加更多的配置选项，如超时设置、成员过滤等
}
