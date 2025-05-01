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
 * 用于 {@link AdminClient#describeFeatures(DescribeFeaturesOptions)} 的配置选项类。
 * 该类用于描述Kafka集群中的特性配置，包括特性版本、支持的特性等信息。
 *
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeFeaturesOptions extends AbstractOptions<DescribeFeaturesOptions> {
    // 目前该类没有额外的配置选项
    // 继承自AbstractOptions以获取通用的选项处理功能
    // 未来可能会添加更多的配置选项，如超时设置、特性过滤等
}
