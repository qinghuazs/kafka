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
import org.apache.kafka.common.quota.ClientQuotaFilter;

/**
 * 用于配置{@link Admin#describeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)}操作的选项类。
 * 
 * 此类用于在查询Kafka客户端配额时设置相关参数。客户端配额是Kafka用来限制客户端资源使用的机制，
 * 比如限制生产者的消息发送速率或消费者的消息获取速率。
 * 
 * 应用场景：
 * 1. 监控客户端资源使用情况
 * 2. 诊断性能问题
 * 3. 验证配额设置是否生效
 * 
 * 注意：该API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeClientQuotasOptions extends AbstractOptions<DescribeClientQuotasOptions> {
}
