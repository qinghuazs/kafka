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
 * Options for {@link Admin#unregisterBroker(int, UnregisterBrokerOptions)}.
 * 用于配置从Kafka集群中注销Broker的选项类。
 *
 * The API of this class is evolving. See {@link Admin} for details.
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 *
 * 该类用于在Kafka集群中注销（移除）一个Broker节点时提供相关配置选项。
 * 继承自AbstractOptions类，获取了通用的选项处理功能，如超时设置等。
 * 目前该类没有额外的特定配置选项，仅使用从父类继承的超时设置功能。
 *
 * 使用场景：
 * 1. 当需要从Kafka集群中安全地移除一个Broker节点时
 * 2. 在集群扩缩容操作中，需要下线某些Broker节点时
 * 3. 在维护或升级特定Broker节点时，需要临时移除该节点
 *
 * 示例用法：
 * UnregisterBrokerOptions options = new UnregisterBrokerOptions()
 *     .timeoutMs(5000); // 设置操作超时时间为5秒
 * adminClient.unregisterBroker(brokerId, options);
 */
@InterfaceStability.Evolving
public class UnregisterBrokerOptions extends AbstractOptions<UpdateFeaturesOptions> {
}
