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
package org.apache.kafka.common.network;

import org.apache.kafka.common.Reconfigurable;

/**
 * 与网络监听器相关的可重配置实体接口。
 * 
 * 该接口扩展了Reconfigurable接口，专门用于处理Kafka网络监听器的动态配置管理。
 * 实现此接口的组件可以：
 * 1. 在运行时动态更新监听器的配置，如安全设置、连接限制等
 * 2. 通过监听器名称关联特定的配置项
 * 3. 支持针对不同监听器的独立配置管理
 * 
 * 应用场景：
 * - 在Kafka Broker中管理多个网络监听器的配置
 * - 动态调整监听器的安全协议和认证机制
 * - 在不重启服务的情况下更新监听器配置
 */
public interface ListenerReconfigurable extends Reconfigurable {

    /**
     * 获取与此可重配置实体关联的监听器名称。
     * 
     * 该方法用于：
     * 1. 标识当前可重配置实体所属的监听器
     * 2. 获取用于重配置的监听器特定配置
     * 3. 确保配置更新应用到正确的监听器
     * 
     * @return 返回ListenerName对象，代表当前监听器的唯一标识
     */
    ListenerName listenerName();
}
