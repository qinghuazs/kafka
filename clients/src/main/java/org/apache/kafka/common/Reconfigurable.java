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
package org.apache.kafka.common;

import org.apache.kafka.common.config.ConfigException;

import java.util.Map;
import java.util.Set;

/**
 * 支持动态配置的类的接口。
 * 
 * 该接口继承自Configurable接口，为Kafka组件提供运行时动态修改配置的能力。
 * 实现此接口的类可以在不重启的情况下更新其配置参数，这对于需要动态调整参数的场景非常有用。
 * 
 * 典型使用场景：
 * 1. 动态调整broker的配置参数
 * 2. 在线修改topic的配置
 * 3. 更新客户端的运行时参数
 */
public interface Reconfigurable extends Configurable {

    /**
     * 返回可以被动态重新配置的配置项名称集合。
     * 
     * 实现说明：
     * 1. 返回的Set中应该只包含那些支持动态修改的配置项名称
     * 2. 不是所有配置都适合动态修改，一些核心配置可能需要重启才能生效
     * 
     * @return 包含所有可动态重配置的配置项名称的Set集合
     */
    Set<String> reconfigurableConfigs();

    /**
     * 验证提供的配置是否有效。
     * 
     * 该方法在实际应用新配置之前被调用，用于验证新的配置是否合法。
     * 如果验证失败（抛出异常），则不会执行实际的重配置操作。
     * 
     * @param configs 包含所有配置的Map，包括可能与初始配置不同的可重配置项
     * @throws ConfigException 如果提供的配置无效。异常消息会通过AlterConfigs响应返回给客户端
     * 
     * 实现说明：
     * 1. 需要仔细验证每个将要修改的配置项的值是否合法
     * 2. 检查配置项之间是否存在冲突
     * 3. 验证新配置是否会导致系统不稳定
     */
    void validateReconfiguration(Map<String, ?> configs) throws ConfigException;

    /**
     * 使用给定的配置对该实例进行重新配置。
     * 
     * 该方法只有在通过validateReconfiguration()的验证后才会被调用。
     * 方法执行时会包含所有配置项，包括那些自上次通过configure()方法配置后发生变化的项。
     * 
     * @param configs 包含所有配置的Map，包括已更改的可重配置项
     * 
     * 实现说明：
     * 1. 实现类应该只更新发生变化的配置项
     * 2. 更新过程应该是原子的，要么全部成功，要么全部失败
     * 3. 更新完成后，组件应该立即使用新的配置
     */
    void reconfigure(Map<String, ?> configs);

}
