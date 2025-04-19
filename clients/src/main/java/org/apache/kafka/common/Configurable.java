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

import java.util.Map;

/**
 * 一个混入式(Mix-in)接口，用于那些需要通过反射实例化并接收配置参数的类。
 * 
 * 该接口的主要用途：
 * 1. 作为一个标记接口，表明实现类支持通过配置参数进行初始化
 * 2. 提供统一的配置方法，使得Kafka可以以一致的方式配置不同的组件
 * 3. 支持通过反射机制动态创建和配置实例，增强了系统的可扩展性
 * 
 * 典型使用场景：
 * - 自定义的序列化器和反序列化器
 * - 分区器实现
 * - 拦截器实现
 * - 各种自定义的插件实现
 */
public interface Configurable {

    /**
     * 使用给定的键值对配置当前类的实例
     * 
     * @param configs 包含配置信息的Map，其中：
     *               - 键(String): 配置项的名称
     *               - 值(Object): 配置项的值，类型可以是任意对象
     *               
     * 实现说明：
     * 1. 实现类应该在该方法中解析和存储所需的配置参数
     * 2. 如果配置无效，应该抛出ConfigException
     * 3. 该方法通常在实例创建后立即调用
     */
    void configure(Map<String, ?> configs);

}
