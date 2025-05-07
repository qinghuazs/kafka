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
package org.apache.kafka.common.config;

import java.util.List;

/**
 * 配置容器类
 * 用于存储和管理一组配置值。
 * 
 * 应用场景：
 * 1. 配置管理：统一管理多个配置项
 * 2. 配置验证：批量验证配置值
 * 3. 配置传递：在系统组件间传递配置信息
 * 4. 配置快照：保存某一时刻的配置状态
 *
 * 设计考虑：
 * 1. 不可变性：使用final确保配置列表不可修改
 * 2. 封装性：通过getter方法控制对配置值的访问
 * 3. 集合管理：统一管理多个配置值
 * 4. 线程安全：通过不可变设计保证线程安全
 */
public class Config {
    /**
     * 配置值列表
     * 存储所有的配置值对象
     * final修饰确保列表引用不可变
     */
    private final List<ConfigValue> configValues;

    /**
     * 构造函数
     * 创建一个新的配置容器
     *
     * @param configValues 配置值列表
     */
    public Config(List<ConfigValue> configValues) {
        // 存储配置值列表
        this.configValues = configValues;
    }

    /**
     * 获取配置值列表
     * 返回所有存储的配置值
     *
     * @return 配置值列表
     */
    public List<ConfigValue> configValues() {
        // 返回配置值列表
        return configValues;
    }
}
