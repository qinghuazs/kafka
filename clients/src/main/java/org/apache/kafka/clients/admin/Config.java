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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 一个包含资源配置项的配置对象。
 * 这个类用于管理Kafka资源（如Topic、Broker等）的配置信息，每个资源可以有多个配置项（ConfigEntry）。
 * <p>
 * 该类的API仍在演进中，详细信息请参考{@link Admin}。
 * 
 * 应用场景：
 * 1. 用于管理和查询Kafka资源的配置信息
 * 2. 在Admin客户端中用于配置更新和检索操作
 * 3. 支持配置项的批量操作和单个查询
 */
@InterfaceStability.Evolving
public class Config {

    /**
     * 存储配置项的Map集合
     * - key: 配置项的名称
     * - value: 对应的ConfigEntry对象
     * 使用final修饰确保引用不可变
     * 使用HashMap提供O(1)的查询性能
     */
    private final Map<String, ConfigEntry> entries = new HashMap<>();

    /**
     * 创建一个包含指定配置项的配置实例
     * 
     * @param entries 配置项集合，每个元素都是一个ConfigEntry对象
     * 
     * 实现细节：
     * 1. 遍历传入的配置项集合
     * 2. 使用配置项的名称作为key，配置项本身作为value存入Map
     * 3. 通过Map结构实现快速查找和去重
     */
    public Config(Collection<ConfigEntry> entries) {
        for (ConfigEntry entry : entries) {
            this.entries.put(entry.name(), entry);
        }
    }

    /**
     * 获取所有配置项
     * 
     * @return 返回不可修改的配置项集合视图
     * 
     * 实现细节：
     * 1. 返回Map中所有值的集合视图
     * 2. 使用Collections.unmodifiableCollection确保返回的集合不可被修改
     * 3. 通过不可变集合保证配置的安全性
     */
    public Collection<ConfigEntry> entries() {
        return Collections.unmodifiableCollection(entries.values());
    }

    /**
     * 根据名称获取特定的配置项
     * 
     * @param name 配置项的名称
     * @return 如果存在返回对应的ConfigEntry对象，否则返回null
     * 
     * 实现细节：
     * 1. 直接通过Map的get方法获取配置项
     * 2. 利用HashMap的O(1)查询性能
     * 3. 当配置项不存在时返回null
     */
    public ConfigEntry get(String name) {
        return entries.get(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Config config = (Config) o;

        return entries.equals(config.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    @Override
    public String toString() {
        return "Config(entries=" + entries.values() + ")";
    }
}
