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
package org.apache.kafka.common.cache;

/**
 * 缓存接口
 * 用于实现半持久化的映射存储，保存键值对直到满足驱逐条件或手动失效。
 * 缓存实现不要求线程安全，但某些实现可能是线程安全的。
 *
 * 应用场景：
 * 1. 临时数据缓存
 * 2. 性能优化
 * 3. 资源重用
 * 4. 减少计算开销
 *
 * 设计考虑：
 * 1. 通用性：支持任意类型的键值对
 * 2. 灵活性：允许不同的驱逐策略
 * 3. 可选的线程安全
 * 4. 简单的接口定义
 *
 * @param <K> 缓存键的类型
 * @param <V> 缓存值的类型
 */
public interface Cache<K, V> {

    /**
     * 在缓存中查找值
     * 
     * 实现要求：
     * 1. 快速检索：应该提供O(1)的平均访问时间
     * 2. 空值处理：当键不存在时返回null
     * 3. 一致性：在并发环境下可能需要同步访问
     *
     * @param key 要查找的键
     * @return 缓存的值，如果不存在则返回null
     */
    V get(K key);

    /**
     * 向缓存中插入条目
     * 
     * 实现要求：
     * 1. 覆盖行为：如果键已存在，新值应该替换旧值
     * 2. 容量处理：可能需要触发驱逐机制
     * 3. 原子性：在并发环境下应确保操作的原子性
     *
     * @param key 要插入的键
     * @param value 要插入的值
     */
    void put(K key, V value);

    /**
     * 手动使键失效，从缓存中清除其条目
     * 
     * 实现要求：
     * 1. 立即生效：应该立即从缓存中移除条目
     * 2. 资源释放：可能需要清理相关资源
     * 3. 返回状态：准确反映操作结果
     *
     * @param key 要移除的键
     * @return 如果键存在于缓存中并且条目被移除则返回true，否则返回false
     */
    boolean remove(K key);

    /**
     * 获取此缓存中的条目数
     * 如果此缓存被多个线程并发使用，返回值将只是近似值
     * 
     * 实现要求：
     * 1. 性能优先：不应该影响正常的缓存操作
     * 2. 近似值：在并发环境下允许返回近似值
     * 3. 非阻塞：不应该阻塞其他缓存操作
     *
     * @return 缓存中的条目数
     */
    long size();
}
