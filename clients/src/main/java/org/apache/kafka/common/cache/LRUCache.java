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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A cache implementing a least recently used policy.
 */
/**
 * LRU缓存实现类
 * 实现了最近最少使用（Least Recently Used）淘汰策略的缓存。
 * 
 * 应用场景：
 * 1. 内存敏感的数据缓存
 * 2. 频繁访问的数据优先保留
 * 3. 有限资源的缓存管理
 * 4. 需要自动淘汰机制的场景
 *
 * 设计考虑：
 * 1. 基于LinkedHashMap实现LRU功能
 * 2. 线程不安全，需要外部同步
 * 3. 固定大小的缓存容量
 * 4. O(1)的访问和更新性能
 *
 * @param <K> 缓存键的类型
 * @param <V> 缓存值的类型
 */
public class LRUCache<K, V> implements Cache<K, V> {
    /**
     * 底层缓存存储
     * 使用LinkedHashMap实现LRU功能，其特点：
     * 1. 维护插入顺序或访问顺序
     * 2. 支持自动删除最老条目
     * 3. 提供O(1)的访问性能
     */
    private final LinkedHashMap<K, V> cache;

    /**
     * 构造函数
     * 创建一个指定最大容量的LRU缓存
     *
     * @param maxSize 缓存的最大容量
     */
    public LRUCache(final int maxSize) {
        // 创建LinkedHashMap实例，参数说明：
        // - 初始容量16：减少早期扩容
        // - 负载因子0.75：平衡空间和性能
        // - accessOrder=true：基于访问顺序而非插入顺序
        cache = new LinkedHashMap<>(16, .75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                // 当缓存大小超过最大容量时，自动移除最老的条目
                return this.size() > maxSize;
            }
        };
    }

    /**
     * 获取缓存中的值
     * 访问会更新条目的位置，将其移到链表末尾
     *
     * @param key 要获取的键
     * @return 对应的值，如果不存在则返回null
     */
    @Override
    public V get(K key) {
        // 获取值的同时会更新访问顺序
        return cache.get(key);
    }

    /**
     * 将键值对放入缓存
     * 如果键已存在，则更新值
     * 如果缓存已满，会触发淘汰机制
     *
     * @param key 要存储的键
     * @param value 要存储的值
     */
    @Override
    public void put(K key, V value) {
        // 添加或更新值，可能触发removeEldestEntry进行淘汰
        cache.put(key, value);
    }

    /**
     * 从缓存中移除指定的键
     *
     * @param key 要移除的键
     * @return 如果键存在并被移除则返回true，否则返回false
     */
    @Override
    public boolean remove(K key) {
        // 移除键值对，如果存在则返回true
        return cache.remove(key) != null;
    }

    /**
     * 获取缓存中当前的条目数
     *
     * @return 缓存中的条目数
     */
    @Override
    public long size() {
        // 返回当前缓存的大小
        return cache.size();
    }
}
