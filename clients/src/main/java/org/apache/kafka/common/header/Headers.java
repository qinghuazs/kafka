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
package org.apache.kafka.common.header;

/**
 * Kafka消息头部集合接口
 * 继承自Iterable<Header>，提供对消息头部的增删改查操作
 * 
 * 设计说明：
 * 1. 支持多个同名的头部，按添加顺序维护
 * 2. 提供只读保护机制，防止头部被意外修改
 * 3. 实现了迭代器模式，方便遍历所有头部
 * 
 * 应用场景：
 * 1. 在消息中存储元数据信息
 * 2. 实现消息追踪和调试
 * 3. 支持自定义消息路由和过滤
 */
public interface Headers extends Iterable<Header> {
    
    /**
     * 添加一个头部到集合末尾
     * 
     * 实现说明：
     * 1. 验证头部对象不为null
     * 2. 检查集合是否处于只读状态
     * 3. 将头部添加到集合末尾
     * 
     * @param header 要添加的头部对象，不能为null
     * @return 当前Headers实例，支持链式调用
     * @throws IllegalStateException 如果集合处于只读状态则抛出此异常
     */
    Headers add(Header header) throws IllegalStateException;

    /**
     * 使用键值对创建并添加一个头部到集合末尾
     * 
     * 实现说明：
     * 1. 验证key不为null
     * 2. 检查集合是否处于只读状态
     * 3. 创建新的Header对象
     * 4. 将头部添加到集合末尾
     *
     * @param key 头部的键，不能为null
     * @param value 头部的值，可以为null
     * @return 当前Headers实例，支持链式调用
     * @throws IllegalStateException 如果集合处于只读状态则抛出此异常
     */
    Headers add(String key, byte[] value) throws IllegalStateException;

    /**
     * 移除所有具有指定键的头部
     * 
     * 实现说明：
     * 1. 验证key不为null
     * 2. 检查集合是否处于只读状态
     * 3. 遍历集合查找并移除所有匹配的头部
     * 
     * @param key 要移除的头部的键
     * @return 当前Headers实例，支持链式调用
     * @throws IllegalStateException 如果集合处于只读状态则抛出此异常
     */
    Headers remove(String key) throws IllegalStateException;

    /**
     * 获取指定键的最后一个头部
     * 
     * 实现说明：
     * 1. 验证key不为null
     * 2. 从后向前遍历集合
     * 3. 返回第一个匹配的头部
     * 
     * @param key 要查找的头部的键
     * @return 最后一个匹配的头部，如果没有找到则返回null
     */
    Header lastHeader(String key);

    /**
     * 获取所有具有指定键的头部
     * 
     * 实现说明：
     * 1. 验证key不为null
     * 2. 创建过滤迭代器
     * 3. 按添加顺序返回所有匹配的头部
     *
     * @param key 要查找的头部的键
     * @return 包含所有匹配头部的Iterable对象，如果没有匹配的头部则返回空的Iterable
     */
    Iterable<Header> headers(String key);

    /**
     * 将所有头部转换为数组
     * 
     * 实现说明：
     * 1. 创建新的Header数组
     * 2. 按添加顺序复制所有头部
     * 3. 返回数组的副本，修改不会影响原集合
     *
     * @return 包含所有头部的数组，如果集合为空则返回空数组
     */
    Header[] toArray();

}
