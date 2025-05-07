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
 * Kafka消息头部接口
 * 定义了消息头部的基本结构，包含键值对形式的元数据。
 * 
 * 应用场景：
 * 1. 消息元数据：存储消息的附加信息
 * 2. 消息路由：基于头部信息进行消息路由
 * 3. 消息过滤：根据头部信息过滤消息
 * 4. 应用集成：在不同系统间传递元数据
 *
 * 设计考虑：
 * 1. 简单性：只定义最基本的键值对接口
 * 2. 灵活性：值使用字节数组支持任意类型
 * 3. 不可变性：建议实现类保持不可变
 * 4. 空值处理：允许值为null但键不能为null
 */
public interface Header {
   
    /**
     * 获取头部的键
     * 
     * 实现要求：
     * 1. 返回值不能为null
     * 2. 返回值应该是不可变的
     * 3. 多次调用应返回相同的值
     * 4. 实现类应该在构造时验证键不为null
     *
     * @return 头部的键
     */
    String key();

    /**
     * 获取头部的值
     * 
     * 实现要求：
     * 1. 返回值可以为null
     * 2. 返回的字节数组可以是任意长度
     * 3. 为了安全性，建议返回值的副本
     * 4. 多次调用应返回相同的值
     *
     * @return 头部的值，可能为null
     */
    byte[] value();
   
}
