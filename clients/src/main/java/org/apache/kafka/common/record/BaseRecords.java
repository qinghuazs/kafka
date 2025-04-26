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
package org.apache.kafka.common.record;

/**
 * 用于访问日志记录的基础接口。这些记录可以是：
 * 1. 存储在磁盘日志文件中的物理记录
 * 2. 内存中的日志记录物化视图
 * 
 * 该接口提供了两个核心功能：
 * 1. 计算记录的字节大小，用于内存管理和磁盘空间分配
 * 2. 将记录转换为可发送对象，用于网络传输
 * 
 * 实现类包括：
 * - MemoryRecords: 内存中的记录表示
 * - FileRecords: 磁盘文件中的记录表示
 */
public interface BaseRecords {
    /**
     * 获取这些记录的字节大小。
     * 
     * 应用场景：
     * 1. 内存管理：跟踪和控制内存中缓存的记录大小
     * 2. 磁盘管理：计算写入磁盘所需的空间
     * 3. 网络传输：预分配发送缓冲区大小
     * 
     * @return 记录的总字节数
     */
    int sizeInBytes();

    /**
     * 将当前记录对象封装为RecordsSend对象，用于网络传输。
     * 
     * 应用场景：
     * 1. 生产者发送消息到broker时，将记录转换为网络传输格式
     * 2. broker之间复制数据时，将记录转换为可传输的格式
     * 3. 消费者从broker拉取消息时，broker将记录转换为传输格式
     * 
     * @return 初始化后的RecordsSend对象，包含了当前记录的传输相关信息
     */
    RecordsSend<? extends BaseRecords> toSend();
}
