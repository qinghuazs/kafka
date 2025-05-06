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

import java.util.Map;

/**
 * 用于在调用{@link Admin#deleteRecords(Map)}时描述要删除的记录
 * 
 * 此类用于指定删除Kafka主题分区中消息记录的条件：
 * 1. 支持按照偏移量（offset）删除消息
 * 2. 删除指定偏移量之前的所有消息记录
 * 3. 提供不可变的偏移量设置
 * 
 * 应用场景：
 * - 清理过期的消息数据
 * - 实现消息留存策略
 * - 手动删除特定时间段的消息
 * - 管理主题存储空间
 * 
 * 注意：该API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class RecordsToDelete {

    /**
     * 用于指定删除操作的偏移量边界
     * 系统将删除该偏移量之前的所有消息记录
     */
    private final long offset;

    /**
     * 私有构造函数，用于创建RecordsToDelete实例
     * 通过私有构造函数确保对象只能通过工厂方法创建
     * 
     * @param offset 指定删除边界的偏移量
     */
    private RecordsToDelete(long offset) {
        this.offset = offset;
    }

    /**
     * 创建一个RecordsToDelete实例，用于删除指定偏移量之前的所有记录
     * 
     * 使用工厂方法模式创建对象，提供更清晰的语义
     * 删除操作将移除所有小于指定偏移量的消息
     * 
     * @param offset 指定的偏移量边界，将删除此偏移量之前的所有记录
     * @return 返回新创建的RecordsToDelete实例
     */
    public static RecordsToDelete beforeOffset(long offset) {
        return new RecordsToDelete(offset);
    }

    /**
     * 获取设置的偏移量边界
     * 
     * 此方法用于查询当前实例的偏移量设置
     * 返回的偏移量表示删除操作的边界值
     * 
     * @return 返回设置的偏移量值
     */
    public long beforeOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RecordsToDelete that = (RecordsToDelete) o;

        return this.offset == that.offset;
    }

    @Override
    public int hashCode() {
        return (int) offset;
    }

    @Override
    public String toString() {
        return "(beforeOffset = " + offset + ")";
    }
}
