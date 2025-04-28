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

import java.util.NoSuchElementException;

/**
 * The timestamp type of the records.
 * 记录的时间戳类型。
 *
 * 在Kafka中，消息记录的时间戳类型有三种：
 * 1. NO_TIMESTAMP_TYPE：表示消息没有时间戳，通常用于老版本的消息格式
 * 2. CREATE_TIME：消息的创建时间，由生产者设置的时间戳
 * 3. LOG_APPEND_TIME：消息追加到日志时的时间，由broker设置的时间戳
 */
public enum TimestampType {
    /**
     * 表示消息不包含时间戳
     * 用于兼容0.10.0版本之前的消息格式，这些版本的消息不支持时间戳
     */
    NO_TIMESTAMP_TYPE(-1, "NoTimestampType"),

    /**
     * 表示使用消息创建时间作为时间戳
     * 这个时间戳由生产者在发送消息时设置，反映了消息产生的实际时间
     * 常用于事件时间处理场景，如流处理中的时间窗口计算
     */
    CREATE_TIME(0, "CreateTime"),

    /**
     * 表示使用消息追加到日志时的时间作为时间戳
     * 这个时间戳由broker在接收到消息并写入日志时设置
     * 常用于需要统一管理消息时间的场景，如日志压缩策略
     */
    LOG_APPEND_TIME(1, "LogAppendTime");

    /**
     * 时间戳类型的唯一标识符
     * 用于在消息格式中标识时间戳类型
     */
    public final int id;

    /**
     * 时间戳类型的名称
     * 用于在配置和日志中表示时间戳类型
     */
    public final String name;

    /**
     * 构造函数
     * @param id 时间戳类型的唯一标识符
     * @param name 时间戳类型的名称
     */
    TimestampType(int id, String name) {
        this.id = id;
        this.name = name;
    }

    /**
     * 根据时间戳类型的名称获取对应的枚举值
     * 主要用于从配置文件中读取时间戳类型设置
     *
     * @param name 时间戳类型的名称
     * @return 对应的TimestampType枚举值
     * @throws NoSuchElementException 当指定的时间戳类型名称无效时抛出异常
     */
    public static TimestampType forName(String name) {
        // 遍历所有时间戳类型，查找匹配的名称
        for (TimestampType t : values())
            if (t.name.equals(name))
                return t;
        // 如果没有找到匹配的类型，抛出异常
        throw new NoSuchElementException("Invalid timestamp type " + name);
    }

    @Override
    public String toString() {
        return name;
    }
}
