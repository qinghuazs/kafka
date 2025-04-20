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

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 消费者组类型的枚举定义。
 * 包含以下类型：
 * - UNKNOWN: 未知类型
 * - CONSUMER: 新版消费者组，支持静态成员和增量式重平衡等特性
 * - CLASSIC: 经典消费者组，使用传统的重平衡机制
 * - SHARE: 共享消费者组，用于共享消费场景
 */
public enum GroupType {
    // 未知类型
    UNKNOWN("Unknown"),
    // 新版消费者组类型
    CONSUMER("Consumer"),
    // 经典消费者组类型
    CLASSIC("Classic"),
    // 共享消费者组类型
    SHARE("Share");

    // 类型名称到枚举值的映射，用于字符串解析
    private static final Map<String, GroupType> NAME_TO_ENUM = Arrays.stream(values())
        .collect(Collectors.toMap(type -> type.name.toLowerCase(Locale.ROOT), Function.identity()));

    // 类型的显示名称
    private final String name;

    /**
     * 构造函数
     * @param name 类型的显示名称
     */
    GroupType(String name) {
        this.name = name;
    }

    /**
     * 将字符串解析为GroupType枚举值(不区分大小写)
     * @param name 类型名称
     * @return 对应的GroupType枚举值，如果未找到或输入为null则返回UNKNOWN
     */
    public static GroupType parse(String name) {
        if (name == null) {
            return UNKNOWN;
        }
        GroupType type = NAME_TO_ENUM.get(name.toLowerCase(Locale.ROOT));
        return type == null ? UNKNOWN : type;
    }

    /**
     * 返回类型的字符串表示
     */
    @Override
    public String toString() {
        return name;
    }
}
