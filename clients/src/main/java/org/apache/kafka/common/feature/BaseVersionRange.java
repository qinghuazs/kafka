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
package org.apache.kafka.common.feature;

import org.apache.kafka.common.utils.Utils;

import java.util.Map;

import static java.util.stream.Collectors.joining;

/**
 * 基础版本范围类
 * 使用两个short类型的属性（min和max）表示一个不可变的基本版本范围。
 * min和max属性需要满足两个规则：
 *  - 它们都必须 >= 0，因为我们只考虑非负版本值有效
 *  - max必须 >= min
 *
 * 应用场景：
 * 1. 版本兼容性：管理功能的版本兼容范围
 * 2. 版本控制：定义功能支持的版本区间
 * 3. 配置管理：支持版本范围的配置转换
 * 4. 版本验证：验证版本是否在有效范围内
 *
 * 设计考虑：
 * 1. 不可变性：使用final确保属性不可修改
 * 2. 验证规则：强制执行版本范围的有效性规则
 * 3. 灵活性：支持可配置的标签名称
 * 4. 映射转换：提供与Map格式的转换功能
 */
class BaseVersionRange {
    /**
     * 最小版本键的标签
     * 仅用于与map格式进行转换，不能为空
     */
    private final String minKeyLabel;

    /**
     * 最小版本值
     * 必须大于等于0
     */
    private final short minValue;

    /**
     * 最大版本键的标签
     * 仅用于与map格式进行转换，不能为空
     */
    private final String maxKeyLabel;

    /**
     * 最大版本值
     * 必须大于等于0且大于等于最小版本值
     */
    private final short maxValue;

    /**
     * 创建版本范围实例
     * 
     * 实现说明：
     * - 验证版本值的有效性
     * - 验证标签的有效性
     * - 初始化不可变字段
     *
     * @param minKeyLabel 最小版本键的标签
     * @param minValue 最小版本值
     * @param maxKeyLabel 最大版本键的标签
     * @param maxValue 最大版本值
     * @throws IllegalArgumentException 当版本值无效或标签为空时抛出
     */
    protected BaseVersionRange(String minKeyLabel, short minValue, String maxKeyLabel, short maxValue) {
        // 验证版本值的有效性：必须非负且最大值不小于最小值
        if (minValue < 0 || maxValue < 0 || maxValue < minValue) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected minValue >= 0, maxValue >= 0 and maxValue >= minValue, but received" +
                    " minValue: %d, maxValue: %d", minValue, maxValue));
        }
        // 验证最小版本标签不能为空
        if (minKeyLabel.isEmpty()) {
            throw new IllegalArgumentException("Expected minKeyLabel to be non-empty.");
        }
        // 验证最大版本标签不能为空
        if (maxKeyLabel.isEmpty()) {
            throw new IllegalArgumentException("Expected maxKeyLabel to be non-empty.");
        }
        // 初始化字段
        this.minKeyLabel = minKeyLabel;
        this.minValue = minValue;
        this.maxKeyLabel = maxKeyLabel;
        this.maxValue = maxValue;
    }

    /**
     * 获取最小版本值
     *
     * @return 最小版本值
     */
    public short min() {
        // 返回最小版本值
        return minValue;
    }

    /**
     * 获取最大版本值
     *
     * @return 最大版本值
     */
    public short max() {
        // 返回最大版本值
        return maxValue;
    }

    /**
     * 将版本范围转换为Map
     * 使用配置的标签作为键
     *
     * @return 包含版本范围信息的Map
     */
    public Map<String, Short> toMap() {
        // 创建包含最小和最大版本的Map
        return Utils.mkMap(Utils.mkEntry(minKeyLabel, min()), Utils.mkEntry(maxKeyLabel, max()));
    }

    /**
     * 将Map转换为字符串表示
     * 格式化为"key1:value1, key2:value2"的形式
     *
     * @param map 要转换的Map
     * @return 格式化的字符串
     */
    private static String mapToString(final Map<String, Short> map) {
        // 使用Stream API将Map转换为字符串
        return map
            .entrySet()
            .stream()
            .map(entry -> String.format("%s:%d", entry.getKey(), entry.getValue()))
            .collect(joining(", "));
    }

    /**
     * 从版本范围Map中获取值
     * 如果键不存在则抛出异常
     *
     * @param key 要获取的键
     * @param versionRangeMap 版本范围Map
     * @return 对应的版本值
     * @throws IllegalArgumentException 当键不存在时抛出
     */
    public static short valueOrThrow(String key, Map<String, Short> versionRangeMap) {
        // 获取版本值
        final Short value = versionRangeMap.get(key);
        // 如果值不存在，抛出异常
        if (value == null) {
            throw new IllegalArgumentException(String.format("%s absent in [%s]", key, mapToString(versionRangeMap)));
        }
        // 返回版本值
        return value;
    }
}
