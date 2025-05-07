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

import java.util.Map;

/**
 * 支持版本范围类
 * 扩展自BaseVersionRange，用于表示支持特性的最小/最大版本范围。
 * 
 * 应用场景：
 * 1. 特性版本管理：定义特性支持的版本范围
 * 2. 版本兼容性：检查版本是否在支持范围内
 * 3. 配置转换：支持与Map格式的相互转换
 * 4. 版本验证：验证特定版本是否兼容
 *
 * 设计考虑：
 * 1. 继承性：继承BaseVersionRange以复用版本范围基础功能
 * 2. 简单性：提供简洁的版本范围定义方式
 * 3. 转换支持：支持与Map格式的转换
 * 4. 兼容性检查：提供版本兼容性验证
 */
public class SupportedVersionRange extends BaseVersionRange {
    /**
     * 最小版本键的标签
     * 仅用于与map格式进行转换
     */
    private static final String MIN_VERSION_KEY_LABEL = "min_version";

    /**
     * 最大版本键的标签
     * 仅用于与map格式进行转换
     */
    private static final String MAX_VERSION_KEY_LABEL = "max_version";

    /**
     * 创建具有指定最小和最大版本的版本范围
     * 
     * 实现说明：
     * - 调用父类构造函数设置版本范围
     * - 使用预定义的标签作为键名
     *
     * @param minVersion 最小版本值
     * @param maxVersion 最大版本值
     */
    public SupportedVersionRange(short minVersion, short maxVersion) {
        // 调用父类构造函数，传入预定义的标签和版本值
        super(MIN_VERSION_KEY_LABEL, minVersion, MAX_VERSION_KEY_LABEL, maxVersion);
    }

    /**
     * 创建具有指定最大版本的版本范围
     * 最小版本默认为0
     * 
     * 实现说明：
     * - 调用两参数构造函数
     * - 将最小版本设置为0
     *
     * @param maxVersion 最大版本值
     */
    public SupportedVersionRange(short maxVersion) {
        // 调用两参数构造函数，最小版本设为0
        this((short) 0, maxVersion);
    }

    /**
     * 从Map创建版本范围对象
     * 
     * 实现说明：
     * - 从Map中提取最小和最大版本值
     * - 使用提取的值创建新的版本范围对象
     *
     * @param versionRangeMap 包含版本范围信息的Map
     * @return 新的SupportedVersionRange对象
     */
    public static SupportedVersionRange fromMap(Map<String, Short> versionRangeMap) {
        // 从Map中提取版本值并创建新的版本范围对象
        return new SupportedVersionRange(
            // 获取最小版本值，如果不存在则抛出异常
            BaseVersionRange.valueOrThrow(MIN_VERSION_KEY_LABEL, versionRangeMap),
            // 获取最大版本值，如果不存在则抛出异常
            BaseVersionRange.valueOrThrow(MAX_VERSION_KEY_LABEL, versionRangeMap));
    }

    /**
     * 检查指定版本是否不在当前版本范围内
     * 
     * 实现说明：
     * - 检查版本是否小于最小版本或大于最大版本
     * - 返回true表示版本不兼容
     *
     * @param version 要检查的版本
     * @return 如果版本不在范围内返回true，否则返回false
     */
    public boolean isIncompatibleWith(short version) {
        // 检查版本是否小于最小版本或大于最大版本
        return min() > version || max() < version;
    }
}
