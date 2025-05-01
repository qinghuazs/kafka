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

import java.util.Objects;

/**
 * 表示集群中每个broker对某个特性支持的版本范围。
 * 该类用于管理和验证Kafka特性的版本兼容性。
 */
public class FinalizedVersionRange {
    // 特性支持的最小版本级别
    private final short minVersionLevel;

    // 特性支持的最大版本级别
    private final short maxVersionLevel;

    /**
     * 构造函数，创建版本范围实例。
     * 除非满足以下条件，否则会抛出异常：
     * {@code minVersionLevel >= 1} 且 {@code maxVersionLevel >= 1} 且 {@code maxVersionLevel >= minVersionLevel}
     *
     * @param minVersionLevel 最小版本级别值
     * @param maxVersionLevel 最大版本级别值
     * @throws IllegalArgumentException 当上述条件不满足时抛出
     */
    public FinalizedVersionRange(final short minVersionLevel, final short maxVersionLevel) {
        // 验证版本范围的有效性：
        // 1. 最小版本不能为负数
        // 2. 最大版本不能为负数
        // 3. 最大版本必须大于或等于最小版本
        if (minVersionLevel < 0 || maxVersionLevel < 0 || maxVersionLevel < minVersionLevel) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected minVersionLevel >= 0, maxVersionLevel >= 0 and" +
                    " maxVersionLevel >= minVersionLevel, but received" +
                    " minVersionLevel: %d, maxVersionLevel: %d", minVersionLevel, maxVersionLevel));
        }
        // 初始化字段
        this.minVersionLevel = minVersionLevel;
        this.maxVersionLevel = maxVersionLevel;
    }

    /**
     * 获取特性支持的最小版本级别
     * 
     * @return 返回最小版本级别
     */
    public short minVersionLevel() {
        // 返回最小版本级别值
        return minVersionLevel;
    }

    /**
     * 获取特性支持的最大版本级别
     * 
     * @return 返回最大版本级别
     */
    public short maxVersionLevel() {
        // 返回最大版本级别值
        return maxVersionLevel;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FinalizedVersionRange)) {
            return false;
        }

        final FinalizedVersionRange that = (FinalizedVersionRange) other;
        return this.minVersionLevel == that.minVersionLevel &&
            this.maxVersionLevel == that.maxVersionLevel;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minVersionLevel, maxVersionLevel);
    }

    @Override
    public String toString() {
        return String.format(
            "FinalizedVersionRange[min_version_level:%d, max_version_level:%d]",
            minVersionLevel,
            maxVersionLevel);
    }
}
