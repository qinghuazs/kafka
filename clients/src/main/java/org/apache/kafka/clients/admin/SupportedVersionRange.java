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
 * 表示特定Kafka broker支持的某个功能的版本范围。
 * 
 * 该类在Kafka的功能版本管理中发挥重要作用：
 * 1. 用于定义和管理broker支持的功能版本范围
 * 2. 确保版本兼容性，防止不兼容的版本设置
 * 3. 在broker功能升级和降级过程中进行版本验证
 */
public class SupportedVersionRange {
    /**
     * broker支持的功能的最小版本号
     * 该字段表示broker能够支持的某个功能的最低版本要求
     */
    private final short minVersion;

    /**
     * broker支持的功能的最大版本号
     * 该字段表示broker能够支持的某个功能的最高版本限制
     */
    private final short maxVersion;

    /**
     * 创建一个版本范围实例，用于定义broker支持的功能版本范围。
     * 构造函数会验证版本范围的有效性，确保：
     * 1. 最小版本号不能为负数
     * 2. 最大版本号不能为负数
     * 3. 最大版本号必须大于或等于最小版本号
     *
     * @param minVersion 功能支持的最小版本号
     * @param maxVersion 功能支持的最大版本号
     * @throws IllegalArgumentException 当版本范围无效时抛出异常
     */
    public SupportedVersionRange(final short minVersion, final short maxVersion) {
        // 验证版本范围的有效性
        if (minVersion < 0 || maxVersion < 0 || maxVersion < minVersion) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected 0 <= minVersion <= maxVersion but received minVersion:%d, maxVersion:%d.",
                    minVersion,
                    maxVersion));
        }
        // 初始化版本范围
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    /**
     * 获取功能支持的最小版本号
     * 
     * @return 返回最小版本号
     */
    public short minVersion() {
        return minVersion;
    }

    /**
     * 获取功能支持的最大版本号
     * 
     * @return 返回最大版本号
     */
    public short maxVersion() {
        return maxVersion;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final SupportedVersionRange that = (SupportedVersionRange) other;
        return this.minVersion == that.minVersion && this.maxVersion == that.maxVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minVersion, maxVersion);
    }

    @Override
    public String toString() {
        return String.format("SupportedVersionRange[min_version:%d, max_version:%d]", minVersion, maxVersion);
    }
}
