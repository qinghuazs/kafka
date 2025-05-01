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
 * Encapsulates details about an update to a finalized feature.
 */
/**
 * 封装已确定（finalized）特性的更新详细信息。
 * 该类用于管理Kafka特性的版本升级和降级操作。
 */
public class FeatureUpdate {
    // 特性的最大版本级别
    private final short maxVersionLevel;
    // 升级类型，指定如何执行版本变更
    private final UpgradeType upgradeType;

    /**
     * 定义特性升级的类型枚举
     */
    public enum UpgradeType {
        // 未知的升级类型，用于处理无法识别的情况
        UNKNOWN(0),
        // 升级操作，用于提升特性版本
        UPGRADE(1),
        // 安全降级操作，确保不会丢失元数据
        SAFE_DOWNGRADE(2),
        // 不安全降级操作，可能会导致元数据丢失
        UNSAFE_DOWNGRADE(3);

        // 存储升级类型的编码
        private final byte code;

        /**
         * 构造函数，初始化升级类型的编码
         * 
         * @param code 升级类型的数字编码
         */
        UpgradeType(int code) {
            // 将int类型的编码转换为byte类型存储
            this.code = (byte) code;
        }

        /**
         * 获取升级类型的编码值
         * 
         * @return 返回升级类型的编码
         */
        public byte code() {
            // 返回当前升级类型的编码值
            return code;
        }

        /**
         * 根据编码值获取对应的升级类型
         * 
         * @param code 要查找的升级类型编码
         * @return 返回对应的UpgradeType枚举值，如果编码未知则返回UNKNOWN
         */
        public static UpgradeType fromCode(int code) {
            // 根据编码值返回对应的升级类型
            if (code == 1) {
                return UPGRADE;
            } else if (code == 2) {
                return SAFE_DOWNGRADE;
            } else if (code == 3) {
                return UNSAFE_DOWNGRADE;
            } else {
                return UNKNOWN;
            }
        }
    }

    /**
     * 构造函数，创建特性更新实例
     * 
     * @param maxVersionLevel 特性的新最大版本级别。
     *                       值为0表示要删除已确定的特性，此时upgradeType必须设置为safe或unsafe。
     * @param upgradeType    指定要执行的升级类型：
     *                       - UPGRADE: 升级特性版本
     *                       - SAFE_DOWNGRADE: 仅允许不会导致元数据丢失的降级
     *                       - UNSAFE_DOWNGRADE: 允许任何降级，包括可能导致元数据丢失的降级
     * @throws IllegalArgumentException 当maxVersionLevel为0但upgradeType为UPGRADE时，
     *                                 或当maxVersionLevel为负数时抛出
     */
    public FeatureUpdate(final short maxVersionLevel, final UpgradeType upgradeType) {
        // 检查当版本级别为0时，升级类型不能为UPGRADE
        if (maxVersionLevel == 0 && upgradeType.equals(UpgradeType.UPGRADE)) {
            throw new IllegalArgumentException(String.format(
                    "The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when the provided maxVersionLevel:%d is < 1.",
                    maxVersionLevel));
        }
        // 检查版本级别不能为负数
        if (maxVersionLevel < 0) {
            throw new IllegalArgumentException("Cannot specify a negative version level.");
        }
        // 初始化字段
        this.maxVersionLevel = maxVersionLevel;
        this.upgradeType = upgradeType;
    }

    /**
     * 获取特性的最大版本级别
     * 
     * @return 返回设置的最大版本级别
     */
    public short maxVersionLevel() {
        return maxVersionLevel;
    }

    /**
     * 获取特性的升级类型
     * 
     * @return 返回设置的升级类型
     */
    public UpgradeType upgradeType() {
        return upgradeType;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof FeatureUpdate)) {
            return false;
        }

        final FeatureUpdate that = (FeatureUpdate) other;
        return this.maxVersionLevel == that.maxVersionLevel && this.upgradeType.equals(that.upgradeType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxVersionLevel, upgradeType);
    }

    @Override
    public String toString() {
        return String.format("FeatureUpdate{maxVersionLevel:%d, upgradeType:%s}", maxVersionLevel, upgradeType);
    }
}
