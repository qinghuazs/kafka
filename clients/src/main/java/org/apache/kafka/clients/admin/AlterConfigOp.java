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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 表示一个配置修改操作的类，包含配置项的名称、值和操作类型。
 * 这个类用于Kafka的动态配置管理，支持对配置进行增量修改，而不需要替换整个配置。
 * 
 * 主要应用场景：
 * 1. 动态修改Broker配置
 * 2. 更新Topic级别的配置
 * 3. 管理Client配置
 * 4. 调整Cluster范围的设置
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class AlterConfigOp {

    /**
     * 配置修改操作的类型枚举
     * 包含四种基本操作：设置值(SET)、删除恢复默认值(DELETE)、追加值(APPEND)和移除值(SUBTRACT)
     */
    public enum OpType {
        /**
         * 设置配置项的值
         * 使用场景：直接设置一个配置项的新值，完全替换原有值
         * 例如：设置topic的retention.ms为7天
         */
        SET((byte) 0),

        /**
         * 删除配置项的当前值，使其恢复为默认值（可能为null）
         * 使用场景：当需要清除自定义配置，恢复到系统默认设置时
         * 例如：重置topic的cleanup.policy为默认值
         */
        DELETE((byte) 1),

        /**
         * 仅用于列表类型的配置项，将指定的值追加到当前配置值中
         * 使用场景：向已有的列表类配置中添加新的值，保留现有值
         * 例如：向broker的listener配置添加新的监听器
         * 注意：如果配置值尚未设置，则添加到默认值中
         */
        APPEND((byte) 2),

        /**
         * 仅用于列表类型的配置项，从当前配置值中移除指定的值
         * 使用场景：从列表类配置中删除特定值，而不影响其他值
         * 例如：从broker的listener配置中移除某个监听器
         * 特点：
         * 1. 可以移除不存在的值（操作会成功，但不产生实际影响）
         * 2. 移除所有项后配置值为空列表，不会恢复为默认值
         */
        SUBTRACT((byte) 3);

        /**
         * 操作类型的映射表，用于在字节值和枚举值之间进行快速转换
         * 使用不可变Map存储，确保线程安全
         * 实现细节：通过Stream API将所有枚举值转换为Map，key为字节值，value为对应的枚举实例
         */
        private static final Map<Byte, OpType> OP_TYPES = Collections.unmodifiableMap(
                Arrays.stream(values()).collect(Collectors.toMap(OpType::id, Function.identity()))
        );

        /**
         * 操作类型的唯一标识符
         * 用于在网络传输和序列化时使用更紧凑的字节表示
         */
        private final byte id;

        /**
         * 构造函数，初始化操作类型的标识符
         * @param id 操作类型的字节标识符
         */
        OpType(final byte id) {
            this.id = id;
        }

        /**
         * 获取操作类型的字节标识符
         * @return 表示当前操作类型的字节值
         */
        public byte id() {
            return id;
        }

        /**
         * 根据字节标识符查找对应的操作类型
         * 使用场景：从网络请求或序列化数据中恢复操作类型
         * @param id 操作类型的字节标识符
         * @return 对应的操作类型枚举值，如果未找到则返回null
         */
        public static OpType forId(final byte id) {
            return OP_TYPES.get(id);
        }
    }

    /**
     * 要修改的配置项
     * 包含配置的名称、值等信息
     */
    private final ConfigEntry configEntry;

    /**
     * 要执行的配置修改操作类型
     * 指定如何修改配置项（设置、删除、追加或移除）
     */
    private final OpType opType;

    /**
     * 创建一个配置修改操作
     * @param configEntry 要修改的配置项，包含配置的名称和值
     * @param operationType 要执行的操作类型，指定如何修改该配置
     */
    public AlterConfigOp(ConfigEntry configEntry, OpType operationType) {
        this.configEntry = configEntry;
        this.opType = operationType;
    }

    /**
     * 获取要修改的配置项
     * @return 配置项对象，包含配置的详细信息
     */
    public ConfigEntry configEntry() {
        return configEntry;
    }

    /**
     * 获取配置修改的操作类型
     * @return 操作类型枚举值
     */
    public OpType opType() {
        return opType;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final AlterConfigOp that = (AlterConfigOp) o;
        return opType == that.opType &&
                Objects.equals(configEntry, that.configEntry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType, configEntry);
    }

    @Override
    public String toString() {
        return "AlterConfigOp{" +
                "opType=" + opType +
                ", configEntry=" + configEntry +
                '}';
    }
}
