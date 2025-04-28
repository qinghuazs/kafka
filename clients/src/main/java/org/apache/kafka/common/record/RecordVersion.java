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
 * Defines the record format versions supported by Kafka.
 * 定义Kafka支持的记录格式版本。
 *
 * For historical reasons, the record format version is also known as `magic` and `message format version`. Note that
 * the version actually applies to the {@link RecordBatch} (instead of the {@link Record}). Finally, the
 * `message.format.version` topic config confusingly expects an ApiVersion instead of a RecordVersion.
 * 由于历史原因，记录格式版本也被称为`magic`（魔数）和`message format version`（消息格式版本）。
 * 需要注意的是，版本实际上是应用于{@link RecordBatch}（而不是{@link Record}）。
 * 另外，`message.format.version`主题配置令人困惑地需要一个ApiVersion而不是RecordVersion。
 */
public enum RecordVersion {
    /**
     * V0版本（魔数值为0）：
     * - 最初的消息格式版本
     * - 不支持消息时间戳
     * - 每个未压缩的消息都是独立的（没有批次概念）
     * - 仅支持基本的消息属性
     */
    V0(0),

    /**
     * V1版本（魔数值为1）：
     * - 引入了消息时间戳支持
     * - 增加了时间戳类型字段（创建时间或日志追加时间）
     * - 仍然保持每个未压缩消息独立的特性
     * - 压缩消息时支持批量处理
     */
    V1(1),

    /**
     * V2版本（魔数值为2）：
     * - 当前最新的消息格式版本
     * - 引入了记录批次的概念（RecordBatch）
     * - 支持事务和幂等性
     * - 支持消息头部（Headers）
     * - 优化了压缩效率
     * - 提供了更好的消息格式向后兼容性
     */
    V2(2);

    /**
     * 缓存所有RecordVersion枚举值的数组
     * - 用于快速查找指定魔数值对应的版本
     * - 避免重复创建枚举值数组
     */
    private static final RecordVersion[] VALUES = values();

    /**
     * 记录格式版本对应的魔数值
     * - 使用byte类型存储，因为魔数值范围较小
     * - 字段为final，确保版本号不可变
     */
    public final byte value;

    /**
     * 构造函数
     * - 接收一个int类型的值并转换为byte类型存储
     * - 用于初始化每个枚举常量的魔数值
     *
     * @param value 版本对应的魔数值
     */
    RecordVersion(int value) {
        this.value = (byte) value;
    }

    /**
     * 检查当前版本是否早于指定的版本
     * - 通过比较魔数值的大小来判断版本的先后顺序
     * - 用于版本兼容性检查和版本降级处理
     *
     * @param other 要比较的另一个版本
     * @return 如果当前版本的魔数值小于other的魔数值则返回true
     */
    public boolean precedes(RecordVersion other) {
        return this.value < other.value;
    }

    /**
     * 根据魔数值查找对应的RecordVersion枚举实例
     * - 用于将字节形式的魔数值转换为对应的版本枚举
     * - 如果魔数值无效则抛出异常
     *
     * @param value 要查找的魔数值
     * @return 对应的RecordVersion枚举实例
     * @throws IllegalArgumentException 当魔数值小于0或大于等于支持的版本数量时
     */
    public static RecordVersion lookup(byte value) {
        if (value < 0 || value >= VALUES.length)
            throw new IllegalArgumentException("Unknown record version: " + value);
        return VALUES[value];
    }

    /**
     * 获取当前支持的最新记录格式版本
     * - 始终返回最新的版本（目前是V2）
     * - 用于确定默认的记录格式版本
     *
     * @return 当前支持的最新RecordVersion枚举实例
     */
    public static RecordVersion current() {
        return V2;
    }

}
