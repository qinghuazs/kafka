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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Kafka消费者的偏移量重置策略类，用于处理以下场景：
 * 1. 当消费者组第一次启动时，没有初始偏移量
 * 2. 当前消费者的偏移量在日志中已经过期（因为数据已被删除）
 * 3. 当前消费者的偏移量不存在（如手动指定了一个不存在的偏移量）
 * 
 * 提供四种重置策略：
 * - EARLIEST: 重置到最早的可用偏移量
 * - LATEST: 重置到最新的偏移量
 * - NONE: 不进行重置，直接抛出异常
 * - BY_DURATION: 重置到距离当前时间指定时间间隔的偏移量
 */
public class AutoOffsetResetStrategy {
    /**
     * 偏移量重置策略的类型枚举
     * - LATEST: 使用最新的偏移量
     * - EARLIEST: 使用最早的可用偏移量
     * - NONE: 不进行重置，抛出异常
     * - BY_DURATION: 根据指定的时间间隔重置
     */
    public enum StrategyType {
        LATEST, EARLIEST, NONE, BY_DURATION;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    public static final AutoOffsetResetStrategy EARLIEST = new AutoOffsetResetStrategy(StrategyType.EARLIEST);
    public static final AutoOffsetResetStrategy LATEST = new AutoOffsetResetStrategy(StrategyType.LATEST);
    public static final AutoOffsetResetStrategy NONE = new AutoOffsetResetStrategy(StrategyType.NONE);

    /**
     * 当前使用的重置策略类型
     */
    private final StrategyType type;
    
    /**
     * 仅在BY_DURATION策略下使用的时间间隔
     * 表示要回溯的时间长度
     */
    private final Optional<Duration> duration;

    /**
     * 构造基本的重置策略实例（EARLIEST、LATEST、NONE）
     * @param type 重置策略类型
     */
    private AutoOffsetResetStrategy(StrategyType type) {
        this.type = type;
        this.duration = Optional.empty();
    }

    /**
     * 构造基于时间间隔的重置策略实例（BY_DURATION）
     * @param duration 要回溯的时间间隔
     */
    private AutoOffsetResetStrategy(Duration duration) {
        this.type = StrategyType.BY_DURATION;
        this.duration = Optional.of(duration);
    }

    /**
     *  Returns the AutoOffsetResetStrategy from the given string.
     */
    /**
     * 从字符串创建对应的重置策略实例
     * 支持的格式：
     * - "earliest"、"latest"、"none"：直接返回对应的策略实例
     * - "by_duration:PT1H"：创建一个回溯1小时的BY_DURATION策略实例
     * 
     * 应用场景：
     * 1. 配置文件中指定消费者的偏移量重置策略
     * 2. 动态调整消费者的重置策略
     * 3. 通过REST API或管理工具设置重置策略
     * 
     * @param offsetStrategy 策略字符串
     * @return 对应的重置策略实例
     * @throws IllegalArgumentException 当策略字符串格式不正确或不支持时
     */
    public static AutoOffsetResetStrategy fromString(String offsetStrategy) {
        // 参数校验：确保策略字符串不为空
        if (offsetStrategy == null) {
            throw new IllegalArgumentException("Auto offset reset strategy is null");
        }

        // 特殊情况处理：如果输入仅为"by_duration"而没有时间间隔部分，抛出异常
        if (StrategyType.BY_DURATION.toString().equals(offsetStrategy)) {
            throw new IllegalArgumentException("<:duration> part is missing in by_duration auto offset reset strategy.");
        }

        // 处理基本策略类型：earliest、latest、none
        // 将输入字符串与枚举类型进行匹配
        if (Arrays.asList(Utils.enumOptions(StrategyType.class)).contains(offsetStrategy)) {
            // 将字符串转换为对应的枚举值（忽略大小写）
            StrategyType type = StrategyType.valueOf(offsetStrategy.toUpperCase(Locale.ROOT));
            // 根据枚举值返回对应的单例策略实例
            switch (type) {
                case EARLIEST:
                    return EARLIEST;
                case LATEST:
                    return LATEST;
                case NONE:
                    return NONE;
                default:
                    throw new IllegalArgumentException("Unknown auto offset reset strategy: " + offsetStrategy);
            }
        }

        // 处理基于时间间隔的策略（by_duration:PT1H格式）
        if (offsetStrategy.startsWith(StrategyType.BY_DURATION + ":")) {
            // 提取时间间隔字符串，去除"by_duration:"前缀
            String isoDuration = offsetStrategy.substring(StrategyType.BY_DURATION.toString().length() + 1);
            try {
                // 解析ISO-8601格式的时间间隔字符串
                // 例如：PT1H（1小时）、PT30M（30分钟）、P1D（1天）
                Duration duration = Duration.parse(isoDuration);
                // 验证时间间隔不能为负数
                if (duration.isNegative()) {
                    throw new IllegalArgumentException("Negative duration is not supported in by_duration offset reset strategy.");
                }
                // 创建基于时间间隔的策略实例
                return new AutoOffsetResetStrategy(duration);
            } catch (Exception e) {
                // 时间间隔格式解析失败时抛出异常
                throw new IllegalArgumentException("Unable to parse duration string in by_duration offset reset strategy.", e);
            }
        }

        // 输入的策略字符串格式不符合任何已知格式时抛出异常
        throw new IllegalArgumentException("Unknown auto offset reset strategy: " + offsetStrategy);
    }

    /**
     * Returns the offset reset strategy type.
     */
    public StrategyType type() {
        return type;
    }

    /**
     * Returns the name of the offset reset strategy.
     */
    public String name() {
        return type.toString();
    }

    /**
     * Return the timestamp to be used for the ListOffsetsRequest.
     * @return the timestamp for the OffsetResetStrategy,
     * if the strategy is EARLIEST or LATEST or duration is provided
     * else return Optional.empty()
     */
    /**
     * 获取用于ListOffsetsRequest的时间戳
     * - EARLIEST: 返回最早时间戳
     * - LATEST: 返回最新时间戳
     * - BY_DURATION: 返回当前时间减去指定时间间隔的时间戳
     * - NONE: 返回空
     * 
     * @return 时间戳的Optional包装，如果是NONE策略则返回empty
     */
    public Optional<Long> timestamp() {
        if (type == StrategyType.EARLIEST)
            return Optional.of(ListOffsetsRequest.EARLIEST_TIMESTAMP);
        else if (type == StrategyType.LATEST)
            return Optional.of(ListOffsetsRequest.LATEST_TIMESTAMP);
        else if (type == StrategyType.BY_DURATION && duration.isPresent()) {
            Instant now = Instant.now();
            return Optional.of(now.minus(duration.get()).toEpochMilli());
        } else
            return Optional.empty();
    }

    /**
     * 获取BY_DURATION策略的时间间隔
     * 
     * @return 时间间隔的Optional包装，仅在BY_DURATION策略下返回非空值
     */
    public Optional<Duration> duration() {
        return duration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoOffsetResetStrategy that = (AutoOffsetResetStrategy) o;
        return type == that.type && Objects.equals(duration, that.duration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, duration);
    }

    @Override
    public String toString() {
        return "AutoOffsetResetStrategy{" +
                "type=" + type +
                (duration.map(value -> ", duration=" + value).orElse("")) +
                '}';
    }

    /**
     * 配置验证器，用于验证偏移量重置策略的配置值是否有效
     * 确保配置值为以下格式之一：
     * - earliest
     * - latest
     * - none
     * - by_duration:PnDTnHnMn.nS（ISO-8601持续时间格式）
     */
    public static class Validator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            String offsetStrategy = (String) value;
            try {
                fromString(offsetStrategy);
            } catch (Exception e) {
                throw new ConfigException(name, value, "Invalid value `" + offsetStrategy + "` for configuration " +
                        name + ". The value must be either 'earliest', 'latest', 'none' or of the format 'by_duration:<PnDTnHnMn.nS.>'.");
            }
        }

        @Override
        public String toString() {
            String values = Arrays.stream(StrategyType.values())
                .map(strategyType -> {
                    if (strategyType == StrategyType.BY_DURATION) {
                        return "by_duration:PnDTnHnMn.nS";
                    }
                    return strategyType.toString();
                }).collect(Collectors.joining(", "));
            return "[" + values + "]";
        }
    }
}
