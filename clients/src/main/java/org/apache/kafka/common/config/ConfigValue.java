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
package org.apache.kafka.common.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 配置值类
 * 用于存储和管理单个配置项的值及其相关信息。
 * 
 * 应用场景：
 * 1. 配置验证：存储配置验证结果和错误信息
 * 2. 配置建议：提供推荐的配置值选项
 * 3. 配置可见性：控制配置在不同场景下的可见性
 * 4. 配置错误处理：收集和管理配置相关的错误信息
 *
 * 设计考虑：
 * 1. 不可变性：关键字段使用final确保不可修改
 * 2. 灵活性：支持任意类型的配置值
 * 3. 错误管理：支持多个错误信息的收集
 * 4. 可见性控制：支持配置的显示控制
 */
public class ConfigValue {

    /**
     * 配置名称
     * final修饰确保名称不可变
     */
    private final String name;

    /**
     * 配置值
     * 支持任意类型的配置值
     */
    private Object value;

    /**
     * 推荐值列表
     * 存储可能的推荐配置值
     */
    private List<Object> recommendedValues;

    /**
     * 错误消息列表
     * 存储配置相关的错误信息
     * final修饰确保列表引用不可变
     */
    private final List<String> errorMessages;

    /**
     * 可见性标志
     * 控制配置是否可见
     */
    private boolean visible;

    /**
     * 创建只有名称的配置值对象
     * 其他字段使用默认值初始化
     *
     * @param name 配置名称
     */
    public ConfigValue(String name) {
        // 调用完整构造函数，使用默认值
        this(name, null, new ArrayList<>(), new ArrayList<>());
    }

    /**
     * 创建完整的配置值对象
     * 
     * 实现说明：
     * - 初始化所有字段
     * - 设置默认可见性为true
     *
     * @param name 配置名称
     * @param value 配置值
     * @param recommendedValues 推荐值列表
     * @param errorMessages 错误消息列表
     */
    public ConfigValue(String name, Object value, List<Object> recommendedValues, List<String> errorMessages) {
        // 初始化配置名称
        this.name = name;
        // 设置配置值
        this.value = value;
        // 设置推荐值列表
        this.recommendedValues = recommendedValues;
        // 设置错误消息列表
        this.errorMessages = errorMessages;
        // 默认设置为可见
        this.visible = true;
    }

    /**
     * 获取配置名称
     *
     * @return 配置名称
     */
    public String name() {
        // 返回配置名称
        return name;
    }

    /**
     * 获取配置值
     *
     * @return 配置值
     */
    public Object value() {
        // 返回配置值
        return value;
    }

    /**
     * 获取推荐值列表
     *
     * @return 推荐值列表
     */
    public List<Object> recommendedValues() {
        // 返回推荐值列表
        return recommendedValues;
    }

    /**
     * 获取错误消息列表
     *
     * @return 错误消息列表
     */
    public List<String> errorMessages() {
        // 返回错误消息列表
        return errorMessages;
    }

    /**
     * 获取可见性状态
     *
     * @return 可见性状态
     */
    public boolean visible() {
        // 返回可见性状态
        return visible;
    }

    /**
     * 设置配置值
     *
     * @param value 新的配置值
     */
    public void value(Object value) {
        // 更新配置值
        this.value = value;
    }

    /**
     * 设置推荐值列表
     *
     * @param recommendedValues 新的推荐值列表
     */
    public void recommendedValues(List<Object> recommendedValues) {
        // 更新推荐值列表
        this.recommendedValues = recommendedValues;
    }

    /**
     * 添加错误消息
     *
     * @param errorMessage 要添加的错误消息
     */
    public void addErrorMessage(String errorMessage) {
        // 向错误消息列表添加新消息
        this.errorMessages.add(errorMessage);
    }

    /**
     * 设置可见性状态
     *
     * @param visible 新的可见性状态
     */
    public void visible(boolean visible) {
        // 更新可见性状态
        this.visible = visible;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfigValue that = (ConfigValue) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(value, that.value) &&
               Objects.equals(recommendedValues, that.recommendedValues) &&
               Objects.equals(errorMessages, that.errorMessages) &&
               Objects.equals(visible, that.visible);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, recommendedValues, errorMessages, visible);
    }

    @Override
    public String toString() {
        return "[" +
                name + "," +
                value + "," +
                recommendedValues + "," +
                errorMessages + "," +
                visible +
                "]";
    }
}
