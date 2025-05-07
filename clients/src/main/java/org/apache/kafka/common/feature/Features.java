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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * 特性管理类
 * 表示一个不可变的字典，键为特性名称，值为版本范围类型<VersionRangeType>。
 * 同时提供API用于在特性及其版本范围与map格式之间进行转换。
 * 
 * 应用场景：
 * 1. 特性版本管理：管理Kafka各个特性的版本范围
 * 2. 特性兼容性：确保特性版本的兼容性
 * 3. 配置转换：支持特性配置的序列化和反序列化
 * 4. 特性查询：提供特性信息的快速访问
 *
 * 设计考虑：
 * 1. 不可变性：使用final确保特性映射不可修改
 * 2. 工厂模式：通过静态工厂方法创建实例
 * 3. 泛型支持：灵活支持不同类型的版本范围
 * 4. 转换功能：支持与Map格式的互相转换
 *
 * @param <VersionRangeType> 版本范围的类型
 * @see SupportedVersionRange
 */
public class Features<VersionRangeType extends BaseVersionRange> {
    /**
     * 特性映射
     * 存储特性名称到版本范围的映射关系
     */
    private final Map<String, VersionRangeType> features;

    /**
     * 私有构造函数
     * 为了可读性，推荐调用者使用静态工厂方法进行实例化
     *
     * @param features 特性名称到版本范围类型的映射
     */
    private Features(Map<String, VersionRangeType> features) {
        // 验证features参数不能为null
        Objects.requireNonNull(features, "Provided features can not be null.");
        // 初始化特性映射
        this.features = features;
    }

    /**
     * 创建支持的特性对象
     * 
     * @param features 特性名称到支持版本范围的映射
     * @return 表示支持特性的Features对象
     */
    public static Features<SupportedVersionRange> supportedFeatures(Map<String, SupportedVersionRange> features) {
        // 创建新的Features实例
        return new Features<>(features);
    }

    /**
     * 创建空的支持特性对象
     *
     * @return 空的Features对象
     */
    public static Features<SupportedVersionRange> emptySupportedFeatures() {
        // 创建包含空HashMap的Features实例
        return new Features<>(new HashMap<>());
    }

    /**
     * 获取特性映射
     *
     * @return 特性名称到版本范围的映射
     */
    public Map<String, VersionRangeType> features() {
        // 返回特性映射
        return features;
    }

    /**
     * 检查特性映射是否为空
     *
     * @return 如果没有特性则返回true
     */
    public boolean empty() {
        // 检查特性映射是否为空
        return features.isEmpty();
    }

    /**
     * 获取指定特性的版本范围
     *
     * @param feature 特性名称
     * @return 对应的版本范围，如果特性不存在则返回null
     */
    public VersionRangeType get(String feature) {
        // 从特性映射中获取指定特性的版本范围
        return features.get(feature);
    }

    /**
     * 将特性转换为Map表示
     * 返回的Map可以通过from*FeaturesMap() API转换回Features对象
     *
     * @return 特性的Map表示
     */
    public Map<String, Map<String, Short>> toMap() {
        // 使用Stream API将特性映射转换为Map格式
        return features.entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().toMap()));
    }

    /**
     * Map到BaseVersionRange转换器接口
     * 定义从Map到BaseVersionRange对象的转换行为
     */
    private interface MapToBaseVersionRangeConverter<V extends BaseVersionRange> {
        /**
         * 将Map表示转换为BaseVersionRange对象
         *
         * @param baseVersionRangeMap BaseVersionRange对象的Map表示
         * @return 类型为V的对象
         */
        V fromMap(Map<String, Short> baseVersionRangeMap);
    }

    /**
     * 从Map创建Features对象
     * 
     * @param featuresMap Map表示的特性
     * @param converter Map到BaseVersionRange的转换器
     * @return 新的Features对象
     */
    private static <V extends BaseVersionRange> Features<V> fromFeaturesMap(
        Map<String, Map<String, Short>> featuresMap, MapToBaseVersionRangeConverter<V> converter) {
        // 使用Stream API将Map转换为Features对象
        return new Features<>(featuresMap.entrySet().stream().collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> converter.fromMap(entry.getValue()))));
    }

    /**
     * 从Map创建Features<SupportedVersionRange>对象
     *
     * @param featuresMap Features<SupportedVersionRange>对象的Map表示
     * @return Features<SupportedVersionRange>对象
     */
    public static Features<SupportedVersionRange> fromSupportedFeaturesMap(
        Map<String, Map<String, Short>> featuresMap) {
        // 使用SupportedVersionRange::fromMap作为转换器调用fromFeaturesMap
        return fromFeaturesMap(featuresMap, SupportedVersionRange::fromMap);
    }
}
