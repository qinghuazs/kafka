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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

/**
 * 封装已完成确定（finalized）和支持的特性的详细信息。
 * 这个类主要用于保存 {@link Admin#describeFeatures(DescribeFeaturesOptions)} API 返回的结果。
 */
public class FeatureMetadata {
    // 存储已确定的特性版本范围映射，键为特性名称，值为版本范围
    private final Map<String, FinalizedVersionRange> finalizedFeatures;

    // 已确定特性的纪元（epoch）值，为空表示特性不可用
    private final Optional<Long> finalizedFeaturesEpoch;

    // 存储支持的特性版本范围映射，键为特性名称，值为版本范围
    private final Map<String, SupportedVersionRange> supportedFeatures;

    /**
     * 构造函数，初始化特性元数据
     * 
     * @param finalizedFeatures 已确定的特性版本范围映射
     * @param finalizedFeaturesEpoch 已确定特性的纪元值
     * @param supportedFeatures 支持的特性版本范围映射
     */
    FeatureMetadata(final Map<String, FinalizedVersionRange> finalizedFeatures,
                   final Optional<Long> finalizedFeaturesEpoch,
                   final Map<String, SupportedVersionRange> supportedFeatures) {
        // 创建新的HashMap以防止外部修改
        this.finalizedFeatures = new HashMap<>(finalizedFeatures);
        // 直接赋值Optional对象，因为它是不可变的
        this.finalizedFeaturesEpoch = finalizedFeaturesEpoch;
        // 创建新的HashMap以防止外部修改
        this.supportedFeatures = new HashMap<>(supportedFeatures);
    }

    /**
     * 获取已确定的特性版本映射。
     * 映射中的每个条目包含特性名称作为键，以及集群中所有broker支持的版本范围作为值。
     * 
     * @return 返回已确定特性版本的映射副本
     */
    public Map<String, FinalizedVersionRange> finalizedFeatures() {
        // 返回映射的副本以防止外部修改
        return new HashMap<>(finalizedFeatures);
    }

    /**
     * 获取已确定特性的纪元值。
     * 如果返回值为空，表示已确定的特性不存在或不可用。
     * 
     * @return 返回已确定特性的纪元值
     */
    public Optional<Long> finalizedFeaturesEpoch() {
        // 直接返回Optional对象，因为它是不可变的
        return finalizedFeaturesEpoch;
    }

    /**
     * 获取支持的特性版本映射。
     * 映射中的每个条目包含特性名称作为键，以及集群中特定broker支持的版本范围作为值。
     * 
     * @return 返回支持的特性版本的映射副本
     */
    public Map<String, SupportedVersionRange> supportedFeatures() {
        // 返回映射的副本以防止外部修改
        return new HashMap<>(supportedFeatures);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FeatureMetadata)) {
            return false;
        }

        final FeatureMetadata that = (FeatureMetadata) other;
        return Objects.equals(this.finalizedFeatures, that.finalizedFeatures) &&
            Objects.equals(this.finalizedFeaturesEpoch, that.finalizedFeaturesEpoch) &&
            Objects.equals(this.supportedFeatures, that.supportedFeatures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(finalizedFeatures, finalizedFeaturesEpoch, supportedFeatures);
    }

    private static <ValueType> String mapToString(final Map<String, ValueType> featureVersionsMap) {
        return String.format(
            "{%s}",
            featureVersionsMap
                .entrySet()
                .stream()
                .map(entry -> String.format("(%s -> %s)", entry.getKey(), entry.getValue()))
                .collect(joining(", "))
        );
    }

    @Override
    public String toString() {
        return String.format(
            "FeatureMetadata{finalizedFeatures:%s, finalizedFeaturesEpoch:%s, supportedFeatures:%s}",
            mapToString(finalizedFeatures),
            finalizedFeaturesEpoch.map(Object::toString).orElse("<none>"),
            mapToString(supportedFeatures));
    }
}
