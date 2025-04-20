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
package org.apache.kafka.clients;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 一个内部类，用于表示特定节点支持的API版本信息。
 * 该类维护了节点支持的所有API版本信息，包括:
 * 1. 已知API的版本范围(supportedVersions)
 * 2. 未知API的版本信息(unknownApis)
 * 3. 支持的特性版本范围(supportedFeatures)
 * 4. 已确定的特性版本(finalizedFeatures)
 */
public class NodeApiVersions {

    // 存储每个API可用版本的映射，键为ApiKeys实例
    // 这个映射包含了所有已知的API及其支持的版本范围
    private final Map<ApiKeys, ApiVersion> supportedVersions = new EnumMap<>(ApiKeys.class);

    // 存储broker支持但客户端未知的API列表
    // 这些API可能是新版本broker引入的，但当前客户端版本还不支持
    private final List<ApiVersion> unknownApis = new ArrayList<>();

    // 存储支持的特性及其版本范围的映射
    // 键为特性名称，值为该特性支持的版本范围
    private final Map<String, SupportedVersionRange> supportedFeatures;

    // 存储已确定的特性及其版本的映射
    // 键为特性名称，值为该特性的最终版本号
    private final Map<String, Short> finalizedFeatures;

    // 已确定特性的时间戳
    private final long finalizedFeaturesEpoch;

    /**
     * 使用当前API版本创建一个NodeApiVersions对象。
     * 这个方法会创建一个空的覆盖集合，使用默认的客户端API版本。
     *
     * @return 一个新的NodeApiVersions对象
     */
    public static NodeApiVersions create() {
        return create(Collections.emptyList());
    }

    /**
     * 创建一个NodeApiVersions对象。
     * 这个方法允许指定要覆盖的API版本，未指定的API版本将使用当前客户端的默认值。
     *
     * @param overrides 要覆盖的API版本集合。任何未在此指定的ApiVersion都将使用当前客户端的值
     * @return 一个新的NodeApiVersions对象
     */
    public static NodeApiVersions create(Collection<ApiVersion> overrides) {
        List<ApiVersion> apiVersions = new LinkedList<>(overrides);
        for (ApiKeys apiKey : ApiKeys.clientApis()) {
            boolean exists = false;
            for (ApiVersion apiVersion : apiVersions) {
                if (apiVersion.apiKey() == apiKey.id) {
                    exists = true;
                    break;
                }
            }
            if (!exists) apiVersions.add(ApiVersionsResponse.toApiVersion(apiKey));
        }
        return new NodeApiVersions(apiVersions, Collections.emptyList(), Collections.emptyList(), -1);
    }


    /**
     * 创建一个只包含单个ApiKey的NodeApiVersions对象。
     * 这个方法主要用于测试目的。
     *
     * @param apiKey API的唯一标识符
     * @param minVersion API支持的最小版本号
     * @param maxVersion API支持的最大版本号
     * @return 一个新的NodeApiVersions对象
     */
    public static NodeApiVersions create(short apiKey, short minVersion, short maxVersion) {
        return create(Collections.singleton(new ApiVersion()
                .setApiKey(apiKey)
                .setMinVersion(minVersion)
                .setMaxVersion(maxVersion)));
    }

    public NodeApiVersions(
            Collection<ApiVersion> nodeApiVersions,
            Collection<SupportedFeatureKey> nodeSupportedFeatures
    ) {
        this(nodeApiVersions, nodeSupportedFeatures, Collections.emptyList(), -1);
    }

    public NodeApiVersions(
            Collection<ApiVersion> nodeApiVersions,
            Collection<SupportedFeatureKey> nodeSupportedFeatures,
            Collection<ApiVersionsResponseData.FinalizedFeatureKey> nodeFinalizedFeatures,
            long finalizedFeaturesEpoch
    ) {
        for (ApiVersion nodeApiVersion : nodeApiVersions) {
            if (ApiKeys.hasId(nodeApiVersion.apiKey())) {
                ApiKeys nodeApiKey = ApiKeys.forId(nodeApiVersion.apiKey());
                supportedVersions.put(nodeApiKey, nodeApiVersion);
            } else {
                // Newer brokers may support ApiKeys we don't know about
                unknownApis.add(nodeApiVersion);
            }
        }

        Map<String, SupportedVersionRange> supportedFeaturesBuilder = new HashMap<>();
        for (SupportedFeatureKey supportedFeature : nodeSupportedFeatures) {
            supportedFeaturesBuilder.put(supportedFeature.name(),
                    new SupportedVersionRange(supportedFeature.minVersion(), supportedFeature.maxVersion()));
        }
        this.supportedFeatures = Collections.unmodifiableMap(supportedFeaturesBuilder);
        this.finalizedFeaturesEpoch = finalizedFeaturesEpoch;
        this.finalizedFeatures = new HashMap<>();
        for (ApiVersionsResponseData.FinalizedFeatureKey finalizedFeature : nodeFinalizedFeatures) {
            this.finalizedFeatures.put(finalizedFeature.name(), finalizedFeature.maxVersionLevel());
        }
    }

    /**
     * 返回节点和本地软件都支持的最新版本号。
     * 这个方法会在API支持的版本范围内选择最高的可用版本。
     */
    public short latestUsableVersion(ApiKeys apiKey) {
        return latestUsableVersion(apiKey, apiKey.oldestVersion(), apiKey.latestVersion());
    }

    /**
     * 获取broker在指定版本范围内支持的最新版本。
     * 这个方法会检查broker支持的版本是否在允许的版本范围内，并返回该范围内的最高版本。
     */
    public short latestUsableVersion(ApiKeys apiKey, short oldestAllowedVersion, short latestAllowedVersion) {
        if (!supportedVersions.containsKey(apiKey))
            throw new UnsupportedVersionException("The node does not support " + apiKey);
        ApiVersion supportedVersion = supportedVersions.get(apiKey);
        Optional<ApiVersion> intersectVersion = ApiVersionsResponse.intersect(supportedVersion,
            new ApiVersion()
                .setApiKey(apiKey.id)
                .setMinVersion(oldestAllowedVersion)
                .setMaxVersion(latestAllowedVersion));

        if (intersectVersion.isPresent())
            return intersectVersion.get().maxVersion();
        else
            throw new UnsupportedVersionException("The node does not support " + apiKey +
                " with version in range [" + oldestAllowedVersion + "," + latestAllowedVersion + "]. The supported" +
                " range is [" + supportedVersion.minVersion() + "," + supportedVersion.maxVersion() + "].");
    }

    /**
     * 将对象转换为不带换行符的字符串。
     * 
     * 注意：这个toString方法的性能开销相对较大，
     * 除非开启了调试日志，否则应避免调用此方法。
     */
    @Override
    public String toString() {
        return toString(false);
    }

    /**
     * 将对象转换为字符串。
     * 这个方法提供了格式化输出的选项，可以控制是否在每个API后添加换行符。
     *
     * @param lineBreaks 如果为true，则在每个api后添加换行符
     */
    public String toString(boolean lineBreaks) {
        // The apiVersion collection may not be in sorted order.  We put it into
        // a TreeMap before printing it out to ensure that we always print in
        // ascending order.
        TreeMap<Short, String> apiKeysText = new TreeMap<>();
        for (ApiVersion supportedVersion : this.supportedVersions.values())
            apiKeysText.put(supportedVersion.apiKey(), apiVersionToText(supportedVersion));
        for (ApiVersion apiVersion : unknownApis)
            apiKeysText.put(apiVersion.apiKey(), apiVersionToText(apiVersion));

        // Also handle the case where some apiKey types are not specified at all in the given ApiVersions,
        // which may happen when the remote is too old.
        for (ApiKeys apiKey : ApiKeys.clientApis()) {
            if (!apiKeysText.containsKey(apiKey.id)) {
                String bld = apiKey.name + "(" +
                        apiKey.id + "): " + "UNSUPPORTED";
                apiKeysText.put(apiKey.id, bld);
            }
        }
        String separator = lineBreaks ? ",\n\t" : ", ";
        StringBuilder bld = new StringBuilder();
        bld.append("(");
        if (lineBreaks)
            bld.append("\n\t");
        bld.append(String.join(separator, apiKeysText.values()));
        if (lineBreaks)
            bld.append("\n");
        bld.append(")");
        return bld.toString();
    }

    /**
     * 将API版本信息转换为文本格式。
     * 这个方法用于生成API版本的详细文本描述，包括版本号范围和兼容性状态。
     *
     * @param apiVersion 要转换的API版本信息
     * @return 格式化后的API版本信息文本
     */
    private String apiVersionToText(ApiVersion apiVersion) {
        // 创建StringBuilder用于构建输出文本
        StringBuilder bld = new StringBuilder();
        ApiKeys apiKey = null;
        
        // 检查是否是已知的API，并添加API名称和ID
        if (ApiKeys.hasId(apiVersion.apiKey())) {
            apiKey = ApiKeys.forId(apiVersion.apiKey());
            bld.append(apiKey.name).append("(").append(apiKey.id).append("): ");
        } else {
            // 处理未知API的情况
            bld.append("UNKNOWN(").append(apiVersion.apiKey()).append("): ");
        }

        // 添加版本范围信息
        if (apiVersion.minVersion() == apiVersion.maxVersion()) {
            // 如果最小版本和最大版本相同，只显示一个版本号
            bld.append(apiVersion.minVersion());
        } else {
            // 显示版本范围
            bld.append(apiVersion.minVersion()).append(" to ").append(apiVersion.maxVersion());
        }

        // 对于已知的API，添加兼容性信息
        if (apiKey != null) {
            ApiVersion supportedVersion = supportedVersions.get(apiKey);
            if (apiKey.latestVersion() < supportedVersion.minVersion()) {
                // 节点版本过新，客户端不支持
                bld.append(" [unusable: node too new]");
            } else if (supportedVersion.maxVersion() < apiKey.oldestVersion()) {
                // 节点版本过旧，客户端不支持
                bld.append(" [unusable: node too old]");
            } else {
                // 计算可用的最高版本号
                short latestUsableVersion = Utils.min(apiKey.latestVersion(), supportedVersion.maxVersion());
                bld.append(" [usable: ").append(latestUsableVersion).append("]");
            }
        }
        return bld.toString();
    }

    /**
     * 获取指定API的版本信息。
     *
     * @param apiKey 要查询的API键
     * @return 如果API支持则返回其版本信息，否则返回null
     */
    public ApiVersion apiVersion(ApiKeys apiKey) {
        return supportedVersions.get(apiKey);
    }

    /**
     * 获取所有支持的API版本信息。
     *
     * @return 包含所有支持的API版本的映射
     */
    public Map<ApiKeys, ApiVersion> allSupportedApiVersions() {
        return supportedVersions;
    }

    /**
     * 获取所有支持的特性及其版本范围。
     *
     * @return 特性名称到版本范围的映射
     */
    public Map<String, SupportedVersionRange> supportedFeatures() {
        return supportedFeatures;
    }

    /**
     * 获取所有已确定的特性及其版本。
     *
     * @return 特性名称到版本号的映射
     */
    public Map<String, Short> finalizedFeatures() {
        return finalizedFeatures;
    }

    /**
     * 获取已确定特性的时间戳。
     *
     * @return 特性确定的时间戳
     */
    public long finalizedFeaturesEpoch() {
        return finalizedFeaturesEpoch;
    }
}
