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

import java.util.HashMap;
import java.util.Map;

/**
 * 维护节点API版本信息，供NetworkClient之外的组件访问（API版本信息最初由NetworkClient收集）。
 * 这种设计模式类似于使用{@link Metadata}来管理主题元数据。
 * 
 * 注意：此类仅供Kafka内部使用。
 */
public class ApiVersions {

    // 存储每个节点ID对应的API版本信息
    private final Map<String, NodeApiVersions> nodeApiVersions = new HashMap<>();

    // 所有节点API版本中最大的已完成特性纪元
    private long maxFinalizedFeaturesEpoch = -1;
    // 存储已完成的特性及其版本信息
    private Map<String, Short> finalizedFeatures;

    /**
     * 已完成特性信息的内部类，用于封装特性纪元和特性版本信息
     */
    public static class FinalizedFeaturesInfo {
        // 已完成特性的纪元号
        public final long finalizedFeaturesEpoch;
        // 已完成特性的映射表，key为特性名称，value为特性版本号
        public final Map<String, Short> finalizedFeatures;
        
        FinalizedFeaturesInfo(long finalizedFeaturesEpoch, Map<String, Short> finalizedFeatures) {
            this.finalizedFeaturesEpoch = finalizedFeaturesEpoch;
            this.finalizedFeatures = finalizedFeatures;
        }
    }

    /**
     * 更新指定节点的API版本信息
     * @param nodeId 节点ID
     * @param nodeApiVersions 节点的API版本信息
     */
    public synchronized void update(String nodeId, NodeApiVersions nodeApiVersions) {
        // 更新节点的API版本信息
        this.nodeApiVersions.put(nodeId, nodeApiVersions);
        // 如果新节点的特性纪元更大，则更新最大特性纪元和特性信息
        if (maxFinalizedFeaturesEpoch < nodeApiVersions.finalizedFeaturesEpoch()) {
            this.maxFinalizedFeaturesEpoch = nodeApiVersions.finalizedFeaturesEpoch();
            this.finalizedFeatures = nodeApiVersions.finalizedFeatures();
        }
    }

    /**
     * 移除指定节点的API版本信息
     * @param nodeId 要移除的节点ID
     */
    public synchronized void remove(String nodeId) {
        this.nodeApiVersions.remove(nodeId);
    }

    /**
     * 获取指定节点的API版本信息
     * @param nodeId 节点ID
     * @return 节点的API版本信息，如果节点不存在则返回null
     */
    public synchronized NodeApiVersions get(String nodeId) {
        return this.nodeApiVersions.get(nodeId);
    }

    /**
     * 获取所有节点中最大的已完成特性纪元
     * @return 最大的已完成特性纪元
     */
    public synchronized long getMaxFinalizedFeaturesEpoch() {
        return maxFinalizedFeaturesEpoch;
    }

    /**
     * 获取当前最新的已完成特性信息
     * @return 包含最大特性纪元和特性版本映射的FinalizedFeaturesInfo对象
     */
    public synchronized FinalizedFeaturesInfo getFinalizedFeaturesInfo() {
        return new FinalizedFeaturesInfo(maxFinalizedFeaturesEpoch, finalizedFeatures);
    }

}
