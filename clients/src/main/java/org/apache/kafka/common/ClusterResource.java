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
package org.apache.kafka.common;


import java.util.Objects;

/**
 * <code>ClusterResource</code>类封装了Kafka集群的元数据信息。
 * 
 * 该类主要用于：
 * 1. 存储集群的唯一标识符(clusterId)
 * 2. 在集群元数据更新时通知监听器
 * 3. 用于区分不同的Kafka集群
 */
public class ClusterResource {

    // 集群的唯一标识符
    private final String clusterId;

    /**
     * 创建{@link ClusterResource}实例。
     * 
     * 注意：如果元数据请求发送到不支持集群ID的broker，clusterId可能为{@code null}。
     * Kafka从0.10.1.0版本开始支持集群ID。
     * 
     * @param clusterId 集群ID
     */
    public ClusterResource(String clusterId) {
        this.clusterId = clusterId;
    }

    /**
     * 获取集群ID。
     * 
     * 注意：如果元数据请求发送到不支持集群ID的broker，返回值可能为{@code null}。
     * Kafka从0.10.1.0版本开始支持集群ID。
     * 
     * @return 集群ID
     */
    public String clusterId() {
        return clusterId;
    }

    @Override
    public String toString() {
        return "ClusterResource(clusterId=" + clusterId + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterResource that = (ClusterResource) o;
        return Objects.equals(clusterId, that.clusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId);
    }
}
