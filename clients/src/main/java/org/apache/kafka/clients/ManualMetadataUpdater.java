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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * MetadataUpdater接口的简单实现，通过构造函数或setNodes方法设置集群节点信息。
 * 
 * 这个实现主要用于不需要自动更新元数据的场景，例如控制器和代理之间的通信。
 * 在这些场景中，节点信息是预先已知的，不需要动态发现和更新。
 * 
 * 注意：这个类不是线程安全的！
 */
public class ManualMetadataUpdater implements MetadataUpdater {
    /**
     * 存储集群节点列表
     * 这个列表包含了所有已知的Kafka集群节点信息
     */
    private List<Node> nodes;

    /**
     * 默认构造函数
     * 初始化一个空的节点列表
     */
    public ManualMetadataUpdater() {
        this(new ArrayList<>(0));
    }

    /**
     * 使用指定的节点列表构造更新器
     * @param nodes 初始的节点列表
     */
    public ManualMetadataUpdater(List<Node> nodes) {
        this.nodes = nodes;
    }

    /**
     * 设置新的节点列表
     * 这个方法用于手动更新元数据中的节点信息
     * @param nodes 新的节点列表
     */
    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    /**
     * 获取当前的节点列表
     * 返回节点列表的副本以保护内部状态
     * @return 当前节点列表的副本
     */
    @Override
    public List<Node> fetchNodes() {
        return new ArrayList<>(nodes);
    }

    /**
     * 检查是否需要更新元数据
     * 由于这是手动更新器，始终返回false表示不需要自动更新
     * @param now 当前时间戳
     * @return 始终返回false
     */
    @Override
    public boolean isUpdateDue(long now) {
        return false;
    }

    /**
     * 尝试更新元数据
     * 由于这是手动更新器，返回Long.MAX_VALUE表示永不自动更新
     * @param now 当前时间戳
     * @return 返回Long.MAX_VALUE，表示不需要定时更新
     */
    @Override
    public long maybeUpdate(long now) {
        return Long.MAX_VALUE;
    }

    /**
     * 处理服务器断开连接的情况
     * 这里不会将代理标记为失败，因为NetworkClient的日志应该足以说明失败原因
     * @param now 当前时间戳
     * @param nodeId 断开连接的节点ID
     * @param maybeAuthException 可能的认证异常
     */
    @Override
    public void handleServerDisconnect(long now, String nodeId, Optional<AuthenticationException> maybeAuthException) {
        // 不将代理标记为失败。NetworkClient的日志应该足以说明失败原因
    }

    /**
     * 处理请求失败的情况
     * 由于是手动更新器，这里不需要特殊处理
     * @param now 当前时间戳
     * @param maybeFatalException 可能的致命异常
     */
    @Override
    public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
        // 不需要特殊处理
    }

    /**
     * 处理成功的元数据响应
     * 由于是手动更新器，这里不需要处理响应
     * @param requestHeader 请求头
     * @param now 当前时间戳
     * @param response 元数据响应
     */
    @Override
    public void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse response) {
        // 不需要处理响应
    }

    /**
     * 关闭更新器
     * 由于没有需要清理的资源，这里不需要特殊处理
     */
    @Override
    public void close() {
        // 不需要特殊处理
    }
}
