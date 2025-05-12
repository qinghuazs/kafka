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
package org.apache.kafka.server.quota;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Map;

/**
 * broker的配额回调接口，用于自定义客户端配额计算
 * 
 * 应用场景：
 * 1. 资源限制：控制客户端对broker资源的使用
 * 2. 多租户管理：为不同用户和客户端分配不同的资源配额
 * 3. 动态配额：支持运行时更新配额设置
 * 4. 自定义策略：允许实现自定义的配额计算逻辑
 */
public interface ClientQuotaCallback extends Configurable {

    /**
     * 配额回调方法，用于确定请求应用的配额度量标签
     * 配额限制与配额度量关联，使用相同度量标签的所有客户端共享配额限制
     * 
     * 实现要求：
     * 1. 线程安全：确保在并发环境下安全运行
     * 2. 高效实现：由于每个请求都会调用，需要快速响应
     * 3. 一致性：确保相同条件下返回相同的标签
     *
     * @param quotaType 请求的配额类型
     * @param principal 请求配额的连接的用户主体
     * @param clientId  与请求关联的客户端ID
     * @return 指示共享此配额的其他客户端的配额度量标签
     */
    Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId);

    /**
     * 返回与提供的度量标签关联的配额限制
     * 这些标签来自之前对quotaMetricTags的调用
     * 当首次处理使用这些标签的请求时，配额管理器调用此方法获取当前配额限制
     * 在配额更新或集群元数据更改后也会调用
     * 
     * 实现要求：
     * 1. 缓存优化：考虑实现缓存机制以提高性能
     * 2. 空值处理：当标签不再使用时返回null
     * 3. 版本兼容：处理不同版本的配额格式
     *
     * @param quotaType  请求的配额类型
     * @param metricTags 配额度量的标签
     * @return 提供的度量标签的配额限制，如果度量标签不再使用则返回null
     */
    Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags);

    /**
     * 配额配置更新回调，当法定人数中实体的配额配置更新时调用
     * 如果使用内置配额配置工具进行配额管理，这对跟踪配置的配额很有用
     * 
     * 实现要求：
     * 1. 原子性：确保更新操作的原子性
     * 2. 持久化：必要时持久化更新的配置
     * 3. 通知机制：可能需要通知其他组件配额变更
     *
     * @param quotaType   正在更新的配额类型
     * @param quotaEntity 正在更新配额的配额实体
     * @param newValue    新的配额值
     */
    void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity, double newValue);

    /**
     * 配额配置移除回调，当法定人数中实体的配额配置被移除时调用
     * 如果使用内置配额配置工具进行配额管理，这对跟踪配置的配额很有用
     * 
     * 实现要求：
     * 1. 清理资源：确保相关资源被正确清理
     * 2. 状态更新：更新相关的状态信息
     * 3. 通知机制：通知相关组件配额被移除
     *
     * @param quotaType   正在更新的配额类型
     * @param quotaEntity 正在更新配额的配额实体
     */
    void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity);

    /**
     * 返回自上次调用此方法以来是否有任何现有配额配置可能已更新
     * 由updateClusterMetadata、updateQuota和removeQuota调用导致的配额更新会自动处理
     * 因此，仅依赖内置配额配置工具的回调始终返回false
     * 具有外部配额配置或影响配额限制的自定义可重配置配额配置的配额回调必须在需要更新现有度量配置时返回true
     * 
     * 实现要求：
     * 1. 高效检查：由于每个请求都会调用，需要快速响应
     * 2. 状态追踪：维护配额变更状态
     * 3. 并发处理：处理并发更新场景
     *
     * @param quotaType 配额类型
     * @return 如果配额已更改且可能需要更新度量配置则返回true
     */
    boolean quotaResetRequired(ClientQuotaType quotaType);

    /**
     * 元数据更新回调，当从控制器收到UpdateMetadata请求时调用
     * 如果配额计算考虑分区，这很有用
     * 正在删除的主题不会包含在cluster中
     * 
     * 实现要求：
     * 1. 分区感知：处理分区相关的配额计算
     * 2. 主题过滤：正确处理删除中的主题
     * 3. 更新追踪：跟踪配额相关的元数据变更
     *
     * @param cluster 包括分区及其已知leader的集群元数据
     * @return 如果配额已更改且可能需要更新度量配置则返回true
     */
    boolean updateClusterMetadata(Cluster cluster);

    /**
     * 关闭此实例
     * 
     * 实现要求：
     * 1. 资源释放：确保所有资源被正确释放
     * 2. 状态清理：清理所有状态信息
     * 3. 优雅关闭：确保正在进行的操作能够完成
     */
    void close();
}
