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

import java.util.List;

/**
 * 配额实体的元数据，用于配置配额
 * 配额可以在不同级别定义，configEntities提供定义此配额实体级别的配置实体列表
 * 
 * 应用场景：
 * 1. 多级配额管理：支持用户级、客户端级和默认级配额
 * 2. 灵活配置：允许组合不同实体类型的配额配置
 * 3. 资源控制：为不同实体设置资源使用限制
 * 4. 默认策略：提供默认用户和客户端的配额设置
 */
public interface ClientQuotaEntity {

    /**
     * 配置实体的类型枚举
     * 定义了所有支持的配额实体类型
     * 
     * 枚举值说明：
     * - USER: 用户级别的配额配置
     * - CLIENT_ID: 客户端ID级别的配额配置
     * - DEFAULT_USER: 默认用户的配额配置
     * - DEFAULT_CLIENT_ID: 默认客户端的配额配置
     */
    enum ConfigEntityType {
        USER,               // 用户级配额
        CLIENT_ID,         // 客户端级配额
        DEFAULT_USER,      // 默认用户配额
        DEFAULT_CLIENT_ID  // 默认客户端配额
    }

    /**
     * 表示配额配置实体的接口
     * 配额可以在包含一个或多个配置实体的级别上配置
     * 例如，{user, client-id}配额使用两个ConfigEntity实例表示，
     * 分别具有USER和CLIENT_ID实体类型
     * 
     * 使用场景：
     * 1. 单一实体配额：如仅用户级或仅客户端级的配额
     * 2. 组合实体配额：如用户和客户端ID组合的配额
     * 3. 默认配额：处理默认用户或客户端的配额设置
     */
    interface ConfigEntity {
        /**
         * 返回此实体的名称
         * 对于默认配额，返回空字符串
         * 
         * @return 实体名称，默认配额返回空字符串
         */
        String name();

        /**
         * 返回此实体的类型
         * 用于标识配额实体的具体类型（用户、客户端等）
         * 
         * @return 实体类型枚举值
         */
        ConfigEntityType entityType();
    }

    /**
     * 返回此配额实体包含的配置实体列表
     * 对于{user}或{clientId}配额，这是单个实体
     * 对于{user, clientId}配额，这是两个实体的列表
     * 
     * 使用场景：
     * 1. 单一配额：返回单个实体的列表
     * 2. 组合配额：返回多个实体的列表
     * 3. 配额查询：用于查询特定实体的配额配置
     * 
     * @return 配置实体列表
     */
    List<ConfigEntity> configEntities();
}
