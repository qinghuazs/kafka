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
package org.apache.kafka.common.security.auth;

/**
 * 可插拔的主体构建器接口，支持通过以下两种方式进行身份认证：
 * 1. SSL认证：使用{@link SslAuthenticationContext}进行基于证书的身份验证
 * 2. SASL认证：使用{@link SaslAuthenticationContext}进行基于SASL机制的身份验证
 *
 * 实现说明：
 * 1. 如果实现了{@link org.apache.kafka.common.Configurable}接口，可以接收配置参数
 * 2. 如果实现了{@link java.io.Closeable}接口，将在关闭时进行资源清理
 * 3. 所有实现类必须提供一个无参数的默认构造函数
 *
 * 应用场景：
 * 1. 用于Kafka的安全认证系统，负责从认证上下文中构建KafkaPrincipal对象
 * 2. 支持自定义认证逻辑，可以扩展实现特定的身份验证需求
 * 3. 常用于经纪人（Broker）和客户端之间的双向认证
 */
public interface KafkaPrincipalBuilder {
    /**
     * 从认证上下文构建Kafka主体对象
     *
     * 实现细节：
     * 1. 根据认证上下文类型（SSL或SASL）提取身份信息
     * 2. 验证认证凭据的有效性
     * 3. 构造对应的KafkaPrincipal对象
     *
     * @param context 认证上下文，可以是{@link SslAuthenticationContext}（用于SSL认证）
     *                或{@link SaslAuthenticationContext}（用于SASL认证）
     * @return 返回构建的KafkaPrincipal对象，可以通过继承{@link KafkaPrincipalBuilder}来提供额外的扩展功能
     */
    KafkaPrincipal build(AuthenticationContext context);
}
