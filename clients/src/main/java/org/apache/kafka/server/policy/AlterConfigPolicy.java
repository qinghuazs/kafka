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
package org.apache.kafka.server.policy;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Map;
import java.util.Objects;

/**
 * 配置修改策略接口
 * 用于对修改配置请求实施策略验证
 * 
 * 常见用例：
 * 1. 验证主题的复制因子是否在允许范围内
 * 2. 验证min.insync.replicas设置是否合规
 * 3. 验证数据保留设置是否在允许范围内
 * 
 * 实现说明：
 * 当alter.config.policy.class.name被定义时，Kafka会：
 * 1. 使用默认构造函数创建指定类的实例
 * 2. 将broker配置传递给configure()方法
 * 3. 在broker关闭时调用close()方法释放资源
 */
public interface AlterConfigPolicy extends Configurable, AutoCloseable {

    /**
     * 请求元数据类
     * 包含创建请求的参数信息
     * 
     * 应用场景：
     * 1. 配置验证：验证配置修改请求的参数
     * 2. 策略执行：根据资源类型和配置执行相应的策略
     * 3. 参数传递：在验证过程中传递必要的请求信息
     */
    class RequestMetadata {
        /**
         * 配置资源对象
         * 表示要修改配置的资源（如主题、broker等）
         */
        private final ConfigResource resource;

        /**
         * 配置映射
         * 存储要修改的配置键值对
         */
        private final Map<String, String> configs;

        /**
         * 创建请求元数据实例
         * 构造函数设为public以便于测试AlterConfigPolicy的实现
         * 
         * @param resource 配置资源对象
         * @param configs 配置映射
         */
        public RequestMetadata(ConfigResource resource, Map<String, String> configs) {
            // 初始化资源对象
            this.resource = resource;
            // 初始化配置映射
            this.configs = configs;
        }

        /**
         * 获取请求中的配置映射
         * 
         * @return 配置键值对映射
         */
        public Map<String, String> configs() {
            return configs;
        }

        /**
         * 获取配置资源对象
         * 
         * @return 配置资源对象
         */
        public ConfigResource resource() {
            return resource;
        }

        @Override
        public int hashCode() {
            return Objects.hash(resource, configs);
        }

        @Override
        public boolean equals(Object o) {
            if ((o == null) || (!o.getClass().equals(getClass()))) return false;
            RequestMetadata other = (RequestMetadata) o;
            return resource.equals(other.resource) &&
                configs.equals(other.configs);
        }

        @Override
        public String toString() {
            return "AlterConfigPolicy.RequestMetadata(resource=" + resource +
                    ", configs=" + configs + ")";
        }
    }

    /**
     * 验证请求参数
     * 如果提供的资源的配置修改请求参数不满足策略要求，
     * 则抛出带有适当错误消息的PolicyViolationException
     * 
     * 实现要求：
     * 1. 验证逻辑：根据资源类型和配置实现具体的验证逻辑
     * 2. 错误处理：提供清晰的错误消息说明违反的策略
     * 3. 部分验证：验证失败只影响相关资源，不影响请求中的其他资源
     * 
     * 使用场景：
     * 1. 配置验证：验证配置值是否在允许范围内
     * 2. 权限控制：验证是否允许进行特定的配置修改
     * 3. 一致性检查：确保配置修改不会破坏系统一致性
     * 
     * @param requestMetadata 要验证的配置修改请求参数（目前只支持主题资源类型的配置更新）
     * @throws PolicyViolationException 当请求参数不满足策略要求时抛出
     */
    void validate(RequestMetadata requestMetadata) throws PolicyViolationException;
}
