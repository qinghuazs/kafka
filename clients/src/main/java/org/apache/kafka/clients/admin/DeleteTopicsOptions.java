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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;

/**
 * 用于配置{@link Admin#deleteTopics(Collection)}操作的选项类。
 * 
 * 此类用于设置删除主题时的各种参数选项：
 * 1. 继承自AbstractOptions，可以设置操作超时时间
 * 2. 支持配置配额违规时的重试策略
 * 3. 提供流式API设置各项参数
 * 
 * 应用场景：
 * - 批量删除废弃的主题
 * - 清理测试环境的主题数据
 * - 实现主题生命周期管理
 * 
 * 注意：该API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class DeleteTopicsOptions extends AbstractOptions<DeleteTopicsOptions> {

    /**
     * 配额违规时是否自动重试的标志
     * 默认为true，表示在遇到配额违规时会自动重试
     */
    private boolean retryOnQuotaViolation = true;

    /**
     * 设置操作的超时时间（毫秒）
     * 
     * @param timeoutMs 超时时间，如果为null则使用AdminClient的默认超时时间
     * @return 当前对象，支持链式调用
     * 
     * 注：该方法保留是为了保持与0.11版本的二进制兼容性
     */
    // This method is retained to keep binary compatibility with 0.11
    public DeleteTopicsOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    /**
     * 设置在遇到配额违规时是否自动重试
     * 
     * @param retryOnQuotaViolation true表示自动重试，false表示不重试
     * @return 当前对象，支持链式调用
     */
    public DeleteTopicsOptions retryOnQuotaViolation(boolean retryOnQuotaViolation) {
        this.retryOnQuotaViolation = retryOnQuotaViolation;
        return this;
    }

    /**
     * 获取配额违规时是否自动重试的设置
     * 
     * @return true表示会自动重试，false表示不会重试
     */
    public boolean shouldRetryOnQuotaViolation() {
        return retryOnQuotaViolation;
    }
}
