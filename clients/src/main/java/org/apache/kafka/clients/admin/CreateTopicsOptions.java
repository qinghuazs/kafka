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
 * 用于{@link Admin#createTopics(Collection)}方法的配置选项类。
 * 该类提供了创建主题时的各种配置选项，包括验证模式、超时时间和配额违规重试等。
 *
 * 此类的API仍在演进中，详情请参见{@link Admin}。
 */
@InterfaceStability.Evolving
public class CreateTopicsOptions extends AbstractOptions<CreateTopicsOptions> {

    /**
     * 是否仅进行验证而不实际创建主题
     * 默认为false，表示会实际创建主题
     * 当设置为true时，仅验证创建主题的请求参数是否合法，不会真正创建主题
     */
    private boolean validateOnly = false;

    /**
     * 是否自动重试配额违规
     * 默认为true，表示在遇到配额违规时会自动重试
     * 配额违规通常发生在客户端请求超过服务器设置的限制时
     */
    private boolean retryOnQuotaViolation = true;

    /**
     * 设置操作的超时时间（毫秒）
     * 如果设置为null，则使用AdminClient的默认API超时时间
     * 
     * 该方法主要用于控制创建主题操作的执行时间，防止操作长时间阻塞
     * 注意：此方法保留是为了保持与0.11版本的二进制兼容性
     *
     * @param timeoutMs 超时时间，单位为毫秒
     * @return 当前CreateTopicsOptions实例，支持链式调用
     */
    public CreateTopicsOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    /**
     * 设置是否仅验证请求而不实际创建主题
     * 
     * 应用场景：
     * 1. 在实际创建主题前预检查配置是否合法
     * 2. 测试环境中验证主题配置
     * 3. 权限检查时验证用户是否有创建主题的权限
     *
     * @param validateOnly 如果为true，则仅验证请求而不创建主题
     * @return 当前CreateTopicsOptions实例，支持链式调用
     */
    public CreateTopicsOptions validateOnly(boolean validateOnly) {
        this.validateOnly = validateOnly;
        return this;
    }

    /**
     * 获取是否仅进行验证而不创建主题的设置
     *
     * @return 如果为true，表示仅进行验证；如果为false，表示会实际创建主题
     */
    public boolean shouldValidateOnly() {
        return validateOnly;
    }

    /**
     * 设置是否自动重试配额违规
     * 
     * 应用场景：
     * 1. 高并发环境下可能临时超过配额限制
     * 2. 网络波动导致的临时配额计算错误
     * 3. 需要确保主题创建操作最终成功的场景
     *
     * @param retryOnQuotaViolation 如果为true，则在遇到配额违规时自动重试
     * @return 当前CreateTopicsOptions实例，支持链式调用
     */
    public CreateTopicsOptions retryOnQuotaViolation(boolean retryOnQuotaViolation) {
        this.retryOnQuotaViolation = retryOnQuotaViolation;
        return this;
    }

    /**
     * 获取是否自动重试配额违规的设置
     *
     * @return 如果为true，表示会自动重试配额违规；如果为false，表示不会重试
     */
    public boolean shouldRetryOnQuotaViolation() {
        return retryOnQuotaViolation;
    }
}
