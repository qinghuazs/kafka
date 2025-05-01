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

import java.util.Map;

/**
 * 用于配置{@link Admin#createPartitions(Map)}操作的选项类。
 * 该类提供了创建Kafka主题分区时的配置选项，包括验证模式和配额违规重试机制。
 *
 * 注意：该类的API仍在演进中，详情请参考{@link Admin}。
 */
@InterfaceStability.Evolving
public class CreatePartitionsOptions extends AbstractOptions<CreatePartitionsOptions> {

    /**
     * 是否仅进行验证而不实际创建分区
     * 默认为false，表示会实际执行创建分区的操作
     */
    private boolean validateOnly = false;

    /**
     * 是否在发生配额违规时自动重试
     * 默认为true，表示会自动重试配额违规的请求
     */
    private boolean retryOnQuotaViolation = true;

    /**
     * 创建CreatePartitionsOptions实例的默认构造函数
     * 初始化配置项为默认值：validateOnly=false, retryOnQuotaViolation=true
     */
    public CreatePartitionsOptions() {
    }

    /**
     * 获取是否仅进行验证模式的标志
     * 
     * @return 如果为true，表示仅验证请求的合法性而不实际创建分区；
     *         如果为false，表示验证通过后会实际执行创建分区的操作
     */
    public boolean validateOnly() {
        return validateOnly;
    }

    /**
     * 设置是否仅进行验证模式
     * 
     * @param validateOnly 如果设置为true，则仅验证请求的合法性而不实际创建分区；
     *                    如果设置为false，则验证通过后会实际执行创建分区的操作
     * @return 当前CreatePartitionsOptions实例，支持方法链式调用
     */
    public CreatePartitionsOptions validateOnly(boolean validateOnly) {
        this.validateOnly = validateOnly;
        return this;
    }

    /**
     * 设置是否在发生配额违规时自动重试
     * 
     * @param retryOnQuotaViolation 如果设置为true，则在发生配额违规时会自动重试；
     *                              如果设置为false，则配额违规时不会重试
     * @return 当前CreatePartitionsOptions实例，支持方法链式调用
     */
    public CreatePartitionsOptions retryOnQuotaViolation(boolean retryOnQuotaViolation) {
        this.retryOnQuotaViolation = retryOnQuotaViolation;
        return this;
    }

    /**
     * 获取是否在发生配额违规时自动重试的标志
     * 
     * @return 如果为true，表示会在发生配额违规时自动重试；
     *         如果为false，表示配额违规时不会重试
     */
    public boolean shouldRetryOnQuotaViolation() {
        return retryOnQuotaViolation;
    }
}
