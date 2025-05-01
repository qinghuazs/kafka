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
 * 用于配置{@link Admin#describeConfigs(Collection)}操作的选项类。
 *
 * 此类提供了查询Kafka配置信息时的配置选项，包括：
 * - 是否包含同义配置信息
 * - 是否包含配置文档信息
 * - 超时设置
 * 
 * 应用场景：
 * 1. 查询主题（Topic）配置
 * 2. 查询Broker配置
 * 3. 配置审计和验证
 * 
 * 注意：该API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeConfigsOptions extends AbstractOptions<DescribeConfigsOptions> {

    /**
     * 是否在响应中包含同义配置信息
     * 同义配置是指可以通过不同名称访问的相同配置项
     */
    private boolean includeSynonyms = false;

    /**
     * 是否在响应中包含配置的文档信息
     * 文档信息包括配置项的描述、用途和建议值等
     */
    private boolean includeDocumentation = false;

    /**
     * 设置操作的超时时间（毫秒）。
     * 如果设置为null，则使用AdminClient的默认API超时时间。
     *
     * @param timeoutMs 超时时间，单位为毫秒
     * @return 当前对象，支持链式调用
     */
    // 此方法保留是为了保持与0.11版本的二进制兼容性
    public DescribeConfigsOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    /**
     * 获取是否包含同义配置信息的设置。
     * 
     * 同义配置对于理解配置项的所有可能表现形式很有帮助，
     * 特别是在处理不同版本的Kafka时。
     * 
     * @return 如果为true，表示响应中将包含同义配置信息
     */
    public boolean includeSynonyms() {
        return includeSynonyms;
    }

    /**
     * 获取是否包含配置文档信息的设置。
     * 
     * 配置文档信息有助于理解每个配置项的作用和推荐设置，
     * 对于系统调优和故障排查很有帮助。
     * 
     * @return 如果为true，表示响应中将包含配置文档信息
     */
    public boolean includeDocumentation() {
        return includeDocumentation;
    }

    /**
     * 设置是否在响应中包含同义配置信息。
     * 
     * @param includeSynonyms 是否包含同义配置信息
     * @return 当前对象，支持链式调用
     */
    public DescribeConfigsOptions includeSynonyms(boolean includeSynonyms) {
        this.includeSynonyms = includeSynonyms;
        return this;
    }

    /**
     * 设置是否在响应中包含配置文档信息。
     * 
     * @param includeDocumentation 是否包含配置文档信息
     * @return 当前对象，支持链式调用
     */
    public DescribeConfigsOptions includeDocumentation(boolean includeDocumentation) {
        this.includeDocumentation = includeDocumentation;
        return this;
    }
}
