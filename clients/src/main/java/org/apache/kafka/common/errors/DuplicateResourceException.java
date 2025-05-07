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
package org.apache.kafka.common.errors;

/**
 * 重复资源异常
 * 
 * 该异常在请求非法地多次引用同一资源时抛出。典型场景包括：
 * 1. 在单个请求中同时创建和删除同一个SCRAM凭证
 * 2. 尝试重复创建已存在的ACL规则
 * 3. 在同一个事务中对同一资源进行冲突的操作
 * 
 * 这个异常的设计目的是：
 * - 防止对同一资源的矛盾操作
 * - 确保资源操作的一致性
 * - 及早发现和报告资源冲突
 * 
 * 异常包含了导致冲突的资源信息，有助于诊断和解决问题。
 */
public class DuplicateResourceException extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 导致冲突的资源名称
     * 可能为null，表示资源信息不可用或不适用
     */
    private final String resource;

    /**
     * 构造函数
     *
     * @param message 异常消息，描述资源重复的具体原因和相关信息
     */
    public DuplicateResourceException(String message) {
        this(null, message);
    }

    /**
     * 构造函数
     *
     * @param message 异常消息，描述资源重复的具体原因和相关信息
     * @param cause 导致此异常的原始异常
     */
    public DuplicateResourceException(String message, Throwable cause) {
        this(null, message, cause);
    }

    /**
     * 构造函数
     *
     * @param resource 被重复引用的资源名称（可能为null）
     * @param message 异常消息，描述资源重复的具体原因和相关信息
     */
    public DuplicateResourceException(String resource, String message) {
        super(message);
        this.resource = resource;
    }

    /**
     * 构造函数
     *
     * @param resource 被重复引用的资源名称（可能为null）
     * @param message 异常消息，描述资源重复的具体原因和相关信息
     * @param cause 导致此异常的原始异常
     */
    public DuplicateResourceException(String resource, String message, Throwable cause) {
        super(message, cause);
        this.resource = resource;
    }

    /**
     * 获取导致冲突的资源名称
     *
     * @return 被重复引用的资源名称（可能为null）
     */
    public String resource() {
        return this.resource;
    }
}