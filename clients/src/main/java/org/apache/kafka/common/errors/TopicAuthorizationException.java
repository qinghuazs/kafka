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

import java.util.Collections;
import java.util.Set;

/**
 * 主题授权异常
 * 
 * 当客户端尝试访问未经授权的主题时抛出此异常。这是一个安全相关的异常，
 * 用于实现Kafka的访问控制机制。
 * 
 * 触发场景：
 * 1. 生产者尝试向未授权的主题发送消息
 * 2. 消费者尝试从未授权的主题读取消息
 * 3. 用户尝试执行未经授权的主题管理操作
 * 
 * 异常信息：
 * - 通过unauthorizedTopics()方法可以获取未授权的主题列表
 * - 如果在异常产生时无法确定具体的未授权主题，则返回空集合
 * 
 * 处理建议：
 * - 检查客户端的ACL配置
 * - 确认用户权限是否正确设置
 * - 申请必要的主题访问权限
 * - 考虑使用更细粒度的权限控制
 */
public class TopicAuthorizationException extends AuthorizationException {
    private final Set<String> unauthorizedTopics;

    public TopicAuthorizationException(String message, Set<String> unauthorizedTopics) {
        super(message);
        this.unauthorizedTopics = unauthorizedTopics;
    }

    public TopicAuthorizationException(Set<String> unauthorizedTopics) {
        this("Not authorized to access topics: " + unauthorizedTopics, unauthorizedTopics);
    }

    public TopicAuthorizationException(String message) {
        this(message, Collections.emptySet());
    }

    /**
     * Get the set of topics which failed authorization. May be empty if the set is not known
     * in the context the exception was raised in.
     *
     * @return possibly empty set of unauthorized topics
     */
    public Set<String> unauthorizedTopics() {
        return unauthorizedTopics;
    }
}
