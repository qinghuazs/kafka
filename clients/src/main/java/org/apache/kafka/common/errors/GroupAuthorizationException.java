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
 * 消费者组授权异常。当客户端尝试访问未经授权的消费者组时抛出此异常。
 * 
 * 应用场景：
 * 1. Kafka的安全机制中，对消费者组的访问进行权限控制
 * 2. 限制未授权的客户端加入或操作特定的消费者组
 * 
 * 触发条件：
 * 1. 客户端尝试加入没有权限的消费者组
 * 2. 客户端尝试查看或管理未授权的消费者组信息
 * 
 * 处理机制：
 * 1. 检查客户端的ACL配置
 * 2. 确保客户端具有必要的消费者组操作权限
 * 
 * 设计考虑：
 * 1. 作为Kafka安全框架的组成部分，实现细粒度的权限控制
 * 2. 通过groupId字段精确定位权限验证失败的消费者组
 */
public class GroupAuthorizationException extends AuthorizationException {
    // 未通过授权的消费者组ID
    private final String groupId;

    /**
     * 创建一个新的消费者组授权异常
     * @param message 异常描述信息
     * @param groupId 未通过授权的消费者组ID
     */
    public GroupAuthorizationException(String message, String groupId) {
        super(message);
        this.groupId = groupId;
    }

    /**
     * 创建一个新的消费者组授权异常
     * @param message 异常描述信息
     */
    public GroupAuthorizationException(String message) {
        this(message, null);
    }

    /**
     * 获取未通过授权的消费者组ID
     * 注意：在某些异常上下文中，可能无法获知具体的消费者组ID，此时返回null
     *
     * @return 消费者组ID，可能为null
     */
    public String groupId() {
        return groupId;
    }

    /**
     * 创建一个针对特定消费者组的授权异常
     * @param groupId 未通过授权的消费者组ID
     * @return 包含指定消费者组ID的授权异常实例
     */
    public static GroupAuthorizationException forGroupId(String groupId) {
        return new GroupAuthorizationException("Not authorized to access group: " + groupId, groupId);
    }

}
