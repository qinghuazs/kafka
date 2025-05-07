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
 * 监听器未找到异常
 * 
 * 当Leader节点上没有与请求元数据的监听器相对应的端点时抛出此异常。
 * 这种情况可能有以下原因：
 * 1. Broker配置错误：监听器配置不正确或缺失
 * 2. 临时性错误：在动态更新监听器时，客户端请求在所有Broker更新完成前就被处理
 * 
 * 应用场景：
 * 1. 监听器配置验证：确保Broker正确配置了所需的监听器
 * 2. 动态监听器管理：处理监听器动态更新过程中的异常情况
 * 3. 客户端连接管理：帮助客户端识别和处理连接问题
 * 
 * 设计考虑：
 * 1. 继承自InvalidMetadataException，表明这是一个元数据相关的问题
 * 2. 目前主要用于Leader Broker的监听器缺失情况
 * 3. 预留了未来扩展到Follower节点的可能性
 * 4. 提供详细的错误信息和原因，便于问题诊断和修复
 */
public class ListenerNotFoundException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    public ListenerNotFoundException(String message) {
        super(message);
    }

    public ListenerNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

}
