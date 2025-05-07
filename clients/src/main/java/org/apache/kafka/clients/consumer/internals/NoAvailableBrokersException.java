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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.errors.InvalidMetadataException;

/**
 * 无可用代理异常
 * 当没有代理节点可用来完成请求时抛出此异常
 * 
 * 应用场景：
 * 1. 所有代理节点都离线
 * 2. 网络连接问题导致无法访问任何代理
 * 3. 客户端配置错误导致无法连接到代理
 * 4. 集群正在维护或重启过程中
 * 
 * 设计考虑：
 * 1. 继承自InvalidMetadataException，表明这是一个元数据相关的异常
 * 2. 作为运行时异常，不需要强制捕获
 * 3. 提供序列化支持，便于在分布式环境中传输
 * 4. 简单的实现，不需要额外的异常信息
 */
public class NoAvailableBrokersException extends InvalidMetadataException {
    /**
     * 序列化版本ID
     * 用于确保序列化和反序列化的兼容性
     */
    private static final long serialVersionUID = 1L;
}
