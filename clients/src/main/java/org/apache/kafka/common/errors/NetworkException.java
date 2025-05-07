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
 * 在发送请求时发生的网络相关IOException异常。这种情况可能是由于客户端的元数据过期，
 * 导致其向一个已经不可用的节点发送请求。
 *
 * 应用场景：
 * 1. 网络连接中断或超时
 * 2. 目标Broker节点已关闭或不可达
 * 3. 客户端元数据过期，使用了无效的Broker连接信息
 * 4. 网络配置错误或网络资源不足
 *
 * 设计考虑：
 * 1. 继承自InvalidMetadataException，表明这可能是由元数据过期导致的问题
 * 2. 作为一个可重试异常的基类，暗示在某些情况下重试可能会成功
 * 3. 提供多个构造方法，支持不同级别的错误信息描述
 * 4. 用于区分网络级别的错误和其他类型的API错误
 */
public class NetworkException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    public NetworkException() {
        super();
    }

    public NetworkException(String message, Throwable cause) {
        super(message, cause);
    }

    public NetworkException(String message) {
        super(message);
    }

    public NetworkException(Throwable cause) {
        super(cause);
    }

}
