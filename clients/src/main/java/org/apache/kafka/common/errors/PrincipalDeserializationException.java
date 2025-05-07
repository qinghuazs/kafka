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
 * 主体反序列化异常
 * 
 * 此异常表示在请求转发过程中，Kafka无法正确反序列化安全认证主体（Principal）信息。
 * 主要发生在以下场景：
 * 1. 集群内部请求转发时，目标broker无法解析认证信息
 * 2. 认证协议版本不匹配或不兼容
 * 3. 认证数据在网络传输过程中被损坏
 * 
 * 安全影响：
 * - 可能导致授权检查失败
 * - 影响跨数据中心的请求转发
 * - 可能需要重新建立安全连接
 * 
 * 处理建议：
 * - 检查集群中所有节点的安全配置是否一致
 * - 确保使用兼容的安全协议版本
 * - 考虑升级存在兼容性问题的节点
 */
public class PrincipalDeserializationException extends ApiException {

    private static final long serialVersionUID = 1L;

    public PrincipalDeserializationException(String message) {
        super(message);
    }

    public PrincipalDeserializationException(String message, Throwable cause) {
        super(message, cause);
    }

}
