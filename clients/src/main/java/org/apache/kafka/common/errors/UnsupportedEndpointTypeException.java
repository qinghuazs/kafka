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
 * 当客户端尝试连接到不支持的端点类型时抛出此异常。
 * 在Kafka中，端点类型通常指的是broker的监听器配置中定义的协议类型，
 * 例如PLAINTEXT、SSL、SASL_PLAINTEXT、SASL_SSL等。
 * 
 * 应用场景：
 * 1. 客户端配置了错误的安全协议
 * 2. 服务器端未开启对应的监听器
 * 3. 客户端和服务器的安全配置不匹配
 */
public class UnsupportedEndpointTypeException extends ApiException {
    
    /**
     * 使用指定的错误消息构造异常
     * 
     * @param message 描述不支持的端点类型的错误消息
     */
    public UnsupportedEndpointTypeException(String message) {
        super(message);
    }
}
