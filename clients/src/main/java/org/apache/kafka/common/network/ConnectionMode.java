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
package org.apache.kafka.common.network;

/**
 * SSL和SASL连接的连接模式。
 * <p>
 * 该枚举类定义了Kafka网络连接中的两种基本角色：
 * <p>
 * CLIENT - 客户端模式：
 * - 用于发起连接的一方，如生产者、消费者或其他Kafka客户端
 * - 负责启动SSL/SASL握手过程
 * - 在认证过程中作为请求发起方
 * <p>
 * SERVER - 服务器模式：
 * - 用于接受连接的一方，通常是Kafka Broker
 * - 处理来自客户端的SSL/SASL握手请求
 * - 在认证过程中负责验证客户端的身份
 * <p>
 * 该枚举在以下场景中使用：
 * 1. SSL/TLS连接建立：确定是作为客户端还是服务器初始化SSL引擎
 * 2. SASL认证：决定认证过程中的角色和行为
 * 3. 安全协议协商：在不同的安全协议（如PLAINTEXT、SSL、SASL_SSL等）切换时确定连接模式
 */
public enum ConnectionMode { CLIENT, SERVER }
