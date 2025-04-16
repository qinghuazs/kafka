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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Kafka请求和响应的基础接口
 * 
 * 该接口作为所有Kafka请求(Request)和响应(Response)类的公共抽象，定义了获取底层消息数据的标准方法。
 * 通过实现这个接口，所有的请求响应类都能以统一的方式提供其消息内容。
 * 
 * 设计目的：
 * 1. 统一接口：为所有请求和响应类提供一致的数据访问方式
 * 2. 类型安全：通过ApiMessage确保消息数据的类型安全
 * 3. 解耦合：将具体的消息实现与通用处理逻辑分离
 */
public interface AbstractRequestResponse {

    /**
     * 获取请求或响应的具体消息数据
     * 
     * @return ApiMessage 返回具体的消息实现，可能是请求数据或响应数据
     *         每个实现类都会返回其对应的特定消息类型
     */
    ApiMessage data();
}
