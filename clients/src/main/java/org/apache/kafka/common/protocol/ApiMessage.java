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

package org.apache.kafka.common.protocol;

/**
 * Kafka顶层API消息接口
 * 
 * 该接口是Kafka协议中所有API消息的基础接口，继承自基础Message接口。
 * 它用于表示和处理Kafka集群中的各种API请求和响应消息，如生产者请求、消费者请求等。
 * 通过实现该接口，可以确保消息具有统一的API标识和序列化能力。
 */
public interface ApiMessage extends Message {
    /**
     * 获取该消息的API键值
     * 
     * API键值用于标识不同类型的Kafka API操作，每个Kafka API都有其唯一的键值。
     * 例如：生产者API、消费者API、管理API等都有其特定的API键值。
     * 
     * @return 返回该消息对应的API键值。如果没有对应的API键值，则返回-1
     */
    short apiKey();
}
