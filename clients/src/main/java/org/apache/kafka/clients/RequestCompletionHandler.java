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
package org.apache.kafka.clients;

/**
 * 请求完成回调接口
 * 
 * 该接口用于在异步请求完成时执行回调操作。当以下情况发生时会触发回调：
 * 1. 请求正常完成并收到对应的响应
 * 2. 在处理请求过程中发生连接断开
 * 
 * 实现类可以通过实现onComplete方法来处理这些场景，从而实现异步请求的后续处理逻辑。
 * 该接口是Kafka客户端异步通信机制的重要组成部分。
 */
public interface RequestCompletionHandler {

    /**
     * 请求完成时的回调方法
     * 
     * @param response 包含请求结果的响应对象，其中包含：
     *                - 原始请求的元数据
     *                - 响应数据
     *                - 请求处理的状态信息（如是否断开连接、是否超时等）
     *                - 请求的延迟时间等统计信息
     */
    void onComplete(ClientResponse response);
}
