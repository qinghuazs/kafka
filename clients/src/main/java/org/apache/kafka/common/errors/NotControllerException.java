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
 * 当客户端向一个非控制器（Non-Controller）的Broker发送需要控制器处理的请求时，将抛出此异常。
 * 
 * 应用场景：
 * 1. 主题管理：创建、删除、修改主题配置等操作必须由控制器处理
 * 2. 分区管理：分区重分配、副本重分配等操作需要控制器协调
 * 3. Broker管理：处理Broker的上线、下线等状态变化
 * 
 * 设计考虑：
 * 1. 继承自RetriableException表明这是一个可重试的异常
 * 2. 在控制器发生切换时可能会临时抛出此异常
 * 3. 客户端收到此异常后应该重新获取元数据并重试请求
 */
public class NotControllerException extends RetriableException {

    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息创建异常实例
     * @param message 描述异常的详细信息，通常包含当前broker不是控制器的说明
     */
    public NotControllerException(String message) {
        super(message);
    }

    /**
     * 使用错误消息和原始异常创建异常实例
     * @param message 描述异常的详细信息
     * @param cause 导致此异常的原始异常
     */
    public NotControllerException(String message, Throwable cause) {
        super(message, cause);
    }

}
