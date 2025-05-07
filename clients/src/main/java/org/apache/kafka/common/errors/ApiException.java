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

import org.apache.kafka.common.KafkaException;

/**
 * API异常基类
 * 作为公共协议一部分的任何API异常都应该是这个类的子类，并且属于这个包。
 * 
 * 应用场景：
 * 1. API错误处理：统一处理Kafka API层面的异常
 * 2. 协议异常：处理协议相关的错误情况
 * 3. 客户端异常：处理客户端API调用异常
 * 4. 服务端异常：处理服务端API响应异常
 *
 * 设计考虑：
 * 1. 继承性：继承自KafkaException以统一异常处理
 * 2. 性能优化：避免生成不必要的堆栈跟踪
 * 3. 异常分类：提供统一的API异常基类
 * 4. 构造灵活：支持多种异常构造方式
 */
public class ApiException extends KafkaException {

    /**
     * 序列化版本ID
     * 用于确保序列化的兼容性
     */
    private static final long serialVersionUID = 1L;

    /**
     * 使用消息和原因构造API异常
     * 
     * 实现说明：
     * - 调用父类构造函数传递异常信息和原因
     *
     * @param message 异常消息
     * @param cause 异常原因
     */
    public ApiException(String message, Throwable cause) {
        // 调用父类构造函数，传入消息和原因
        super(message, cause);
    }

    /**
     * 使用消息构造API异常
     * 
     * 实现说明：
     * - 调用父类构造函数传递异常信息
     *
     * @param message 异常消息
     */
    public ApiException(String message) {
        // 调用父类构造函数，仅传入消息
        super(message);
    }

    /**
     * 使用原因构造API异常
     * 
     * 实现说明：
     * - 调用父类构造函数传递异常原因
     *
     * @param cause 异常原因
     */
    public ApiException(Throwable cause) {
        // 调用父类构造函数，仅传入原因
        super(cause);
    }

    /**
     * 构造无参数的API异常
     * 
     * 实现说明：
     * - 调用父类的无参构造函数
     */
    public ApiException() {
        // 调用父类的无参构造函数
        super();
    }

    /**
     * 重写fillInStackTrace方法以避免生成堆栈跟踪
     * 
     * 实现说明：
     * - 直接返回this避免生成昂贵且无用的堆栈跟踪
     * - 提高异常处理的性能
     *
     * @return 当前异常实例
     */
    @Override
    public Throwable fillInStackTrace() {
        // 返回当前实例而不生成堆栈跟踪
        return this;
    }
}
