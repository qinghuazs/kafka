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
 * 表示记录的时间戳无效的异常。
 * 
 * 应用场景：
 * 1. 当生产者发送消息时，如果消息的时间戳格式不正确或超出有效范围
 * 2. 当时间戳小于0或大于允许的最大值时
 * 3. 当使用CreateTime或LogAppendTime时间戳类型，但提供的时间戳值不符合要求
 * 
 * 设计考虑：
 * 1. 继承自ApiException，表示这是一个不可重试的异常
 * 2. 提供带有详细错误信息的构造函数，方便定位问题
 * 3. 支持异常链，可以包含导致时间戳无效的原始异常
 */
public class InvalidTimestampException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidTimestampException(String message) {
        super(message);
    }

    public InvalidTimestampException(String message, Throwable cause) {
        super(message, cause);
    }
}
