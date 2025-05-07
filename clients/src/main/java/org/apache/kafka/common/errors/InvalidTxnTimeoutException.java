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
 * 事务超时异常
 * 
 * 当事务协调器（Transaction Coordinator）接收到的InitProducerIdRequest请求中的超时时间值
 * 大于服务器端配置的`transaction.max.timeout.ms`值时，将抛出此异常。
 * 
 * 应用场景：
 * 1. 用于限制事务的最大执行时间，防止事务长期占用系统资源
 * 2. 确保生产者事务的超时设置在合理范围内
 * 3. 帮助及时发现和处理事务超时配置不当的问题
 * 
 * 设计考虑：
 * 1. 继承自ApiException，表明这是一个API层面的异常
 * 2. 提供带有异常信息和原因的构造方法，方便异常信息的传递
 * 3. 通过serialVersionUID确保序列化的版本一致性
 */
public class InvalidTxnTimeoutException extends ApiException {
    private static final long serialVersionUID = 1L;

    public InvalidTxnTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidTxnTimeoutException(String message) {
        super(message);
    }
}
