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
 * 未知消费者组成员ID异常
 * 
 * 当消费者组协调器（Group Coordinator）无法识别请求中的成员ID时，会抛出此异常。
 * 
 * 应用场景：
 * 1. 消费者组成员加入组时使用了无效或过期的成员ID
 * 2. 消费者组成员重新加入组，但之前的会话已过期
 * 3. 消费者组进行再平衡（Rebalance）时，某个成员的ID无法识别
 * 
 * 错误处理：
 * 1. 消费者需要使用空的成员ID重新加入组
 * 2. 重新进行消费者组的成员身份认证
 * 3. 等待新的成员ID分配
 */
public class UnknownMemberIdException extends ApiException {
    private static final long serialVersionUID = 1L;

    /**
     * 创建一个未知消费者组成员ID异常
     */
    public UnknownMemberIdException() {
        super();
    }

    /**
     * 创建一个未知消费者组成员ID异常
     * 
     * @param message 异常描述信息
     * @param cause 导致此异常的原始异常
     */
    public UnknownMemberIdException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 创建一个未知消费者组成员ID异常
     * 
     * @param message 异常描述信息
     */
    public UnknownMemberIdException(String message) {
        super(message);
    }

    /**
     * 创建一个未知消费者组成员ID异常
     * 
     * @param cause 导致此异常的原始异常
     */
    public UnknownMemberIdException(Throwable cause) {
        super(cause);
    }
}
