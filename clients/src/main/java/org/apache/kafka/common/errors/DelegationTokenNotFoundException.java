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
 * 委托令牌未找到异常
 * 
 * 该异常在以下场景中抛出：
 * 1. 当客户端尝试使用不存在的委托令牌进行认证时
 * 2. 当尝试续期或删除一个不存在的委托令牌时
 * 3. 当代理服务器无法在其令牌存储中找到指定的令牌时
 * 
 * 这种情况通常发生在：
 * - 令牌已被管理员手动删除
 * - 令牌ID不正确或已损坏
 * - 集群重启后令牌存储被清空
 */
public class DelegationTokenNotFoundException extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 构造函数
     * 
     * @param message 异常消息，通常包含无法找到令牌的具体原因和相关信息
     */
    public DelegationTokenNotFoundException(String message) {
        super(message);
    }

    /**
     * 构造函数
     * 
     * @param message 异常消息，通常包含无法找到令牌的具体原因和相关信息
     * @param cause 导致此异常的原始异常，可能包含更详细的错误信息
     */
    public DelegationTokenNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

}
