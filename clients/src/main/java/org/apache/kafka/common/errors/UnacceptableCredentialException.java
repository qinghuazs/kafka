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
 * 当尝试定义一个不符合可接受标准的凭证时抛出此异常。
 * 在Kafka的安全认证机制中，凭证（如用户名和密码）必须满足特定的格式和安全要求。
 * 例如，在使用SCRAM认证时，如果尝试创建带有空用户名、空密码，或者迭代次数不合规的凭证，就会抛出此异常。
 * 
 * 应用场景：
 * 1. 创建SCRAM凭证时使用了空的用户名或密码
 * 2. 配置的密码哈希迭代次数过少或过多
 * 3. 凭证格式不符合安全策略要求
 * 4. 在更新现有凭证时使用了不合规的新凭证
 */
public class UnacceptableCredentialException extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 构造一个UnacceptableCredentialException异常实例
     *
     * @param message 异常描述信息，通常包含凭证不可接受的具体原因，如格式错误或安全要求不满足
     */
    public UnacceptableCredentialException(String message) {
        super(message);
    }

    /**
     * 构造一个UnacceptableCredentialException异常实例
     *
     * @param message 异常描述信息，通常包含凭证不可接受的具体原因，如格式错误或安全要求不满足
     * @param cause 导致此异常的原始异常，用于异常链的构建和问题追踪
     */
    public UnacceptableCredentialException(String message, Throwable cause) {
        super(message, cause);
    }
}