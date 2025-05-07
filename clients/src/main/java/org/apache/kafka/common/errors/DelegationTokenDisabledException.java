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
 * 当Kafka集群中的委托令牌（Delegation Token）功能被禁用时抛出此异常。
 * 
 * 应用场景：
 * 1. 当用户尝试创建委托令牌，但集群配置中禁用了该功能
 * 2. 当尝试使用委托令牌进行认证，但服务器不支持令牌认证
 * 3. 当管理员禁用了委托令牌功能后，客户端仍尝试使用相关功能
 * 
 * 设计考虑：
 * 1. 继承自ApiException，表明这是一个API层面的异常
 * 2. 用于明确指示令牌功能的禁用状态
 * 3. 提供清晰的错误信息，帮助用户理解配置问题
 * 4. 支持异常链，便于追踪问题根源
 */
public class DelegationTokenDisabledException extends ApiException {

    private static final long serialVersionUID = 1L;

    public DelegationTokenDisabledException(String message) {
        super(message);
    }

    public DelegationTokenDisabledException(String message, Throwable cause) {
        super(message, cause);
    }

}
