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
 * 当指定的安全认证主体类型无效时抛出此异常。
 * 
 * 应用场景：
 * 1. 配置ACL时使用了不支持的主体类型
 * 2. 安全协议与主体类型不匹配
 * 3. 自定义认证机制使用了未注册的主体类型
 * 
 * 设计考虑：
 * - 用于Kafka安全框架中的身份认证管理
 * - 帮助管理员正确配置和维护访问控制列表（ACL）
 * - 确保认证主体类型的一致性和有效性
 */
public class InvalidPrincipalTypeException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidPrincipalTypeException(String message) {
        super(message);
    }

    public InvalidPrincipalTypeException(String message, Throwable cause) {
        super(message, cause);
    }

}
