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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.List;

/**
 * 用于修改用户SCRAM凭证的选项类。
 * 
 * SCRAM（Salted Challenge Response Authentication Mechanism）是Kafka支持的一种安全认证机制，
 * 用于验证客户端身份。此类提供了修改用户SCRAM凭证时的配置选项，包括：
 * 
 * 1. 创建新的用户凭证
 * 2. 更新现有用户的凭证
 * 3. 删除用户的凭证
 * 
 * 该类继承自AbstractOptions，提供了通用的选项设置功能。通过AdminClient的alterUserScramCredentials方法使用，
 * 可以批量修改多个用户的SCRAM凭证。
 * 
 * 使用场景：
 * - 系统管理员需要批量创建新用户时
 * - 定期更新用户密码以提高安全性时
 * - 删除过期或不再使用的用户凭证时
 * 
 * 注意：此API仍在演进中，后续版本可能会有变更，详见{@link AdminClient}。
 * 
 * @see AdminClient#alterUserScramCredentials(List, AlterUserScramCredentialsOptions)
 */
@InterfaceStability.Evolving
public class AlterUserScramCredentialsOptions extends AbstractOptions<AlterUserScramCredentialsOptions> {
}
