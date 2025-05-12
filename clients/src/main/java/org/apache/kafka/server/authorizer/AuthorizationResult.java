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

package org.apache.kafka.server.authorizer;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * @InterfaceStability.Evolving // 表明该接口目前处于演进阶段，其API可能会在未来的版本中发生变化。
 *                               // 使用者应该意识到这一点，并谨慎地在生产环境中使用，或者准备好应对可能的API不兼容变更。
 *                               // 设计考虑：此注解有助于API的维护者在不破坏现有稳定接口的前提下，逐步完善和改进新的API。
 *                               // 它也提醒了API的使用者，对于标记为Evolving的接口，需要更加关注其版本更新说明。
 */
@InterfaceStability.Evolving
public enum AuthorizationResult { // 定义授权结果的枚举类。在Kafka中，当对资源执行操作进行权限检查时，此枚举用于表示检查的结果。
    /**
     * 表示操作被允许。
     * 应用场景：当授权器（Authorizer）检查一个操作（例如，读取主题、写入主题）的权限时，
     * 如果根据配置的ACL（访问控制列表）确定该操作是允许的，则返回此结果。
     * 例如，如果一个用户有权限读取名为 "test-topic" 的主题，那么当该用户尝试读取此主题时，授权结果将是 ALLOWED。
     */
    ALLOWED,

    /**
     * 表示操作被拒绝。
     * 应用场景：当授权器检查一个操作的权限时，如果根据配置的ACL确定该操作是不允许的，则返回此结果。
     * 例如，如果一个用户没有权限写入名为 "secure-topic" 的主题，那么当该用户尝试写入此主题时，授权结果将是 DENIED。
     * 设计考虑：明确的拒绝状态有助于系统快速响应未授权的访问尝试，并可以记录相关的安全审计日志。
     */
    DENIED
}
