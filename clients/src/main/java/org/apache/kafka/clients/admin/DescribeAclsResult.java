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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;

/**
 * {@link KafkaAdminClient#describeAcls(AclBindingFilter)}调用的结果类。
 * 用于异步获取Kafka集群中的ACL（访问控制列表）配置信息。
 *
 * 该类的API仍在演进中，详见{@link Admin}。
 *
 * 应用场景：
 * 1. 权限查询：异步获取指定资源的ACL配置
 * 2. 安全审计：批量检查多个资源的访问控制规则
 * 3. 配置验证：验证ACL变更是否生效
 */
@InterfaceStability.Evolving
public class DescribeAclsResult {
    /**
     * 存储ACL查询结果的Future对象
     * 包含了一个AclBinding集合，每个AclBinding代表一条访问控制规则
     */
    private final KafkaFuture<Collection<AclBinding>> future;

    /**
     * 构造函数，初始化包含ACL查询结果的Future对象
     * 
     * @param future 异步操作的Future对象，完成时将返回ACL绑定集合
     */
    DescribeAclsResult(KafkaFuture<Collection<AclBinding>> future) {
        this.future = future;
    }

    /**
     * 返回包含请求的ACL信息的Future对象
     * 
     * 实现说明：
     * - 返回原始的Future对象，允许调用者以异步方式处理结果
     * - 可以通过Future的get()方法获取ACL绑定集合
     * - 支持超时和异常处理机制
     * 
     * @return 返回KafkaFuture对象，其结果为AclBinding集合
     */
    public KafkaFuture<Collection<AclBinding>> values() {
        return future;
    }
}
