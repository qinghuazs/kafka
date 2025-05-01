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
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Map;

/**
 * {@link Admin#createAcls(Collection)} 调用的结果类。
 * 该类用于处理批量创建ACL（访问控制列表）操作的异步结果。
 * 
 * 应用场景：
 * 1. 当需要为Kafka资源（如Topic、Group等）批量设置访问权限时使用
 * 2. 支持异步获取每个ACL创建操作的执行状态
 * 3. 提供聚合方法检查所有ACL是否都创建成功
 * 
 * 注意：该类的API仍在演进中，详见 {@link Admin}
 */
@InterfaceStability.Evolving
public class CreateAclsResult {
    /**
     * 存储每个ACL绑定对应的异步操作结果
     * - Key: AclBinding对象，表示具体的ACL绑定信息（包含资源类型、权限类型等）
     * - Value: KafkaFuture对象，用于异步获取对应ACL的创建结果
     */
    private final Map<AclBinding, KafkaFuture<Void>> futures;

    /**
     * 构造函数，初始化ACL创建结果映射
     * @param futures ACL绑定到其对应创建操作Future的映射
     */
    CreateAclsResult(Map<AclBinding, KafkaFuture<Void>> futures) {
        this.futures = futures;
    }

    /**
     * 获取所有ACL绑定及其对应的创建状态Future
     * 
     * 实现细节：
     * - 直接返回内部futures映射，允许调用者分别检查每个ACL的创建状态
     * - Future完成时，如果没有异常表示对应的ACL创建成功
     * 
     * @return 返回ACL绑定到对应Future的映射，可用于检查每个ACL的创建状态
     */
    public Map<AclBinding, KafkaFuture<Void>> values() {
        return futures;
    }

    /**
     * 获取一个聚合的Future，用于检查所有ACL是否都创建成功
     * 
     * 实现细节：
     * 1. 将所有单个ACL的Future转换为数组
     * 2. 使用KafkaFuture.allOf方法创建一个聚合Future
     * 3. 只有当所有ACL都创建成功时，该Future才会成功完成
     * 
     * @return 返回一个KafkaFuture，可用于等待所有ACL创建操作完成
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}
