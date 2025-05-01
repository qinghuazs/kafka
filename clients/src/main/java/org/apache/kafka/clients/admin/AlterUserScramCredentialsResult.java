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
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link Admin#alterUserScramCredentials(List)}调用的结果类。
 * 
 * 该类用于处理修改用户SCRAM（Salted Challenge Response Authentication Mechanism）凭证的操作结果。
 * SCRAM是一种基于密码的质询响应认证机制，用于安全地验证客户端身份。
 * 
 * 此API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class AlterUserScramCredentialsResult {
    /**
     * 存储每个用户的凭证修改操作结果的Future映射。
     * key: 用户名
     * value: 对应用户的凭证修改操作的Future结果
     * Future<Void>表示操作成功时返回null，失败时抛出异常
     */
    private final Map<String, KafkaFuture<Void>> futures;

    /**
     * 构造函数，初始化修改SCRAM凭证操作的结果对象
     *
     * @param futures 包含用户名到对应操作结果Future的映射，每个Future代表该用户的凭证修改操作结果
     *                使用Collections.unmodifiableMap确保返回的Map不可修改，提供线程安全保证
     */
    public AlterUserScramCredentialsResult(Map<String, KafkaFuture<Void>> futures) {
        // 使用Objects.requireNonNull确保futures参数不为null
        // 使用Collections.unmodifiableMap包装map，确保其不可修改
        this.futures = Collections.unmodifiableMap(Objects.requireNonNull(futures));
    }

    /**
     * 返回用户名到对应操作结果Future的映射
     * 
     * @return 不可修改的Map，包含每个用户的凭证修改操作结果
     *         可用于分别检查每个用户的操作是否成功
     */
    public Map<String, KafkaFuture<Void>> values() {
        return this.futures;
    }

    /**
     * 返回一个聚合的Future，仅当所有用户的SCRAM凭证修改操作都成功时才成功
     * 
     * @return 聚合的KafkaFuture
     *         - 如果所有操作都成功，Future完成且返回null
     *         - 如果任何操作失败，Future将抛出异常
     */
    public KafkaFuture<Void> all() {
        // 将所有Future转换为数组并使用KafkaFuture.allOf等待所有操作完成
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}
