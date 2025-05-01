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
import org.apache.kafka.common.quota.ClientQuotaEntity;

import java.util.Collection;
import java.util.Map;

/**
 * {@link Admin#alterClientQuotas(Collection, AlterClientQuotasOptions)}调用的结果类
 *
 * 该类用于表示修改客户端配额操作的执行结果。每个配额实体的修改操作都会返回一个Future对象，
 * 可以通过这些Future对象来检查各个修改操作的执行状态。
 *
 * 注意：这个类的API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class AlterClientQuotasResult {

    /**
     * 存储每个配额实体对应的修改操作Future结果
     * Key为配额实体，Value为对应的修改操作Future
     */
    private final Map<ClientQuotaEntity, KafkaFuture<Void>> futures;

    /**
     * 构造函数，将配额实体映射到其修改结果
     *
     * @param futures 配额实体到其修改操作Future的映射，用于跟踪每个实体的修改状态
     */
    public AlterClientQuotasResult(Map<ClientQuotaEntity, KafkaFuture<Void>> futures) {
        // 初始化futures映射，存储每个配额实体的修改操作结果
        this.futures = futures;
    }

    /**
     * 返回配额实体到Future的映射，可用于检查每个实体的修改操作状态
     *
     * @return 返回配额实体到其修改操作Future的映射
     */
    public Map<ClientQuotaEntity, KafkaFuture<Void>> values() {
        return futures;
    }

    /**
     * 返回一个Future，只有当所有配额修改操作都成功时，该Future才会成功完成
     *
     * @return 返回一个组合了所有修改操作结果的Future
     */
    public KafkaFuture<Void> all() {
        // 使用KafkaFuture.allOf组合所有Future，只有全部成功才返回成功
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}
