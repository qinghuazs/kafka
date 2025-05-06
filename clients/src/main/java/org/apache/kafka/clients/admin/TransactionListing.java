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

import java.util.Objects;

/**
 * TransactionListing类用于表示Kafka中的事务信息。
 * 该类封装了事务的基本属性，包括事务ID、生产者ID和事务状态。
 * 主要用于在Admin客户端中列举和查询事务信息，是事务管理的核心数据结构之一。
 *
 * 应用场景：
 * 1. 监控事务状态：可用于监控系统中正在进行的事务
 * 2. 事务追踪：通过transactionalId和producerId追踪特定事务
 * 3. 故障诊断：通过查看事务状态协助诊断事务相关问题
 */
@InterfaceStability.Evolving
public class TransactionListing {
    /**
     * 事务ID，用于唯一标识一个事务
     * 由客户端在初始化事务时指定，在整个事务生命周期中保持不变
     */
    private final String transactionalId;

    /**
     * 生产者ID，由Kafka自动分配给生产者的唯一标识符
     * 用于在集群中追踪特定生产者的事务操作
     */
    private final long producerId;

    /**
     * 事务当前的状态
     * 可能的值定义在TransactionState枚举中，如ONGOING、PREPARE_COMMIT等
     * 用于表示事务的执行阶段和状态
     */
    private final TransactionState transactionState;

    /**
     * 构造函数，创建一个新的事务列表项
     *
     * @param transactionalId 事务ID，用于唯一标识事务
     * @param producerId 生产者ID，标识执行事务的生产者
     * @param transactionState 事务状态，表示事务的当前状态
     */
    public TransactionListing(
        String transactionalId,
        long producerId,
        TransactionState transactionState
    ) {
        this.transactionalId = transactionalId;
        this.producerId = producerId;
        this.transactionState = transactionState;
    }

    /**
     * 获取事务ID
     * 用于在需要查询或操作特定事务时标识目标事务
     *
     * @return 返回事务的唯一标识符
     */
    public String transactionalId() {
        return transactionalId;
    }

    /**
     * 获取生产者ID
     * 用于标识和追踪执行事务的特定生产者
     *
     * @return 返回生产者的唯一标识符
     */
    public long producerId() {
        return producerId;
    }

    /**
     * 获取事务状态
     * 用于查询事务的当前执行状态，便于监控和管理事务
     *
     * @return 返回当前的事务状态
     */
    public TransactionState state() {
        return transactionState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionListing that = (TransactionListing) o;
        return producerId == that.producerId &&
            Objects.equals(transactionalId, that.transactionalId) &&
            transactionState == that.transactionState;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionalId, producerId, transactionState);
    }

    @Override
    public String toString() {
        return "TransactionListing(" +
            "transactionalId='" + transactionalId + '\'' +
            ", producerId=" + producerId +
            ", transactionState=" + transactionState +
            ')';
    }
}
