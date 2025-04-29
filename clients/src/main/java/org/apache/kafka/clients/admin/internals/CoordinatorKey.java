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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.requests.FindCoordinatorRequest;

import java.util.Objects;

/**
 * 协调器键类，用于标识和查找Kafka集群中的协调器节点
 * 协调器主要用于两个场景：
 * 1. 消费者组协调器：负责管理消费者组的成员关系和分区分配
 * 2. 事务协调器：负责管理事务的状态和提交/回滚操作
 */
public class CoordinatorKey {
    /**
     * 协调器的标识值，可以是消费者组ID或事务ID
     */
    public final String idValue;

    /**
     * 协调器的类型，可以是GROUP（消费者组）或TRANSACTION（事务）
     * @see FindCoordinatorRequest.CoordinatorType
     */
    public final FindCoordinatorRequest.CoordinatorType type;

    /**
     * 私有构造函数，通过静态工厂方法创建实例
     * @param type 协调器类型
     * @param idValue 协调器标识值
     */
    private CoordinatorKey(FindCoordinatorRequest.CoordinatorType type, String idValue) {
        this.idValue = idValue;
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CoordinatorKey that = (CoordinatorKey) o;
        return Objects.equals(idValue, that.idValue) &&
            type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(idValue, type);
    }

    @Override
    public String toString() {
        return "CoordinatorKey(" +
            "idValue='" + idValue + '\'' +
            ", type=" + type +
            ')';
    }

    /**
     * 创建消费者组协调器的键
     * 用于查找管理指定消费者组的协调器节点
     * @param groupId 消费者组ID
     * @return 消费者组协调器的键
     */
    public static CoordinatorKey byGroupId(String groupId) {
        return new CoordinatorKey(FindCoordinatorRequest.CoordinatorType.GROUP, groupId);
    }

    /**
     * 创建事务协调器的键
     * 用于查找管理指定事务的协调器节点
     * @param transactionalId 事务ID
     * @return 事务协调器的键
     */
    public static CoordinatorKey byTransactionalId(String transactionalId) {
        return new CoordinatorKey(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
    }

}
