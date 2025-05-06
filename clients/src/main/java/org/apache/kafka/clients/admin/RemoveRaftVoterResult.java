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

/**
 * {@link org.apache.kafka.clients.admin.Admin#removeRaftVoter(int, org.apache.kafka.common.Uuid, org.apache.kafka.clients.admin.RemoveRaftVoterOptions)}方法的结果类
 *
 * 该类用于表示从Kafka集群中移除Raft投票者操作的执行结果。通过Future对象可以异步地获取操作的完成状态。
 * Raft投票者的移除是集群配置变更的一部分，需要等待集群完成配置同步。
 * 
 * 应用场景：
 * 1. 缩减集群规模时移除多余的投票者
 * 2. 替换故障的投票者节点
 * 3. 集群维护和重构时临时移除投票者
 *
 * 注意：这个类的API已经稳定，不会有重大变更。
 */
@InterfaceStability.Stable
public class RemoveRaftVoterResult {
    /**
     * 操作的Future结果
     * 当投票者成功从集群中移除后，这个Future会完成
     */
    private final KafkaFuture<Void> result;

    /**
     * 构造函数
     *
     * @param result 表示移除投票者操作的Future结果
     */
    RemoveRaftVoterResult(KafkaFuture<Void> result) {
        // 初始化操作结果Future
        this.result = result;
    }

    /**
     * 返回一个Future，该Future在投票者被成功从集群中移除后完成
     * 
     * @return 返回操作的Future结果，可用于检查操作是否成功完成
     */
    public KafkaFuture<Void> all() {
        // 返回移除操作的Future结果
        return result;
    }

}
