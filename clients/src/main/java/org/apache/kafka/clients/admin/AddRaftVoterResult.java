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
 * {@link org.apache.kafka.clients.admin.Admin#addRaftVoter(int, org.apache.kafka.common.Uuid, java.util.Set, org.apache.kafka.clients.admin.AddRaftVoterOptions)}方法的结果类
 *
 * 该类用于表示向Kafka集群添加Raft投票者操作的执行结果。通过Future对象可以异步地获取操作的完成状态。
 * Raft投票者的添加是集群配置变更的一部分，需要等待集群完成配置同步。
 *
 * 注意：这个类的API已经稳定，不会有重大变更。
 */
@InterfaceStability.Stable
public class AddRaftVoterResult {
    /**
     * 操作的Future结果
     * 当投票者成功添加到集群后，这个Future会完成
     */
    private final KafkaFuture<Void> result;

    /**
     * 构造函数
     *
     * @param result 表示添加投票者操作的Future结果
     */
    AddRaftVoterResult(KafkaFuture<Void> result) {
        // 初始化操作结果Future
        this.result = result;
    }

    /**
     * 返回一个Future，该Future在投票者被成功添加到集群后完成
     *
     * @return 返回操作的Future结果，可用于检查操作是否成功完成
     */
    public KafkaFuture<Void> all() {
        return result;
    }

}
