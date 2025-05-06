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

import java.util.Collections;
import java.util.List;

/**
 * 分区重分配状态类，用于通过 {@link AdminClient#listPartitionReassignments()} 获取分区重分配的当前状态。
 *
 * 应用场景：
 * 1. 监控分区重分配进度：查看哪些broker正在添加或移除分区副本
 * 2. 故障诊断：当重分配过程出现问题时，可以查看具体状态进行排查
 * 3. 集群管理：了解分区副本在broker间的分布和迁移情况
 * 4. 负载均衡：评估分区重分配对集群负载的影响
 */
public class PartitionReassignment {

    /**
     * 当前分区所在的所有broker ID列表，包括：
     * 1. 正在保有该分区副本的broker
     * 2. 正在添加该分区副本的broker
     * 3. 即将移除该分区副本的broker
     */
    private final List<Integer> replicas;

    /**
     * 正在添加该分区副本的broker ID列表
     * 这些broker将成为该分区的新副本持有者
     */
    private final List<Integer> addingReplicas;

    /**
     * 正在移除该分区副本的broker ID列表
     * 这些broker上的分区副本将被删除
     */
    private final List<Integer> removingReplicas;

    /**
     * 创建一个分区重分配状态对象
     *
     * @param replicas 当前所有相关的broker ID列表
     * @param addingReplicas 正在添加副本的broker ID列表
     * @param removingReplicas 正在移除副本的broker ID列表
     */
    public PartitionReassignment(List<Integer> replicas, List<Integer> addingReplicas, List<Integer> removingReplicas) {
        // 使用Collections.unmodifiableList确保返回不可修改的列表，防止外部修改内部状态
        this.replicas = Collections.unmodifiableList(replicas);
        this.addingReplicas = Collections.unmodifiableList(addingReplicas);
        this.removingReplicas = Collections.unmodifiableList(removingReplicas);
    }

    /**
     * 获取当前分区所在的所有broker ID列表
     *
     * @return 不可修改的broker ID列表，包含所有相关的broker
     */
    public List<Integer> replicas() {
        return replicas;
    }

    /**
     * 获取正在添加该分区副本的broker ID列表
     *
     * @return 不可修改的broker ID列表，这些broker正在接收该分区的新副本
     */
    public List<Integer> addingReplicas() {
        return addingReplicas;
    }

    /**
     * 获取正在移除该分区副本的broker ID列表
     *
     * @return 不可修改的broker ID列表，这些broker上的分区副本将被删除
     */
    public List<Integer> removingReplicas() {
        return removingReplicas;
    }

    @Override
    public String toString() {
        return "PartitionReassignment(" +
                "replicas=" + replicas +
                ", addingReplicas=" + addingReplicas +
                ", removingReplicas=" + removingReplicas +
                ')';
    }
}
