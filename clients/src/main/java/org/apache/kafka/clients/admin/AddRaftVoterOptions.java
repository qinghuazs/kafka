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

import java.util.Optional;

/**
 * {@link Admin#addRaftVoter}方法的选项类
 *
 * 该类用于配置向Kafka集群添加Raft投票者的操作。继承自AbstractOptions，
 * 除了基本的超时设置外，还提供了集群ID的可选配置。
 *
 * Raft投票者是Kafka集群中参与领导者选举的节点，通过添加投票者可以扩展集群的容错能力。
 */
@InterfaceStability.Stable
public class AddRaftVoterOptions extends AbstractOptions<AddRaftVoterOptions> {
    /**
     * 可选的集群ID
     * 用于验证操作针对的是正确的集群
     */
    private Optional<String> clusterId = Optional.empty();

    /**
     * 设置集群ID
     *
     * @param clusterId 集群ID，如果提供则用于验证操作的目标集群
     * @return 返回当前对象实例，支持方法链式调用
     */
    public AddRaftVoterOptions setClusterId(Optional<String> clusterId) {
        // 设置集群ID
        this.clusterId = clusterId;
        // 返回当前实例，支持链式调用
        return this;
    }

    /**
     * 获取配置的集群ID
     *
     * @return 返回配置的集群ID，如果未设置则返回空Optional
     */
    public Optional<String> clusterId() {
        return clusterId;
    }
}
