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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Set;

/**
 * {@link KafkaAdminClient#describeCluster()}调用的结果类。
 *
 * 此类封装了Kafka集群信息查询的异步操作结果，包含以下信息：
 * - 集群中的所有节点信息
 * - 控制器节点信息
 * - 集群ID
 * - 授权操作列表
 * 
 * 应用场景：
 * 1. 集群状态监控和管理
 * 2. 集群节点健康检查
 * 3. 权限管理和审计
 * 
 * 注意：该API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeClusterResult {
    /**
     * 存储集群节点信息的Future对象
     */
    private final KafkaFuture<Collection<Node>> nodes;
    
    /**
     * 存储控制器节点信息的Future对象
     */
    private final KafkaFuture<Node> controller;
    
    /**
     * 存储集群ID的Future对象
     */
    private final KafkaFuture<String> clusterId;
    
    /**
     * 存储授权操作集合的Future对象
     */
    private final KafkaFuture<Set<AclOperation>> authorizedOperations;

    /**
     * 构造函数，初始化集群查询结果。
     *
     * @param nodes 集群节点列表的Future对象
     * @param controller 控制器节点的Future对象
     * @param clusterId 集群ID的Future对象
     * @param authorizedOperations 授权操作集合的Future对象
     */
    DescribeClusterResult(KafkaFuture<Collection<Node>> nodes,
                          KafkaFuture<Node> controller,
                          KafkaFuture<String> clusterId,
                          KafkaFuture<Set<AclOperation>> authorizedOperations) {
        this.nodes = nodes;
        this.controller = controller;
        this.clusterId = clusterId;
        this.authorizedOperations = authorizedOperations;
    }

    /**
     * 获取集群节点列表的Future对象。
     * 
     * 使用示例：
     * Collection<Node> clusterNodes = result.nodes().get();
     * 
     * @return 包含集群所有节点信息的Future对象
     */
    public KafkaFuture<Collection<Node>> nodes() {
        return nodes;
    }

    /**
     * 获取当前控制器节点的Future对象。
     * 
     * 注意：如果控制器节点ID尚未确定，该方法返回的Future可能会得到null值。
     * 这种情况可能发生在集群启动过程中或控制器选举期间。
     * 
     * @return 包含控制器节点信息的Future对象
     */
    public KafkaFuture<Node> controller() {
        return controller;
    }

    /**
     * 获取集群ID的Future对象。
     * 
     * 当broker版本为0.10.1.0或更高时，返回的Future值将不为null；
     * 否则返回null。这是因为集群ID功能是在0.10.1.0版本中引入的。
     * 
     * @return 包含集群ID的Future对象
     */
    public KafkaFuture<String> clusterId() {
        return clusterId;
    }

    /**
     * 获取授权操作集合的Future对象。
     * 
     * 如果broker提供了授权信息，返回的Future值将不为null；
     * 否则返回null。这通常用于权限管理和审计目的。
     * 
     * @return 包含授权操作集合的Future对象
     */
    public KafkaFuture<Set<AclOperation>> authorizedOperations() {
        return authorizedOperations;
    }
}
