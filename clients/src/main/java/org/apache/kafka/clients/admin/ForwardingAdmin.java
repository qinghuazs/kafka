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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * {@code ForwardingAdmin} 是 MirrorMaker 中 {@code forwarding.admin.class} 的默认值。
 * 用户如果想要自定义 MirrorMaker 创建主题和访问控制列表的行为，可以继承此类，
 * 而不需要提供完整的 {@code Admin} 实现。
 * 该类必须有一个签名为 {@code (Map<String, Object> config)} 的构造函数，用于配置
 * 装饰的 {@link KafkaAdminClient} 和其他用于外部资源管理的客户端。
 */
public class ForwardingAdmin implements Admin {
    // 委托的Admin实例，用于实际执行管理操作
    private final Admin delegate;

    /**
     * 构造函数，使用配置创建ForwardingAdmin实例
     * 
     * @param configs 配置参数映射
     */
    public ForwardingAdmin(Map<String, Object> configs) {
        // 使用配置创建新的Admin实例
        this.delegate = Admin.create(configs);
    }

    /**
     * 关闭Admin客户端
     * 
     * @param timeout 关闭操作的超时时间
     */
    @Override
    public void close(Duration timeout) {
        // 委托给实际的Admin实例执行关闭操作
        delegate.close(timeout);
    }

    /**
     * 创建新的主题
     * 
     * @param newTopics 要创建的主题集合
     * @param options 创建主题的选项
     * @return 返回创建主题的结果
     */
    @Override
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
        // 委托给实际的Admin实例执行创建主题操作
        return delegate.createTopics(newTopics, options);
    }

    /**
     * 删除指定的主题集合
     * 
     * @param topics 要删除的主题集合
     * @param options 删除主题的选项
     * @return 返回删除主题的操作结果
     */
    @Override
    public DeleteTopicsResult deleteTopics(TopicCollection topics, DeleteTopicsOptions options) {
        // 委托给实际的Admin实例执行删除主题操作
        return delegate.deleteTopics(topics, options);
    }

    /**
     * 列出Kafka集群中的所有主题
     * 
     * @param options 列出主题的选项
     * @return 返回主题列表的操作结果
     */
    @Override
    public ListTopicsResult listTopics(ListTopicsOptions options) {
        // 委托给实际的Admin实例执行列出主题操作
        return delegate.listTopics(options);
    }

    /**
     * 描述指定主题的详细信息
     * 
     * @param topics 要描述的主题集合
     * @param options 描述主题的选项
     * @return 返回主题描述的操作结果
     */
    @Override
    public DescribeTopicsResult describeTopics(TopicCollection topics, DescribeTopicsOptions options) {
        // 委托给实际的Admin实例执行描述主题操作
        return delegate.describeTopics(topics, options);
    }

    /**
     * 描述Kafka集群的信息
     * 
     * @param options 描述集群的选项
     * @return 返回集群描述的操作结果
     */
    @Override
    public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        // 委托给实际的Admin实例执行描述集群操作
        return delegate.describeCluster(options);
    }

    /**
     * 描述访问控制列表（ACL）的信息
     * 
     * @param filter ACL绑定过滤器
     * @param options 描述ACL的选项
     * @return 返回ACL描述的操作结果
     */
    @Override
    public DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options) {
        // 委托给实际的Admin实例执行描述ACL操作
        return delegate.describeAcls(filter, options);
    }

    /**
     * 创建新的访问控制列表（ACL）
     * 
     * @param acls 要创建的ACL绑定集合
     * @param options 创建ACL的选项
     * @return 返回创建ACL的操作结果
     */
    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        // 委托给实际的Admin实例执行创建ACL操作
        return delegate.createAcls(acls, options);
    }

    /**
     * 删除指定的访问控制列表（ACL）
     * 
     * @param filters ACL绑定过滤器集合
     * @param options 删除ACL的选项
     * @return 返回删除ACL的操作结果
     */
    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
        // 委托给实际的Admin实例执行删除ACL操作
        return delegate.deleteAcls(filters, options);
    }

    /**
     * 描述配置资源的详细信息
     * 
     * @param resources 要描述的配置资源集合
     * @param options 描述配置的选项
     * @return 返回配置描述的操作结果
     */
    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
        // 委托给实际的Admin实例执行描述配置操作
        return delegate.describeConfigs(resources, options);
    }

    /**
     * 增量修改配置资源的配置项
     * 
     * @param configs 配置资源到配置操作的映射
     * @param options 修改配置的选项
     * @return 返回配置修改的操作结果
     */
    @Override
    public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs, AlterConfigsOptions options) {
        // 委托给实际的Admin实例执行增量修改配置操作
        return delegate.incrementalAlterConfigs(configs, options);
    }

    /**
     * 修改副本日志目录的分配
     * 
     * @param replicaAssignment 副本到日志目录的分配映射
     * @param options 修改日志目录的选项
     * @return 返回日志目录修改的操作结果
     */
    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment, AlterReplicaLogDirsOptions options) {
        // 委托给实际的Admin实例执行修改副本日志目录操作
        return delegate.alterReplicaLogDirs(replicaAssignment, options);
    }

    /**
     * 描述broker的日志目录信息
     * 
     * @param brokers 要描述的broker ID集合
     * @param options 描述日志目录的选项
     * @return 返回日志目录描述的操作结果
     */
    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
        // 委托给实际的Admin实例执行描述日志目录操作
        return delegate.describeLogDirs(brokers, options);
    }

    /**
     * 描述副本的日志目录信息
     * 
     * @param replicas 要描述的主题分区副本集合
     * @param options 描述副本日志目录的选项
     * @return 返回副本日志目录描述的操作结果
     */
    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options) {
        // 委托给实际的Admin实例执行描述副本日志目录操作
        return delegate.describeReplicaLogDirs(replicas, options);
    }

    /**
     * 为主题创建新的分区
     * 
     * @param newPartitions 主题名称到新分区信息的映射
     * @param options 创建分区的选项
     * @return 返回创建分区的操作结果
     */
    @Override
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions, CreatePartitionsOptions options) {
        // 委托给实际的Admin实例执行创建分区操作
        return delegate.createPartitions(newPartitions, options);
    }

    /**
     * 删除指定主题分区的记录
     * 
     * @param recordsToDelete 主题分区到要删除记录的映射
     * @param options 删除记录的选项
     * @return 返回删除记录的操作结果
     */
    @Override
    public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete, DeleteRecordsOptions options) {
        // 委托给实际的Admin实例执行删除记录操作
        return delegate.deleteRecords(recordsToDelete, options);
    }

    /**
     * 创建委托令牌
     * 
     * @param options 创建委托令牌的选项
     * @return 返回创建委托令牌的操作结果
     */
    @Override
    public CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options) {
        // 委托给实际的Admin实例执行创建委托令牌操作
        return delegate.createDelegationToken(options);
    }

    /**
     * 续期委托令牌
     * 
     * @param hmac 用于验证的HMAC值
     * @param options 续期令牌的选项
     * @return 返回续期令牌的操作结果
     */
    @Override
    public RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options) {
        // 委托给实际的Admin实例执行续期令牌操作
        return delegate.renewDelegationToken(hmac, options);
    }

    /**
     * 使委托令牌过期
     * 
     * @param hmac 用于验证的HMAC值
     * @param options 使令牌过期的选项
     * @return 返回使令牌过期的操作结果
     */
    @Override
    public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options) {
        // 委托给实际的Admin实例执行使令牌过期操作
        return delegate.expireDelegationToken(hmac, options);
    }

    /**
     * 描述委托令牌的详细信息
     * 
     * @param options 描述令牌的选项
     * @return 返回令牌描述的操作结果
     */
    @Override
    public DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options) {
        // 委托给实际的Admin实例执行描述令牌操作
        return delegate.describeDelegationToken(options);
    }

    /**
     * 描述消费者组的详细信息
     * 
     * @param groupIds 要描述的消费者组ID集合
     * @param options 描述消费者组的选项
     * @return 返回消费者组描述的操作结果
     */
    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
        // 委托给实际的Admin实例执行描述消费者组操作
        return delegate.describeConsumerGroups(groupIds, options);
    }

    /**
     * 列出所有消费者组
     * 
     * @param options 列出消费者组的选项
     * @return 返回消费者组列表的操作结果
     */
    @Override
    public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
        // 委托给实际的Admin实例执行列出消费者组操作
        return delegate.listConsumerGroups(options);
    }

    /**
     * 列出消费者组的偏移量信息
     * 
     * @param groupSpecs 消费者组规范的映射
     * @param options 列出偏移量的选项
     * @return 返回消费者组偏移量的操作结果
     */
    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, ListConsumerGroupOffsetsOptions options) {
        // 委托给实际的Admin实例执行列出消费者组偏移量操作
        return delegate.listConsumerGroupOffsets(groupSpecs, options);
    }

    /**
     * 删除指定的消费者组
     * 
     * @param groupIds 要删除的消费者组ID集合
     * @param options 删除消费者组的选项
     * @return 返回删除消费者组的操作结果
     */
    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options) {
        // 委托给实际的Admin实例执行删除消费者组操作
        return delegate.deleteConsumerGroups(groupIds, options);
    }

    /**
     * 删除消费者组的偏移量信息
     * 
     * @param groupId 消费者组ID
     * @param partitions 要删除偏移量的分区集合
     * @param options 删除偏移量的选项
     * @return 返回删除偏移量的操作结果
     */
    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions, DeleteConsumerGroupOffsetsOptions options) {
        // 委托给实际的Admin实例执行删除消费者组偏移量操作
        return delegate.deleteConsumerGroupOffsets(groupId, partitions, options);
    }

    /**
     * 选举分区的领导者
     * 
     * @param electionType 选举类型
     * @param partitions 要选举领导者的分区集合
     * @param options 选举领导者的选项
     * @return 返回领导者选举的操作结果
     */
    @Override
    public ElectLeadersResult electLeaders(ElectionType electionType, Set<TopicPartition> partitions, ElectLeadersOptions options) {
        // 委托给实际的Admin实例执行领导者选举操作
        return delegate.electLeaders(electionType, partitions, options);
    }

    /**
     * 修改分区的重分配计划
     * 
     * @param reassignments 分区到重分配计划的映射
     * @param options 修改重分配的选项
     * @return 返回修改重分配的操作结果
     */
    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments, AlterPartitionReassignmentsOptions options) {
        // 委托给实际的Admin实例执行修改分区重分配操作
        return delegate.alterPartitionReassignments(reassignments, options);
    }

    /**
     * 列出分区的重分配计划
     * 
     * @param partitions 要列出重分配的分区集合（可选）
     * @param options 列出重分配的选项
     * @return 返回重分配列表的操作结果
     */
    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions, ListPartitionReassignmentsOptions options) {
        // 委托给实际的Admin实例执行列出分区重分配操作
        return delegate.listPartitionReassignments(partitions, options);
    }

    /**
     * 从消费者组中移除成员
     * 
     * @param groupId 消费者组ID
     * @param options 移除成员的选项
     * @return 返回移除成员的操作结果
     */
    @Override
    public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId, RemoveMembersFromConsumerGroupOptions options) {
        // 委托给实际的Admin实例执行移除消费者组成员操作
        return delegate.removeMembersFromConsumerGroup(groupId, options);
    }

    /**
     * 修改消费者组的偏移量
     * 
     * @param groupId 消费者组ID
     * @param offsets 分区到偏移量和元数据的映射
     * @param options 修改偏移量的选项
     * @return 返回修改偏移量的操作结果
     */
    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets, AlterConsumerGroupOffsetsOptions options) {
        // 委托给实际的Admin实例执行修改消费者组偏移量操作
        return delegate.alterConsumerGroupOffsets(groupId, offsets, options);
    }

    /**
     * 列出主题分区的偏移量信息
     * 
     * @param topicPartitionOffsets 主题分区到偏移量规范的映射
     * @param options 列出偏移量的选项
     * @return 返回偏移量列表的操作结果
     */
    @Override
    public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, ListOffsetsOptions options) {
        // 委托给实际的Admin实例执行列出偏移量操作
        return delegate.listOffsets(topicPartitionOffsets, options);
    }

    /**
     * 描述客户端配额的详细信息
     * 
     * @param filter 客户端配额过滤器
     * @param options 描述配额的选项
     * @return 返回客户端配额描述的操作结果
     */
    @Override
    public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter, DescribeClientQuotasOptions options) {
        // 委托给实际的Admin实例执行描述客户端配额操作
        return delegate.describeClientQuotas(filter, options);
    }

    /**
     * 修改客户端配额设置
     * 
     * @param entries 要修改的客户端配额变更集合
     * @param options 修改配额的选项
     * @return 返回修改配额的操作结果
     */
    @Override
    public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) {
        // 委托给实际的Admin实例执行修改客户端配额操作
        return delegate.alterClientQuotas(entries, options);
    }

    /**
     * 描述用户的SCRAM凭证信息
     * SCRAM (Salted Challenge Response Authentication Mechanism) 是一种用户认证机制
     * 
     * @param users 要描述的用户列表
     * @param options 描述SCRAM凭证的选项
     * @return 返回用户SCRAM凭证描述的操作结果
     */
    @Override
    public DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users, DescribeUserScramCredentialsOptions options) {
        // 委托给实际的Admin实例执行描述用户SCRAM凭证操作
        return delegate.describeUserScramCredentials(users, options);
    }

    /**
     * 修改用户的SCRAM凭证
     * 
     * @param alterations 要修改的SCRAM凭证变更列表
     * @param options 修改SCRAM凭证的选项
     * @return 返回修改SCRAM凭证的操作结果
     */
    @Override
    public AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations, AlterUserScramCredentialsOptions options) {
        // 委托给实际的Admin实例执行修改用户SCRAM凭证操作
        return delegate.alterUserScramCredentials(alterations, options);
    }

    /**
     * 描述Kafka集群支持的特性信息
     * 
     * @param options 描述特性的选项
     * @return 返回特性描述的操作结果
     */
    @Override
    public DescribeFeaturesResult describeFeatures(DescribeFeaturesOptions options) {
        // 委托给实际的Admin实例执行描述特性操作
        return delegate.describeFeatures(options);
    }

    /**
     * 更新Kafka集群的特性配置
     * 
     * @param featureUpdates 特性名称到更新信息的映射
     * @param options 更新特性的选项
     * @return 返回更新特性的操作结果
     */
    @Override
    public UpdateFeaturesResult updateFeatures(Map<String, FeatureUpdate> featureUpdates, UpdateFeaturesOptions options) {
        // 委托给实际的Admin实例执行更新特性操作
        return delegate.updateFeatures(featureUpdates, options);
    }

    /**
     * 描述Kafka元数据仲裁信息
     * 
     * @param options 描述元数据仲裁的选项
     * @return 返回元数据仲裁描述的操作结果
     */
    @Override
    public DescribeMetadataQuorumResult describeMetadataQuorum(DescribeMetadataQuorumOptions options) {
        // 委托给实际的Admin实例执行描述元数据仲裁操作
        return delegate.describeMetadataQuorum(options);
    }

    /**
     * 注销指定的Broker
     * 
     * @param brokerId 要注销的Broker ID
     * @param options 注销Broker的选项
     * @return 返回注销Broker的操作结果
     */
    @Override
    public UnregisterBrokerResult unregisterBroker(int brokerId, UnregisterBrokerOptions options) {
        // 委托给实际的Admin实例执行注销Broker操作
        return delegate.unregisterBroker(brokerId, options);
    }

    /**
     * 描述主题分区的生产者信息
     * 
     * @param partitions 要描述的主题分区集合
     * @param options 描述生产者的选项
     * @return 返回生产者描述的操作结果
     */
    @Override
    public DescribeProducersResult describeProducers(Collection<TopicPartition> partitions, DescribeProducersOptions options) {
        // 委托给实际的Admin实例执行描述生产者操作
        return delegate.describeProducers(partitions, options);
    }

    /**
     * 描述事务的详细信息
     * 
     * @param transactionalIds 要描述的事务ID集合
     * @param options 描述事务的选项
     * @return 返回事务描述的操作结果
     */
    @Override
    public DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds, DescribeTransactionsOptions options) {
        // 委托给实际的Admin实例执行描述事务操作
        return delegate.describeTransactions(transactionalIds, options);
    }

    /**
     * 中止指定的事务
     * 
     * @param spec 事务中止规范
     * @param options 中止事务的选项
     * @return 返回中止事务的操作结果
     */
    @Override
    public AbortTransactionResult abortTransaction(AbortTransactionSpec spec, AbortTransactionOptions options) {
        // 委托给实际的Admin实例执行中止事务操作
        return delegate.abortTransaction(spec, options);
    }

    /**
     * 列出所有活跃的事务
     * 
     * @param options 列出事务的选项
     * @return 返回事务列表的操作结果
     */
    @Override
    public ListTransactionsResult listTransactions(ListTransactionsOptions options) {
        // 委托给实际的Admin实例执行列出事务操作
        return delegate.listTransactions(options);
    }

    /**
     * 隔离指定的事务生产者
     * 
     * @param transactionalIds 要隔离的事务ID集合
     * @param options 隔离生产者的选项
     * @return 返回隔离生产者的操作结果
     */
    @Override
    public FenceProducersResult fenceProducers(Collection<String> transactionalIds, FenceProducersOptions options) {
        // 委托给实际的Admin实例执行隔离生产者操作
        return delegate.fenceProducers(transactionalIds, options);
    }

    /**
     * 列出客户端度量资源
     * 
     * @param options 列出度量资源的选项
     * @return 返回度量资源列表的操作结果
     */
    @Override
    public ListClientMetricsResourcesResult listClientMetricsResources(ListClientMetricsResourcesOptions options) {
        // 委托给实际的Admin实例执行列出客户端度量资源操作
        return delegate.listClientMetricsResources(options);
    }

    /**
     * 获取客户端实例ID
     * 
     * @param timeout 获取实例ID的超时时间
     * @return 返回客户端实例的唯一标识符
     */
    @Override
    public Uuid clientInstanceId(Duration timeout) {
        // 委托给实际的Admin实例执行获取客户端实例ID操作
        return delegate.clientInstanceId(timeout);
    }

    /**
     * 添加Raft投票者
     * 
     * @param voterId 投票者ID
     * @param voterDirectoryId 投票者目录ID
     * @param endpoints 投票者端点集合
     * @param options 添加投票者的选项
     * @return 返回添加Raft投票者的操作结果
     */
    @Override
    public AddRaftVoterResult addRaftVoter(int voterId, Uuid voterDirectoryId, Set<RaftVoterEndpoint> endpoints, AddRaftVoterOptions options) {
        // 委托给实际的Admin实例执行添加Raft投票者操作
        return delegate.addRaftVoter(voterId, voterDirectoryId, endpoints, options);
    }

    /**
     * 移除Raft投票者
     * 
     * @param voterId 投票者ID
     * @param voterDirectoryId 投票者目录ID
     * @param options 移除投票者的选项
     * @return 返回移除Raft投票者的操作结果
     */
    @Override
    public RemoveRaftVoterResult removeRaftVoter(int voterId, Uuid voterDirectoryId, RemoveRaftVoterOptions options) {
        // 委托给实际的Admin实例执行移除Raft投票者操作
        return delegate.removeRaftVoter(voterId, voterDirectoryId, options);
    }

    /**
     * 描述共享组的详细信息
     * 
     * @param groupIds 要描述的共享组ID集合
     * @param options 描述共享组的选项
     * @return 返回共享组描述的操作结果
     */
    @Override
    public DescribeShareGroupsResult describeShareGroups(Collection<String> groupIds, DescribeShareGroupsOptions options) {
        // 委托给实际的Admin实例执行描述共享组操作
        return delegate.describeShareGroups(groupIds, options);
    }

    /**
     * 列出共享组的偏移量信息
     * 
     * @param groupSpecs 共享组规范的映射
     * @param options 列出偏移量的选项
     * @return 返回共享组偏移量的操作结果
     */
    @Override
    public ListShareGroupOffsetsResult listShareGroupOffsets(Map<String, ListShareGroupOffsetsSpec> groupSpecs, ListShareGroupOffsetsOptions options) {
        // 委托给实际的Admin实例执行列出共享组偏移量操作
        return delegate.listShareGroupOffsets(groupSpecs, options);
    }

    /**
     * 列出所有组
     * 
     * @param options 列出组的选项
     * @return 返回组列表的操作结果
     */
    @Override
    public ListGroupsResult listGroups(ListGroupsOptions options) {
        // 委托给实际的Admin实例执行列出组操作
        return delegate.listGroups(options);
    }

    /**
     * 描述经典消费者组的详细信息
     * 
     * @param groupIds 要描述的经典组ID集合
     * @param options 描述经典组的选项
     * @return 返回经典组描述的操作结果
     */
    @Override
    public DescribeClassicGroupsResult describeClassicGroups(Collection<String> groupIds, DescribeClassicGroupsOptions options) {
        // 委托给实际的Admin实例执行描述经典组操作
        return delegate.describeClassicGroups(groupIds, options);
    }

    /**
     * 注册指标订阅
     * 注意：此操作当前不支持
     * 
     * @param metric 要注册的Kafka指标
     * @throws UnsupportedOperationException 当尝试执行此操作时总是抛出此异常
     */
    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        // 抛出不支持操作异常，表明此功能未实现
        throw new UnsupportedOperationException();
    }

    /**
     * 取消注册指标订阅
     * 注意：此操作当前不支持
     * 
     * @param metric 要取消注册的Kafka指标
     * @throws UnsupportedOperationException 当尝试执行此操作时总是抛出此异常
     */
    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        // 抛出不支持操作异常，表明此功能未实现
        throw new UnsupportedOperationException();
    }

    /**
     * 获取所有注册的指标
     * 
     * @return 返回指标名称到指标实例的映射
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        // 委托给实际的Admin实例执行获取指标操作
        return delegate.metrics();
    }
}
