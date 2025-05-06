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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.DefaultHostResolver;
import org.apache.kafka.clients.HostResolver;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.LeastLoadedNode;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResult;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec.TimestampSpec;
import org.apache.kafka.clients.admin.internals.AbortTransactionHandler;
import org.apache.kafka.clients.admin.internals.AdminApiDriver;
import org.apache.kafka.clients.admin.internals.AdminApiFuture;
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture;
import org.apache.kafka.clients.admin.internals.AdminApiHandler;
import org.apache.kafka.clients.admin.internals.AdminBootstrapAddresses;
import org.apache.kafka.clients.admin.internals.AdminFetchMetricsManager;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.apache.kafka.clients.admin.internals.AllBrokersStrategy;
import org.apache.kafka.clients.admin.internals.AlterConsumerGroupOffsetsHandler;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.admin.internals.DeleteConsumerGroupOffsetsHandler;
import org.apache.kafka.clients.admin.internals.DeleteConsumerGroupsHandler;
import org.apache.kafka.clients.admin.internals.DeleteRecordsHandler;
import org.apache.kafka.clients.admin.internals.DescribeClassicGroupsHandler;
import org.apache.kafka.clients.admin.internals.DescribeConsumerGroupsHandler;
import org.apache.kafka.clients.admin.internals.DescribeProducersHandler;
import org.apache.kafka.clients.admin.internals.DescribeShareGroupsHandler;
import org.apache.kafka.clients.admin.internals.DescribeTransactionsHandler;
import org.apache.kafka.clients.admin.internals.FenceProducersHandler;
import org.apache.kafka.clients.admin.internals.ListConsumerGroupOffsetsHandler;
import org.apache.kafka.clients.admin.internals.ListOffsetsHandler;
import org.apache.kafka.clients.admin.internals.ListShareGroupOffsetsHandler;
import org.apache.kafka.clients.admin.internals.ListTransactionsHandler;
import org.apache.kafka.clients.admin.internals.PartitionLeaderStrategy;
import org.apache.kafka.clients.admin.internals.RemoveMembersFromConsumerGroupHandler;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicCollection.TopicIdCollection;
import org.apache.kafka.common.TopicCollection.TopicNameCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.MismatchedEndpointTypeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnacceptableCredentialException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedEndpointTypeException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDir;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData.AclCreation;
import org.apache.kafka.common.message.CreateAclsResponseData.AclCreationResult;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData.CreatableRenewers;
import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopicCollection;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicConfigs;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData.DeleteAclsFilter;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult;
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsMatchingAcl;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData.DescribableLogDirTopic;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData.TopicRequest;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData.UserName;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.ListClientMetricsResourcesRequestData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.UnregisterBrokerRequestData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateFeaturesResponseData.UpdatableFeatureResult;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddRaftVoterRequest;
import org.apache.kafka.common.requests.AddRaftVoterResponse;
import org.apache.kafka.common.requests.AlterClientQuotasRequest;
import org.apache.kafka.common.requests.AlterClientQuotasResponse;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.AlterReplicaLogDirsRequest;
import org.apache.kafka.common.requests.AlterReplicaLogDirsResponse;
import org.apache.kafka.common.requests.AlterUserScramCredentialsRequest;
import org.apache.kafka.common.requests.AlterUserScramCredentialsResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreateAclsResponse;
import org.apache.kafka.common.requests.CreateDelegationTokenRequest;
import org.apache.kafka.common.requests.CreateDelegationTokenResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeClientQuotasRequest;
import org.apache.kafka.common.requests.DescribeClientQuotasResponse;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeDelegationTokenRequest;
import org.apache.kafka.common.requests.DescribeDelegationTokenResponse;
import org.apache.kafka.common.requests.DescribeLogDirsRequest;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.requests.DescribeQuorumRequest;
import org.apache.kafka.common.requests.DescribeQuorumRequest.Builder;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest;
import org.apache.kafka.common.requests.DescribeTopicPartitionsResponse;
import org.apache.kafka.common.requests.DescribeUserScramCredentialsRequest;
import org.apache.kafka.common.requests.DescribeUserScramCredentialsResponse;
import org.apache.kafka.common.requests.ElectLeadersRequest;
import org.apache.kafka.common.requests.ElectLeadersResponse;
import org.apache.kafka.common.requests.ExpireDelegationTokenRequest;
import org.apache.kafka.common.requests.ExpireDelegationTokenResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.ListClientMetricsResourcesRequest;
import org.apache.kafka.common.requests.ListClientMetricsResourcesResponse;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.ListPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RemoveRaftVoterRequest;
import org.apache.kafka.common.requests.RemoveRaftVoterResponse;
import org.apache.kafka.common.requests.RenewDelegationTokenRequest;
import org.apache.kafka.common.requests.RenewDelegationTokenResponse;
import org.apache.kafka.common.requests.UnregisterBrokerRequest;
import org.apache.kafka.common.requests.UnregisterBrokerResponse;
import org.apache.kafka.common.requests.UpdateFeaturesRequest;
import org.apache.kafka.common.requests.UpdateFeaturesResponse;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.admin.internals.AdminUtils.validAclOperations;
import static org.apache.kafka.common.internals.Topic.CLUSTER_METADATA_TOPIC_NAME;
import static org.apache.kafka.common.internals.Topic.CLUSTER_METADATA_TOPIC_PARTITION;
import static org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition;
import static org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse;
import static org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse;
import static org.apache.kafka.common.message.ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics;
import static org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingPartitionReassignment;
import static org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingTopicReassignment;
import static org.apache.kafka.common.requests.MetadataRequest.convertToMetadataRequestTopic;
import static org.apache.kafka.common.requests.MetadataRequest.convertTopicIdsToMetadataRequestTopic;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * Admin接口的默认实现类。该类的实例通过调用AdminClient中的create()方法创建。
 * 用户不应该直接引用这个类。
 *
 * <p>
 * 这个类是线程安全的。
 * </p>
 * 该类的API仍在演进中，详见Admin接口的说明。
 */
@InterfaceStability.Evolving
public class KafkaAdminClient extends AdminClient {

    /**
     * 用于生成KafkaAdminClient实例名称的序列号
     * 当用户没有显式指定客户端名称时使用此序列号生成默认名称
     */
    private static final AtomicInteger ADMIN_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * 用于JMX指标的前缀
     * 所有与该类相关的JMX指标都将使用此前缀
     */
    private static final String JMX_PREFIX = "kafka.admin.client";

    /**
     * 表示尚未执行关闭操作的无效关闭时间
     * 用于初始化关闭时间戳
     */
    private static final long INVALID_SHUTDOWN_TIME = -1;

    /**
     * LeaveGroupRequest的默认原因说明
     * 当管理员移除消费者组成员时使用此默认说明
     */
    static final String DEFAULT_LEAVE_GROUP_REASON = "member was removed by an admin";

    /**
     * 管理客户端网络线程的名称前缀
     * 用于标识属于管理客户端的网络线程
     */
    static final String NETWORK_THREAD_PREFIX = "kafka-admin-client-thread";

    private final Logger log;                      // 日志记录器
    private final LogContext logContext;           // 日志上下文

    /**
     * 操作的默认超时时间（毫秒）
     * 用于控制管理操作的执行时长
     */
    private final int defaultApiTimeoutMs;

    /**
     * 单个请求的超时时间（毫秒）
     * 用于控制单个网络请求的执行时长
     */
    private final int requestTimeoutMs;

    /**
     * 当前AdminClient实例的名称
     * 用于标识和区分不同的管理客户端实例
     */
    private final String clientId;

    /**
     * 时间提供者
     * 用于获取系统时间，便于测试和时间控制
     */
    private final Time time;

    /**
     * 集群元数据管理器
     * 用于管理和维护Kafka集群的元数据信息
     */
    private final AdminMetadataManager metadataManager;

    /**
     * 当前KafkaAdminClient的度量指标
     * 用于监控和统计客户端的各项性能指标
     */
    final Metrics metrics;

    /**
     * 网络客户端实例
     * 用于处理与Kafka集群的网络通信
     */
    private final KafkaClient client;

    /**
     * 管理客户端服务线程中使用的运行对象
     * 处理异步操作和回调
     */
    private final AdminClientRunnable runnable;

    /**
     * 管理客户端的网络服务线程
     * 负责执行网络通信和请求处理
     */
    private final Thread thread;

    /**
     * 关闭操作的强制超时时间
     * 在关闭操作期间，这是我们将超时所有待处理操作并强制RPC线程退出的时间
     * 如果管理客户端未在关闭过程中，该值为0
     */
    private final AtomicLong hardShutdownTimeMs = new AtomicLong(INVALID_SHUTDOWN_TIME);

    /**
     * 超时处理器工厂
     * 用于为RPC线程创建超时处理器
     */
    private final TimeoutProcessorFactory timeoutProcessorFactory;

    // 重试相关配置
    private final int maxRetries;                  // 最大重试次数
    private final long retryBackoffMs;            // 重试退避时间（毫秒）
    private final long retryBackoffMaxMs;         // 最大重试退避时间（毫秒）
    private final ExponentialBackoff retryBackoff; // 指数退避策略
    private final MetadataRecoveryStrategy metadataRecoveryStrategy; // 元数据恢复策略
    private final Map<TopicPartition, Integer> partitionLeaderCache; // 分区领导者缓存
    private final AdminFetchMetricsManager adminFetchMetricsManager; // 管理端获取指标管理器
    private final Optional<ClientTelemetryReporter> clientTelemetryReporter; // 客户端遥测报告器

    /**
     * 遥测请求的客户端实例ID
     * 用于唯一标识遥测请求的来源
     */
    private Uuid clientInstanceId;

    /**
     * Get or create a list value from a map.
     *
     * @param map   The map to get or create the element from.
     * @param key   The key.
     * @param <K>   The key type.
     * @param <V>   The value type.
     * @return      The list value.
     */
    /**
     * 从Map中获取或创建一个List值
     * 如果指定key的List不存在，则创建一个新的LinkedList
     *
     * @param map   要获取或创建元素的Map
     * @param key   键值
     * @param <K>   键类型
     * @param <V>   值类型
     * @return      返回与key关联的List，如果不存在则创建新的
     */
    static <K, V> List<V> getOrCreateListValue(Map<K, List<V>> map, K key) {
        // 使用computeIfAbsent方法，如果key不存在，则创建新的LinkedList
        return map.computeIfAbsent(key, k -> new LinkedList<>());
    }

    /**
     * Send an exception to every element in a collection of KafkaFutureImpls.
     *
     * @param futures   The collection of KafkaFutureImpl objects.
     * @param exc       The exception
     * @param <T>       The KafkaFutureImpl result type.
     */
    /**
     * 将异常发送给KafkaFutureImpl集合中的每个元素
     * 用于批量处理异常情况
     *
     * @param futures   KafkaFutureImpl对象的集合
     * @param exc       要设置的异常
     * @param <T>       KafkaFutureImpl的结果类型
     */
    private static <T> void completeAllExceptionally(Collection<KafkaFutureImpl<T>> futures, Throwable exc) {
        // 将集合转换为流，然后调用流版本的方法处理
        completeAllExceptionally(futures.stream(), exc);
    }

    /**
     * Send an exception to all futures in the provided stream
     *
     * @param futures   The stream of KafkaFutureImpl objects.
     * @param exc       The exception
     * @param <T>       The KafkaFutureImpl result type.
     */
    /**
     * 将异常发送给提供的流中的所有futures
     * 支持流式处理的异常设置
     *
     * @param futures   KafkaFutureImpl对象的流
     * @param exc       要设置的异常
     * @param <T>       KafkaFutureImpl的结果类型
     */
    private static <T> void completeAllExceptionally(Stream<KafkaFutureImpl<T>> futures, Throwable exc) {
        // 对流中的每个future执行completeExceptionally操作
        futures.forEach(future -> future.completeExceptionally(exc));
    }

    /**
     * Get the current time remaining before a deadline as an integer.
     *
     * @param now           The current time in milliseconds.
     * @param deadlineMs    The deadline time in milliseconds.
     * @return              The time delta in milliseconds.
     */
    /**
     * 计算截止时间前的剩余时间（毫秒）
     * 处理超出整数范围的情况，确保返回有效的整数值
     *
     * @param now           当前时间（毫秒）
     * @param deadlineMs    截止时间（毫秒）
     * @return              剩余时间（毫秒），已转换为整数类型
     */
    static int calcTimeoutMsRemainingAsInt(long now, long deadlineMs) {
        // 计算时间差
        long deltaMs = deadlineMs - now;
        // 处理超出整数范围的情况
        if (deltaMs > Integer.MAX_VALUE)
            deltaMs = Integer.MAX_VALUE;
        else if (deltaMs < Integer.MIN_VALUE)
            deltaMs = Integer.MIN_VALUE;
        // 转换为整数返回
        return (int) deltaMs;
    }

    /**
     * Generate the client id based on the configuration.
     *
     * @param config    The configuration
     *
     * @return          The client id
     */
    /**
     * 根据配置生成客户端ID
     * 如果配置中未指定，则生成默认的ID
     *
     * @param config    AdminClient配置对象
     * @return          生成的客户端ID
     */
    static String generateClientId(AdminClientConfig config) {
        // 从配置中获取客户端ID
        String clientId = config.getString(AdminClientConfig.CLIENT_ID_CONFIG);
        // 如果配置中有指定ID，则直接返回
        if (!clientId.isEmpty())
            return clientId;
        // 否则生成默认的ID，格式为"adminclient-序号"
        return "adminclient-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement();
    }

    /**
     * 获取客户端ID
     * 
     * @return 返回当前客户端的ID
     */
    String getClientId() {
        return clientId;
    }

    /**
     * Get the deadline for a particular call.
     *
     * @param now               The current time in milliseconds.
     * @param optionTimeoutMs   The timeout option given by the user.
     *
     * @return                  The deadline in milliseconds.
     */
    /**
     * 计算特定调用的截止时间
     * 根据用户提供的超时选项或默认超时时间计算
     *
     * @param now               当前时间（毫秒）
     * @param optionTimeoutMs   用户指定的超时选项（毫秒）
     * @return                  计算得到的截止时间（毫秒）
     */
    private long calcDeadlineMs(long now, Integer optionTimeoutMs) {
        // 如果提供了超时选项，使用该选项计算截止时间
        if (optionTimeoutMs != null)
            return now + Math.max(0, optionTimeoutMs);
        // 否则使用默认的API超时时间
        return now + defaultApiTimeoutMs;
    }

    /**
     * 格式化异常信息为简洁的可读字符串
     * 
     * @param throwable 需要格式化的异常对象
     * @return 格式化后的异常信息字符串
     */
    static String prettyPrintException(Throwable throwable) {
        // 如果异常对象为空，返回空异常提示
        if (throwable == null)
            return "Null exception.";
        // 如果异常包含错误信息，返回异常类名和错误信息
        if (throwable.getMessage() != null) {
            return throwable.getClass().getSimpleName() + ": " + throwable.getMessage();
        }
        // 如果异常不包含错误信息，只返回异常类名
        return throwable.getClass().getSimpleName();
    }

    /**
     * 创建KafkaAdminClient实例的内部方法
     * 
     * @param config 管理客户端配置
     * @param timeoutProcessorFactory 超时处理器工厂
     * @return 新创建的KafkaAdminClient实例
     */
    static KafkaAdminClient createInternal(AdminClientConfig config, TimeoutProcessorFactory timeoutProcessorFactory) {
        // 调用重载方法创建客户端，使用默认的主机解析器
        return createInternal(config, timeoutProcessorFactory, null);
    }

    /**
     * 创建KafkaAdminClient实例的核心内部方法
     * 
     * @param config 管理客户端配置
     * @param timeoutProcessorFactory 超时处理器工厂
     * @param hostResolver 主机名解析器，用于解析broker地址
     * @return 新创建的KafkaAdminClient实例
     */
    static KafkaAdminClient createInternal(
        AdminClientConfig config,
        TimeoutProcessorFactory timeoutProcessorFactory,
        HostResolver hostResolver
    ) {
        // 初始化度量指标和网络客户端对象
        Metrics metrics = null;
        NetworkClient networkClient = null;
        // 使用系统时间
        Time time = Time.SYSTEM;
        // 生成客户端唯一标识符
        String clientId = generateClientId(config);
        // 创建API版本管理器
        ApiVersions apiVersions = new ApiVersions();
        // 创建日志上下文
        LogContext logContext = createLogContext(clientId);
        // 声明遥测报告器
        Optional<ClientTelemetryReporter> clientTelemetryReporter;

        try {
            // 从配置中获取bootstrap地址信息
            // 由于只请求节点信息，允许自动创建主题是安全的(这也简化了与旧版本broker的通信)
            AdminBootstrapAddresses adminAddresses = AdminBootstrapAddresses.fromConfig(config);
            // 创建元数据管理器，负责管理集群元数据
            AdminMetadataManager metadataManager = new AdminMetadataManager(logContext,
                config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG),
                adminAddresses.usingBootstrapControllers());
            // 使用bootstrap地址更新集群元数据
            metadataManager.update(Cluster.bootstrap(adminAddresses.addresses()), time.milliseconds());
            
            // 配置度量指标系统
            // 创建度量指标报告器列表
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            // 创建遥测报告器
            clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);
            // 如果存在遥测报告器，添加到reporters列表
            clientTelemetryReporter.ifPresent(reporters::add);
            // 设置度量指标标签
            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            // 配置度量指标参数
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(AdminClientConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(AdminClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .recordLevel(Sensor.RecordingLevel.forName(config.getString(AdminClientConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                .tags(metricTags);
            // 创建度量指标上下文
            MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX,
                    config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
            // 创建度量指标系统实例
            metrics = new Metrics(metricConfig, reporters, time, metricsContext);
            
            // 创建网络客户端
            networkClient = ClientUtils.createNetworkClient(config,
                clientId,
                metrics,
                "admin-client",
                logContext,
                apiVersions,
                time,
                1, // 最大连接数
                (int) TimeUnit.HOURS.toMillis(1), // 连接最大空闲时间
                null, // 不使用内存池
                metadataManager.updater(),
                (hostResolver == null) ? new DefaultHostResolver() : hostResolver,
                null, // 不使用安全协议
                clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null));
            
            // 创建并返回KafkaAdminClient实例
            return new KafkaAdminClient(config, clientId, time, metadataManager, metrics, networkClient,
                timeoutProcessorFactory, logContext, clientTelemetryReporter);
        } catch (Throwable exc) {
            // 发生异常时，安全关闭已创建的资源
            closeQuietly(metrics, "Metrics");
            closeQuietly(networkClient, "NetworkClient");
            throw new KafkaException("Failed to create new KafkaAdminClient", exc);
        }
    }

    /**
     * 用于测试的KafkaAdminClient创建方法
     * 允许传入模拟的元数据管理器和客户端，便于单元测试
     * 
     * @param config 管理客户端配置
     * @param metadataManager 元数据管理器
     * @param client Kafka客户端
     * @param time 时间实例
     * @return 新创建的KafkaAdminClient实例
     */
    // Visible for tests
    static KafkaAdminClient createInternal(AdminClientConfig config,
                                           AdminMetadataManager metadataManager,
                                           KafkaClient client,
                                           Time time) {
        // 初始化度量指标对象
        Metrics metrics = null;
        // 生成客户端ID
        String clientId = generateClientId(config);
        // 创建遥测报告器
        Optional<ClientTelemetryReporter> clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);

        try {
            // 创建简单的度量指标系统，用于测试
            metrics = new Metrics(new MetricConfig(), new LinkedList<>(), time);
            // 创建日志上下文
            LogContext logContext = createLogContext(clientId);
            // 创建并返回AdminClient实例，使用传入的模拟组件
            return new KafkaAdminClient(config, clientId, time, metadataManager, metrics,
                client, null, logContext, clientTelemetryReporter);
        } catch (Throwable exc) {
            // 发生异常时，安全关闭度量指标系统
            closeQuietly(metrics, "Metrics");
            throw new KafkaException("Failed to create new KafkaAdminClient", exc);
        }
    }

    /**
     * 创建日志上下文
     * 用于生成带有客户端ID标识的日志前缀
     * 
     * @param clientId 客户端ID
     * @return 配置好的日志上下文对象
     */
    static LogContext createLogContext(String clientId) {
        // 创建带有客户端标识的日志上下文
        return new LogContext("[AdminClient clientId=" + clientId + "] ");
    }

    /**
     * KafkaAdminClient的私有构造函数
     * 初始化所有必要的组件和配置
     * 
     * @param config 管理客户端配置
     * @param clientId 客户端唯一标识符
     * @param time 时间实例
     * @param metadataManager 元数据管理器
     * @param metrics 度量指标系统
     * @param client Kafka网络客户端
     * @param timeoutProcessorFactory 超时处理器工厂
     * @param logContext 日志上下文
     * @param clientTelemetryReporter 遥测报告器
     */
    private KafkaAdminClient(AdminClientConfig config,
                             String clientId,
                             Time time,
                             AdminMetadataManager metadataManager,
                             Metrics metrics,
                             KafkaClient client,
                             TimeoutProcessorFactory timeoutProcessorFactory,
                             LogContext logContext,
                             Optional<ClientTelemetryReporter> clientTelemetryReporter) {
        // 初始化基本属性
        this.clientId = clientId;
        this.log = logContext.logger(KafkaAdminClient.class);
        this.logContext = logContext;
        
        // 配置超时参数
        this.requestTimeoutMs = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.defaultApiTimeoutMs = configureDefaultApiTimeoutMs(config);
        
        // 设置核心组件
        this.time = time;
        this.metadataManager = metadataManager;
        this.metrics = metrics;
        this.client = client;
        
        // 初始化网络线程
        this.runnable = new AdminClientRunnable();
        String threadName = NETWORK_THREAD_PREFIX + " | " + clientId;
        this.thread = new KafkaThread(threadName, runnable, true);
        
        // 配置超时处理器
        this.timeoutProcessorFactory = (timeoutProcessorFactory == null) ?
            new TimeoutProcessorFactory() : timeoutProcessorFactory;
        
        // 配置重试机制
        this.maxRetries = config.getInt(AdminClientConfig.RETRIES_CONFIG);
        this.retryBackoffMs = config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG);
        this.retryBackoffMaxMs = config.getLong(AdminClientConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        // 创建指数退避重试策略
        this.retryBackoff = new ExponentialBackoff(
            retryBackoffMs,
            CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
            retryBackoffMaxMs,
            CommonClientConfigs.RETRY_BACKOFF_JITTER);
        
        // 配置度量指标和遥测报告
        List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(this.clientId, config);
        this.clientTelemetryReporter = clientTelemetryReporter;
        this.clientTelemetryReporter.ifPresent(reporters::add);
        
        // 配置元数据恢复策略
        this.metadataRecoveryStrategy = MetadataRecoveryStrategy.forName(config.getString(AdminClientConfig.METADATA_RECOVERY_STRATEGY_CONFIG));
        
        // 初始化分区leader缓存
        this.partitionLeaderCache = new HashMap<>();
        // 创建管理员获取度量指标管理器
        this.adminFetchMetricsManager = new AdminFetchMetricsManager(metrics);
        
        // 记录未使用的配置并注册JMX信息
        config.logUnused();
        AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
        
        // 启动网络线程
        log.debug("Kafka admin client initialized");
        thread.start();
    }

    /**
     * 配置默认的API超时时间。
     * 
     * 处理逻辑：
     * 1. 如果显式指定了default.api.timeout.ms，且其值小于request.timeout.ms，则抛出异常
     * 2. 如果未配置default.api.timeout.ms，则使用request.timeout.ms的值，并记录警告日志
     * 3. 其他情况下使用配置文件中指定的值
     *
     * @param config 管理客户端配置对象
     * @return 最终确定的默认API超时时间(毫秒)
     */
    private int configureDefaultApiTimeoutMs(AdminClientConfig config) {
        // 获取请求超时时间配置
        int requestTimeoutMs = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
        // 获取默认API超时时间配置
        int defaultApiTimeoutMs = config.getInt(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);

        // 如果默认API超时时间小于请求超时时间
        if (defaultApiTimeoutMs < requestTimeoutMs) {
            // 检查是否显式配置了默认API超时时间
            if (config.originals().containsKey(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)) {
                // 如果是显式配置，则抛出配置异常
                throw new ConfigException("The specified value of " + AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG +
                        " must be no smaller than the value of " + AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG + ".");
            } else {
                // 如果是默认配置，则使用请求超时时间作为默认API超时时间，并记录警告日志
                log.warn("Overriding the default value for {} ({}) with the explicitly configured request timeout {}",
                        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, this.defaultApiTimeoutMs,
                        requestTimeoutMs);
                return requestTimeoutMs;
            }
        }
        return defaultApiTimeoutMs;
    }

    /**
     * 关闭AdminClient，实现优雅关闭机制
     * 
     * 关闭流程：
     * 1. 验证并调整超时时间
     * 2. 关闭遥测报告器和度量指标
     * 3. 设置硬关闭时间并唤醒客户端线程
     * 4. 等待I/O线程退出
     * 
     * @param timeout 等待关闭完成的最大时间
     */
    @Override
    public void close(Duration timeout) {
        // 将Duration转换为毫秒，并进行参数验证
        long waitTimeMs = timeout.toMillis();
        if (waitTimeMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        // 限制最大等待时间为1年
        waitTimeMs = Math.min(TimeUnit.DAYS.toMillis(365), waitTimeMs);
        
        // 计算新的硬关闭时间点
        long now = time.milliseconds();
        long newHardShutdownTimeMs = now + waitTimeMs;
        long prev = INVALID_SHUTDOWN_TIME;
        
        // 关闭遥测报告器和度量指标
        clientTelemetryReporter.ifPresent(ClientTelemetryReporter::initiateClose);
        metrics.close();
        
        // 使用CAS操作设置硬关闭时间
        while (true) {
            if (hardShutdownTimeMs.compareAndSet(prev, newHardShutdownTimeMs)) {
                if (prev == INVALID_SHUTDOWN_TIME) {
                    log.debug("Initiating close operation.");
                } else {
                    log.debug("Moving hard shutdown time forward.");
                }
                // 唤醒可能在poll()中阻塞的线程
                client.wakeup();
                break;
            }
            prev = hardShutdownTimeMs.get();
            // 如果已存在更早的关闭时间，则使用较早的时间
            if (prev < newHardShutdownTimeMs) {
                log.debug("Hard shutdown time is already earlier than requested.");
                newHardShutdownTimeMs = prev;
                break;
            }
        }
        
        // 记录等待关闭的时间
        if (log.isDebugEnabled()) {
            long deltaMs = Math.max(0, newHardShutdownTimeMs - time.milliseconds());
            log.debug("Waiting for the I/O thread to exit. Hard shutdown in {} ms.", deltaMs);
        }
        
        try {
            // 避免死锁：如果当前线程是AdminClient线程，则不等待自身结束
            if (Thread.currentThread() != thread) {
                // 等待I/O线程结束
                thread.join(waitTimeMs);
            }
            log.debug("Kafka admin client closed.");
        } catch (InterruptedException e) {
            log.debug("Interrupted while joining I/O thread", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 节点提供者接口，用于为API调用提供目标节点
     */
    private interface NodeProvider {
        /**
         * 提供一个可用的Kafka节点
         * @return 目标节点，如果无可用节点则返回null
         */
        Node provide();
        
        /**
         * 判断是否支持使用控制器
         * @return true表示支持使用控制器节点
         */
        boolean supportsUseControllers();
    }

    /**
     * 基于元数据更新的节点提供者实现
     * 通过负载均衡选择最少连接的节点，支持元数据重启动机制
     */
    private class MetadataUpdateNodeIdProvider implements NodeProvider {
        @Override
        public Node provide() {
            long now = time.milliseconds();
            // 获取负载最小的节点
            LeastLoadedNode leastLoadedNode = client.leastLoadedNode(now);
            // 如果使用重启动策略且节点不可用，则重新引导元数据
            if (metadataRecoveryStrategy == MetadataRecoveryStrategy.REBOOTSTRAP
                    && !leastLoadedNode.hasNodeAvailableOrConnectionReady()) {
                metadataManager.rebootstrap(now);
            }

            return leastLoadedNode.node();
        }

        @Override
        public boolean supportsUseControllers() {
            return true;
        }
    }

    /**
     * 固定节点ID的节点提供者实现
     * 用于需要与特定节点通信的场景，如与控制器节点通信
     */
    private class ConstantNodeIdProvider implements NodeProvider {
        private final int nodeId; // 目标节点ID
        private final boolean supportsUseControllers; // 是否支持使用控制器

        /**
         * 创建支持控制器的固定节点提供者
         * @param nodeId 目标节点ID
         * @param supportsUseControllers 是否支持使用控制器
         */
        ConstantNodeIdProvider(int nodeId, boolean supportsUseControllers) {
            this.nodeId = nodeId;
            this.supportsUseControllers = supportsUseControllers;
        }

        /**
         * 创建不支持控制器的固定节点提供者
         * @param nodeId 目标节点ID
         */
        ConstantNodeIdProvider(int nodeId) {
            this.nodeId = nodeId;
            this.supportsUseControllers = false;
        }

        @Override
        public Node provide() {
            // 如果元数据已就绪且能找到指定ID的节点，则返回该节点
            if (metadataManager.isReady() &&
                    (metadataManager.nodeById(nodeId) != null)) {
                return metadataManager.nodeById(nodeId);
            }
            // 如果找不到指定ID的节点，请求更新元数据
            // 这在集群启动时特别有用，因为并非所有节点都立即可用
            metadataManager.requestUpdate();
            return null;
        }

        @Override
        public boolean supportsUseControllers() {
            return supportsUseControllers;
        }
    }

    /**
     * 提供控制器节点的实现类。
     * 该类负责获取Kafka集群中的控制器节点信息，用于需要与控制器交互的管理操作。
     */
    private class ControllerNodeProvider implements NodeProvider {
        /**
         * 标识是否支持使用控制器。
         * true表示支持使用控制器进行管理操作，false表示不支持。
         */
        private final boolean supportsUseControllers;

        /**
         * 构造函数，指定是否支持使用控制器。
         * 
         * @param supportsUseControllers 是否支持使用控制器的标志
         */
        ControllerNodeProvider(boolean supportsUseControllers) {
            this.supportsUseControllers = supportsUseControllers;
        }

        /**
         * 默认构造函数，默认不支持使用控制器。
         */
        ControllerNodeProvider() {
            this.supportsUseControllers = false;
        }

        /**
         * 提供控制器节点。
         * 如果元数据已就绪且控制器节点存在，则返回控制器节点；
         * 否则请求更新元数据并返回null。
         *
         * @return 控制器节点，如果不可用则返回null
         */
        @Override
        public Node provide() {
            // 检查元数据是否就绪且控制器节点存在
            if (metadataManager.isReady() &&
                    (metadataManager.controller() != null)) {
                // 返回当前的控制器节点
                return metadataManager.controller();
            }
            // 请求更新元数据
            metadataManager.requestUpdate();
            return null;
        }

        /**
         * 返回是否支持使用控制器。
         *
         * @return true表示支持使用控制器，false表示不支持
         */
        @Override
        public boolean supportsUseControllers() {
            return supportsUseControllers;
        }
    }

    /**
     * 提供负载最小的节点的实现类。
     * 该类负责选择集群中当前负载最小的broker节点，用于负载均衡。
     */
    private class LeastLoadedNodeProvider implements NodeProvider {
        /**
         * 提供负载最小的节点。
         * 如果元数据已就绪，则返回当前负载最小的节点；
         * 否则请求更新元数据并返回null。
         *
         * @return 负载最小的节点，如果所有节点都忙或元数据未就绪则返回null
         */
        @Override
        public Node provide() {
            // 检查元数据是否就绪
            if (metadataManager.isReady()) {
                // 获取负载最小的节点，如果所有节点都忙则返回null
                // 在这种情况下，我们会推迟节点分配
                return client.leastLoadedNode(time.milliseconds()).node();
            }
            // 请求更新元数据
            metadataManager.requestUpdate();
            return null;
        }

        /**
         * 返回是否支持使用控制器。
         * 该实现类不支持使用控制器，因为它专注于负载均衡。
         *
         * @return 始终返回false，表示不支持使用控制器
         */
        @Override
        public boolean supportsUseControllers() {
            return false;
        }
    }

    /**
     * 提供固定broker节点或活动KRaft控制器的实现类。
     * 当使用bootstrap.controllers配置时，返回活动的KRaft控制器；
     * 否则返回指定ID的broker节点。
     */
    private class ConstantBrokerOrActiveKController implements NodeProvider {
        /**
         * 指定的节点ID
         */
        private final int nodeId;

        /**
         * 构造函数，指定要使用的节点ID。
         *
         * @param nodeId 要使用的broker节点ID
         */
        ConstantBrokerOrActiveKController(int nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * 提供节点。
         * 如果使用了bootstrap.controllers，则返回活动的KRaft控制器；
         * 否则返回指定ID的broker节点。
         *
         * @return 活动的KRaft控制器或指定ID的broker节点，如果不可用则返回null
         */
        @Override
        public Node provide() {
            // 检查元数据是否就绪
            if (metadataManager.isReady()) {
                // 如果使用了bootstrap.controllers，返回活动的KRaft控制器
                if (metadataManager.usingBootstrapControllers()) {
                    return metadataManager.controller();
                } 
                // 否则返回指定ID的broker节点
                else if (metadataManager.nodeById(nodeId) != null) {
                    return metadataManager.nodeById(nodeId);
                }
            }
            // 请求更新元数据
            metadataManager.requestUpdate();
            return null;
        }

        /**
         * 返回是否支持使用控制器。
         * 该实现类支持使用控制器，因为它可能需要与KRaft控制器交互。
         *
         * @return 始终返回true，表示支持使用控制器
         */
        @Override
        public boolean supportsUseControllers() {
            return true;
        }
    }

    /**
     * 提供负载最小的broker节点或活动KRaft控制器的实现类。
     * 当使用bootstrap.controllers配置时，返回活动的KRaft控制器；
     * 否则返回当前负载最小的broker节点。
     */
    private class LeastLoadedBrokerOrActiveKController implements NodeProvider {
        /**
         * 提供节点。
         * 如果使用了bootstrap.controllers，则返回活动的KRaft控制器；
         * 否则返回当前负载最小的broker节点。
         *
         * @return 活动的KRaft控制器或负载最小的broker节点，如果不可用则返回null
         */
        @Override
        public Node provide() {
            // 检查元数据是否就绪
            if (metadataManager.isReady()) {
                // 如果使用了bootstrap.controllers，返回活动的KRaft控制器
                if (metadataManager.usingBootstrapControllers()) {
                    return metadataManager.controller();
                } else {
                    // 否则返回负载最小的broker节点
                    // 如果所有节点都忙，可能返回null
                    // 在这种情况下，我们会推迟节点分配
                    return client.leastLoadedNode(time.milliseconds()).node();
                }
            }
            // 请求更新元数据
            metadataManager.requestUpdate();
            return null;
        }

        /**
         * 返回是否支持使用控制器。
         * 该实现类支持使用控制器，因为它可能需要与KRaft控制器交互。
         *
         * @return 始终返回true，表示支持使用控制器
         */
        @Override
        public boolean supportsUseControllers() {
            return true;
        }
    }

    /**
     * 抽象的调用类，用于处理各种管理操作的请求。
     * 该类提供了重试机制、超时处理和错误处理等基础功能。
     */
    abstract class Call {
        /**
         * 是否为内部调用
         */
        private final boolean internal;

        /**
         * 调用的名称，用于日志和调试
         */
        private final String callName;

        /**
         * 调用的截止时间（毫秒）
         */
        private final long deadlineMs;

        /**
         * 节点提供者，用于获取要发送请求的目标节点
         */
        private final NodeProvider nodeProvider;

        /**
         * 当前重试次数
         */
        protected int tries;

        /**
         * 当前正在使用的节点
         */
        private Node curNode = null;

        /**
         * 下一次允许重试的时间（毫秒）
         */
        private long nextAllowedTryMs;

        /**
         * 完整构造函数
         *
         * @param internal 是否为内部调用
         * @param callName 调用名称
         * @param nextAllowedTryMs 下次允许重试的时间
         * @param tries 当前重试次数
         * @param deadlineMs 截止时间
         * @param nodeProvider 节点提供者
         */
        Call(boolean internal,
             String callName,
             long nextAllowedTryMs,
             int tries,
             long deadlineMs,
             NodeProvider nodeProvider
        ) {
            this.internal = internal;
            this.callName = callName;
            this.nextAllowedTryMs = nextAllowedTryMs;
            this.tries = tries;
            this.deadlineMs = deadlineMs;
            this.nodeProvider = nodeProvider;
        }

        /**
         * 用于内部调用的构造函数
         */
        Call(boolean internal, String callName, long deadlineMs, NodeProvider nodeProvider) {
            this(internal, callName, 0, 0, deadlineMs, nodeProvider);
        }

        /**
         * 用于外部调用的构造函数
         */
        Call(String callName, long deadlineMs, NodeProvider nodeProvider) {
            this(false, callName, 0, 0, deadlineMs, nodeProvider);
        }

        /**
         * 用于重试的构造函数
         */
        Call(String callName, long nextAllowedTryMs, int tries, long deadlineMs, NodeProvider nodeProvider) {
            this(false, callName, nextAllowedTryMs, tries, deadlineMs, nodeProvider);
        }

        /**
         * 获取当前正在使用的节点
         *
         * @return 当前节点
         */
        protected Node curNode() {
            return curNode;
        }

        /**
         * 处理调用失败的情况。
         * 根据异常类型和已重试次数，决定是失败还是重试。
         * 在某些情况下打印堆栈跟踪很重要，因为这些信息在ApiVersionException对象中不一定会保留。
         *
         * @param now 当前时间（毫秒）
         * @param throwable 失败异常
         */
        final void fail(long now, Throwable throwable) {
            // 清理当前节点状态
            if (curNode != null) {
                runnable.nodeReadyDeadlines.remove(curNode);
                curNode = null;
            }

            // 如果管理客户端正在关闭，不能重试
            if (runnable.closing) {
                handleFailure(throwable);
                return;
            }

            // 处理不支持的API版本异常
            // 如果可以通过降级协议来重试，则不增加重试计数
            if ((throwable instanceof UnsupportedVersionException) &&
                     handleUnsupportedVersionException((UnsupportedVersionException) throwable)) {
                log.debug("{} attempting protocol downgrade and then retry.", this);
                runnable.pendingCalls.add(this);
                return;
            }

            // 计算下次重试时间
            nextAllowedTryMs = now + retryBackoff.backoff(tries++);

            // 检查是否超时
            if (calcTimeoutMsRemainingAsInt(now, deadlineMs) <= 0) {
                handleTimeoutFailure(now, throwable);
                return;
            }

            // 检查异常是否可重试
            if (!(throwable instanceof RetriableException)) {
                if (log.isDebugEnabled()) {
                    log.debug("{} failed with non-retriable exception after {} attempt(s)", this, tries,
                        new Exception(prettyPrintException(throwable)));
                }
                handleFailure(throwable);
                return;
            }

            // 检查是否超过最大重试次数
            if (tries > maxRetries) {
                handleTimeoutFailure(now, throwable);
                return;
            }

            // 记录重试日志
            if (log.isDebugEnabled()) {
                log.debug("{} failed: {}. Beginning retry #{}",
                    this, prettyPrintException(throwable), tries);
            }

            // 尝试重试
            maybeRetry(now, throwable);
        }

        /**
         * 将调用添加到待处理队列中进行重试
         *
         * @param now 当前时间（毫秒）
         * @param throwable 导致重试的异常
         */
        void maybeRetry(long now, Throwable throwable) {
            runnable.pendingCalls.add(this);
        }

        /**
         * 处理超时失败的情况
         * 根据异常类型进行不同的处理:
         * 1. 如果是TimeoutException直接处理
         * 2. 其他异常会被包装成TimeoutException再处理
         * 
         * @param now 当前时间戳
         * @param cause 导致超时的原因
         */
        private void handleTimeoutFailure(long now, Throwable cause) {
            if (log.isDebugEnabled()) {
                // 记录详细的超时信息,包括重试次数和异常堆栈
                log.debug("{} timed out at {} after {} attempt(s)", this, now, tries,
                    new Exception(prettyPrintException(cause)));
            }
            if (cause instanceof TimeoutException) {
                // 如果已经是超时异常,直接处理
                handleFailure(cause);
            } else {
                // 其他异常包装成超时异常再处理
                handleFailure(new TimeoutException(this + " timed out at " + now
                    + " after " + tries + " attempt(s)", cause));
            }
        }

        /**
         * 为当前调用创建请求构建器
         * 子类必须实现此方法来构建具体的请求
         *
         * @param timeoutMs 超时时间(毫秒)
         * @return 请求构建器
         */
        abstract AbstractRequest.Builder<?> createRequest(int timeoutMs);

        /**
         * 处理调用响应
         * 子类必须实现此方法来处理服务端的响应
         *
         * @param abstractResponse 服务端响应
         */
        abstract void handleResponse(AbstractResponse abstractResponse);

        /**
         * 处理失败情况
         * 当异常不可重试或发生超时时会调用此方法
         *
         * @param throwable 异常对象
         */
        abstract void handleFailure(Throwable throwable);

        /**
         * 处理不支持的API版本异常
         * 默认实现返回false,表示无法处理该异常
         * 子类可以覆盖此方法提供自定义的处理逻辑
         *
         * @param exception 不支持的API版本异常
         * @return true表示异常已处理; false表示无法处理
         */
        boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
            return false;
        }

        @Override
        public String toString() {
            return "Call(callName=" + callName + ", deadlineMs=" + deadlineMs +
                ", tries=" + tries + ", nextAllowedTryMs=" + nextAllowedTryMs + ")";
        }

        /**
         * 判断是否为内部调用
         * @return true表示是内部调用
         */
        public boolean isInternal() {
            return internal;
        }
    }

    /**
     * 超时处理器工厂类
     * 用于创建超时处理器实例
     */
    static class TimeoutProcessorFactory {
        TimeoutProcessor create(long now) {
            return new TimeoutProcessor(now);
        }
    }

    /**
     * 超时处理器
     * 负责检查和处理已超时的调用
     */
    static class TimeoutProcessor {
        /**
         * 当前时间戳(毫秒)
         */
        private final long now;

        /**
         * 距离下一次超时检查的毫秒数
         * 用于优化性能,避免过于频繁的超时检查
         */
        private int nextTimeoutMs;

        /**
         * 创建新的超时处理器
         *
         * @param now 当前时间戳(毫秒)
         */
        TimeoutProcessor(long now) {
            this.now = now;
            this.nextTimeoutMs = Integer.MAX_VALUE;
        }

        /**
         * 检查并处理已超时的调用
         * 1. 遍历所有调用检查是否超时
         * 2. 移除并标记失败的超时调用
         * 3. 更新下一次超时检查时间
         *
         * @param calls 待检查的调用集合
         * @param msg 超时错误消息
         * @return 超时的调用数量
         */
        int handleTimeouts(Collection<Call> calls, String msg) {
            int numTimedOut = 0;
            for (Iterator<Call> iter = calls.iterator(); iter.hasNext(); ) {
                Call call = iter.next();
                int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
                if (remainingMs < 0) {
                    // 调用已超时,标记失败并从集合中移除
                    call.fail(now, new TimeoutException(msg + " Call: " + call.callName));
                    iter.remove();
                    numTimedOut++;
                } else {
                    // 更新下一次超时检查时间
                    nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
                }
            }
            return numTimedOut;
        }

        /**
         * 检查单个调用是否已超时
         * 同时更新下一次超时检查时间
         *
         * @param call 待检查的调用
         * @return true表示调用已超时
         */
        boolean callHasExpired(Call call) {
            int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
            if (remainingMs < 0)
                return true;
            nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
            return false;
        }

        /**
         * 获取距离下一次超时检查的毫秒数
         */
        int nextTimeoutMs() {
            return nextTimeoutMs;
        }
    }

    /**
     * AdminClient的核心运行线程类
     * 负责管理请求的生命周期,包括:
     * 1. 请求的分发和路由
     * 2. 超时处理
     * 3. 响应处理
     * 4. 失败重试
     */
    private final class AdminClientRunnable implements Runnable {
        /**
         * 尚未分配给节点的调用列表
         * 只能由当前线程访问,确保线程安全
         */
        private final ArrayList<Call> pendingCalls = new ArrayList<>();

        /**
         * 节点到待发送调用的映射
         * 记录每个节点上等待发送的调用列表
         * 只能由当前线程访问
         */
        private final Map<Node, List<Call>> callsToSend = new HashMap<>();

        /**
         * 节点ID到已发送调用的映射
         * 记录每个节点上正在处理的调用
         * 只能由当前线程访问
         */
        private final Map<String, Call> callsInFlight = new HashMap<>();

        /**
         * 关联ID到已发送调用的映射
         * 用于请求-响应的匹配
         * 只能由当前线程访问
         */
        private final Map<Integer, Call> correlationIdToCalls = new HashMap<>();

        /**
         * 新的待处理调用列表
         * 由对象监视器保护,支持多线程访问
         */
        private final List<Call> newCalls = new LinkedList<>();

        /**
         * 节点就绪期限映射
         * 记录每个节点的就绪截止时间
         * 当节点有待发送的调用且没有在途调用时,节点会出现在此映射中
         */
        private final Map<Node, Long> nodeReadyDeadlines = new HashMap<>();

        /**
         * AdminClient是否正在关闭
         * volatile保证多线程可见性
         */
        private volatile boolean closing = false;

        /**
         * 处理pendingCalls列表中已过期的调用
         * 将超时的调用标记为失败并从列表中移除
         *
         * @param processor 超时处理器
         */
        private void timeoutPendingCalls(TimeoutProcessor processor) {
            int numTimedOut = processor.handleTimeouts(pendingCalls, "Timed out waiting for a node assignment.");
            if (numTimedOut > 0)
                log.debug("Timed out {} pending calls.", numTimedOut);
        }

        /**
         * 处理已分配给节点但尚未发送的超时调用
         * 遍历所有节点的待发送调用列表,检查并处理超时的调用
         *
         * @param processor 超时处理器
         * @return 超时的调用总数
         */
        private int timeoutCallsToSend(TimeoutProcessor processor) {
            int numTimedOut = 0;
            for (List<Call> callList : callsToSend.values()) {
                numTimedOut += processor.handleTimeouts(callList,
                    "Timed out waiting to send the call.");
            }
            if (numTimedOut > 0)
                log.debug("Timed out {} call(s) with assigned nodes.", numTimedOut);
            return numTimedOut;
        }

        /**
         * 将所有新请求从newCalls转移到pendingCalls队列中。
         * 
         * 该函数持有锁的时间尽可能短，以避免阻塞其他需要添加新请求的AdminClient用户。
         * 这是请求生命周期的第一个阶段 - 从新建状态转移到待处理状态。
         */
        private synchronized void drainNewCalls() {
            transitionToPendingAndClearList(newCalls);
        }

        /**
         * 将请求添加到pendingCalls队列，然后清空输入列表。
         * 同时清除每个Call对象的curNode字段。
         * 
         * @param calls 要添加的请求列表
         */
        private void transitionToPendingAndClearList(List<Call> calls) {
            for (Call call : calls) {
                // 清除当前节点引用，准备重新分配
                call.curNode = null;
                // 将请求添加到待处理队列
                pendingCalls.add(call);
            }
            // 清空输入列表，避免重复处理
            calls.clear();
        }

        /**
         * 为pendingCalls列表中的请求选择目标节点。
         * 这是请求处理的关键步骤，实现了请求到节点的映射。
         *
         * @param now 当前时间戳(毫秒)
         * @return 如果有请求正在退避等待重试，返回最小的等待超时时间；否则返回Long.MAX_VALUE
         */
        private long maybeDrainPendingCalls(long now) {
            long pollTimeout = Long.MAX_VALUE;
            log.trace("Trying to choose nodes for {} at {}", pendingCalls, now);

            List<Call> toRemove = new ArrayList<>();
            // 在循环前获取pendingCalls的大小，避免无限循环
            // 因为如果call.fail持续往pendingCalls添加请求
            // 使用for (int i = 0; i < pendingCalls.size(); i++)这样的循环将无法停止
            int pendingSize = pendingCalls.size();
            // pendingCalls可能在循环中被修改
            // 因此使用普通for循环而不是迭代器，以避免ConcurrentModificationException
            for (int i = 0; i < pendingSize; i++) {
                Call call = pendingCalls.get(i);
                // 如果请求正在重试中，需要等待适当的退避时间后再尝试选择节点
                if (now < call.nextAllowedTryMs) {
                    pollTimeout = Math.min(pollTimeout, call.nextAllowedTryMs - now);
                } else if (maybeDrainPendingCall(call, now)) {
                    toRemove.add(call);
                }
            }

            // 使用remove而不是removeAll，避免删除所有匹配的元素
            for (Call call : toRemove) {
                pendingCalls.remove(call);
            }
            return pollTimeout;
        }

        /**
         * 检查是否可以为待处理的请求分配节点。
         * 如果请求被成功转移到callsToSend集合或请求失败，返回true；
         * 如果请求应该保持待处理状态，返回false。
         * 
         * 该方法实现了节点选择的核心逻辑，包括错误处理和状态转换。
         */
        private boolean maybeDrainPendingCall(Call call, long now) {
            try {
                // 通过节点提供器获取合适的目标节点
                Node node = call.nodeProvider.provide();
                if (node != null) {
                    log.trace("Assigned {} to node {}", call, node);
                    // 设置当前节点并将请求添加到发送队列
                    call.curNode = node;
                    getOrCreateListValue(callsToSend, node).add(call);
                    return true;
                } else {
                    log.trace("Unable to assign {} to a node.", call);
                    return false;
                }
            } catch (Throwable t) {
                // 处理节点选择过程中的认证错误
                log.debug("Unable to choose node for {}", call, t);
                call.fail(now, t);
                return true;
            }
        }

        /**
         * 发送已就绪的请求。
         * 这是请求生命周期的最后阶段，负责实际的网络通信。
         *
         * @param now 当前时间戳(毫秒)
         * @return 下一次poll操作需要的最小超时时间
         */
        private long sendEligibleCalls(long now) {
            long pollTimeout = Long.MAX_VALUE;
            for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry<Node, List<Call>> entry = iter.next();
                List<Call> calls = entry.getValue();
                if (calls.isEmpty()) {
                    iter.remove();
                    continue;
                }
                Node node = entry.getKey();
                // 检查节点是否有未完成的请求
                if (callsInFlight.containsKey(node.idString())) {
                    log.trace("Still waiting for other calls to finish on node {}.", node);
                    nodeReadyDeadlines.remove(node);
                    continue;
                }
                // 检查节点是否就绪
                if (!client.ready(node, now)) {
                    Long deadline = nodeReadyDeadlines.get(node);
                    if (deadline != null) {
                        if (now >= deadline) {
                            // 如果节点准备时间过长，断开连接并重新分配请求
                            log.info("Disconnecting from {} and revoking {} node assignment(s) " +
                                "because the node is taking too long to become ready.",
                                node.idString(), calls.size());
                            transitionToPendingAndClearList(calls);
                            client.disconnect(node.idString());
                            nodeReadyDeadlines.remove(node);
                            iter.remove();
                            continue;
                        }
                        pollTimeout = Math.min(pollTimeout, deadline - now);
                    } else {
                        nodeReadyDeadlines.put(node, now + requestTimeoutMs);
                    }
                    long nodeTimeout = client.pollDelayMs(node, now);
                    pollTimeout = Math.min(pollTimeout, nodeTimeout);
                    log.trace("Client is not ready to send to {}. Must delay {} ms", node, nodeTimeout);
                    continue;
                }
                // 从总请求时间中减去等待节点就绪的时间
                int remainingRequestTime;
                Long deadlineMs = nodeReadyDeadlines.remove(node);
                if (deadlineMs == null) {
                    remainingRequestTime = requestTimeoutMs;
                } else {
                    remainingRequestTime = calcTimeoutMsRemainingAsInt(now, deadlineMs);
                }
                while (!calls.isEmpty()) {
                    Call call = calls.remove(0);
                    // 计算实际的超时时间
                    int timeoutMs = Math.min(remainingRequestTime,
                        calcTimeoutMsRemainingAsInt(now, call.deadlineMs));
                    AbstractRequest.Builder<?> requestBuilder;
                    try {
                        requestBuilder = call.createRequest(timeoutMs);
                    } catch (Throwable t) {
                        call.fail(now, new KafkaException(String.format(
                            "Internal error sending %s to %s.", call.callName, node), t));
                        continue;
                    }
                    // 创建并发送客户端请求
                    ClientRequest clientRequest = client.newClientRequest(node.idString(),
                        requestBuilder, now, true, timeoutMs, null);
                    log.debug("Sending {} to {}. correlationId={}, timeoutMs={}",
                        requestBuilder, node, clientRequest.correlationId(), timeoutMs);
                    client.send(clientRequest, now);
                    // 更新请求状态追踪
                    callsInFlight.put(node.idString(), call);
                    correlationIdToCalls.put(clientRequest.correlationId(), call);
                    break;
                }
            }
            return pollTimeout;
        }

        /**
         * 处理已超时的在途请求。
         * 
         * 在途请求可能已经部分或完全发送到网络中，甚至可能正在被远程服务器处理。
         * 目前处理超时的唯一选择是关闭整个连接。
         * 
         * @param processor 超时处理器
         */
        private void timeoutCallsInFlight(TimeoutProcessor processor) {
            int numTimedOut = 0;
            for (Map.Entry<String, Call> entry : callsInFlight.entrySet()) {
                Call call = entry.getValue();
                String nodeId = entry.getKey();
                if (processor.callHasExpired(call)) {
                    // 对于超时的请求，关闭与节点的连接
                    log.info("Disconnecting from {} due to timeout while awaiting {}", nodeId, call);
                    client.disconnect(nodeId);
                    numTimedOut++;
                    // 不从callsInFlight数据结构中移除任何内容
                    // 因为连接已关闭，这些请求将在下一次client#poll()时返回
                    // 并在那时进行处理
                }
            }
            if (numTimedOut > 0)
                log.debug("Timed out {} call(s) in flight.", numTimedOut);
        }

        /**
         * 处理来自服务器的响应。
         * 该方法负责处理从Kafka服务器接收到的所有响应，包括成功响应和错误响应。
         * 主要处理以下几种情况:
         * 1. 正常响应的处理
         * 2. 版本不匹配的处理
         * 3. 连接断开的处理
         * 4. 认证异常的处理
         * 5. 其他异常情况的处理
         *
         * @param now 当前时间戳(毫秒)
         * @param responses 从KafkaClient收到的最新响应列表
         */
        private void handleResponses(long now, List<ClientResponse> responses) {
            for (ClientResponse response : responses) {
                // 获取响应的关联ID，用于匹配请求和响应的对应关系
                int correlationId = response.requestHeader().correlationId();

                // 根据关联ID查找对应的调用对象
                Call call = correlationIdToCalls.get(correlationId);
                if (call == null) {
                    // 如果找不到对应的调用对象，说明发生了内部服务器错误
                    // 这种情况下需要断开连接并记录错误日志
                    log.error("Internal server error on {}: server returned information about unknown " +
                        "correlation ID {}, requestHeader = {}", response.destination(), correlationId,
                        response.requestHeader());
                    client.disconnect(response.destination());
                    continue;
                }

                // 从跟踪映射中移除已完成的调用
                correlationIdToCalls.remove(correlationId);
                if (!callsInFlight.remove(response.destination(), call)) {
                    // 如果在运行中的调用集合中找不到该调用，记录错误并继续处理下一个响应
                    log.error("Internal server error on {}: ignoring call {} in correlationIdToCall " +
                        "that did not exist in callsInFlight", response.destination(), call);
                    continue;
                }

                // 处理调用结果，根据不同的响应类型采取相应的处理措施
                if (response.versionMismatch() != null) {
                    // 处理API版本不匹配的情况
                    call.fail(now, response.versionMismatch());
                } else if (response.wasDisconnected()) {
                    // 处理连接断开的情况
                    AuthenticationException authException = client.authenticationException(call.curNode());
                    if (authException != null) {
                        // 如果是认证异常导致的断开连接
                        call.fail(now, authException);
                    } else {
                        // 如果是其他原因导致的断开连接
                        call.fail(now, new DisconnectException(String.format(
                            "Cancelled %s request with correlation id %d due to node %s being disconnected",
                            call.callName, correlationId, response.destination())));
                    }
                } else {
                    try {
                        // 处理正常的响应
                        call.handleResponse(response.responseBody());
                        // 记录请求延迟指标
                        adminFetchMetricsManager.recordLatency(response.destination(), response.requestLatencyMs());
                        if (log.isTraceEnabled())
                            log.trace("{} got response {}", call, response.responseBody());
                    } catch (Throwable t) {
                        // 处理响应处理过程中发生的异常
                        if (log.isTraceEnabled())
                            log.trace("{} handleResponse failed with {}", call, prettyPrintException(t));
                        call.fail(now, t);
                    }
                }
            }
        }

        /**
         * 根据指定条件重新分配尚未发送的调用请求。
         * 该方法主要用于处理节点连接断开等异常情况下的请求重新分配。
         * 处理流程:
         * 1. 遍历所有待发送的调用
         * 2. 检查每个节点上的调用列表
         * 3. 如果节点满足重新分配条件，将其调用转移到待处理队列
         * 4. 清理相关的节点状态
         *
         * 应用场景:
         * - 节点连接断开时的请求重新分配
         * - 负载均衡时的请求重新分配
         * - 节点故障时的请求重新分配
         *
         * @param shouldUnassign 重新分配的条件判断函数。当此断言为true时，
         *                     对应节点上的调用将被放回pendingCalls集合中等待重新分配
         */
        private void unassignUnsentCalls(Predicate<Node> shouldUnassign) {
            // 遍历待发送调用映射表的所有条目
            for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry<Node, List<Call>> entry = iter.next();
                Node node = entry.getKey();
                List<Call> awaitingCalls = entry.getValue();

                if (awaitingCalls.isEmpty()) {
                    // 如果节点没有待发送的调用，直接从映射表中移除该节点
                    iter.remove();
                } else if (shouldUnassign.test(node)) {
                    // 如果节点满足重新分配条件:
                    // 1. 移除节点就绪期限记录
                    nodeReadyDeadlines.remove(node);
                    // 2. 将该节点的所有调用转移到待处理队列
                    transitionToPendingAndClearList(awaitingCalls);
                    // 3. 从待发送映射表中移除该节点
                    iter.remove();
                }
            }
        }

        /**
         * 检查给定调用集合中是否存在外部调用。
         * 外部调用是指由用户发起的调用请求，而不是系统内部的元数据更新等操作。
         *
         * @param calls 要检查的调用集合
         * @return 如果存在外部调用返回true，否则返回false
         */
        private boolean hasActiveExternalCalls(Collection<Call> calls) {
            // 遍历所有调用
            for (Call call : calls) {
                // 如果发现非内部调用，立即返回true
                if (!call.isInternal()) {
                    return true;
                }
            }
            return false;
        }

        /**
         * 检查当前是否存在活跃的外部调用请求。
         * 该方法会检查以下三个调用集合:
         * 1. 待处理的调用(pendingCalls)
         * 2. 待发送的调用(callsToSend)
         * 3. 已发送待响应的调用(correlationIdToCalls)
         *
         * 应用场景:
         * - 在关闭AdminClient时判断是否还有未完成的用户请求
         * - 在进行资源清理时确保所有外部请求都已处理完成
         *
         * @return 如果存在任何活跃的外部调用返回true，否则返回false
         */
        private boolean hasActiveExternalCalls() {
            // 首先检查待处理的调用
            if (hasActiveExternalCalls(pendingCalls)) {
                return true;
            }
            // 然后检查所有待发送的调用
            for (List<Call> callList : callsToSend.values()) {
                if (hasActiveExternalCalls(callList)) {
                    return true;
                }
            }
            // 最后检查已发送待响应的调用
            return hasActiveExternalCalls(correlationIdToCalls.values());
        }

        /**
         * 判断AdminClient线程是否应该退出
         * 
         * @param now 当前时间戳(毫秒)
         * @param curHardShutdownTimeMs 强制关闭的截止时间戳(毫秒)
         * @return 如果线程应该退出返回true，否则返回false
         */
        private boolean threadShouldExit(long now, long curHardShutdownTimeMs) {
            // 如果没有活跃的外部调用请求，线程可以退出
            if (!hasActiveExternalCalls()) {
                log.trace("All work has been completed, and the I/O thread is now exiting.");
                return true;
            }
            // 如果当前时间超过了强制关闭时间，强制退出线程
            if (now >= curHardShutdownTimeMs) {
                log.info("Forcing a hard I/O thread shutdown. Requests in progress will be aborted.");
                return true;
            }
            // 记录距离强制关闭还剩多少时间
            log.debug("Hard shutdown in {} ms.", curHardShutdownTimeMs - now);
            return false;
        }

        /**
         * AdminClient线程的主运行方法
         * 负责处理所有的管理请求，包括请求的发送、响应处理和超时管理
         */
        @Override
        public void run() {
            log.debug("Thread starting");
            try {
                // 处理所有管理请求
                processRequests();
            } finally {
                // 标记线程正在关闭
                closing = true;
                // 注销JMX监控
                AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);

                // 处理所有未完成请求的超时
                int numTimedOut = 0;
                TimeoutProcessor timeoutProcessor = new TimeoutProcessor(Long.MAX_VALUE);
                // 处理新请求队列中的超时
                synchronized (this) {
                    numTimedOut += timeoutProcessor.handleTimeouts(newCalls, "The AdminClient thread has exited.");
                }
                // 处理待处理请求队列中的超时
                numTimedOut += timeoutProcessor.handleTimeouts(pendingCalls, "The AdminClient thread has exited.");
                // 处理待发送请求的超时
                numTimedOut += timeoutCallsToSend(timeoutProcessor);
                // 处理已发送但未收到响应的请求超时
                numTimedOut += timeoutProcessor.handleTimeouts(correlationIdToCalls.values(),
                        "The AdminClient thread has exited.");
                if (numTimedOut > 0) {
                    log.info("Timed out {} remaining operation(s) during close.", numTimedOut);
                }
                // 关闭网络客户端和监控指标
                closeQuietly(client, "KafkaClient");
                closeQuietly(metrics, "Metrics");
                log.debug("Exiting AdminClientRunnable thread.");
            }
        }

        /**
         * 处理AdminClient的所有请求
         * 这是AdminClient的核心处理循环，负责管理请求的生命周期，包括:
         * 1. 请求的排队和调度
         * 2. 超时处理
         * 3. 元数据更新
         * 4. 网络I/O
         * 5. 响应处理
         */
        private void processRequests() {
            long now = time.milliseconds();
            while (true) {
                // 将新请求从newCalls队列转移到pendingCalls队列
                drainNewCalls();

                // 检查AdminClient线程是否需要关闭
                long curHardShutdownTimeMs = hardShutdownTimeMs.get();
                if ((curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) && threadShouldExit(now, curHardShutdownTimeMs))
                    break;

                // 处理各类请求的超时
                TimeoutProcessor timeoutProcessor = timeoutProcessorFactory.create(now);
                timeoutPendingCalls(timeoutProcessor);  // 处理待处理请求超时
                timeoutCallsToSend(timeoutProcessor);   // 处理待发送请求超时
                timeoutCallsInFlight(timeoutProcessor); // 处理已发送请求超时

                // 计算poll超时时间，默认最大20分钟
                long pollTimeout = Math.min(1200000, timeoutProcessor.nextTimeoutMs());
                if (curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) {
                    pollTimeout = Math.min(pollTimeout, curHardShutdownTimeMs - now);
                }

                // 为待处理的请求选择目标节点
                pollTimeout = Math.min(pollTimeout, maybeDrainPendingCalls(now));
                
                // 检查是否需要更新元数据
                long metadataFetchDelayMs = metadataManager.metadataFetchDelayMs(now);
                if (metadataFetchDelayMs == 0) {
                    // 创建新的元数据获取请求
                    metadataManager.transitionToUpdatePending(now);
                    Call metadataCall = makeMetadataCall(now);
                    // 将元数据请求添加到pendingCalls队列末尾
                    // 只为新请求分配节点(其他待处理节点已在上面处理)
                    if (!maybeDrainPendingCall(metadataCall, now))
                        pendingCalls.add(metadataCall);
                }
                
                // 发送符合条件的请求
                pollTimeout = Math.min(pollTimeout, sendEligibleCalls(now));

                // 如果需要获取元数据，调整poll超时时间
                if (metadataFetchDelayMs > 0) {
                    pollTimeout = Math.min(pollTimeout, metadataFetchDelayMs);
                }

                // 如果有待发送的请求，使用较小的poll超时时间
                if (!pendingCalls.isEmpty())
                    pollTimeout = Math.min(pollTimeout, retryBackoffMs);

                // 等待网络响应
                log.trace("Entering KafkaClient#poll(timeout={})", pollTimeout);
                List<ClientResponse> responses = client.poll(Math.max(0L, pollTimeout), now);
                log.trace("KafkaClient#poll retrieved {} response(s)", responses.size());

                // 取消已断开连接节点上的未发送请求的分配
                unassignUnsentCalls(client::connectionFailed);

                // 更新当前时间并处理最新的响应
                now = time.milliseconds();
                handleResponses(now, responses);
            }
        }

        /**
         * 将请求加入发送队列
         * 
         * 如果AdminClient线程已退出，此操作会失败。否则即使AdminClient正在关闭，也会成功。
         * 此方法通常用于重试现有请求。
         *
         * @param call 新的请求对象
         * @param now 当前时间戳(毫秒)
         */
        void enqueue(Call call, long now) {
            // 检查是否超过最大重试次数
            if (call.tries > maxRetries) {
                log.debug("Max retries {} for {} reached", maxRetries, call);
                call.handleTimeoutFailure(time.milliseconds(), new TimeoutException(
                    "Exceeded maxRetries after " + call.tries + " tries."));
                return;
            }
            
            // 记录请求入队信息
            if (log.isDebugEnabled()) {
                log.debug("Queueing {} with a timeout {} ms from now.", call,
                    Math.min(requestTimeoutMs, call.deadlineMs - now));
            }
            
            // 尝试将请求加入新请求队列
            boolean accepted = false;
            synchronized (this) {
                if (!closing) {
                    newCalls.add(call);
                    accepted = true;
                }
            }
            
            // 如果请求被接受，唤醒可能在poll()中等待的线程
            if (accepted) {
                client.wakeup(); // 唤醒可能在poll()中的线程
            } else {
                // 如果请求未被接受(因为线程正在关闭)，将请求标记为超时
                log.debug("The AdminClient thread has exited. Timing out {}.", call);
                call.handleTimeoutFailure(time.milliseconds(),
                    new TimeoutException("The AdminClient thread has exited."));
            }
        }

        /**
         * 初始化一个新的调用。
         * 
         * 如果AdminClient已计划关闭，此调用将失败。
         *
         * @param call      新的调用对象
         * @param now       当前时间戳(毫秒)
         */
        void call(Call call, long now) {
            // 检查AdminClient是否正在关闭
            if (hardShutdownTimeMs.get() != INVALID_SHUTDOWN_TIME) {
                log.debug("Cannot accept new call {} when AdminClient is closing.", call);
                // 如果正在关闭，则调用失败处理
                call.handleFailure(new IllegalStateException("Cannot accept new calls when AdminClient is closing."));
            } else if (metadataManager.usingBootstrapControllers() &&
                    (!call.nodeProvider.supportsUseControllers())) {
                // 如果使用引导控制器但不支持控制器端点，则调用失败
                call.fail(now, new UnsupportedEndpointTypeException("This Admin API is not " +
                    "yet supported when communicating directly with the controller quorum."));
            } else {
                // 正常情况下将调用加入队列
                enqueue(call, now);
            }
        }

        /**
         * 创建一个新的元数据调用。
         * 根据是否使用引导控制器来决定创建控制器元数据调用还是broker元数据调用。
         * 
         * @param now 当前时间戳(毫秒)
         * @return 元数据调用对象
         */
        private Call makeMetadataCall(long now) {
            // 根据是否使用引导控制器选择不同的元数据调用方式
            if (metadataManager.usingBootstrapControllers()) {
                return makeControllerMetadataCall(now);
            } else {
                return makeBrokerMetadataCall(now);
            }
        }

        /**
         * 创建一个控制器元数据调用。
         * 使用DescribeCluster API来获取集群元数据，这是KIP-919中指定的方式。
         * 
         * @param now 当前时间戳(毫秒)
         * @return 控制器元数据调用对象
         */
        private Call makeControllerMetadataCall(long now) {
            // 根据KIP-919规范，使用DescribeCluster API
            return new Call(true, "describeCluster", calcDeadlineMs(now, requestTimeoutMs),
                    new MetadataUpdateNodeIdProvider()) {
                @Override
                public DescribeClusterRequest.Builder createRequest(int timeoutMs) {
                    // 创建DescribeCluster请求，不包含集群授权操作，指定端点类型为CONTROLLER
                    return new DescribeClusterRequest.Builder(new DescribeClusterRequestData()
                        .setIncludeClusterAuthorizedOperations(false)
                        .setEndpointType(EndpointType.CONTROLLER.id()));
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    DescribeClusterResponse response = (DescribeClusterResponse) abstractResponse;
                    Cluster cluster;
                    try {
                        // 解析响应数据构建集群对象
                        cluster = parseDescribeClusterResponse(response.data());
                    } catch (ApiException e) {
                        handleFailure(e);
                        return;
                    }
                    long now = time.milliseconds();
                    // 使用新的集群信息更新元数据管理器
                    metadataManager.update(cluster, now);

                    // 元数据刷新后，取消分配所有未发送的请求
                    // 这样可以根据新的元数据重新选择目标节点
                    unassignUnsentCalls(node -> true);
                }

                @Override
                boolean handleUnsupportedVersionException(final UnsupportedVersionException e) {
                    // 处理不支持的API版本异常
                    metadataManager.updateFailed(e);
                    return false;
                }

                @Override
                public void handleFailure(Throwable e) {
                    // 处理其他失败情况
                    metadataManager.updateFailed(e);
                }
            };
        }

        /**
         * 创建一个broker元数据调用。
         * 使用MetadataRequest来支持那些太旧而无法处理DescribeCluster的broker。
         * 
         * @param now 当前时间戳(毫秒)
         * @return broker元数据调用对象
         */
        private Call makeBrokerMetadataCall(long now) {
            // 使用MetadataRequest以保持对旧版本broker的兼容性
            return new Call(true, "fetchMetadata", calcDeadlineMs(now, requestTimeoutMs),
                    new MetadataUpdateNodeIdProvider()) {
                @Override
                public MetadataRequest.Builder createRequest(int timeoutMs) {
                    // 由于只请求节点信息，设置allowAutoTopicCreation为true是安全的
                    // 这样可以简化与旧版本broker的通信
                    return new MetadataRequest.Builder(new MetadataRequestData()
                        .setTopics(Collections.emptyList())
                        .setAllowAutoTopicCreation(true));
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    MetadataResponse response = (MetadataResponse) abstractResponse;
                    long now = time.milliseconds();

                    // 处理响应中的顶层错误
                    if (response.topLevelError() == Errors.REBOOTSTRAP_REQUIRED)
                        // 如果需要重新引导，则初始化重新引导过程
                        metadataManager.initiateRebootstrap();
                    else
                        // 否则使用响应中的集群信息更新元数据
                        metadataManager.update(response.buildCluster(), now);

                    // 元数据刷新后，取消分配所有未发送的请求
                    // 这样可以根据新的元数据重新选择目标节点
                    unassignUnsentCalls(node -> true);
                }

                @Override
                boolean handleUnsupportedVersionException(final UnsupportedVersionException e) {
                    // 处理不支持的API版本异常
                    metadataManager.updateFailed(e);
                    return false;
                }

                @Override
                public void handleFailure(Throwable e) {
                    // 处理其他失败情况
                    metadataManager.updateFailed(e);
                }
            };
        }
    }

    /**
     * 解析DescribeCluster响应数据，构建Cluster对象。
     * 
     * @param response DescribeCluster响应数据
     * @return 解析后的Cluster对象
     * @throws ApiError.Exception 如果响应中包含错误
     * @throws MismatchedEndpointTypeException 如果响应不是来自控制器端点
     */
    static Cluster parseDescribeClusterResponse(DescribeClusterResponseData response) {
        // 检查响应中的错误码
        ApiError apiError = new ApiError(response.errorCode(), response.errorMessage());
        if (apiError.isFailure()) {
            throw apiError.exception();
        }
        // 验证响应是否来自控制器端点
        if (response.endpointType() != EndpointType.CONTROLLER.id()) {
            throw new MismatchedEndpointTypeException("Expected response from CONTROLLER " +
                "endpoint, but got response from endpoint type " + (int) response.endpointType());
        }
        
        // 构建节点列表和控制器节点
        List<Node> nodes = new ArrayList<>();
        Node controllerNode = null;
        for (DescribeClusterResponseData.DescribeClusterBroker node : response.brokers()) {
            // 为每个broker创建Node对象
            Node newNode = new Node(node.brokerId(), node.host(), node.port(), node.rack());
            nodes.add(newNode);
            // 标识控制器节点
            if (node.brokerId() == response.controllerId()) {
                controllerNode = newNode;
            }
        }
        
        // 创建并返回Cluster对象
        // 注意：这里的主题分区信息为空，因为DescribeCluster API只返回broker信息
        return new Cluster(response.clusterId(),
            nodes,
            Collections.emptyList(),
            Collections.emptySet(),
            Collections.emptySet(),
            controllerNode);
    }

    /**
     * 检查主题名称是否可以在RPC中表示。
     * 此函数不检查名称是否过长、是否包含无效字符等。这些策略最好在服务器端强制执行，
     * 这样将来如果需要可以更改这些策略。
     * 
     * @param topicName 主题名称
     * @return 如果主题名称为null或空字符串则返回true
     */
    private static boolean topicNameIsUnrepresentable(String topicName) {
        return topicName == null || topicName.isEmpty();
    }

    /**
     * 检查主题ID是否可以在RPC中表示。
     * 
     * @param topicId 主题ID
     * @return 如果主题ID为null或为零UUID则返回true
     */
    private static boolean topicIdIsUnrepresentable(Uuid topicId) {
        return topicId == null || topicId.equals(Uuid.ZERO_UUID);
    }

    // 用于测试目的
    int numPendingCalls() {
        return runnable.pendingCalls.size();
    }

    /**
     * 处理未完成的Future对象。
     * 当响应处理器期望某个实体有结果但实际没有结果时使用此方法。
     * 
     * @param futures 需要处理的Future流
     * @param messageFormatter 用于格式化错误消息的函数
     */
    private static <K, V> void completeUnrealizedFutures(
            Stream<Map.Entry<K, KafkaFutureImpl<V>>> futures,
            Function<K, String> messageFormatter) {
        // 过滤出未完成的Future，并用ApiException异常完成它们
        futures.filter(entry -> !entry.getValue().isDone()).forEach(entry ->
                entry.getValue().completeExceptionally(new ApiException(messageFormatter.apply(entry.getKey()))));
    }

    /**
     * 处理因超出配额而重试的Future对象。
     * 如果请求超时，我们将初始错误传播回调用者。
     * 
     * @param shouldRetryOnQuotaViolation 是否在配额违规时重试
     * @param throwable 抛出的异常
     * @param futures Future对象映射
     * @param quotaExceededExceptions 配额超限异常映射
     * @param throttleTimeDelta 限流时间增量
     */
    private static <K, V> void maybeCompleteQuotaExceededException(
            boolean shouldRetryOnQuotaViolation,
            Throwable throwable,
            Map<K, KafkaFutureImpl<V>> futures,
            Map<K, ThrottlingQuotaExceededException> quotaExceededExceptions,
            int throttleTimeDelta) {
        // 只有在启用配额违规重试且发生超时异常时才处理
        if (shouldRetryOnQuotaViolation && throwable instanceof TimeoutException) {
            // 用调整后的限流时间完成对应的Future
            quotaExceededExceptions.forEach((key, value) -> futures.get(key).completeExceptionally(
                new ThrottlingQuotaExceededException(
                    Math.max(0, value.throttleTimeMs() - throttleTimeDelta),
                    value.getMessage())));
        }
    }

    /**
     * 创建主题的实现方法。
     * 
     * @param newTopics 要创建的主题集合
     * @param options 创建主题的选项
     * @return 创建主题的结果
     */
    @Override
    public CreateTopicsResult createTopics(final Collection<NewTopic> newTopics,
                                           final CreateTopicsOptions options) {
        // 为每个主题创建对应的Future对象
        final Map<String, KafkaFutureImpl<TopicMetadataAndConfig>> topicFutures = new HashMap<>(newTopics.size());
        final CreatableTopicCollection topics = new CreatableTopicCollection();
        
        // 处理每个要创建的主题
        for (NewTopic newTopic : newTopics) {
            if (topicNameIsUnrepresentable(newTopic.name())) {
                // 主题名称不合法，直接完成Future并返回异常
                KafkaFutureImpl<TopicMetadataAndConfig> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                    newTopic.name() + "' cannot be represented in a request."));
                topicFutures.put(newTopic.name(), future);
            } else if (!topicFutures.containsKey(newTopic.name())) {
                // 主题名称合法且不重复，添加到待创建集合
                topicFutures.put(newTopic.name(), new KafkaFutureImpl<>());
                topics.add(newTopic.convertToCreatableTopic());
            }
        }
        
        // 如果有主题需要创建，发送创建请求
        if (!topics.isEmpty()) {
            final long now = time.milliseconds();
            final long deadline = calcDeadlineMs(now, options.timeoutMs());
            final Call call = getCreateTopicsCall(options, topicFutures, topics,
                Collections.emptyMap(), now, deadline);
            runnable.call(call, now);
        }
        
        return new CreateTopicsResult(new HashMap<>(topicFutures));
    }

    /**
     * 创建用于创建主题的Call对象。
     * 
     * @param options 创建主题的选项
     * @param futures 主题创建的Future映射
     * @param topics 要创建的主题集合
     * @param quotaExceededExceptions 配额超限异常映射
     * @param now 当前时间戳
     * @param deadline 截止时间戳
     * @return 创建主题的Call对象
     */
    private Call getCreateTopicsCall(final CreateTopicsOptions options,
                                     final Map<String, KafkaFutureImpl<TopicMetadataAndConfig>> futures,
                                     final CreatableTopicCollection topics,
                                     final Map<String, ThrottlingQuotaExceededException> quotaExceededExceptions,
                                     final long now,
                                     final long deadline) {
        return new Call("createTopics", deadline, new ControllerNodeProvider()) {
            @Override
            public CreateTopicsRequest.Builder createRequest(int timeoutMs) {
                // 构建创建主题的请求
                return new CreateTopicsRequest.Builder(
                    new CreateTopicsRequestData()
                        .setTopics(topics)
                        .setTimeoutMs(timeoutMs)
                        .setValidateOnly(options.shouldValidateOnly()));
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                // 检查控制器变更
                handleNotControllerError(abstractResponse);
                
                // 处理服务器对特定主题的响应
                final CreateTopicsResponse response = (CreateTopicsResponse) abstractResponse;
                final CreatableTopicCollection retryTopics = new CreatableTopicCollection();
                final Map<String, ThrottlingQuotaExceededException> retryTopicQuotaExceededExceptions = new HashMap<>();
                
                // 处理每个主题的创建结果
                for (CreatableTopicResult result : response.data().topics()) {
                    KafkaFutureImpl<TopicMetadataAndConfig> future = futures.get(result.name());
                    if (future == null) {
                        // 响应中包含未知主题
                        log.warn("Server response mentioned unknown topic {}", result.name());
                    } else {
                        ApiError error = new ApiError(result.errorCode(), result.errorMessage());
                        if (error.isFailure()) {
                            // 处理创建失败的情况
                            if (error.is(Errors.THROTTLING_QUOTA_EXCEEDED)) {
                                // 处理配额超限错误
                                ThrottlingQuotaExceededException quotaExceededException = new ThrottlingQuotaExceededException(
                                    response.throttleTimeMs(), error.messageWithFallback());
                                if (options.shouldRetryOnQuotaViolation()) {
                                    // 如果配置了重试，将主题加入重试集合
                                    retryTopics.add(topics.find(result.name()).duplicate());
                                    retryTopicQuotaExceededExceptions.put(result.name(), quotaExceededException);
                                } else {
                                    // 否则直接完成Future并返回异常
                                    future.completeExceptionally(quotaExceededException);
                                }
                            } else {
                                // 处理其他错误
                                future.completeExceptionally(error.exception());
                            }
                        } else {
                            // 处理创建成功的情况
                            TopicMetadataAndConfig topicMetadataAndConfig;
                            if (result.topicConfigErrorCode() != Errors.NONE.code()) {
                                // 配置错误
                                topicMetadataAndConfig = new TopicMetadataAndConfig(
                                    Errors.forCode(result.topicConfigErrorCode()).exception());
                            } else if (result.numPartitions() == CreateTopicsResult.UNKNOWN) {
                                // 不支持的版本
                                topicMetadataAndConfig = new TopicMetadataAndConfig(new UnsupportedVersionException(
                                    "Topic metadata and configs in CreateTopics response not supported"));
                            } else {
                                // 创建成功，构建主题元数据和配置
                                List<CreatableTopicConfigs> configs = result.configs();
                                Config topicConfig = new Config(configs.stream()
                                    .map(this::configEntry)
                                    .collect(Collectors.toSet()));
                                topicMetadataAndConfig = new TopicMetadataAndConfig(result.topicId(), result.numPartitions(),
                                    result.replicationFactor(),
                                    topicConfig);
                            }
                            future.complete(topicMetadataAndConfig);
                        }
                    }
                }
                
                // 处理需要重试的主题
                if (retryTopics.isEmpty()) {
                    // 没有需要重试的主题，完成所有未实现的Future
                    completeUnrealizedFutures(futures.entrySet().stream(),
                        topic -> "The controller response did not contain a result for topic " + topic);
                } else {
                    // 重新发送创建请求
                    final long now = time.milliseconds();
                    final Call call = getCreateTopicsCall(options, futures, retryTopics,
                        retryTopicQuotaExceededExceptions, now, deadline);
                    runnable.call(call, now);
                }
            }

            /**
             * 将CreatableTopicConfigs转换为ConfigEntry对象
             */
            private ConfigEntry configEntry(CreatableTopicConfigs config) {
                return new ConfigEntry(
                    config.name(),
                    config.value(),
                    configSource(DescribeConfigsResponse.ConfigSource.forId(config.configSource())),
                    config.isSensitive(),
                    config.readOnly(),
                    Collections.emptyList(),
                    null,
                    null);
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理因配额超限而重试的主题
                maybeCompleteQuotaExceededException(options.shouldRetryOnQuotaViolation(),
                    throwable, futures, quotaExceededExceptions, (int) (time.milliseconds() - now));
                // 使所有剩余的Future异常完成
                completeAllExceptionally(futures.values(), throwable);
            }
        };
    }

    /**
     * 删除主题的实现方法
     * 
     * @param topics 要删除的主题集合，可以是主题ID集合或主题名称集合
     * @param options 删除主题的选项配置
     * @return DeleteTopicsResult 删除主题的结果，包含每个主题的删除状态
     * @throws IllegalArgumentException 当提供的主题集合类型不支持时抛出
     */
    @Override
    public DeleteTopicsResult deleteTopics(final TopicCollection topics,
                                           final DeleteTopicsOptions options) {
        // 根据主题集合的类型选择不同的处理方法
        if (topics instanceof TopicIdCollection)
            // 如果是主题ID集合，调用handleDeleteTopicsUsingIds方法处理
            return DeleteTopicsResult.ofTopicIds(handleDeleteTopicsUsingIds(((TopicIdCollection) topics).topicIds(), options));
        else if (topics instanceof TopicNameCollection)
            // 如果是主题名称集合，调用handleDeleteTopicsUsingNames方法处理
            return DeleteTopicsResult.ofTopicNames(handleDeleteTopicsUsingNames(((TopicNameCollection) topics).topicNames(), options));
        else
            // 如果是其他类型，抛出不支持的参数类型异常
            throw new IllegalArgumentException("The TopicCollection: " + topics + " provided did not match any supported classes for deleteTopics.");
    }

    /**
     * 使用主题名称删除主题的内部处理方法
     * 
     * @param topicNames 要删除的主题名称集合
     * @param options 删除主题的选项配置
     * @return Map<String, KafkaFuture<Void>> 每个主题的删除操作Future
     */
    private Map<String, KafkaFuture<Void>> handleDeleteTopicsUsingNames(final Collection<String> topicNames,
                                                                        final DeleteTopicsOptions options) {
        // 创建存储每个主题删除结果的Future映射
        final Map<String, KafkaFutureImpl<Void>> topicFutures = new HashMap<>(topicNames.size());
        // 创建有效主题名称列表
        final List<String> validTopicNames = new ArrayList<>(topicNames.size());
        
        // 遍历所有主题名称，验证并初始化对应的Future
        for (String topicName : topicNames) {
            if (topicNameIsUnrepresentable(topicName)) {
                // 如果主题名称不合法，创建一个异常完成的Future
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                    topicName + "' cannot be represented in a request."));
                topicFutures.put(topicName, future);
            } else if (!topicFutures.containsKey(topicName)) {
                // 如果主题名称合法且未处理过，创建新的Future并添加到有效主题列表
                topicFutures.put(topicName, new KafkaFutureImpl<>());
                validTopicNames.add(topicName);
            }
        }
        
        // 如果存在有效的主题，创建并执行删除请求
        if (!validTopicNames.isEmpty()) {
            final long now = time.milliseconds();
            final long deadline = calcDeadlineMs(now, options.timeoutMs());
            // 创建删除主题的调用对象
            final Call call = getDeleteTopicsCall(options, topicFutures, validTopicNames,
                Collections.emptyMap(), now, deadline);
            // 执行删除调用
            runnable.call(call, now);
        }
        
        return new HashMap<>(topicFutures);
    }

    /**
     * 使用主题ID删除主题的内部处理方法
     * 
     * @param topicIds 要删除的主题ID集合
     * @param options 删除主题的选项配置
     * @return Map<Uuid, KafkaFuture<Void>> 每个主题的删除操作Future
     */
    private Map<Uuid, KafkaFuture<Void>> handleDeleteTopicsUsingIds(final Collection<Uuid> topicIds,
                                                                    final DeleteTopicsOptions options) {
        // 创建存储每个主题删除结果的Future映射
        final Map<Uuid, KafkaFutureImpl<Void>> topicFutures = new HashMap<>(topicIds.size());
        // 创建有效主题ID列表
        final List<Uuid> validTopicIds = new ArrayList<>(topicIds.size());
        
        // 遍历所有主题ID，验证并初始化对应的Future
        for (Uuid topicId : topicIds) {
            if (topicId.equals(Uuid.ZERO_UUID)) {
                // 如果主题ID是ZERO_UUID（无效ID），创建一个异常完成的Future
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic ID '" +
                    topicId + "' cannot be represented in a request."));
                topicFutures.put(topicId, future);
            } else if (!topicFutures.containsKey(topicId)) {
                // 如果主题ID有效且未处理过，创建新的Future并添加到有效主题ID列表
                topicFutures.put(topicId, new KafkaFutureImpl<>());
                validTopicIds.add(topicId);
            }
        }
        
        // 如果存在有效的主题ID，创建并执行删除请求
        if (!validTopicIds.isEmpty()) {
            final long now = time.milliseconds();
            final long deadline = calcDeadlineMs(now, options.timeoutMs());
            // 创建删除主题的调用对象
            final Call call = getDeleteTopicsWithIdsCall(options, topicFutures, validTopicIds,
                Collections.emptyMap(), now, deadline);
            // 执行删除调用
            runnable.call(call, now);
        }
        
        return new HashMap<>(topicFutures);
    }

    /**
     * 创建删除主题的调用对象
     * 
     * @param options 删除主题的选项配置
     * @param futures 存储每个主题删除结果的Future映射
     * @param topics 要删除的主题名称列表
     * @param quotaExceededExceptions 配额超限异常映射
     * @param now 当前时间戳
     * @param deadline 操作截止时间
     * @return Call 删除主题的调用对象
     */
    private Call getDeleteTopicsCall(final DeleteTopicsOptions options,
                                     final Map<String, KafkaFutureImpl<Void>> futures,
                                     final List<String> topics,
                                     final Map<String, ThrottlingQuotaExceededException> quotaExceededExceptions,
                                     final long now,
                                     final long deadline) {
        return new Call("deleteTopics", deadline, new ControllerNodeProvider()) {
            @Override
            DeleteTopicsRequest.Builder createRequest(int timeoutMs) {
                // 创建删除主题请求
                return new DeleteTopicsRequest.Builder(
                    new DeleteTopicsRequestData()
                        .setTopicNames(topics)
                        .setTimeoutMs(timeoutMs));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                // 检查控制器节点是否发生变更
                handleNotControllerError(abstractResponse);
                
                // 处理服务器对特定主题的响应
                final DeleteTopicsResponse response = (DeleteTopicsResponse) abstractResponse;
                final List<String> retryTopics = new ArrayList<>();
                final Map<String, ThrottlingQuotaExceededException> retryTopicQuotaExceededExceptions = new HashMap<>();
                
                // 遍历每个主题的删除结果
                for (DeletableTopicResult result : response.data().responses()) {
                    KafkaFutureImpl<Void> future = futures.get(result.name());
                    if (future == null) {
                        // 如果响应中包含未知主题，记录警告日志
                        log.warn("Server response mentioned unknown topic {}", result.name());
                    } else {
                        ApiError error = new ApiError(result.errorCode(), result.errorMessage());
                        if (error.isFailure()) {
                            if (error.is(Errors.THROTTLING_QUOTA_EXCEEDED)) {
                                // 处理配额超限异常
                                ThrottlingQuotaExceededException quotaExceededException = new ThrottlingQuotaExceededException(
                                    response.throttleTimeMs(), error.messageWithFallback());
                                if (options.shouldRetryOnQuotaViolation()) {
                                    // 如果配置了重试，将主题添加到重试列表
                                    retryTopics.add(result.name());
                                    retryTopicQuotaExceededExceptions.put(result.name(), quotaExceededException);
                                } else {
                                    // 否则直接完成Future并标记异常
                                    future.completeExceptionally(quotaExceededException);
                                }
                            } else {
                                // 处理其他错误
                                future.completeExceptionally(error.exception());
                            }
                        } else {
                            // 删除成功，完成Future
                            future.complete(null);
                        }
                    }
                }
                
                // 处理需要重试的主题
                if (retryTopics.isEmpty()) {
                    // 如果没有需要重试的主题，检查是否所有Future都已完成
                    completeUnrealizedFutures(futures.entrySet().stream(),
                        topic -> "The controller response did not contain a result for topic " + topic);
                } else {
                    // 如果有需要重试的主题，创建新的调用进行重试
                    final long now = time.milliseconds();
                    final Call call = getDeleteTopicsCall(options, futures, retryTopics,
                        retryTopicQuotaExceededExceptions, now, deadline);
                    runnable.call(call, now);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理由于配额超限导致的重试请求超时
                maybeCompleteQuotaExceededException(options.shouldRetryOnQuotaViolation(),
                    throwable, futures, quotaExceededExceptions, (int) (time.milliseconds() - now));
                // 将所有未完成的Future标记为失败
                completeAllExceptionally(futures.values(), throwable);
            }
        };
    }

    /**
     * 根据主题ID删除主题的内部调用方法
     * 
     * @param options 删除主题的配置选项
     * @param futures 用于跟踪每个主题删除操作结果的Future映射
     * @param topicIds 要删除的主题ID列表
     * @param quotaExceededExceptions 记录每个主题的配额超限异常
     * @param now 当前时间戳
     * @param deadline 操作截止时间
     * @return 封装了删除主题请求的Call对象
     */
    private Call getDeleteTopicsWithIdsCall(final DeleteTopicsOptions options,
                                            final Map<Uuid, KafkaFutureImpl<Void>> futures,
                                            final List<Uuid> topicIds,
                                            final Map<Uuid, ThrottlingQuotaExceededException> quotaExceededExceptions,
                                            final long now,
                                            final long deadline) {
        return new Call("deleteTopics", deadline, new ControllerNodeProvider()) {
            @Override
            DeleteTopicsRequest.Builder createRequest(int timeoutMs) {
                // 构建删除主题请求，将主题ID列表转换为DeleteTopicState对象列表
                return new DeleteTopicsRequest.Builder(
                        new DeleteTopicsRequestData()
                                .setTopics(topicIds.stream().map(
                                    topic -> new DeleteTopicState().setTopicId(topic)).collect(Collectors.toList()))
                                .setTimeoutMs(timeoutMs));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                // 检查控制器节点是否发生变更
                handleNotControllerError(abstractResponse);
                // 处理服务器对各个主题的响应
                final DeleteTopicsResponse response = (DeleteTopicsResponse) abstractResponse;
                final List<Uuid> retryTopics = new ArrayList<>();
                final Map<Uuid, ThrottlingQuotaExceededException> retryTopicQuotaExceededExceptions = new HashMap<>();
                
                // 遍历每个主题的删除结果
                for (DeletableTopicResult result : response.data().responses()) {
                    KafkaFutureImpl<Void> future = futures.get(result.topicId());
                    if (future == null) {
                        // 服务器响应中包含未知的主题ID
                        log.warn("Server response mentioned unknown topic ID {}", result.topicId());
                    } else {
                        ApiError error = new ApiError(result.errorCode(), result.errorMessage());
                        if (error.isFailure()) {
                            if (error.is(Errors.THROTTLING_QUOTA_EXCEEDED)) {
                                // 处理配额超限异常
                                ThrottlingQuotaExceededException quotaExceededException = new ThrottlingQuotaExceededException(
                                        response.throttleTimeMs(), error.messageWithFallback());
                                if (options.shouldRetryOnQuotaViolation()) {
                                    // 如果配置了重试，将主题加入重试列表
                                    retryTopics.add(result.topicId());
                                    retryTopicQuotaExceededExceptions.put(result.topicId(), quotaExceededException);
                                } else {
                                    // 否则直接标记操作失败
                                    future.completeExceptionally(quotaExceededException);
                                }
                            } else {
                                // 处理其他类型的错误
                                future.completeExceptionally(error.exception());
                            }
                        } else {
                            // 删除操作成功
                            future.complete(null);
                        }
                    }
                }
                
                // 处理需要重试的主题
                if (retryTopics.isEmpty()) {
                    // 服务器应该为每个主题返回响应，这里做个安全检查
                    completeUnrealizedFutures(futures.entrySet().stream(),
                        topic -> "The controller response did not contain a result for topic " + topic);
                } else {
                    // 重新发送删除请求
                    final long now = time.milliseconds();
                    final Call call = getDeleteTopicsWithIdsCall(options, futures, retryTopics,
                            retryTopicQuotaExceededExceptions, now, deadline);
                    runnable.call(call, now);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 如果之前有因配额超限而重试的主题，且请求超时，则将原始错误返回给调用方
                maybeCompleteQuotaExceededException(options.shouldRetryOnQuotaViolation(),
                        throwable, futures, quotaExceededExceptions, (int) (time.milliseconds() - now));
                // 将所有未完成的Future标记为失败
                completeAllExceptionally(futures.values(), throwable);
            }
        };
    }

    /**
     * 列出集群中的所有主题
     * 
     * @param options 列出主题的配置选项，如是否包含内部主题等
     * @return ListTopicsResult对象，包含主题列表的Future
     */
    @Override
    public ListTopicsResult listTopics(final ListTopicsOptions options) {
        // 创建一个Future用于返回主题列表结果
        final KafkaFutureImpl<Map<String, TopicListing>> topicListingFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("listTopics", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                // 创建获取所有主题元数据的请求
                return MetadataRequest.Builder.allTopics();
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                Map<String, TopicListing> topicListing = new HashMap<>();
                // 遍历响应中的主题元数据
                for (MetadataResponse.TopicMetadata topicMetadata : response.topicMetadata()) {
                    String topicName = topicMetadata.topic();
                    boolean isInternal = topicMetadata.isInternal();
                    // 根据配置选项决定是否包含内部主题
                    if (!topicMetadata.isInternal() || options.shouldListInternal())
                        topicListing.put(topicName, new TopicListing(topicName, topicMetadata.topicId(), isInternal));
                }
                // 完成Future，返回主题列表
                topicListingFuture.complete(topicListing);
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理异常情况
                topicListingFuture.completeExceptionally(throwable);
            }
        }, now);
        return new ListTopicsResult(topicListingFuture);
    }

    /**
     * 获取主题的详细信息，包括分区数、副本分配等
     * 
     * @param topics 要查询的主题集合，可以是主题ID集合或主题名称集合
     * @param options 查询主题的配置选项
     * @return DescribeTopicsResult对象，包含主题详细信息的Future
     * @throws IllegalArgumentException 如果提供的主题集合类型不支持
     */
    @Override
    public DescribeTopicsResult describeTopics(final TopicCollection topics, DescribeTopicsOptions options) {
        // 根据主题集合类型选择不同的处理方法
        if (topics instanceof TopicIdCollection)
            // 使用主题ID查询
            return DescribeTopicsResult.ofTopicIds(handleDescribeTopicsByIds(((TopicIdCollection) topics).topicIds(), options));
        else if (topics instanceof TopicNameCollection)
            // 使用主题名称查询
            return DescribeTopicsResult.ofTopicNames(handleDescribeTopicsByNamesWithDescribeTopicPartitionsApi(((TopicNameCollection) topics).topicNames(), options));
        else
            // 不支持的主题集合类型
            throw new IllegalArgumentException("The TopicCollection: " + topics + " provided did not match any supported classes for describeTopics.");
    }

    /**
     * 使用元数据API生成描述主题的调用
     * 
     * @param topicNamesList 需要描述的主题名称列表
     * @param topicFutures 存储每个主题对应的Future结果的Map
     * @param options 描述主题的选项配置
     * @param now 当前时间戳
     * @return 返回一个Call对象，用于执行主题描述请求
     */
    private Call generateDescribeTopicsCallWithMetadataApi(
        List<String> topicNamesList,
        Map<String, KafkaFutureImpl<TopicDescription>> topicFutures,
        DescribeTopicsOptions options,
        long now
    ) {
        return new Call("describeTopics", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            // 标记是否支持禁用自动创建主题的功能
            private boolean supportsDisablingTopicCreation = true;

            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                if (supportsDisablingTopicCreation)
                    // 如果支持禁用自动创建主题，创建一个禁用了自动创建主题的请求
                    return new MetadataRequest.Builder(new MetadataRequestData()
                        .setTopics(convertToMetadataRequestTopic(topicNamesList)) // 设置要查询的主题列表
                        .setAllowAutoTopicCreation(false) // 禁用自动创建主题
                        .setIncludeTopicAuthorizedOperations(options.includeAuthorizedOperations())); // 是否包含主题的授权操作信息
                else
                    // 如果不支持禁用自动创建主题，则请求所有主题的元数据
                    return MetadataRequest.Builder.allTopics();
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                // 从响应中构建集群信息
                Cluster cluster = response.buildCluster();
                Map<String, Errors> errors = response.errors();
                // 处理每个主题的响应结果
                for (Map.Entry<String, KafkaFutureImpl<TopicDescription>> entry : topicFutures.entrySet()) {
                    String topicName = entry.getKey();
                    KafkaFutureImpl<TopicDescription> future = entry.getValue();
                    Errors topicError = errors.get(topicName);
                    // 如果主题存在错误，完成Future并带上异常
                    if (topicError != null) {
                        future.completeExceptionally(topicError.exception());
                        continue;
                    }
                    // 如果主题不存在，完成Future并带上UnknownTopicOrPartitionException异常
                    if (!cluster.topics().contains(topicName)) {
                        future.completeExceptionally(new UnknownTopicOrPartitionException("Topic " + topicName + " not found."));
                        continue;
                    }
                    // 获取主题ID和授权操作信息
                    Uuid topicId = cluster.topicId(topicName);
                    Integer authorizedOperations = response.topicAuthorizedOperations(topicName).get();
                    // 从集群信息中构建主题描述对象
                    TopicDescription topicDescription = getTopicDescriptionFromCluster(cluster, topicName, topicId, authorizedOperations);
                    // 成功完成Future
                    future.complete(topicDescription);
                }
            }

            @Override
            boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
                // 如果遇到不支持的版本异常，且当前支持禁用自动创建主题
                if (supportsDisablingTopicCreation) {
                    // 将标志设置为false，表示不再尝试禁用自动创建主题
                    supportsDisablingTopicCreation = false;
                    return true; // 返回true表示需要重试请求
                }
                return false; // 返回false表示不需要重试
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理失败情况，将所有Future都标记为异常完成
                completeAllExceptionally(topicFutures.values(), throwable);
            }
        };
    }

    /**
     * 使用DescribeTopicPartitions API生成描述主题的调用
     * 该方法支持分页获取主题分区信息，可以处理大量分区的场景
     * 
     * @param topicNamesList 需要描述的主题名称列表
     * @param topicFutures 存储每个主题对应的Future结果的Map
     * @param nodes 集群中的节点信息，key为节点ID，value为Node对象
     * @param options 描述主题的选项配置
     * @param now 当前时间戳
     * @return 返回一个Call对象，用于执行主题描述请求
     */
    private Call generateDescribeTopicsCallWithDescribeTopicPartitionsApi(
        List<String> topicNamesList,
        Map<String, KafkaFutureImpl<TopicDescription>> topicFutures,
        Map<Integer, Node> nodes,
        DescribeTopicsOptions options,
        long now
    ) {
        // 创建一个有序的Map来存储主题请求，保证请求顺序
        final Map<String, TopicRequest> topicsRequests = new LinkedHashMap<>();
        topicNamesList.stream().sorted().forEach(topic ->
            topicsRequests.put(topic, new TopicRequest().setName(topic))
        );
        return new Call("describeTopicPartitions", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {
            // 用于存储当前正在处理但尚未完成的主题描述信息
            TopicDescription partiallyFinishedTopicDescription = null;

            @Override
            DescribeTopicPartitionsRequest.Builder createRequest(int timeoutMs) {
                // 创建请求数据对象
                DescribeTopicPartitionsRequestData request = new DescribeTopicPartitionsRequestData()
                    .setTopics(new ArrayList<>(topicsRequests.values())) // 设置要查询的主题列表
                    .setResponsePartitionLimit(options.partitionSizeLimitPerResponse()); // 设置每个响应中的分区数量限制
                if (partiallyFinishedTopicDescription != null) {
                    // 如果存在未完成的主题描述，设置游标信息
                    // 注意：如果前一个游标指向分区0，这里不会设置游标，而是将前一个游标主题作为请求中的第一个主题
                    request.setCursor(new DescribeTopicPartitionsRequestData.Cursor()
                        .setTopicName(partiallyFinishedTopicDescription.name())
                        .setPartitionIndex(partiallyFinishedTopicDescription.partitions().size())
                    );
                }
                return new DescribeTopicPartitionsRequest.Builder(request);
            }

            @SuppressWarnings("NPathComplexity")
            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                DescribeTopicPartitionsResponse response = (DescribeTopicPartitionsResponse) abstractResponse;
                // 获取响应中的游标信息，用于分页
                DescribeTopicPartitionsResponseData.Cursor responseCursor = response.data().nextCursor();
                // 当前批次中游标主题的描述信息
                TopicDescription nextTopicDescription = null;

                // 处理响应中的每个主题
                for (DescribeTopicPartitionsResponseTopic topic : response.data().topics()) {
                    String topicName = topic.name();
                    Errors error = Errors.forCode(topic.errorCode());

                    KafkaFutureImpl<TopicDescription> future = topicFutures.get(topicName);

                    // 处理错误情况
                    if (error != Errors.NONE) {
                        future.completeExceptionally(error.exception());
                        topicsRequests.remove(topicName);
                        if (responseCursor != null && responseCursor.topicName().equals(topicName)) {
                            responseCursor = null;
                        }
                        continue;
                    }

                    // 从响应中构建主题描述对象
                    TopicDescription currentTopicDescription = getTopicDescriptionFromDescribeTopicsResponseTopic(topic, nodes, options.includeAuthorizedOperations());

                    // 如果是上一批次未完成的主题，将新的分区信息添加到已有的描述中
                    if (partiallyFinishedTopicDescription != null && partiallyFinishedTopicDescription.name().equals(topicName)) {
                        partiallyFinishedTopicDescription.partitions().addAll(currentTopicDescription.partitions());
                        continue;
                    }

                    // 如果是当前批次的游标主题，缓存其描述信息
                    if (responseCursor != null && responseCursor.topicName().equals(topicName)) {
                        nextTopicDescription = currentTopicDescription;
                        continue;
                    }

                    // 完成主题的处理
                    topicsRequests.remove(topicName);
                    future.complete(currentTopicDescription);
                }

                // 处理未完成的主题描述
                if (partiallyFinishedTopicDescription != null &&
                        (responseCursor == null || !responseCursor.topicName().equals(partiallyFinishedTopicDescription.name()))) {
                    // 不能简单地通过检查nextTopicDescription != null来关闭partiallyFinishedTopicDescription
                    // 因为responseCursor主题可能不会出现在响应中
                    String topicName = partiallyFinishedTopicDescription.name();
                    topicFutures.get(topicName).complete(partiallyFinishedTopicDescription);
                    topicsRequests.remove(topicName);
                    partiallyFinishedTopicDescription = null;
                }
                // 更新未完成的主题描述
                if (nextTopicDescription != null) {
                    partiallyFinishedTopicDescription = nextTopicDescription;
                }

                // 如果还有未处理的主题，继续发送请求
                if (!topicsRequests.isEmpty()) {
                    runnable.call(this, time.milliseconds());
                }
            }

            @Override
            boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
                // 如果服务器不支持DescribeTopicPartitions API，回退到使用Metadata API
                final long now = time.milliseconds();
                runnable.call(generateDescribeTopicsCallWithMetadataApi(topicNamesList, topicFutures, options, now), now);
                return false; // 不需要重试当前请求
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理失败情况，但不处理UnsupportedVersionException（已在上面处理）
                if (!(throwable instanceof UnsupportedVersionException)) {
                    completeAllExceptionally(topicFutures.values(), throwable);
                }
            }
        };
    }

    /**
     * 使用DescribeTopicPartitionsApi处理通过主题名称获取主题描述的请求
     * 
     * @param topicNames 需要获取描述信息的主题名称集合
     * @param options 描述主题的选项配置
     * @return 主题名称到其描述信息Future的映射
     */
    private Map<String, KafkaFuture<TopicDescription>> handleDescribeTopicsByNamesWithDescribeTopicPartitionsApi(
        final Collection<String> topicNames,
        DescribeTopicsOptions options
    ) {
        // 为每个主题创建一个Future，用于异步返回结果
        final Map<String, KafkaFutureImpl<TopicDescription>> topicFutures = new HashMap<>(topicNames.size());
        final ArrayList<String> topicNamesList = new ArrayList<>();
        
        // 遍历所有主题名称，进行预处理
        for (String topicName : topicNames) {
            if (topicNameIsUnrepresentable(topicName)) {
                // 如果主题名称不合法，直接完成对应的Future并返回异常
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                    topicName + "' cannot be represented in a request."));
                topicFutures.put(topicName, future);
            } else if (!topicFutures.containsKey(topicName)) {
                // 如果主题名称合法且未处理过，创建对应的Future并添加到待处理列表
                topicFutures.put(topicName, new KafkaFutureImpl<>());
                topicNamesList.add(topicName);
            }
        }

        // 如果没有需要处理的主题，直接返回结果
        if (topicNamesList.isEmpty()) {
            return new HashMap<>(topicFutures);
        }

        // 首先获取集群节点信息
        DescribeClusterResult clusterResult = describeCluster();
        clusterResult.nodes().whenComplete(
            (nodes, exception) -> {
                if (exception != null) {
                    // 如果获取节点信息失败，将所有Future标记为异常完成
                    completeAllExceptionally(topicFutures.values(), exception);
                    return;
                }

                // 获取当前时间戳
                final long now = time.milliseconds();
                // 将节点列表转换为ID到节点的映射
                Map<Integer, Node> nodeIdMap = nodes.stream().collect(Collectors.toMap(Node::id, node -> node));
                // 生成并执行描述主题的请求
                runnable.call(
                    generateDescribeTopicsCallWithDescribeTopicPartitionsApi(topicNamesList, topicFutures, nodeIdMap, options, now),
                    now
                );
            });

        return new HashMap<>(topicFutures);
    }

    /**
     * 通过主题ID获取主题描述信息
     * 
     * @param topicIds 需要获取描述信息的主题ID集合
     * @param options 描述主题的选项配置
     * @return 主题ID到其描述信息Future的映射
     */
    private Map<Uuid, KafkaFuture<TopicDescription>> handleDescribeTopicsByIds(Collection<Uuid> topicIds, DescribeTopicsOptions options) {
        // 为每个主题ID创建一个Future，用于异步返回结果
        final Map<Uuid, KafkaFutureImpl<TopicDescription>> topicFutures = new HashMap<>(topicIds.size());
        final List<Uuid> topicIdsList = new ArrayList<>();
        
        // 遍历所有主题ID，进行预处理
        for (Uuid topicId : topicIds) {
            if (topicIdIsUnrepresentable(topicId)) {
                // 如果主题ID不合法，直接完成对应的Future并返回异常
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic id '" +
                        topicId + "' cannot be represented in a request."));
                topicFutures.put(topicId, future);
            } else if (!topicFutures.containsKey(topicId)) {
                // 如果主题ID合法且未处理过，创建对应的Future并添加到待处理列表
                topicFutures.put(topicId, new KafkaFutureImpl<>());
                topicIdsList.add(topicId);
            }
        }

        // 获取当前时间戳
        final long now = time.milliseconds();
        
        // 创建元数据请求调用对象
        Call call = new Call("describeTopicsWithIds", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {

            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                // 构建元数据请求，包含主题ID列表和授权操作信息
                return new MetadataRequest.Builder(new MetadataRequestData()
                        .setTopics(convertTopicIdsToMetadataRequestTopic(topicIdsList))
                        .setAllowAutoTopicCreation(false)
                        .setIncludeTopicAuthorizedOperations(options.includeAuthorizedOperations()));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                // 处理服务器响应
                MetadataResponse response = (MetadataResponse) abstractResponse;
                // 构建集群信息并获取错误信息
                Cluster cluster = response.buildCluster();
                Map<Uuid, Errors> errors = response.errorsByTopicId();
                
                // 处理每个主题的响应结果
                for (Map.Entry<Uuid, KafkaFutureImpl<TopicDescription>> entry : topicFutures.entrySet()) {
                    Uuid topicId = entry.getKey();
                    KafkaFutureImpl<TopicDescription> future = entry.getValue();

                    // 获取主题名称
                    String topicName = cluster.topicName(topicId);
                    if (topicName == null) {
                        // 如果找不到主题名称，返回未知主题ID异常
                        future.completeExceptionally(new UnknownTopicIdException("TopicId " + topicId + " not found."));
                        continue;
                    }
                    
                    // 检查是否有主题级别的错误
                    Errors topicError = errors.get(topicId);
                    if (topicError != null) {
                        future.completeExceptionally(topicError.exception());
                        continue;
                    }

                    // 获取授权操作信息并构建主题描述对象
                    Integer authorizedOperations = response.topicAuthorizedOperations(topicName).get();
                    TopicDescription topicDescription = getTopicDescriptionFromCluster(cluster, topicName, topicId, authorizedOperations);
                    future.complete(topicDescription);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理请求失败的情况，将所有Future标记为异常完成
                completeAllExceptionally(topicFutures.values(), throwable);
            }
        };
        
        // 如果有需要处理的主题ID，执行请求
        if (!topicIdsList.isEmpty()) {
            runnable.call(call, now);
        }
        return new HashMap<>(topicFutures);
    }

    /**
     * 从DescribeTopicPartitionsResponse中解析主题描述信息
     * 
     * @param topic 主题响应数据
     * @param nodes 节点ID到节点对象的映射
     * @param includeAuthorizedOperations 是否包含授权操作信息
     * @return 主题的描述信息对象
     */
    private TopicDescription getTopicDescriptionFromDescribeTopicsResponseTopic(
        DescribeTopicPartitionsResponseTopic topic,
        Map<Integer, Node> nodes,
        boolean includeAuthorizedOperations
    ) {
        // 获取分区信息列表
        List<DescribeTopicPartitionsResponsePartition> partitionInfos = topic.partitions();
        List<TopicPartitionInfo> partitions = new ArrayList<>(partitionInfos.size());
        
        // 将每个分区的响应数据转换为TopicPartitionInfo对象
        for (DescribeTopicPartitionsResponsePartition partitionInfo : partitionInfos) {
            partitions.add(DescribeTopicPartitionsResponse.partitionToTopicPartitionInfo(partitionInfo, nodes));
        }
        
        // 获取授权操作信息（如果需要）
        Set<AclOperation> authorisedOperations = includeAuthorizedOperations ? validAclOperations(topic.topicAuthorizedOperations()) : null;
        
        // 创建并返回主题描述对象
        return new TopicDescription(topic.name(), topic.isInternal(), partitions, authorisedOperations, topic.topicId());
    }

    /**
     * 从集群元数据中构建主题描述信息
     * 
     * @param cluster 集群元数据
     * @param topicName 主题名称
     * @param topicId 主题ID
     * @param authorizedOperations 授权操作信息
     * @return 主题的描述信息对象
     */
    private TopicDescription getTopicDescriptionFromCluster(Cluster cluster, String topicName, Uuid topicId,
                                                            Integer authorizedOperations) {
        // 判断是否为内部主题
        boolean isInternal = cluster.internalTopics().contains(topicName);
        
        // 获取主题的所有分区信息
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topicName);
        List<TopicPartitionInfo> partitions = new ArrayList<>(partitionInfos.size());
        
        // 将每个分区信息转换为TopicPartitionInfo对象
        for (PartitionInfo partitionInfo : partitionInfos) {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(
                    partitionInfo.partition(), // 分区号
                    leader(partitionInfo),      // leader副本
                    Arrays.asList(partitionInfo.replicas()), // 所有副本列表
                    Arrays.asList(partitionInfo.inSyncReplicas())); // 同步副本列表
            partitions.add(topicPartitionInfo);
        }
        
        // 按分区号排序
        partitions.sort(Comparator.comparingInt(TopicPartitionInfo::partition));
        
        // 创建并返回主题描述对象
        return new TopicDescription(topicName, isInternal, partitions, validAclOperations(authorizedOperations), topicId);
    }

    /**
     * 获取分区的leader节点
     * 
     * @param partitionInfo 分区信息
     * @return leader节点，如果没有leader或leader是无效节点则返回null
     */
    private Node leader(PartitionInfo partitionInfo) {
        // 检查leader是否存在且是否为有效节点
        if (partitionInfo.leader() == null || partitionInfo.leader().id() == Node.noNode().id())
            return null;
        return partitionInfo.leader();
    }

    /**
     * 描述Kafka集群的信息，包括：
     * 1. 集群中的节点列表
     * 2. 控制器节点信息
     * 3. 集群ID
     * 4. 授权的操作列表
     * 
     * 实现说明：
     * - 支持两种请求方式：DescribeCluster请求(新版本)和Metadata请求(旧版本)
     * - 优先使用DescribeCluster请求，如果版本不支持则降级使用Metadata请求
     * - 处理响应时会分别填充四个Future对象，包含不同维度的集群信息
     * 
     * 应用场景：
     * - 监控集群状态
     * - 获取集群拓扑信息
     * - 检查客户端权限
     */
    @Override
    public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        // 创建四个Future对象用于异步返回不同类型的结果
        final KafkaFutureImpl<Collection<Node>> describeClusterFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<Node> controllerFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<String> clusterIdFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<Set<AclOperation>> authorizedOperationsFuture = new KafkaFutureImpl<>();

        // 获取当前时间戳，用于计算请求超时时间
        final long now = time.milliseconds();
        runnable.call(new Call("listNodes", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedBrokerOrActiveKController()) {

            // 标记是否使用Metadata请求，用于版本兼容处理
            private boolean useMetadataRequest = false;

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                if (!useMetadataRequest) {
                    // 检查是否可以从控制器获取已停止的broker信息
                    if (metadataManager.usingBootstrapControllers() && options.includeFencedBrokers()) {
                        throw new IllegalArgumentException("Cannot request fenced brokers from controller endpoint");
                    }
                    // 创建DescribeCluster请求，设置是否包含授权操作和已停止的broker
                    return new DescribeClusterRequest.Builder(new DescribeClusterRequestData()
                        .setIncludeClusterAuthorizedOperations(options.includeAuthorizedOperations())
                        .setEndpointType(metadataManager.usingBootstrapControllers() ?
                                EndpointType.CONTROLLER.id() : EndpointType.BROKER.id())
                        .setIncludeFencedBrokers(options.includeFencedBrokers()));
                } else {
                    // 降级使用Metadata请求，这种方式只获取节点信息
                    // 由于不涉及创建主题，allowAutoTopicCreation可以安全地设置为true
                    return new MetadataRequest.Builder(new MetadataRequestData()
                        .setTopics(Collections.emptyList())
                        .setAllowAutoTopicCreation(true)
                        .setIncludeClusterAuthorizedOperations(
                            options.includeAuthorizedOperations()));
                }
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                if (!useMetadataRequest) {
                    // 处理DescribeCluster响应
                    DescribeClusterResponse response = (DescribeClusterResponse) abstractResponse;
                    Errors error = Errors.forCode(response.data().errorCode());
                    if (error != Errors.NONE) {
                        // 如果响应包含错误，将异常传递给所有Future
                        ApiError apiError = new ApiError(error, response.data().errorMessage());
                        handleFailure(apiError.exception());
                        return;
                    }

                    // 获取节点信息并完成对应的Future
                    Map<Integer, Node> nodes = response.nodes();
                    describeClusterFuture.complete(nodes.values());
                    // 如果controllerId为NO_CONTROLLER_ID，则controller为null
                    controllerFuture.complete(nodes.get(response.data().controllerId()));
                    clusterIdFuture.complete(response.data().clusterId());
                    authorizedOperationsFuture.complete(
                        validAclOperations(response.data().clusterAuthorizedOperations()));
                } else {
                    // 处理Metadata响应
                    MetadataResponse response = (MetadataResponse) abstractResponse;
                    describeClusterFuture.complete(response.brokers());
                    controllerFuture.complete(controller(response));
                    clusterIdFuture.complete(response.clusterId());
                    authorizedOperationsFuture.complete(
                        validAclOperations(response.clusterAuthorizedOperations()));
                }
            }

            // 从Metadata响应中提取controller节点信息
            private Node controller(MetadataResponse response) {
                if (response.controller() == null || response.controller().id() == MetadataResponse.NO_CONTROLLER_ID)
                    return null;
                return response.controller();
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 发生异常时，将异常传递给所有Future
                describeClusterFuture.completeExceptionally(throwable);
                controllerFuture.completeExceptionally(throwable);
                clusterIdFuture.completeExceptionally(throwable);
                authorizedOperationsFuture.completeExceptionally(throwable);
            }

            @Override
            boolean handleUnsupportedVersionException(final UnsupportedVersionException exception) {
                // 处理版本不兼容的情况
                if (metadataManager.usingBootstrapControllers()) {
                    return false;
                }
                if (useMetadataRequest) {
                    return false;
                }

                // 如果是因为includeFencedBrokers选项导致的版本不兼容(仅在版本2+支持)
                // 则不降级使用Metadata请求
                if (options.includeFencedBrokers()) {
                    return false;
                }

                // 降级使用Metadata请求
                useMetadataRequest = true;
                return true;
            }
        }, now);

        // 返回包含所有Future的结果对象
        return new DescribeClusterResult(describeClusterFuture, controllerFuture, clusterIdFuture,
            authorizedOperationsFuture);
    }

    /**
     * 描述指定过滤条件的ACL(访问控制列表)信息
     * 
     * 实现说明：
     * - 首先验证过滤条件的有效性
     * - 创建异步请求获取ACL信息
     * - 处理响应并返回结果
     * 
     * 应用场景：
     * - 查询特定资源的访问权限配置
     * - 审计安全策略
     * - 权限管理
     */
    @Override
    public DescribeAclsResult describeAcls(final AclBindingFilter filter, DescribeAclsOptions options) {
        // 检查过滤条件是否包含未知元素
        if (filter.isUnknown()) {
            KafkaFutureImpl<Collection<AclBinding>> future = new KafkaFutureImpl<>();
            future.completeExceptionally(new InvalidRequestException("The AclBindingFilter " +
                    "must not contain UNKNOWN elements."));
            return new DescribeAclsResult(future);
        }
        
        // 获取当前时间戳，用于计算请求超时时间
        final long now = time.milliseconds();
        final KafkaFutureImpl<Collection<AclBinding>> future = new KafkaFutureImpl<>();
        
        // 创建并发送DescribeAcls请求
        runnable.call(new Call("describeAcls", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedBrokerOrActiveKController()) {

            @Override
            DescribeAclsRequest.Builder createRequest(int timeoutMs) {
                // 创建请求，设置ACL过滤条件
                return new DescribeAclsRequest.Builder(filter);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                // 处理响应
                DescribeAclsResponse response = (DescribeAclsResponse) abstractResponse;
                if (response.error().isFailure()) {
                    // 如果响应包含错误，将异常传递给Future
                    future.completeExceptionally(response.error().exception());
                } else {
                    // 成功获取ACL信息，完成Future
                    future.complete(DescribeAclsResponse.aclBindings(response.acls()));
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 发生异常时，将异常传递给Future
                future.completeExceptionally(throwable);
            }
        }, now);
        return new DescribeAclsResult(future);
    }

    /**
     * 创建ACL(访问控制列表)规则
     * 
     * @param acls 要创建的ACL规则集合
     * @param options 创建ACL的选项配置
     * @return CreateAclsResult 包含每个ACL创建操作的Future结果
     */
    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        // 获取当前时间戳,用于计算请求超时
        final long now = time.milliseconds();
        // 存储每个ACL绑定对应的Future结果
        final Map<AclBinding, KafkaFutureImpl<Void>> futures = new HashMap<>();
        // 存储要创建的ACL规则列表
        final List<AclCreation> aclCreations = new ArrayList<>();
        // 存储已发送的ACL绑定列表
        final List<AclBinding> aclBindingsSent = new ArrayList<>();

        // 遍历处理每个ACL规则
        for (AclBinding acl : acls) {
            if (futures.get(acl) == null) { // 避免重复处理相同的ACL
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                futures.put(acl, future);
                // 检查ACL规则是否有不确定的字段
                String indefinite = acl.toFilter().findIndefiniteField();
                if (indefinite == null) {
                    // ACL规则有效,添加到待创建列表
                    aclCreations.add(CreateAclsRequest.aclCreation(acl));
                    aclBindingsSent.add(acl);
                } else {
                    // ACL规则无效,将对应的Future标记为异常完成
                    future.completeExceptionally(new InvalidRequestException("Invalid ACL creation: " +
                        indefinite));
                }
            }
        }

        // 构造创建ACL的请求数据
        final CreateAclsRequestData data = new CreateAclsRequestData().setCreations(aclCreations);

        // 发送创建ACL的请求
        runnable.call(new Call("createAcls", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedBrokerOrActiveKController()) {

            @Override
            CreateAclsRequest.Builder createRequest(int timeoutMs) {
                return new CreateAclsRequest.Builder(data);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                // 处理非Controller节点的错误
                handleNotControllerError(abstractResponse);
                CreateAclsResponse response = (CreateAclsResponse) abstractResponse;
                List<AclCreationResult> responses = response.results();
                Iterator<AclCreationResult> iter = responses.iterator();

                // 处理每个ACL的创建结果
                for (AclBinding aclBinding : aclBindingsSent) {
                    KafkaFutureImpl<Void> future = futures.get(aclBinding);
                    if (!iter.hasNext()) {
                        // 服务器没有返回该ACL的创建结果
                        future.completeExceptionally(new UnknownServerException(
                            "The broker reported no creation result for the given ACL: " + aclBinding));
                    } else {
                        // 处理创建结果
                        AclCreationResult creation = iter.next();
                        Errors error = Errors.forCode(creation.errorCode());
                        ApiError apiError = new ApiError(error, creation.errorMessage());
                        if (apiError.isFailure())
                            future.completeExceptionally(apiError.exception());
                        else
                            future.complete(null);
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理整体失败情况,将所有Future标记为异常完成
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);

        return new CreateAclsResult(new HashMap<>(futures));
    }

    /**
     * 删除ACL(访问控制列表)规则
     * 
     * @param filters ACL过滤器集合,用于匹配要删除的ACL规则
     * @param options 删除ACL的选项配置
     * @return DeleteAclsResult 包含每个过滤器匹配到的ACL删除结果
     */
    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
        // 获取当前时间戳,用于计算请求超时
        final long now = time.milliseconds();
        // 存储每个过滤器对应的Future结果
        final Map<AclBindingFilter, KafkaFutureImpl<FilterResults>> futures = new HashMap<>();
        // 存储已发送的过滤器列表
        final List<AclBindingFilter> aclBindingFiltersSent = new ArrayList<>();
        // 存储要删除的ACL过滤器列表
        final List<DeleteAclsFilter> deleteAclsFilters = new ArrayList<>();

        // 遍历处理每个ACL过滤器
        for (AclBindingFilter filter : filters) {
            if (futures.get(filter) == null) { // 避免重复处理相同的过滤器
                aclBindingFiltersSent.add(filter);
                deleteAclsFilters.add(DeleteAclsRequest.deleteAclsFilter(filter));
                futures.put(filter, new KafkaFutureImpl<>());
            }
        }

        // 构造删除ACL的请求数据
        final DeleteAclsRequestData data = new DeleteAclsRequestData().setFilters(deleteAclsFilters);

        // 发送删除ACL的请求
        runnable.call(new Call("deleteAcls", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedBrokerOrActiveKController()) {

            @Override
            DeleteAclsRequest.Builder createRequest(int timeoutMs) {
                return new DeleteAclsRequest.Builder(data);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                // 处理非Controller节点的错误
                handleNotControllerError(abstractResponse);
                DeleteAclsResponse response = (DeleteAclsResponse) abstractResponse;
                List<DeleteAclsResponseData.DeleteAclsFilterResult> results = response.filterResults();
                Iterator<DeleteAclsResponseData.DeleteAclsFilterResult> iter = results.iterator();

                // 处理每个过滤器的删除结果
                for (AclBindingFilter bindingFilter : aclBindingFiltersSent) {
                    KafkaFutureImpl<FilterResults> future = futures.get(bindingFilter);
                    if (!iter.hasNext()) {
                        // 服务器没有返回该过滤器的删除结果
                        future.completeExceptionally(new UnknownServerException(
                            "The broker reported no deletion result for the given filter."));
                    } else {
                        // 处理删除结果
                        DeleteAclsFilterResult filterResult = iter.next();
                        ApiError error = new ApiError(Errors.forCode(filterResult.errorCode()), filterResult.errorMessage());
                        if (error.isFailure()) {
                            // 删除操作整体失败
                            future.completeExceptionally(error.exception());
                        } else {
                            // 处理每个匹配的ACL的删除结果
                            List<FilterResult> filterResults = new ArrayList<>();
                            for (DeleteAclsMatchingAcl matchingAcl : filterResult.matchingAcls()) {
                                ApiError aclError = new ApiError(Errors.forCode(matchingAcl.errorCode()),
                                    matchingAcl.errorMessage());
                                AclBinding aclBinding = DeleteAclsResponse.aclBinding(matchingAcl);
                                filterResults.add(new FilterResult(aclBinding, aclError.exception()));
                            }
                            future.complete(new FilterResults(filterResults));
                        }
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理整体失败情况,将所有Future标记为异常完成
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);

        return new DeleteAclsResult(new HashMap<>(futures));
    }

    /**
     * 获取配置资源的详细配置信息
     * 
     * @param configResources 要查询的配置资源集合
     * @param options 查询配置的选项配置
     * @return DescribeConfigsResult 包含每个配置资源的Future结果
     */
    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> configResources, final DescribeConfigsOptions options) {
        // 根据配置资源所属的broker节点进行分组
        // null broker表示可以从任意broker获取的配置资源
        final Map<Integer, Map<ConfigResource, KafkaFutureImpl<Config>>> nodeFutures = new HashMap<>(configResources.size());

        // 遍历每个配置资源,确定其所属的broker节点
        for (ConfigResource resource : configResources) {
            Integer broker = nodeFor(resource);
            nodeFutures.compute(broker, (key, value) -> {
                if (value == null) {
                    value = new HashMap<>();
                }
                value.put(resource, new KafkaFutureImpl<>());
                return value;
            });
        }

        // 获取当前时间戳,用于计算请求超时
        final long now = time.milliseconds();
        
        // 对每个broker节点发送查询配置请求
        for (Map.Entry<Integer, Map<ConfigResource, KafkaFutureImpl<Config>>> entry : nodeFutures.entrySet()) {
            final Integer node = entry.getKey();
            Map<ConfigResource, KafkaFutureImpl<Config>> unified = entry.getValue();

            runnable.call(new Call("describeConfigs", calcDeadlineMs(now, options.timeoutMs()),
                node != null ? new ConstantNodeIdProvider(node, true) : new LeastLoadedBrokerOrActiveKController()) {

                @Override
                DescribeConfigsRequest.Builder createRequest(int timeoutMs) {
                    // 构造查询配置的请求数据
                    return new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
                        .setResources(unified.keySet().stream()
                            .map(config ->
                                new DescribeConfigsRequestData.DescribeConfigsResource()
                                    .setResourceName(config.name())
                                    .setResourceType(config.type().id())
                                    .setConfigurationKeys(null))
                            .collect(Collectors.toList()))
                        .setIncludeSynonyms(options.includeSynonyms()) // 是否包含同义词配置
                        .setIncludeDocumentation(options.includeDocumentation())); // 是否包含配置文档
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    DescribeConfigsResponse response = (DescribeConfigsResponse) abstractResponse;
                    // 处理每个配置资源的查询结果
                    for (Map.Entry<ConfigResource, DescribeConfigsResponseData.DescribeConfigsResult> entry : response.resultMap().entrySet()) {
                        ConfigResource configResource = entry.getKey();
                        DescribeConfigsResponseData.DescribeConfigsResult describeConfigsResult = entry.getValue();
                        KafkaFutureImpl<Config> future = unified.get(configResource);
                        if (future == null) {
                            // 响应中包含未请求的配置资源
                            if (node != null) {
                                log.warn("The config {} in the response from node {} is not in the request",
                                        configResource, node);
                            } else {
                                log.warn("The config {} in the response from the least loaded broker is not in the request",
                                        configResource);
                            }
                        } else {
                            // 处理查询结果
                            if (describeConfigsResult.errorCode() != Errors.NONE.code()) {
                                future.completeExceptionally(Errors.forCode(describeConfigsResult.errorCode())
                                        .exception(describeConfigsResult.errorMessage()));
                            } else {
                                future.complete(describeConfigResult(describeConfigsResult));
                            }
                        }
                    }
                    // 处理未收到响应的配置资源
                    completeUnrealizedFutures(
                        unified.entrySet().stream(),
                        configResource -> "The node response did not contain a result for config resource " + configResource);
                }

                @Override
                void handleFailure(Throwable throwable) {
                    // 处理整体失败情况,将所有Future标记为异常完成
                    completeAllExceptionally(unified.values(), throwable);
                }
            }, now);
        }

        // 将所有节点的Future结果合并到一个Map中返回
        return new DescribeConfigsResult(
            nodeFutures.entrySet()
                .stream()
                .flatMap(x -> x.getValue().entrySet().stream()) // 展开所有节点的结果
                .collect(Collectors.toMap(
                    Map.Entry::getKey,  // 使用ConfigResource作为key
                    Map.Entry::getValue, // 使用对应的Future作为value
                    (oldValue, newValue) -> {
                        // 如果出现重复的key,说明配置出现了冲突,抛出异常
                        throw new IllegalStateException(String.format("Duplicate key for values: %s and %s", oldValue, newValue));
                    },
                    HashMap::new // 使用HashMap存储结果
                ))
        );
    }

    /**
     * 将DescribeConfigsResult响应数据转换为Config对象
     * 
     * @param describeConfigsResult 配置描述结果数据
     * @return 转换后的Config对象,包含所有配置项
     */
    private Config describeConfigResult(DescribeConfigsResponseData.DescribeConfigsResult describeConfigsResult) {
        return new Config(describeConfigsResult.configs().stream().map(config -> new ConfigEntry(
                config.name(),  // 配置项名称
                config.value(), // 配置项值
                DescribeConfigsResponse.ConfigSource.forId(config.configSource()).source(), // 配置来源
                config.isSensitive(), // 是否敏感配置
                config.readOnly(),    // 是否只读
                // 转换配置同义词列表
                (config.synonyms().stream().map(synonym -> new ConfigEntry.ConfigSynonym(synonym.name(), synonym.value(),
                        DescribeConfigsResponse.ConfigSource.forId(synonym.source()).source()))).collect(Collectors.toList()),
                DescribeConfigsResponse.ConfigType.forId(config.configType()).type(), // 配置类型
                config.documentation() // 配置文档
        )).collect(Collectors.toList()));
    }

    /**
     * 将服务端的配置源类型转换为客户端的配置源类型
     * 
     * @param source 服务端配置源类型
     * @return 客户端配置源类型
     */
    private ConfigEntry.ConfigSource configSource(DescribeConfigsResponse.ConfigSource source) {
        ConfigEntry.ConfigSource configSource;
        switch (source) {
            case TOPIC_CONFIG: // 主题级别的动态配置
                configSource = ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG;
                break;
            case DYNAMIC_BROKER_CONFIG: // Broker级别的动态配置
                configSource = ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG;
                break;
            case DYNAMIC_DEFAULT_BROKER_CONFIG: // Broker默认的动态配置
                configSource = ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG;
                break;
            case STATIC_BROKER_CONFIG: // Broker的静态配置
                configSource = ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG;
                break;
            case DYNAMIC_BROKER_LOGGER_CONFIG: // Broker日志相关的动态配置
                configSource = ConfigEntry.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG;
                break;
            case DEFAULT_CONFIG: // 默认配置
                configSource = ConfigEntry.ConfigSource.DEFAULT_CONFIG;
                break;
            default:
                throw new IllegalArgumentException("Unexpected config source " + source);
        }
        return configSource;
    }

    /**
     * 增量修改配置的实现方法
     * 
     * @param configs 要修改的配置资源及其操作集合
     * @param options 配置修改选项
     * @return 修改结果
     */
    @Override
    public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs,
                                                      final AlterConfigsOptions options) {
        final Map<ConfigResource, KafkaFutureImpl<Void>> allFutures = new HashMap<>();
        // BROKER_LOGGER请求总是发送到特定的broker或controller节点
        //
        // 特定BROKER资源的变更请求:
        // - 使用bootstrap.servers时发送到对应的节点
        // - 使用bootstrap.controllers时直接发送到活跃的controller
        //
        // 其他所有请求:
        // - 使用bootstrap.servers时发送到负载最小的broker
        // - 使用bootstrap.controllers时发送到活跃的controller
        final Collection<ConfigResource> unifiedRequestResources = new ArrayList<>();

        // 遍历所有配置资源,确定请求发送的目标节点
        for (ConfigResource resource : configs.keySet()) {
            Integer node = nodeFor(resource); // 获取资源对应的目标节点
            if (metadataManager.usingBootstrapControllers()) {
                // 使用controller模式时,只有BROKER_LOGGER类型保留特定节点
                if (!resource.type().equals(ConfigResource.Type.BROKER_LOGGER)) {
                    node = null;
                }
            }
            if (node != null) {
                // 发送到特定节点
                NodeProvider nodeProvider = new ConstantNodeIdProvider(node, true);
                allFutures.putAll(incrementalAlterConfigs(configs, options, Collections.singleton(resource), nodeProvider));
            } else {
                // 添加到统一请求资源列表
                unifiedRequestResources.add(resource);
            }
        }
        
        // 处理需要统一发送的请求
        if (!unifiedRequestResources.isEmpty()) {
            allFutures.putAll(incrementalAlterConfigs(configs, options, unifiedRequestResources, 
                new LeastLoadedBrokerOrActiveKController()));
        }

        return new AlterConfigsResult(new HashMap<>(allFutures));
    }

    /**
     * 执行增量配置修改的内部方法
     * 
     * @param configs 要修改的配置资源及其操作集合
     * @param options 配置修改选项
     * @param resources 本次请求包含的资源集合
     * @param nodeProvider 目标节点提供者
     * @return 每个资源的修改结果Future
     */
    private Map<ConfigResource, KafkaFutureImpl<Void>> incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs,
                                                                               final AlterConfigsOptions options,
                                                                               Collection<ConfigResource> resources,
                                                                               NodeProvider nodeProvider) {
        // 为每个资源创建对应的Future
        final Map<ConfigResource, KafkaFutureImpl<Void>> futures = new HashMap<>();
        for (ConfigResource resource : resources)
            futures.put(resource, new KafkaFutureImpl<>());

        final long now = time.milliseconds();
        // 创建并执行RPC调用
        runnable.call(new Call("incrementalAlterConfigs", calcDeadlineMs(now, options.timeoutMs()), nodeProvider) {

            @Override
            public IncrementalAlterConfigsRequest.Builder createRequest(int timeoutMs) {
                // 创建增量配置修改请求
                return new IncrementalAlterConfigsRequest.Builder(resources, configs, options.shouldValidateOnly());
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                // 处理非controller错误
                handleNotControllerError(abstractResponse);
                IncrementalAlterConfigsResponse response = (IncrementalAlterConfigsResponse) abstractResponse;
                // 解析响应中的错误信息
                Map<ConfigResource, ApiError> errors = IncrementalAlterConfigsResponse.fromResponseData(response.data());
                // 处理每个资源的结果
                for (Map.Entry<ConfigResource, KafkaFutureImpl<Void>> entry : futures.entrySet()) {
                    KafkaFutureImpl<Void> future = entry.getValue();
                    ApiException exception = errors.get(entry.getKey()).exception();
                    if (exception != null) {
                        // 如果有错误,完成Future并携带异常
                        future.completeExceptionally(exception);
                    } else {
                        // 成功完成Future
                        future.complete(null);
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理整体调用失败的情况
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);
        return futures;
    }

    /**
     * 修改副本日志目录的位置
     * 
     * @param replicaAssignment 副本到目标日志目录的映射关系，key为主题分区副本，value为目标日志目录路径
     * @param options 操作的配置选项，如超时时间等
     * @return 修改操作的结果，包含每个副本的操作状态
     */
    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment, final AlterReplicaLogDirsOptions options) {
        // 为每个副本创建一个Future对象来跟踪操作结果
        final Map<TopicPartitionReplica, KafkaFutureImpl<Void>> futures = new HashMap<>(replicaAssignment.size());

        // 初始化所有副本的Future
        for (TopicPartitionReplica replica : replicaAssignment.keySet())
            futures.put(replica, new KafkaFutureImpl<>());

        // 按broker分组构建请求数据
        Map<Integer, AlterReplicaLogDirsRequestData> replicaAssignmentByBroker = new HashMap<>();
        for (Map.Entry<TopicPartitionReplica, String> entry: replicaAssignment.entrySet()) {
            TopicPartitionReplica replica = entry.getKey();
            String logDir = entry.getValue();
            int brokerId = replica.brokerId();
            // 获取或创建broker的请求数据对象
            AlterReplicaLogDirsRequestData value = replicaAssignmentByBroker.computeIfAbsent(brokerId,
                key -> new AlterReplicaLogDirsRequestData());
            // 查找或创建日志目录对象
            AlterReplicaLogDir alterReplicaLogDir = value.dirs().find(logDir);
            if (alterReplicaLogDir == null) {
                alterReplicaLogDir = new AlterReplicaLogDir();
                alterReplicaLogDir.setPath(logDir);
                value.dirs().add(alterReplicaLogDir);
            }
            // 查找或创建主题对象
            AlterReplicaLogDirTopic alterReplicaLogDirTopic = alterReplicaLogDir.topics().find(replica.topic());
            if (alterReplicaLogDirTopic == null) {
                alterReplicaLogDirTopic = new AlterReplicaLogDirTopic().setName(replica.topic());
                alterReplicaLogDir.topics().add(alterReplicaLogDirTopic);
            }
            // 添加分区信息
            alterReplicaLogDirTopic.partitions().add(replica.partition());
        }

        // 记录当前时间戳
        final long now = time.milliseconds();
        // 对每个broker发送修改请求
        for (Map.Entry<Integer, AlterReplicaLogDirsRequestData> entry: replicaAssignmentByBroker.entrySet()) {
            final int brokerId = entry.getKey();
            final AlterReplicaLogDirsRequestData assignment = entry.getValue();

            // 创建并发送请求
            runnable.call(new Call("alterReplicaLogDirs", calcDeadlineMs(now, options.timeoutMs()),
                new ConstantNodeIdProvider(brokerId)) {

                @Override
                public AlterReplicaLogDirsRequest.Builder createRequest(int timeoutMs) {
                    return new AlterReplicaLogDirsRequest.Builder(assignment);
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    AlterReplicaLogDirsResponse response = (AlterReplicaLogDirsResponse) abstractResponse;
                    // 处理每个主题的响应结果
                    for (AlterReplicaLogDirTopicResult topicResult: response.data().results()) {
                        // 处理每个分区的响应结果
                        for (AlterReplicaLogDirPartitionResult partitionResult: topicResult.partitions()) {
                            TopicPartitionReplica replica = new TopicPartitionReplica(
                                    topicResult.topicName(), partitionResult.partitionIndex(), brokerId);
                            KafkaFutureImpl<Void> future = futures.get(replica);
                            if (future == null) {
                                // 如果响应中的分区不在请求中，记录警告
                                log.warn("The partition {} in the response from broker {} is not in the request",
                                        new TopicPartition(topicResult.topicName(), partitionResult.partitionIndex()),
                                        brokerId);
                            } else if (partitionResult.errorCode() == Errors.NONE.code()) {
                                // 操作成功，完成Future
                                future.complete(null);
                            } else {
                                // 操作失败，用异常完成Future
                                future.completeExceptionally(Errors.forCode(partitionResult.errorCode()).exception());
                            }
                        }
                    }
                    // 检查是否所有请求的副本都收到了响应
                    completeUnrealizedFutures(
                        futures.entrySet().stream().filter(entry -> entry.getKey().brokerId() == brokerId),
                        replica -> "The response from broker " + brokerId +
                                " did not contain a result for replica " + replica);
                }
                
                @Override
                void handleFailure(Throwable throwable) {
                    // 处理请求失败，仅完成该broker上的副本的Future
                    completeAllExceptionally(
                        futures.entrySet().stream()
                            .filter(entry -> entry.getKey().brokerId() == brokerId)
                            .map(Map.Entry::getValue),
                        throwable);
                }
            }, now);
        }

        return new AlterReplicaLogDirsResult(new HashMap<>(futures));
    }

    /**
     * 查询指定broker上的日志目录信息
     * 
     * @param brokers 要查询的broker ID列表
     * @param options 操作的配置选项，如超时时间等
     * @return 查询结果，包含每个broker上所有日志目录的详细信息
     */
    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
        // 为每个broker创建一个Future对象来跟踪查询结果
        final Map<Integer, KafkaFutureImpl<Map<String, LogDirDescription>>> futures = new HashMap<>(brokers.size());

        final long now = time.milliseconds();
        // 对每个broker发送查询请求
        for (final Integer brokerId : brokers) {
            // 创建该broker的Future对象
            KafkaFutureImpl<Map<String, LogDirDescription>> future = new KafkaFutureImpl<>();
            futures.put(brokerId, future);

            // 创建并发送请求
            runnable.call(new Call("describeLogDirs", calcDeadlineMs(now, options.timeoutMs()),
                new ConstantNodeIdProvider(brokerId)) {

                @Override
                public DescribeLogDirsRequest.Builder createRequest(int timeoutMs) {
                    // 查询所有日志目录中的分区信息，setTopics(null)表示查询所有主题
                    return new DescribeLogDirsRequest.Builder(new DescribeLogDirsRequestData().setTopics(null));
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    DescribeLogDirsResponse response = (DescribeLogDirsResponse) abstractResponse;
                    // 解析响应数据，获取日志目录描述信息
                    Map<String, LogDirDescription> descriptions = logDirDescriptions(response);
                    if (!descriptions.isEmpty()) {
                        // 如果成功获取到日志目录信息，完成Future
                        future.complete(descriptions);
                    } else {
                        // 处理错误情况：
                        // 在v3版本之前的DescribeLogDirsResponse没有错误码字段，默认为NONE
                        // 如果错误码为NONE，说明可能是授权失败
                        // 否则，使用响应中的错误码创建异常
                        Errors error = response.data().errorCode() == Errors.NONE.code()
                                ? Errors.CLUSTER_AUTHORIZATION_FAILED
                                : Errors.forCode(response.data().errorCode());
                        future.completeExceptionally(error.exception());
                    }
                }
                
                @Override
                void handleFailure(Throwable throwable) {
                    // 处理请求失败的情况
                    future.completeExceptionally(throwable);
                }
            }, now);
        }

        return new DescribeLogDirsResult(new HashMap<>(futures));
    }

    /**
     * 解析DescribeLogDirsResponse响应，提取日志目录的详细信息
     * 
     * @param response 从broker收到的DescribeLogDirs响应
     * @return 日志目录路径到LogDirDescription的映射，包含每个目录的详细信息
     */
    private static Map<String, LogDirDescription> logDirDescriptions(DescribeLogDirsResponse response) {
        // 创建结果Map，key为日志目录路径，value为目录描述信息
        Map<String, LogDirDescription> result = new HashMap<>(response.data().results().size());
        // 遍历每个日志目录的结果
        for (DescribeLogDirsResponseData.DescribeLogDirsResult logDirResult : response.data().results()) {
            // 创建该日志目录下所有副本的信息映射
            Map<TopicPartition, ReplicaInfo> replicaInfoMap = new HashMap<>();
            // 遍历该日志目录下的所有主题
            for (DescribeLogDirsResponseData.DescribeLogDirsTopic t : logDirResult.topics()) {
                // 遍历主题下的所有分区
                for (DescribeLogDirsResponseData.DescribeLogDirsPartition p : t.partitions()) {
                    // 将分区的副本信息添加到映射中
                    replicaInfoMap.put(
                            new TopicPartition(t.name(), p.partitionIndex()),
                            new ReplicaInfo(p.partitionSize(), p.offsetLag(), p.isFutureKey()));
                }
            }
            // 创建日志目录描述对象，包含：
            // 1. 可能的错误信息
            // 2. 该目录下所有副本的信息
            // 3. 目录总空间大小
            // 4. 目录可用空间大小
            result.put(logDirResult.logDir(), new LogDirDescription(
                    Errors.forCode(logDirResult.errorCode()).exception(),
                    replicaInfoMap,
                    logDirResult.totalBytes(),
                    logDirResult.usableBytes()));
        }
        return result;
    }

    /**
     * 描述指定副本的日志目录信息
     * 
     * @param replicas 需要查询的主题分区副本集合，每个副本包含主题、分区和broker ID信息
     * @param options 查询选项，包含超时时间等配置
     * @return 返回DescribeReplicaLogDirsResult对象，包含每个副本的日志目录信息
     */
    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options) {
        // 创建用于存储每个副本查询结果的Future映射，key为副本信息，value为包含日志目录信息的Future
        final Map<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> futures = new HashMap<>(replicas.size());

        // 为每个副本创建一个Future对象
        for (TopicPartitionReplica replica : replicas) {
            futures.put(replica, new KafkaFutureImpl<>());
        }

        // 按broker ID分组的请求数据映射，用于批量查询同一broker上的多个副本
        Map<Integer, DescribeLogDirsRequestData> partitionsByBroker = new HashMap<>();

        // 遍历所有副本，按broker ID组织请求数据
        for (TopicPartitionReplica replica: replicas) {
            // 获取或创建对应broker的请求数据对象
            DescribeLogDirsRequestData requestData = partitionsByBroker.computeIfAbsent(replica.brokerId(),
                brokerId -> new DescribeLogDirsRequestData());
            // 查找该主题是否已存在于请求数据中
            DescribableLogDirTopic describableLogDirTopic = requestData.topics().find(replica.topic());
            if (describableLogDirTopic == null) {
                // 如果主题不存在，创建新的主题数据对象并添加分区信息
                List<Integer> partitions = new ArrayList<>();
                partitions.add(replica.partition());
                describableLogDirTopic = new DescribableLogDirTopic().setTopic(replica.topic())
                        .setPartitions(partitions);
                requestData.topics().add(describableLogDirTopic);
            } else {
                // 如果主题已存在，直接添加分区信息
                describableLogDirTopic.partitions().add(replica.partition());
            }
        }

        // 获取当前时间戳，用于计算请求超时
        final long now = time.milliseconds();
        // 遍历每个broker的请求数据，分别发送请求
        for (Map.Entry<Integer, DescribeLogDirsRequestData> entry: partitionsByBroker.entrySet()) {
            final int brokerId = entry.getKey();
            final DescribeLogDirsRequestData topicPartitions = entry.getValue();
            // 创建用于存储每个分区副本日志目录信息的映射
            final Map<TopicPartition, ReplicaLogDirInfo> replicaDirInfoByPartition = new HashMap<>();
            // 初始化每个主题分区的日志目录信息对象
            for (DescribableLogDirTopic topicPartition: topicPartitions.topics()) {
                for (Integer partitionId : topicPartition.partitions()) {
                    // 为每个主题分区创建一个空的ReplicaLogDirInfo对象
                    replicaDirInfoByPartition.put(new TopicPartition(topicPartition.topic(), partitionId), new ReplicaLogDirInfo());
                }
            }

            // 创建并发送请求到指定的broker
            runnable.call(new Call("describeReplicaLogDirs", calcDeadlineMs(now, options.timeoutMs()),
                new ConstantNodeIdProvider(brokerId)) {

                @Override
                public DescribeLogDirsRequest.Builder createRequest(int timeoutMs) {
                    // 创建请求构建器，查询所有日志目录中的指定分区信息
                    return new DescribeLogDirsRequest.Builder(topicPartitions);
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    // 将响应转换为DescribeLogDirsResponse类型
                    DescribeLogDirsResponse response = (DescribeLogDirsResponse) abstractResponse;
                    // 遍历每个日志目录的描述信息
                    for (Map.Entry<String, LogDirDescription> responseEntry: logDirDescriptions(response).entrySet()) {
                        String logDir = responseEntry.getKey();
                        LogDirDescription logDirInfo = responseEntry.getValue();

                        // 如果日志目录离线，将不会提供副本信息，直接跳过处理
                        if (logDirInfo.error() instanceof KafkaStorageException)
                            continue;
                        // 如果存在其他错误，抛出异常
                        if (logDirInfo.error() != null)
                            handleFailure(new IllegalStateException(
                                "The error " + logDirInfo.error().getClass().getName() + " for log directory " + logDir + " in the response from broker " + brokerId + " is illegal"));

                        // 处理日志目录中的每个副本信息
                        for (Map.Entry<TopicPartition, ReplicaInfo> replicaInfoEntry: logDirInfo.replicaInfos().entrySet()) {
                            TopicPartition tp = replicaInfoEntry.getKey();
                            ReplicaInfo replicaInfo = replicaInfoEntry.getValue();
                            ReplicaLogDirInfo replicaLogDirInfo = replicaDirInfoByPartition.get(tp);
                            if (replicaLogDirInfo == null) {
                                // 如果收到未知分区的响应，记录警告日志
                                log.warn("Server response from broker {} mentioned unknown partition {}", brokerId, tp);
                            } else if (replicaInfo.isFuture()) {
                                // 如果是未来副本，更新其日志目录信息
                                replicaDirInfoByPartition.put(tp, new ReplicaLogDirInfo(replicaLogDirInfo.getCurrentReplicaLogDir(),
                                                                                        replicaLogDirInfo.getCurrentReplicaOffsetLag(),
                                                                                        logDir,
                                                                                        replicaInfo.offsetLag()));
                            } else {
                                // 如果是当前副本，更新其日志目录信息
                                replicaDirInfoByPartition.put(tp, new ReplicaLogDirInfo(logDir,
                                                                                        replicaInfo.offsetLag(),
                                                                                        replicaLogDirInfo.getFutureReplicaLogDir(),
                                                                                        replicaLogDirInfo.getFutureReplicaOffsetLag()));
                            }
                        }
                    }

                    // 完成所有分区的Future结果
                    for (Map.Entry<TopicPartition, ReplicaLogDirInfo> entry: replicaDirInfoByPartition.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        // 根据主题分区和broker ID获取对应的Future对象
                        KafkaFutureImpl<ReplicaLogDirInfo> future = futures.get(new TopicPartitionReplica(tp.topic(), tp.partition(), brokerId));
                        // 设置Future的完成结果
                        future.complete(entry.getValue());
                    }
                }
                @Override
                void handleFailure(Throwable throwable) {
                    // 发生异常时，使用异常信息完成所有Future
                    completeAllExceptionally(futures.values(), throwable);
                }
            }, now);
        }

        return new DescribeReplicaLogDirsResult(new HashMap<>(futures));
    }

    /**
     * 为指定主题创建新的分区
     * 
     * @param newPartitions 主题名称到新分区配置的映射，指定每个主题要创建的分区数量和分配方案
     * @param options 创建分区的选项，包含超时时间、是否仅验证等配置
     * @return 返回CreatePartitionsResult对象，包含每个主题的创建结果
     */
    @Override
    public CreatePartitionsResult createPartitions(final Map<String, NewPartitions> newPartitions,
                                                   final CreatePartitionsOptions options) {
        // 创建用于存储每个主题创建结果的Future映射
        final Map<String, KafkaFutureImpl<Void>> futures = new HashMap<>(newPartitions.size());
        // 创建主题分区集合，用于构建请求
        final CreatePartitionsTopicCollection topics = new CreatePartitionsTopicCollection(newPartitions.size());
        // 遍历每个需要创建分区的主题
        for (Map.Entry<String, NewPartitions> entry : newPartitions.entrySet()) {
            final String topic = entry.getKey();
            final NewPartitions newPartition = entry.getValue();
            // 获取新分区的broker分配方案
            List<List<Integer>> newAssignments = newPartition.assignments();
            // 将broker分配方案转换为请求所需的格式
            List<CreatePartitionsAssignment> assignments = newAssignments == null ? null :
                newAssignments.stream()
                    .map(brokerIds -> new CreatePartitionsAssignment().setBrokerIds(brokerIds))
                    .collect(Collectors.toList());
            // 添加主题的分区创建请求
            topics.add(new CreatePartitionsTopic()
                .setName(topic)
                .setCount(newPartition.totalCount())
                .setAssignments(assignments));
            // 为每个主题创建一个Future对象
            futures.put(topic, new KafkaFutureImpl<>());
        }
        // 如果有需要创建分区的主题，发送请求
        if (!topics.isEmpty()) {
            final long now = time.milliseconds();
            // 计算请求的截止时间
            final long deadline = calcDeadlineMs(now, options.timeoutMs());
            // 创建请求调用对象，初始化时没有配额超限异常
            final Call call = getCreatePartitionsCall(options, futures, topics,
                Collections.emptyMap(), now, deadline);
            // 执行请求调用
            runnable.call(call, now);
        }
        // 返回创建结果，包含每个主题的Future对象
        return new CreatePartitionsResult(new HashMap<>(futures));
    }

    /**
     * 创建分区请求调用对象
     * 
     * @param options 创建分区的选项
     * @param futures 存储每个主题创建结果的Future映射
     * @param topics 需要创建分区的主题集合
     * @param quotaExceededExceptions 之前发生的配额超限异常映射
     * @param now 当前时间戳
     * @param deadline 请求截止时间
     * @return 返回请求调用对象
     */
    private Call getCreatePartitionsCall(final CreatePartitionsOptions options,
                                         final Map<String, KafkaFutureImpl<Void>> futures,
                                         final CreatePartitionsTopicCollection topics,
                                         final Map<String, ThrottlingQuotaExceededException> quotaExceededExceptions,
                                         final long now,
                                         final long deadline) {
        // 创建请求调用对象，使用ControllerNodeProvider确保请求发送到控制器节点
        return new Call("createPartitions", deadline, new ControllerNodeProvider()) {
            @Override
            public CreatePartitionsRequest.Builder createRequest(int timeoutMs) {
                // 构建创建分区请求，设置主题列表、是否仅验证和超时时间
                return new CreatePartitionsRequest.Builder(
                    new CreatePartitionsRequestData()
                        .setTopics(topics)
                        .setValidateOnly(options.validateOnly())
                        .setTimeoutMs(timeoutMs));
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                // 检查是否需要处理控制器变更错误
                handleNotControllerError(abstractResponse);
                // 处理服务器对特定主题的响应
                final CreatePartitionsResponse response = (CreatePartitionsResponse) abstractResponse;
                // 创建需要重试的主题集合
                final CreatePartitionsTopicCollection retryTopics = new CreatePartitionsTopicCollection();
                // 存储需要重试的主题的配额超限异常
                final Map<String, ThrottlingQuotaExceededException> retryTopicQuotaExceededExceptions = new HashMap<>();
                // 处理每个主题的创建结果
                for (CreatePartitionsTopicResult result : response.data().results()) {
                    KafkaFutureImpl<Void> future = futures.get(result.name());
                    if (future == null) {
                        // 如果响应中包含未知主题，记录警告日志
                        log.warn("Server response mentioned unknown topic {}", result.name());
                    } else {
                        // 解析响应中的错误信息
                        ApiError error = new ApiError(result.errorCode(), result.errorMessage());
                        if (error.isFailure()) {
                            if (error.is(Errors.THROTTLING_QUOTA_EXCEEDED)) {
                                // 处理配额超限异常
                                ThrottlingQuotaExceededException quotaExceededException = new ThrottlingQuotaExceededException(
                                    response.throttleTimeMs(), error.messageWithFallback());
                                if (options.shouldRetryOnQuotaViolation()) {
                                    // 如果配置了重试，将主题添加到重试集合
                                    retryTopics.add(topics.find(result.name()).duplicate());
                                    retryTopicQuotaExceededExceptions.put(result.name(), quotaExceededException);
                                } else {
                                    // 否则直接完成Future，带有异常信息
                                    future.completeExceptionally(quotaExceededException);
                                }
                            } else {
                                // 处理其他类型的错误
                                future.completeExceptionally(error.exception());
                            }
                        } else {
                            // 创建成功，完成Future
                            future.complete(null);
                        }
                    }
                }
                // 处理重试逻辑：如果有需要重试的主题则重试，否则完成未实现的Future
                if (retryTopics.isEmpty()) {
                    // 服务器应该为每个主题返回响应，这里做一个健全性检查
                    completeUnrealizedFutures(futures.entrySet().stream(),
                        topic -> "The controller response did not contain a result for topic " + topic);
                } else {
                    // 获取当前时间戳
                    final long now = time.milliseconds();
                    // 创建新的请求调用对象，包含需要重试的主题
                    final Call call = getCreatePartitionsCall(options, futures, retryTopics,
                        retryTopicQuotaExceededExceptions, now, deadline);
                    // 执行重试请求
                    runnable.call(call, now);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 如果之前有因配额超限而重试的主题，且请求超时，
                // 将初始的配额超限异常传递给调用者
                maybeCompleteQuotaExceededException(options.shouldRetryOnQuotaViolation(),
                    throwable, futures, quotaExceededExceptions, (int) (time.milliseconds() - now));
                // 使用异常信息完成所有剩余的Future
                completeAllExceptionally(futures.values(), throwable);
            }
        };
    }

    /**
     * 删除指定主题分区中的消息记录
     * 
     * @param recordsToDelete 要删除的记录映射，key为主题分区，value为要删除的记录信息
     * @param options 删除操作的配置选项
     * @return DeleteRecordsResult 删除操作的结果
     */
    @Override
    public DeleteRecordsResult deleteRecords(final Map<TopicPartition, RecordsToDelete> recordsToDelete,
                                             final DeleteRecordsOptions options) {
        // 创建一个分区leader策略的Future，用于跟踪删除操作的完成状态
        PartitionLeaderStrategy.PartitionLeaderFuture<DeletedRecords> future =
            DeleteRecordsHandler.newFuture(recordsToDelete.keySet(), partitionLeaderCache);
        
        // 设置超时时间，如果options中指定了超时时间则使用指定的，否则使用默认值
        int timeoutMs = defaultApiTimeoutMs;
        if (options.timeoutMs() != null) {
            timeoutMs = options.timeoutMs();
        }
        
        // 创建删除记录的处理器
        DeleteRecordsHandler handler = new DeleteRecordsHandler(recordsToDelete, logContext, timeoutMs);
        
        // 调用驱动程序执行删除操作
        invokeDriver(handler, future, options.timeoutMs);

        // 返回删除操作的结果
        return new DeleteRecordsResult(future.all());
    }

    /**
     * 创建委托令牌，用于授权其他用户访问Kafka集群
     * 
     * @param options 创建委托令牌的配置选项，包含令牌的所有者、可续期者列表和最大生命周期等信息
     * @return CreateDelegationTokenResult 创建委托令牌的结果
     */
    @Override
    public CreateDelegationTokenResult createDelegationToken(final CreateDelegationTokenOptions options) {
        // 创建一个Future用于异步获取创建结果
        final KafkaFutureImpl<DelegationToken> delegationTokenFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        
        // 将可续期者列表转换为请求所需的格式
        List<CreatableRenewers> renewers = new ArrayList<>();
        for (KafkaPrincipal principal : options.renewers()) {
            renewers.add(new CreatableRenewers()
                    .setPrincipalName(principal.getName())
                    .setPrincipalType(principal.getPrincipalType()));
        }
        
        // 创建并执行创建令牌的请求
        runnable.call(new Call("createDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            CreateDelegationTokenRequest.Builder createRequest(int timeoutMs) {
                // 构建创建令牌请求数据
                CreateDelegationTokenRequestData data = new CreateDelegationTokenRequestData()
                    .setRenewers(renewers)
                    .setMaxLifetimeMs(options.maxLifetimeMs());
                // 如果指定了令牌所有者，则设置所有者信息
                if (options.owner().isPresent()) {
                    data.setOwnerPrincipalName(options.owner().get().getName());
                    data.setOwnerPrincipalType(options.owner().get().getPrincipalType());
                }
                return new CreateDelegationTokenRequest.Builder(data);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                CreateDelegationTokenResponse response = (CreateDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    // 如果响应包含错误，则完成Future并抛出异常
                    delegationTokenFuture.completeExceptionally(response.error().exception());
                } else {
                    // 创建成功，构建令牌信息并完成Future
                    CreateDelegationTokenResponseData data = response.data();
                    TokenInformation tokenInfo = new TokenInformation(data.tokenId(), 
                        new KafkaPrincipal(data.principalType(), data.principalName()),
                        new KafkaPrincipal(data.tokenRequesterPrincipalType(), data.tokenRequesterPrincipalName()),
                        options.renewers(), data.issueTimestampMs(), data.maxTimestampMs(), data.expiryTimestampMs());
                    DelegationToken token = new DelegationToken(tokenInfo, data.hmac());
                    delegationTokenFuture.complete(token);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理请求失败的情况
                delegationTokenFuture.completeExceptionally(throwable);
            }
        }, now);

        return new CreateDelegationTokenResult(delegationTokenFuture);
    }

    /**
     * 续期委托令牌，延长令牌的有效期
     * 
     * @param hmac 要续期的令牌的HMAC值
     * @param options 续期选项，包含续期时长等信息
     * @return RenewDelegationTokenResult 续期操作的结果，包含更新后的过期时间
     */
    @Override
    public RenewDelegationTokenResult renewDelegationToken(final byte[] hmac, final RenewDelegationTokenOptions options) {
        // 创建一个Future用于异步获取续期结果（新的过期时间）
        final KafkaFutureImpl<Long>  expiryTimeFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        
        // 创建并执行续期令牌的请求
        runnable.call(new Call("renewDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            RenewDelegationTokenRequest.Builder createRequest(int timeoutMs) {
                // 构建续期请求数据，包含令牌的HMAC和续期时长
                return new RenewDelegationTokenRequest.Builder(
                        new RenewDelegationTokenRequestData()
                        .setHmac(hmac)
                        .setRenewPeriodMs(options.renewTimePeriodMs()));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                RenewDelegationTokenResponse response = (RenewDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    // 如果响应包含错误，则完成Future并抛出异常
                    expiryTimeFuture.completeExceptionally(response.error().exception());
                } else {
                    // 续期成功，完成Future并返回新的过期时间
                    expiryTimeFuture.complete(response.expiryTimestamp());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理请求失败的情况
                expiryTimeFuture.completeExceptionally(throwable);
            }
        }, now);

        return new RenewDelegationTokenResult(expiryTimeFuture);
    }

    /**
     * 使委托令牌过期，可以立即使令牌失效或设置一个新的过期时间
     * 
     * @param hmac 要过期的令牌的HMAC值
     * @param options 过期选项，包含新的过期时间等信息
     * @return ExpireDelegationTokenResult 过期操作的结果，包含实际的过期时间
     */
    @Override
    public ExpireDelegationTokenResult expireDelegationToken(final byte[] hmac, final ExpireDelegationTokenOptions options) {
        // 创建一个Future用于异步获取过期操作的结果
        final KafkaFutureImpl<Long>  expiryTimeFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        
        // 创建并执行使令牌过期的请求
        runnable.call(new Call("expireDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            ExpireDelegationTokenRequest.Builder createRequest(int timeoutMs) {
                // 构建过期请求数据，包含令牌的HMAC和新的过期时间
                return new ExpireDelegationTokenRequest.Builder(
                        new ExpireDelegationTokenRequestData()
                            .setHmac(hmac)
                            .setExpiryTimePeriodMs(options.expiryTimePeriodMs()));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                ExpireDelegationTokenResponse response = (ExpireDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    // 如果响应包含错误，则完成Future并抛出异常
                    expiryTimeFuture.completeExceptionally(response.error().exception());
                } else {
                    // 过期操作成功，完成Future并返回实际的过期时间
                    expiryTimeFuture.complete(response.expiryTimestamp());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理请求失败的情况
                expiryTimeFuture.completeExceptionally(throwable);
            }
        }, now);

        return new ExpireDelegationTokenResult(expiryTimeFuture);
    }

    /**
     * 描述委托令牌的方法，用于获取指定所有者的委托令牌信息
     * 
     * @param options 描述委托令牌的选项，包含令牌所有者等信息
     * @return 返回DescribeDelegationTokenResult对象，包含令牌列表的Future
     */
    @Override
    public DescribeDelegationTokenResult describeDelegationToken(final DescribeDelegationTokenOptions options) {
        // 创建一个Future来存储令牌列表的结果
        final KafkaFutureImpl<List<DelegationToken>>  tokensFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        // 调用异步请求，使用负载最小的节点处理请求
        runnable.call(new Call("describeDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            DescribeDelegationTokenRequest.Builder createRequest(int timeoutMs) {
                // 创建描述委托令牌的请求，传入令牌所有者列表
                return new DescribeDelegationTokenRequest.Builder(options.owners());
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                // 处理服务器的响应
                DescribeDelegationTokenResponse response = (DescribeDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    // 如果响应中包含错误，则将异常传递给Future
                    tokensFuture.completeExceptionally(response.error().exception());
                } else {
                    // 如果成功，则将令牌列表传递给Future
                    tokensFuture.complete(response.tokens());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理请求失败的情况，将异常传递给Future
                tokensFuture.completeExceptionally(throwable);
            }
        }, now);

        return new DescribeDelegationTokenResult(tokensFuture);
    }

    /**
     * 用于存储和管理消费者组列表查询的结果
     * 包含错误信息、组列表信息以及未完成的节点集合
     */
    private static final class ListGroupsResults {
        // 存储查询过程中遇到的错误
        private final List<Throwable> errors;
        // 存储消费者组信息，key为groupId
        private final HashMap<String, GroupListing> listings;
        // 存储尚未完成查询的节点集合
        private final HashSet<Node> remaining;
        // 用于异步返回结果的Future
        private final KafkaFutureImpl<Collection<Object>> future;

        ListGroupsResults(Collection<Node> leaders,
                          KafkaFutureImpl<Collection<Object>> future) {
            this.errors = new ArrayList<>();
            this.listings = new HashMap<>();
            this.remaining = new HashSet<>(leaders);
            this.future = future;
            tryComplete();
        }

        /**
         * 添加查询过程中遇到的错误
         * @param throwable 异常对象
         * @param node 发生错误的节点
         */
        synchronized void addError(Throwable throwable, Node node) {
            ApiError error = ApiError.fromThrowable(throwable);
            if (error.message() == null || error.message().isEmpty()) {
                errors.add(error.error().exception("Error listing groups on " + node));
            } else {
                errors.add(error.error().exception("Error listing groups on " + node + ": " + error.message()));
            }
        }

        /**
         * 添加一个消费者组信息到结果集中
         * @param listing 消费者组信息
         */
        synchronized void addListing(GroupListing listing) {
            listings.put(listing.groupId(), listing);
        }

        /**
         * 标记一个节点的查询已完成
         * @param leader 完成查询的节点
         */
        synchronized void tryComplete(Node leader) {
            remaining.remove(leader);
            tryComplete();
        }

        /**
         * 检查是否所有节点都已完成查询，如果是则完成Future
         */
        private synchronized void tryComplete() {
            if (remaining.isEmpty()) {
                // 将所有查询结果和错误信息合并
                ArrayList<Object> results = new ArrayList<>(listings.values());
                results.addAll(errors);
                future.complete(results);
            }
        }
    }

    /**
     * 列出集群中的所有消费者组
     * 
     * @param options 列出消费者组的选项，包含过滤条件等
     * @return 返回ListGroupsResult对象，包含消费者组列表的Future
     */
    @Override
    public ListGroupsResult listGroups(ListGroupsOptions options) {
        // 创建一个Future来存储最终的结果
        final KafkaFutureImpl<Collection<Object>> all = new KafkaFutureImpl<>();
        final long nowMetadata = time.milliseconds();
        final long deadline = calcDeadlineMs(nowMetadata, options.timeoutMs());
        
        // 首先获取所有Broker的元数据信息
        runnable.call(new Call("findAllBrokers", deadline, new LeastLoadedNodeProvider()) {
            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                // 创建元数据请求，不指定特定的主题
                return new MetadataRequest.Builder(new MetadataRequestData()
                    .setTopics(Collections.emptyList())
                    .setAllowAutoTopicCreation(true));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                // 处理元数据响应
                MetadataResponse metadataResponse = (MetadataResponse) abstractResponse;
                Collection<Node> nodes = metadataResponse.brokers();
                if (nodes.isEmpty())
                    throw new StaleMetadataException("Metadata fetch failed due to missing broker list");

                // 创建结果收集器
                HashSet<Node> allNodes = new HashSet<>(nodes);
                final ListGroupsResults results = new ListGroupsResults(allNodes, all);

                // 向每个Broker发送列出消费者组的请求
                for (final Node node : allNodes) {
                    final long nowList = time.milliseconds();
                    runnable.call(new Call("listGroups", deadline, new ConstantNodeIdProvider(node.id())) {
                        @Override
                        ListGroupsRequest.Builder createRequest(int timeoutMs) {
                            // 将消费者组类型和状态转换为字符串列表
                            List<String> groupTypes = options.types()
                                .stream()
                                .map(GroupType::toString)
                                .collect(Collectors.toList());
                            List<String> groupStates = options.groupStates()
                                .stream()
                                .map(GroupState::toString)
                                .collect(Collectors.toList());
                            // 创建列出消费者组的请求，设置过滤条件
                            return new ListGroupsRequest.Builder(new ListGroupsRequestData()
                                .setTypesFilter(groupTypes)
                                .setStatesFilter(groupStates)
                            );
                        }

                        /**
                         * 处理单个消费者组信息，将其添加到结果集中
                         */
                        private void maybeAddGroup(ListGroupsResponseData.ListedGroup group) {
                            final String groupId = group.groupId();
                            // 解析消费者组类型
                            final Optional<GroupType> type;
                            if (group.groupType() == null || group.groupType().isEmpty()) {
                                type = Optional.empty();
                            } else {
                                type = Optional.of(GroupType.parse(group.groupType()));
                            }
                            final String protocolType = group.protocolType();
                            // 解析消费者组状态
                            final Optional<GroupState> groupState;
                            if (group.groupState() == null || group.groupState().isEmpty()) {
                                groupState = Optional.empty();
                            } else {
                                groupState = Optional.of(GroupState.parse(group.groupState()));
                            }
                            // 创建消费者组列表项并添加到结果中
                            final GroupListing groupListing = new GroupListing(
                                groupId,
                                type,
                                protocolType,
                                groupState
                            );
                            results.addListing(groupListing);
                        }

                        @Override
                        void handleResponse(AbstractResponse abstractResponse) {
                            // 处理列出消费者组的响应
                            final ListGroupsResponse response = (ListGroupsResponse) abstractResponse;
                            synchronized (results) {
                                Errors error = Errors.forCode(response.data().errorCode());
                                if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.COORDINATOR_NOT_AVAILABLE) {
                                    // 如果协调器正在加载或不可用，抛出异常
                                    throw error.exception();
                                } else if (error != Errors.NONE) {
                                    // 如果有其他错误，添加到错误列表
                                    results.addError(error.exception(), node);
                                } else {
                                    // 处理每个消费者组的信息
                                    for (ListGroupsResponseData.ListedGroup group : response.data().groups()) {
                                        maybeAddGroup(group);
                                    }
                                }
                                // 标记该节点的查询已完成
                                results.tryComplete(node);
                            }
                        }

                        @Override
                        void handleFailure(Throwable throwable) {
                            // 处理请求失败的情况
                            synchronized (results) {
                                results.addError(throwable, node);
                                results.tryComplete(node);
                            }
                        }
                    }, nowList);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理获取元数据失败的情况
                KafkaException exception = new KafkaException("Failed to find brokers to send ListGroups", throwable);
                all.complete(Collections.singletonList(exception));
            }
        }, nowMetadata);

        return new ListGroupsResult(all);
    }

    /**
     * 描述指定的消费者组，获取消费者组的详细信息
     * 
     * @param groupIds 要描述的消费者组ID集合
     * @param options 描述消费者组的选项配置
     * @return 返回DescribeConsumerGroupsResult对象，包含每个消费者组的描述信息
     */
    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(final Collection<String> groupIds,
                                                               final DescribeConsumerGroupsOptions options) {
        // 创建一个Future对象来存储异步操作的结果
        SimpleAdminApiFuture<CoordinatorKey, ConsumerGroupDescription> future =
                DescribeConsumerGroupsHandler.newFuture(groupIds);
        // 创建处理器实例，用于处理描述消费者组的请求
        DescribeConsumerGroupsHandler handler = new DescribeConsumerGroupsHandler(options.includeAuthorizedOperations(), logContext);
        // 调用驱动程序执行请求
        invokeDriver(handler, future, options.timeoutMs);
        // 将结果转换为Map格式并返回
        return new DescribeConsumerGroupsResult(future.all().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().idValue, Map.Entry::getValue)));
    }

    /**
     * 用于存储和管理消费者组列表查询的结果
     * 包含错误信息、消费者组列表以及未完成的节点集合
     */
    private static final class ListConsumerGroupsResults {
        private final List<Throwable> errors;  // 存储查询过程中的错误
        private final HashMap<String, ConsumerGroupListing> listings;  // 存储消费者组列表，key为groupId
        private final HashSet<Node> remaining;  // 存储尚未完成查询的节点
        private final KafkaFutureImpl<Collection<Object>> future;  // 用于异步返回结果

        /**
         * 构造函数，初始化结果收集器
         * @param leaders 需要查询的broker节点集合
         * @param future 用于返回最终结果的Future对象
         */
        ListConsumerGroupsResults(Collection<Node> leaders,
                                  KafkaFutureImpl<Collection<Object>> future) {
            this.errors = new ArrayList<>();
            this.listings = new HashMap<>();
            this.remaining = new HashSet<>(leaders);
            this.future = future;
            tryComplete();
        }

        /**
         * 添加查询过程中遇到的错误
         * @param throwable 异常对象
         * @param node 发生错误的节点
         */
        synchronized void addError(Throwable throwable, Node node) {
            ApiError error = ApiError.fromThrowable(throwable);
            if (error.message() == null || error.message().isEmpty()) {
                errors.add(error.error().exception("Error listing groups on " + node));
            } else {
                errors.add(error.error().exception("Error listing groups on " + node + ": " + error.message()));
            }
        }

        /**
         * 添加一个消费者组到结果集中
         * @param listing 消费者组信息
         */
        synchronized void addListing(ConsumerGroupListing listing) {
            listings.put(listing.groupId(), listing);
        }

        /**
         * 标记一个节点的查询已完成
         * @param leader 完成查询的节点
         */
        synchronized void tryComplete(Node leader) {
            remaining.remove(leader);
            tryComplete();
        }

        /**
         * 检查是否所有节点都已完成查询，如果是则完成Future
         */
        private synchronized void tryComplete() {
            if (remaining.isEmpty()) {
                ArrayList<Object> results = new ArrayList<>(listings.values());
                results.addAll(errors);
                future.complete(results);
            }
        }
    }

    /**
     * 列出集群中的所有消费者组
     * 
     * @param options 列出消费者组的选项配置
     * @return 返回ListConsumerGroupsResult对象，包含所有消费者组的信息
     */
    @Override
    public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
        // 创建Future对象用于存储异步操作的结果
        final KafkaFutureImpl<Collection<Object>> all = new KafkaFutureImpl<>();
        final long nowMetadata = time.milliseconds();
        final long deadline = calcDeadlineMs(nowMetadata, options.timeoutMs());

        // 第一步：查找所有的broker节点
        runnable.call(new Call("findAllBrokers", deadline, new LeastLoadedNodeProvider()) {
            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                // 创建元数据请求，不指定特定的topic
                return new MetadataRequest.Builder(new MetadataRequestData()
                    .setTopics(Collections.emptyList())
                    .setAllowAutoTopicCreation(true));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                // 处理元数据响应，获取所有broker节点
                MetadataResponse metadataResponse = (MetadataResponse) abstractResponse;
                Collection<Node> nodes = metadataResponse.brokers();
                if (nodes.isEmpty())
                    throw new StaleMetadataException("Metadata fetch failed due to missing broker list");

                HashSet<Node> allNodes = new HashSet<>(nodes);
                final ListConsumerGroupsResults results = new ListConsumerGroupsResults(allNodes, all);

                // 第二步：向每个broker发送列出消费者组的请求
                for (final Node node : allNodes) {
                    final long nowList = time.milliseconds();
                    runnable.call(new Call("listConsumerGroups", deadline, new ConstantNodeIdProvider(node.id())) {
                        @Override
                        ListGroupsRequest.Builder createRequest(int timeoutMs) {
                            // 构建请求，设置过滤条件
                            List<String> states = options.groupStates()
                                    .stream()
                                    .map(GroupState::toString)
                                    .collect(Collectors.toList());
                            List<String> groupTypes = options.types()
                                    .stream()
                                    .map(GroupType::toString)
                                    .collect(Collectors.toList());
                            return new ListGroupsRequest.Builder(new ListGroupsRequestData()
                                .setStatesFilter(states)
                                .setTypesFilter(groupTypes)
                            );
                        }

                        /**
                         * 处理单个消费者组信息，如果是有效的消费者组则添加到结果集
                         */
                        private void maybeAddConsumerGroup(ListGroupsResponseData.ListedGroup group) {
                            String protocolType = group.protocolType();
                            // 只处理消费者协议类型的组或空协议类型
                            if (protocolType.equals(ConsumerProtocol.PROTOCOL_TYPE) || protocolType.isEmpty()) {
                                final String groupId = group.groupId();
                                final Optional<GroupState> groupState = group.groupState().isEmpty()
                                        ? Optional.empty()
                                        : Optional.of(GroupState.parse(group.groupState()));
                                final Optional<GroupType> type = group.groupType().isEmpty()
                                        ? Optional.empty()
                                        : Optional.of(GroupType.parse(group.groupType()));
                                final ConsumerGroupListing groupListing = new ConsumerGroupListing(
                                        groupId,
                                        groupState,
                                        type,
                                        protocolType.isEmpty()
                                    );
                                results.addListing(groupListing);
                            }
                        }

                        @Override
                        void handleResponse(AbstractResponse abstractResponse) {
                            // 处理响应结果
                            final ListGroupsResponse response = (ListGroupsResponse) abstractResponse;
                            synchronized (results) {
                                Errors error = Errors.forCode(response.data().errorCode());
                                if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.COORDINATOR_NOT_AVAILABLE) {
                                    // 如果协调器正在加载或不可用，抛出异常
                                    throw error.exception();
                                } else if (error != Errors.NONE) {
                                    // 处理其他错误
                                    results.addError(error.exception(), node);
                                } else {
                                    // 处理成功响应，添加消费者组信息
                                    for (ListGroupsResponseData.ListedGroup group : response.data().groups()) {
                                        maybeAddConsumerGroup(group);
                                    }
                                }
                                results.tryComplete(node);
                            }
                        }

                        @Override
                        void handleFailure(Throwable throwable) {
                            // 处理请求失败的情况
                            synchronized (results) {
                                results.addError(throwable, node);
                                results.tryComplete(node);
                            }
                        }
                    }, nowList);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理查找broker失败的情况
                KafkaException exception = new KafkaException("Failed to find brokers to send ListGroups", throwable);
                all.complete(Collections.singletonList(exception));
            }
        }, nowMetadata);

        return new ListConsumerGroupsResult(all);
    }

    /**
     * 列出消费者组的偏移量信息
     * 
     * @param groupSpecs 消费者组规范映射，key为消费者组ID，value为对应的规范配置
     * @param options 列出消费者组偏移量的选项配置
     * @return 包含消费者组偏移量信息的结果对象
     *
     * 实现细节：
     * 1. 创建一个AdminApiFuture来存储异步操作结果
     * 2. 创建一个专门的Handler来处理偏移量查询请求
     * 3. 调用底层驱动执行实际的查询操作
     * 4. 返回包装后的结果对象
     */
    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs,
                                                                   ListConsumerGroupOffsetsOptions options) {
        SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> future =
                ListConsumerGroupOffsetsHandler.newFuture(groupSpecs.keySet());
        ListConsumerGroupOffsetsHandler handler =
            new ListConsumerGroupOffsetsHandler(groupSpecs, options.requireStable(), logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new ListConsumerGroupOffsetsResult(future.all());
    }

    /**
     * 删除指定的消费者组
     * 
     * @param groupIds 要删除的消费者组ID集合
     * @param options 删除消费者组的选项配置
     * @return 包含删除操作结果的对象
     *
     * 实现细节：
     * 1. 创建一个AdminApiFuture来存储异步删除操作结果
     * 2. 创建专门的Handler处理删除请求
     * 3. 调用底层驱动执行删除操作
     * 4. 将结果转换为以消费者组ID为key的映射并返回
     */
    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options) {
        SimpleAdminApiFuture<CoordinatorKey, Void> future =
                DeleteConsumerGroupsHandler.newFuture(groupIds);
        DeleteConsumerGroupsHandler handler = new DeleteConsumerGroupsHandler(logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new DeleteConsumerGroupsResult(future.all().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().idValue, Map.Entry::getValue)));
    }

    /**
     * 删除指定消费者组在特定分区上的偏移量
     * 
     * @param groupId 消费者组ID
     * @param partitions 要删除偏移量的主题分区集合
     * @param options 删除偏移量的选项配置
     * @return 包含删除操作结果的对象
     *
     * 实现细节：
     * 1. 创建一个AdminApiFuture来存储异步删除操作结果
     * 2. 创建专门的Handler处理偏移量删除请求
     * 3. 调用底层驱动执行删除操作
     * 4. 返回包含操作结果和相关分区信息的结果对象
     */
    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(
            String groupId,
            Set<TopicPartition> partitions,
            DeleteConsumerGroupOffsetsOptions options) {
        SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, Errors>> future =
                DeleteConsumerGroupOffsetsHandler.newFuture(groupId);
        DeleteConsumerGroupOffsetsHandler handler = new DeleteConsumerGroupOffsetsHandler(groupId, partitions, logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new DeleteConsumerGroupOffsetsResult(future.get(CoordinatorKey.byGroupId(groupId)), partitions);
    }

    /**
     * 描述共享消费者组的详细信息
     * 
     * @param groupIds 要描述的共享消费者组ID集合
     * @param options 描述共享消费者组的选项配置
     * @return 包含共享消费者组描述信息的结果对象
     *
     * 实现细节：
     * 1. 创建一个AdminApiFuture来存储异步查询操作结果
     * 2. 创建专门的Handler处理描述请求，可选择是否包含授权操作信息
     * 3. 调用底层驱动执行查询操作
     * 4. 将结果转换为以消费者组ID为key的映射并返回
     */
    @Override
    public DescribeShareGroupsResult describeShareGroups(final Collection<String> groupIds,
                                                         final DescribeShareGroupsOptions options) {
        SimpleAdminApiFuture<CoordinatorKey, ShareGroupDescription> future =
                DescribeShareGroupsHandler.newFuture(groupIds);
        DescribeShareGroupsHandler handler = new DescribeShareGroupsHandler(options.includeAuthorizedOperations(), logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new DescribeShareGroupsResult(future.all().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().idValue, Map.Entry::getValue)));
    }

    /**
     * 列出共享消费者组的偏移量信息
     * 
     * @param groupSpecs 共享消费者组规范映射，key为消费者组ID，value为对应的规范配置
     * @param options 列出共享消费者组偏移量的选项配置
     * @return 包含共享消费者组偏移量信息的结果对象
     *
     * 实现细节：
     * 1. 创建一个AdminApiFuture来存储异步查询操作结果
     * 2. 创建专门的Handler处理偏移量查询请求
     * 3. 调用底层驱动执行查询操作
     * 4. 返回包含所有查询结果的对象
     */
    @Override
    public ListShareGroupOffsetsResult listShareGroupOffsets(final Map<String, ListShareGroupOffsetsSpec> groupSpecs,
                                                             final ListShareGroupOffsetsOptions options) {
        SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, Long>> future = ListShareGroupOffsetsHandler.newFuture(groupSpecs.keySet());
        ListShareGroupOffsetsHandler handler = new ListShareGroupOffsetsHandler(groupSpecs, logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new ListShareGroupOffsetsResult(future.all());
    }

    /**
     * 描述经典消费者组的详细信息
     * 
     * @param groupIds 要描述的经典消费者组ID集合
     * @param options 描述经典消费者组的选项配置
     * @return 包含经典消费者组描述信息的结果对象
     *
     * 实现细节：
     * 1. 创建一个AdminApiFuture来存储异步查询操作结果
     * 2. 创建专门的Handler处理描述请求，可选择是否包含授权操作信息
     * 3. 调用底层驱动执行查询操作
     * 4. 将结果转换为以消费者组ID为key的映射并返回
     */
    @Override
    public DescribeClassicGroupsResult describeClassicGroups(final Collection<String> groupIds,
                                                             final DescribeClassicGroupsOptions options) {
        SimpleAdminApiFuture<CoordinatorKey, ClassicGroupDescription> future =
            DescribeClassicGroupsHandler.newFuture(groupIds);
        DescribeClassicGroupsHandler handler = new DescribeClassicGroupsHandler(options.includeAuthorizedOperations(), logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new DescribeClassicGroupsResult(future.all().entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey().idValue, Map.Entry::getValue)));
    }

    /**
     * 获取当前AdminClient的所有度量指标
     * 
     * @return 不可修改的度量指标映射，key为指标名称，value为指标值
     *
     * 实现细节：
     * 1. 返回内部metrics对象中所有度量指标的只读视图
     * 2. 通过Collections.unmodifiableMap确保返回的Map不可被修改
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * 为指定的主题分区选举新的leader副本
     * 
     * @param electionType 选举类型，可以是优先副本选举或普通leader选举
     * @param topicPartitions 需要进行leader选举的主题分区集合
     * @param options leader选举的选项配置
     * @return 包含选举结果的对象
     *
     * 实现细节：
     * 1. 创建一个KafkaFutureImpl来存储选举操作的结果
     * 2. 构建一个新的Call对象来处理leader选举请求：
     *   - 创建ElectLeadersRequest请求
     *   - 处理服务器的响应，包括成功和失败情况
     *   - 处理请求过程中的异常
     * 3. 通过runnable执行这个Call
     * 4. 返回包装后的选举结果
     *
     * 异常处理：
     * - 如果响应中包含错误码，将对应的异常传递给future
     * - 请求失败时，将异常传递给future
     */
    @Override
    public ElectLeadersResult electLeaders(
            final ElectionType electionType,
            final Set<TopicPartition> topicPartitions,
            ElectLeadersOptions options) {
        final KafkaFutureImpl<Map<TopicPartition, Optional<Throwable>>> electionFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("electLeaders", calcDeadlineMs(now, options.timeoutMs()),
                new ControllerNodeProvider()) {

            @Override
            public ElectLeadersRequest.Builder createRequest(int timeoutMs) {
                return new ElectLeadersRequest.Builder(electionType, topicPartitions, timeoutMs);
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                ElectLeadersResponse response = (ElectLeadersResponse) abstractResponse;
                Map<TopicPartition, Optional<Throwable>> result = ElectLeadersResponse.electLeadersResult(response.data());

                // 对于版本0，errorCode为0表示Errors.NONE
                Errors error = Errors.forCode(response.data().errorCode());
                if (error != Errors.NONE) {
                    electionFuture.completeExceptionally(error.exception());
                    return;
                }

                electionFuture.complete(result);
            }

            @Override
            void handleFailure(Throwable throwable) {
                electionFuture.completeExceptionally(throwable);
            }
        }, now);

        return new ElectLeadersResult(electionFuture);
    }

    /**
     * 修改分区副本分配方案
     * 
     * @param reassignments 分区重分配映射，key为主题分区，value为新的分区分配方案
     * @param options 分区重分配的选项配置
     * @return 包含重分配操作结果的对象
     *
     * 实现细节：
     * 1. 初始化数据结构
     *   - 创建futures映射存储每个分区的操作结果
     *   - 创建topicsToReassignments映射组织重分配请求数据
     *
     * 2. 处理输入的重分配请求
     *   - 验证主题名称的有效性
     *   - 验证分区索引的有效性
     *   - 按主题组织分区的重分配信息
     *
     * 3. 构建和发送请求
     *   - 创建AlterPartitionReassignmentsRequest请求
     *   - 设置超时时间
     *   - 通过Controller节点发送请求
     *
     * 4. 处理响应
     *   - 处理顶层错误（如NOT_CONTROLLER）
     *   - 验证每个主题分区的响应
     *   - 确保响应数量与请求匹配
     *   - 更新每个分区的操作结果
     *
     * 异常处理：
     * - 无效的主题名称或分区索引
     * - 控制器节点错误
     * - 请求执行失败
     * - 响应数量不匹配
     */
    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(
            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments,
            AlterPartitionReassignmentsOptions options) {
        final Map<TopicPartition, KafkaFutureImpl<Void>> futures = new HashMap<>();
        final Map<String, Map<Integer, Optional<NewPartitionReassignment>>> topicsToReassignments = new TreeMap<>();
        for (Map.Entry<TopicPartition, Optional<NewPartitionReassignment>> entry : reassignments.entrySet()) {
            String topic = entry.getKey().topic();
            int partition = entry.getKey().partition();
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            Optional<NewPartitionReassignment> reassignment = entry.getValue();
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
            futures.put(topicPartition, future);

            if (topicNameIsUnrepresentable(topic)) {
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                        topic + "' cannot be represented in a request."));
            } else if (topicPartition.partition() < 0) {
                future.completeExceptionally(new InvalidTopicException("The given partition index " +
                        topicPartition.partition() + " is not valid."));
            } else {
                Map<Integer, Optional<NewPartitionReassignment>> partitionReassignments =
                        topicsToReassignments.get(topicPartition.topic());
                if (partitionReassignments == null) {
                    partitionReassignments = new TreeMap<>();
                    topicsToReassignments.put(topic, partitionReassignments);
                }

                partitionReassignments.put(partition, reassignment);
            }
        }

        final long now = time.milliseconds();
        Call call = new Call("alterPartitionReassignments", calcDeadlineMs(now, options.timeoutMs()),
                new ControllerNodeProvider(true)) {

            @Override
            public AlterPartitionReassignmentsRequest.Builder createRequest(int timeoutMs) {
                AlterPartitionReassignmentsRequestData data =
                        new AlterPartitionReassignmentsRequestData();
                for (Map.Entry<String, Map<Integer, Optional<NewPartitionReassignment>>> entry :
                        topicsToReassignments.entrySet()) {
                    String topicName = entry.getKey();
                    Map<Integer, Optional<NewPartitionReassignment>> partitionsToReassignments = entry.getValue();

                    List<ReassignablePartition> reassignablePartitions = new ArrayList<>();
                    for (Map.Entry<Integer, Optional<NewPartitionReassignment>> partitionEntry :
                            partitionsToReassignments.entrySet()) {
                        int partitionIndex = partitionEntry.getKey();
                        Optional<NewPartitionReassignment> reassignment = partitionEntry.getValue();

                        ReassignablePartition reassignablePartition = new ReassignablePartition()
                                .setPartitionIndex(partitionIndex)
                                .setReplicas(reassignment.map(NewPartitionReassignment::targetReplicas).orElse(null));
                        reassignablePartitions.add(reassignablePartition);
                    }

                    ReassignableTopic reassignableTopic = new ReassignableTopic()
                            .setName(topicName)
                            .setPartitions(reassignablePartitions);
                    data.topics().add(reassignableTopic);
                }
                data.setTimeoutMs(timeoutMs);
                return new AlterPartitionReassignmentsRequest.Builder(data);
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                AlterPartitionReassignmentsResponse response = (AlterPartitionReassignmentsResponse) abstractResponse;
                Map<TopicPartition, ApiException> errors = new HashMap<>();
                int receivedResponsesCount = 0;

                Errors topLevelError = Errors.forCode(response.data().errorCode());
                switch (topLevelError) {
                    case NONE:
                        receivedResponsesCount += validateTopicResponses(response.data().responses(), errors);
                        break;
                    case NOT_CONTROLLER:
                        handleNotControllerError(topLevelError);
                        break;
                    default:
                        for (ReassignableTopicResponse topicResponse : response.data().responses()) {
                            String topicName = topicResponse.name();
                            for (ReassignablePartitionResponse partition : topicResponse.partitions()) {
                                errors.put(
                                        new TopicPartition(topicName, partition.partitionIndex()),
                                        new ApiError(topLevelError, response.data().errorMessage()).exception()
                                );
                                receivedResponsesCount += 1;
                            }
                        }
                        break;
                }

                assertResponseCountMatch(errors, receivedResponsesCount);
                for (Map.Entry<TopicPartition, ApiException> entry : errors.entrySet()) {
                    ApiException exception = entry.getValue();
                    if (exception == null)
                        futures.get(entry.getKey()).complete(null);
                    else
                        futures.get(entry.getKey()).completeExceptionally(exception);
                }
            }

            /**
             * 验证响应中的分区数量是否与请求匹配
             * 
             * @param errors 收集到的错误信息
             * @param receivedResponsesCount 实际收到的响应数量
             * @throws UnknownServerException 当响应数量与预期不符时
             */
            private void assertResponseCountMatch(Map<TopicPartition, ApiException> errors, int receivedResponsesCount) {
                int expectedResponsesCount = topicsToReassignments.values().stream().mapToInt(Map::size).sum();
                if (errors.values().stream().noneMatch(Objects::nonNull) && receivedResponsesCount != expectedResponsesCount) {
                    String quantifier = receivedResponsesCount > expectedResponsesCount ? "many" : "less";
                    throw new UnknownServerException("The server returned too " + quantifier + " results." +
                        "Expected " + expectedResponsesCount + " but received " + receivedResponsesCount);
                }
            }

            /**
             * 验证主题响应并收集错误信息
             * 
             * @param topicResponses 主题响应列表
             * @param errors 用于存储错误信息的映射
             * @return 处理的响应总数
             */
            private int validateTopicResponses(List<ReassignableTopicResponse> topicResponses,
                                               Map<TopicPartition, ApiException> errors) {
                int receivedResponsesCount = 0;

                for (ReassignableTopicResponse topicResponse : topicResponses) {
                    String topicName = topicResponse.name();
                    for (ReassignablePartitionResponse partResponse : topicResponse.partitions()) {
                        Errors partitionError = Errors.forCode(partResponse.errorCode());

                        TopicPartition tp = new TopicPartition(topicName, partResponse.partitionIndex());
                        if (partitionError == Errors.NONE) {
                            errors.put(tp, null);
                        } else {
                            errors.put(tp, new ApiError(partitionError, partResponse.errorMessage()).exception());
                        }
                        receivedResponsesCount += 1;
                    }
                }

                return receivedResponsesCount;
            }

            @Override
            void handleFailure(Throwable throwable) {
                for (KafkaFutureImpl<Void> future : futures.values()) {
                    future.completeExceptionally(throwable);
                }
            }
        };
        if (!topicsToReassignments.isEmpty()) {
            runnable.call(call, now);
        }
        return new AlterPartitionReassignmentsResult(new HashMap<>(futures));
    }

    /**
     * 列出当前正在进行的分区重分配任务
     * 
     * @param partitions 可选参数，指定要查询的主题分区集合。如果为空，则查询所有正在进行重分配的分区
     * @param options 操作的配置选项，包含超时时间等参数
     * @return ListPartitionReassignmentsResult 包含分区重分配信息的异步结果
     *         返回的Map中，key为TopicPartition(主题分区)，value为PartitionReassignment(包含当前副本、正在添加的副本和正在移除的副本)
     */
    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions,
                                                                       ListPartitionReassignmentsOptions options) {
        // 创建一个Future对象来存储异步操作的结果
        final KafkaFutureImpl<Map<TopicPartition, PartitionReassignment>> partitionReassignmentsFuture = new KafkaFutureImpl<>();
        
        // 如果指定了要查询的分区集合，则进行参数验证
        if (partitions.isPresent()) {
            for (TopicPartition tp : partitions.get()) {
                String topic = tp.topic();
                int partition = tp.partition();
                // 检查主题名称是否合法
                if (topicNameIsUnrepresentable(topic)) {
                    partitionReassignmentsFuture.completeExceptionally(new InvalidTopicException("The given topic name '"
                            + topic + "' cannot be represented in a request."));
                // 检查分区号是否合法
                } else if (partition < 0) {
                    partitionReassignmentsFuture.completeExceptionally(new InvalidTopicException("The given partition index " +
                            partition + " is not valid."));
                }
                // 如果参数验证失败，直接返回异常结果
                if (partitionReassignmentsFuture.isCompletedExceptionally())
                    return new ListPartitionReassignmentsResult(partitionReassignmentsFuture);
            }
        }
        // 获取当前时间戳
        final long now = time.milliseconds();
        // 创建并执行一个异步调用，该调用会发送到Kafka集群的控制器节点
        runnable.call(new Call("listPartitionReassignments", calcDeadlineMs(now, options.timeoutMs()),
            new ControllerNodeProvider(true)) {

            @Override
            ListPartitionReassignmentsRequest.Builder createRequest(int timeoutMs) {
                // 创建请求数据对象
                ListPartitionReassignmentsRequestData listData = new ListPartitionReassignmentsRequestData();
                // 设置请求超时时间
                listData.setTimeoutMs(timeoutMs);

                // 如果指定了要查询的分区集合，则构建请求数据
                if (partitions.isPresent()) {
                    // 创建一个Map来存储每个主题的重分配信息
                    Map<String, ListPartitionReassignmentsTopics> reassignmentTopicByTopicName = new HashMap<>();

                    // 遍历所有指定的主题分区
                    for (TopicPartition tp : partitions.get()) {
                        // 如果主题不存在于Map中，则创建一个新的主题条目
                        if (!reassignmentTopicByTopicName.containsKey(tp.topic()))
                            reassignmentTopicByTopicName.put(tp.topic(), new ListPartitionReassignmentsTopics().setName(tp.topic()));

                        // 将分区号添加到对应主题的分区列表中
                        reassignmentTopicByTopicName.get(tp.topic()).partitionIndexes().add(tp.partition());
                    }

                    // 将所有主题的重分配信息设置到请求数据中
                    listData.setTopics(new ArrayList<>(reassignmentTopicByTopicName.values()));
                }
                // 创建并返回请求构建器
                return new ListPartitionReassignmentsRequest.Builder(listData);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                // 将响应转换为ListPartitionReassignmentsResponse类型
                ListPartitionReassignmentsResponse response = (ListPartitionReassignmentsResponse) abstractResponse;
                // 获取响应中的错误码
                Errors error = Errors.forCode(response.data().errorCode());
                // 根据错误类型进行处理
                switch (error) {
                    case NONE:  // 没有错误，继续处理响应数据
                        break;
                    case NOT_CONTROLLER:  // 当前节点不是控制器，需要重新查找控制器节点
                        handleNotControllerError(error);
                        break;
                    default:  // 其他错误，将异常信息设置到Future中
                        partitionReassignmentsFuture.completeExceptionally(new ApiError(error, response.data().errorMessage()).exception());
                        break;
                }
                // 创建一个Map来存储分区重分配信息
                Map<TopicPartition, PartitionReassignment> reassignmentMap = new HashMap<>();

                // 遍历响应中的所有主题
                for (OngoingTopicReassignment topicReassignment : response.data().topics()) {
                    String topicName = topicReassignment.name();
                    // 遍历主题中的所有分区
                    for (OngoingPartitionReassignment partitionReassignment : topicReassignment.partitions()) {
                        // 将分区重分配信息添加到Map中
                        // key为主题分区，value为包含当前副本、正在添加的副本和正在移除的副本的信息
                        reassignmentMap.put(
                            new TopicPartition(topicName, partitionReassignment.partitionIndex()),
                            new PartitionReassignment(partitionReassignment.replicas(), partitionReassignment.addingReplicas(), partitionReassignment.removingReplicas())
                        );
                    }
                }

                // 将结果设置到Future中，完成异步操作
                partitionReassignmentsFuture.complete(reassignmentMap);
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理请求失败的情况，将异常信息设置到Future中
                partitionReassignmentsFuture.completeExceptionally(throwable);
            }
        }, now);

        // 返回包含异步操作结果的对象
        return new ListPartitionReassignmentsResult(partitionReassignmentsFuture);
    }

    /**
     * 处理请求返回的非控制器错误
     * 当请求发送到非控制器节点时，会返回NOT_CONTROLLER错误
     * 当使用引导控制器且请求发送到从属控制器时，可能返回NOT_LEADER_OR_FOLLOWER错误
     *
     * @param response 服务端的响应对象
     * @throws ApiException 如果发生错误则抛出异常
     */
    private void handleNotControllerError(AbstractResponse response) throws ApiException {
        // 当直接向从属控制器发送请求时，可能会返回NOT_LEADER_OR_FOLLOWER错误
        if (response.errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
            handleNotControllerError(Errors.NOT_CONTROLLER);
        } else if (metadataManager.usingBootstrapControllers() && response.errorCounts().containsKey(Errors.NOT_LEADER_OR_FOLLOWER)) {
            handleNotControllerError(Errors.NOT_LEADER_OR_FOLLOWER);
        }
    }

    /**
     * 处理非控制器错误的具体逻辑
     * 
     * @param error 错误类型
     * @throws ApiException 抛出对应的异常
     */
    private void handleNotControllerError(Errors error) throws ApiException {
        // 清除当前缓存的控制器信息
        metadataManager.clearController();
        // 请求更新元数据以获取新的控制器信息
        metadataManager.requestUpdate();
        // 抛出对应的异常
        throw error.exception();
    }

    /**
     * 返回与给定资源相关的broker id，如果该资源不与特定broker关联则返回null
     * Returns the broker id pertaining to the given resource, or null if the resource is not associated
     * with a particular broker.
     * 
     * @param resource 配置资源对象
     * @return 如果资源是broker或broker logger类型则返回broker id，否则返回null
     */
    private Integer nodeFor(ConfigResource resource) {
        // 判断资源类型是否为broker(且不是默认配置)或broker logger
        if ((resource.type() == ConfigResource.Type.BROKER && !resource.isDefault())
                || resource.type() == ConfigResource.Type.BROKER_LOGGER) {
            // 将资源名称转换为broker id并返回
            return Integer.valueOf(resource.name());
        } else {
            return null;
        }
    }

    /**
     * 从消费者组中获取成员列表
     * 
     * @param groupId 消费者组ID
     * @param reason 移除成员的原因
     * @return 包含组成员身份信息的Future对象
     */
    private KafkaFutureImpl<List<MemberIdentity>> getMembersFromGroup(String groupId, String reason) {
        // 创建Future对象用于异步返回结果
        KafkaFutureImpl<List<MemberIdentity>> future = new KafkaFutureImpl<>();

        // 调用describeConsumerGroups API获取组信息
        describeConsumerGroups(Collections.singleton(groupId)).describedGroups().get(groupId).whenComplete((res, ex) -> {
            if (ex != null) {
                // 如果发生异常，使用KafkaException包装异常并完成Future
                future.completeExceptionally(new KafkaException("Encounter exception when trying to get members from group: " + groupId, ex));
            } else {
                // 将消费者组成员转换为MemberIdentity对象列表
                List<MemberIdentity> membersToRemove = res.members().stream().map(member ->
                    // 优先使用group instance id，如果没有则使用member id
                    member.groupInstanceId().map(id -> new MemberIdentity().setGroupInstanceId(id))
                    .orElseGet(() -> new MemberIdentity().setMemberId(member.consumerId()))
                    .setReason(reason)
                ).collect(Collectors.toList());

                // 完成Future并返回结果
                future.complete(membersToRemove);
            }
        });

        return future;
    }

    /**
     * 注册指标订阅
     * 
     * @param metric 要注册的Kafka指标对象
     */
    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        // 如果存在遥测报告器，则注册指标变更
        if (clientTelemetryReporter.isPresent()) {
            ClientTelemetryReporter reporter = clientTelemetryReporter.get();
            reporter.metricChange(metric);
        }
    }

    /**
     * 取消指标订阅
     * 
     * @param metric 要取消订阅的Kafka指标对象
     */
    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        // 如果存在遥测报告器，则移除指标
        if (clientTelemetryReporter.isPresent()) {
            ClientTelemetryReporter reporter = clientTelemetryReporter.get();
            reporter.metricRemoval(metric);
        }
    }

    /**
     * 从消费者组中移除成员
     * 
     * @param groupId 消费者组ID
     * @param options 移除成员的选项，包含要移除的成员列表和原因
     * @return 移除成员的结果
     */
    @Override
    public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId,
                                                                               RemoveMembersFromConsumerGroupOptions options) {
        // 获取移除原因，如果未指定则使用默认原因
        String reason = options.reason() == null || options.reason().isEmpty() ?
            DEFAULT_LEAVE_GROUP_REASON : JoinGroupRequest.maybeTruncateReason(options.reason());

        // 创建AdminApiFuture用于处理移除结果
        final SimpleAdminApiFuture<CoordinatorKey, Map<MemberIdentity, Errors>> adminFuture =
                RemoveMembersFromConsumerGroupHandler.newFuture(groupId);

        KafkaFutureImpl<List<MemberIdentity>> memFuture;
        if (options.removeAll()) {
            // 如果要移除所有成员，则获取组中所有成员
            memFuture = getMembersFromGroup(groupId, reason);
        } else {
            // 否则只处理指定的成员列表
            memFuture = new KafkaFutureImpl<>();
            memFuture.complete(options.members().stream()
                    .map(m -> m.toMemberIdentity().setReason(reason))
                    .collect(Collectors.toList()));
        }

        // 处理成员移除的结果
        memFuture.whenComplete((members, ex) -> {
            if (ex != null) {
                // 如果发生异常，将异常信息添加到结果中
                adminFuture.completeExceptionally(Collections.singletonMap(CoordinatorKey.byGroupId(groupId), ex));
            } else {
                // 创建处理器并执行移除操作
                RemoveMembersFromConsumerGroupHandler handler = new RemoveMembersFromConsumerGroupHandler(groupId, members, logContext);
                invokeDriver(handler, adminFuture, options.timeoutMs());
            }
        });

        // 返回移除结果
        return new RemoveMembersFromConsumerGroupResult(adminFuture.get(CoordinatorKey.byGroupId(groupId)), options.members());
    }

    /**
     * 修改消费者组的偏移量
     * 
     * 该方法用于修改指定消费者组在特定主题分区上的消费偏移量。这对于手动调整消费位置、恢复数据或故障处理非常有用。
     * 
     * @param groupId 要修改偏移量的消费者组ID
     * @param offsets 需要修改的主题分区及其对应的新偏移量和元数据的映射
     * @param options 操作的配置选项，如超时时间等
     * @return 返回AlterConsumerGroupOffsetsResult对象，包含每个分区的修改结果
     */
    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(
        String groupId,
        Map<TopicPartition, OffsetAndMetadata> offsets,
        AlterConsumerGroupOffsetsOptions options
    ) {
        // 创建一个Future对象来处理异步操作结果
        SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, Errors>> future =
                AlterConsumerGroupOffsetsHandler.newFuture(groupId);
        // 创建处理器实例来执行实际的偏移量修改操作
        AlterConsumerGroupOffsetsHandler handler = new AlterConsumerGroupOffsetsHandler(groupId, offsets, logContext);
        // 调用驱动程序执行请求
        invokeDriver(handler, future, options.timeoutMs);
        // 返回结果对象，其中包含了每个分区的修改操作状态
        return new AlterConsumerGroupOffsetsResult(future.get(CoordinatorKey.byGroupId(groupId)));
    }

    /**
     * 获取主题分区的偏移量信息
     * 
     * 该方法用于查询指定主题分区的偏移量信息。可以查询最早、最新或特定时间点的偏移量。
     * 常用于监控、数据审计或故障恢复场景。
     * 
     * @param topicPartitionOffsets 主题分区和对应的偏移量规范(最早、最新、时间戳等)的映射
     * @param options 查询选项，包含超时时间等配置
     * @return 返回ListOffsetsResult对象，包含每个分区的偏移量查询结果
     */
    @Override
    public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets,
                                         ListOffsetsOptions options) {
        // 创建一个Future对象来处理异步查询结果，使用分区leader缓存优化性能
        PartitionLeaderStrategy.PartitionLeaderFuture<ListOffsetsResultInfo> future =
            ListOffsetsHandler.newFuture(topicPartitionOffsets.keySet(), partitionLeaderCache);
        // 将偏移量规范转换为具体的偏移量查询值
        Map<TopicPartition, Long> offsetQueriesByPartition = topicPartitionOffsets.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> getOffsetFromSpec(e.getValue())));
        // 创建处理器来执行实际的偏移量查询操作
        ListOffsetsHandler handler = new ListOffsetsHandler(offsetQueriesByPartition, options, logContext, defaultApiTimeoutMs);
        // 调用驱动程序执行请求
        invokeDriver(handler, future, options.timeoutMs);
        // 返回包含所有分区查询结果的对象
        return new ListOffsetsResult(future.all());
    }

    /**
     * 查询客户端配额信息
     * 
     * 该方法用于获取Kafka集群中设置的客户端配额信息。配额可以限制客户端的资源使用，
     * 如网络带宽、请求速率等。通过配额过滤器可以精确查询特定类型客户端的配额设置。
     * 
     * @param filter 配额过滤器，用于指定要查询的客户端类型和配额类型
     * @param options 查询选项，包含超时时间等配置
     * @return 返回DescribeClientQuotasResult对象，包含匹配的客户端配额信息
     */
    @Override
    public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter, DescribeClientQuotasOptions options) {
        // 创建Future对象来处理异步查询结果
        KafkaFutureImpl<Map<ClientQuotaEntity, Map<String, Double>>> future = new KafkaFutureImpl<>();

        final long now = time.milliseconds();
        // 创建并执行查询请求，使用最小负载节点策略选择目标broker
        runnable.call(new Call("describeClientQuotas", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {

                @Override
                DescribeClientQuotasRequest.Builder createRequest(int timeoutMs) {
                    // 创建查询请求，包含配额过滤条件
                    return new DescribeClientQuotasRequest.Builder(filter);
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    // 处理响应，将结果存入Future对象
                    DescribeClientQuotasResponse response = (DescribeClientQuotasResponse) abstractResponse;
                    response.complete(future);
                }

                @Override
                void handleFailure(Throwable throwable) {
                    // 处理异常情况，将异常信息存入Future对象
                    future.completeExceptionally(throwable);
                }
            }, now);

        return new DescribeClientQuotasResult(future);
    }

    /**
     * 修改客户端配额设置
     * 
     * 该方法用于修改Kafka集群中的客户端配额设置。可以同时修改多个客户端的多种配额类型，
     * 如网络带宽限制、请求速率等。支持验证模式，可以在实际修改前检查修改是否有效。
     * 
     * @param entries 要修改的配额条目集合，每个条目包含客户端实体和配额值
     * @param options 修改选项，包含是否仅验证、超时时间等配置
     * @return 返回AlterClientQuotasResult对象，包含每个修改操作的执行结果
     */
    @Override
    public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) {
        // 为每个待修改的配额条目创建对应的Future对象
        Map<ClientQuotaEntity, KafkaFutureImpl<Void>> futures = new HashMap<>(entries.size());
        for (ClientQuotaAlteration entry : entries) {
            futures.put(entry.entity(), new KafkaFutureImpl<>());
        }

        final long now = time.milliseconds();
        // 创建并执行修改请求，使用最小负载节点策略选择目标broker
        runnable.call(new Call("alterClientQuotas", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {

                @Override
                AlterClientQuotasRequest.Builder createRequest(int timeoutMs) {
                    // 创建修改请求，指定是否仅验证模式
                    return new AlterClientQuotasRequest.Builder(entries, options.validateOnly());
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    // 处理响应，更新每个修改操作的执行结果
                    AlterClientQuotasResponse response = (AlterClientQuotasResponse) abstractResponse;
                    response.complete(futures);
                }

                @Override
                void handleFailure(Throwable throwable) {
                    // 处理异常情况，将异常信息传播给所有Future对象
                    completeAllExceptionally(futures.values(), throwable);
                }
            }, now);

        return new AlterClientQuotasResult(Collections.unmodifiableMap(futures));
    }

    /**
     * 查询用户的SCRAM凭证信息
     * 
     * 该方法用于获取指定用户的SCRAM（Salted Challenge Response Authentication Mechanism）凭证信息。
     * SCRAM是Kafka支持的一种安全认证机制，用于验证客户端身份。
     * 
     * @param users 要查询SCRAM凭证的用户列表，如果为null或空则查询所有用户
     * @param options 查询选项，包含超时时间等配置
     * @return 返回DescribeUserScramCredentialsResult对象，包含用户的SCRAM凭证信息
     */
    @Override
    public DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users, DescribeUserScramCredentialsOptions options) {
        // 创建Future对象来处理异步查询结果
        final KafkaFutureImpl<DescribeUserScramCredentialsResponseData> dataFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        // 创建查询请求，使用最小负载节点策略选择目标broker
        Call call = new Call("describeUserScramCredentials", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {
            @Override
            public DescribeUserScramCredentialsRequest.Builder createRequest(final int timeoutMs) {
                // 创建请求数据对象
                final DescribeUserScramCredentialsRequestData requestData = new DescribeUserScramCredentialsRequestData();

                // 如果指定了用户列表，则添加到请求中
                if (users != null && !users.isEmpty()) {
                    final List<UserName> userNames = new ArrayList<>(users.size());

                    for (final String user : users) {
                        if (user != null) {
                            userNames.add(new UserName().setName(user));
                        }
                    }

                    requestData.setUsers(userNames);
                }

                return new DescribeUserScramCredentialsRequest.Builder(requestData);
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                // 处理响应数据
                DescribeUserScramCredentialsResponse response = (DescribeUserScramCredentialsResponse) abstractResponse;
                DescribeUserScramCredentialsResponseData data = response.data();
                short messageLevelErrorCode = data.errorCode();
                // 检查是否有错误发生
                if (messageLevelErrorCode != Errors.NONE.code()) {
                    // 如果有错误，将异常信息存入Future对象
                    dataFuture.completeExceptionally(Errors.forCode(messageLevelErrorCode).exception(data.errorMessage()));
                } else {
                    // 如果成功，将结果数据存入Future对象
                    dataFuture.complete(data);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理请求失败的情况
                dataFuture.completeExceptionally(throwable);
            }
        };
        // 执行请求
        runnable.call(call, now);
        return new DescribeUserScramCredentialsResult(dataFuture);
    }

    /**
     * 修改用户的SCRAM凭证信息，包括添加、更新和删除操作
     * SCRAM(Salted Challenge Response Authentication Mechanism)是一种基于密码的身份验证机制
     * 
     * @param alterations 要执行的凭证变更操作列表，可以包含添加/更新(UserScramCredentialUpsertion)和删除(UserScramCredentialDeletion)操作
     * @param options 操作的配置选项，如超时时间等
     * @return AlterUserScramCredentialsResult 包含每个用户操作的Future结果
     */
    @Override
    public AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations,
                                                                     AlterUserScramCredentialsOptions options) {
        // 获取当前时间戳，用于计算操作超时
        final long now = time.milliseconds();
        // 为每个用户创建一个Future，用于异步返回操作结果
        final Map<String, KafkaFutureImpl<Void>> futures = new HashMap<>();
        for (UserScramCredentialAlteration alteration: alterations) {
            futures.put(alteration.user(), new KafkaFutureImpl<>());
        }
        // 用于存储非法操作的异常信息，key为用户名
        final Map<String, Exception> userIllegalAlterationExceptions = new HashMap<>();
        // 需要跟踪使用未知SCRAM机制进行删除操作的用户
        final String usernameMustNotBeEmptyMsg = "Username must not be empty";
        String passwordMustNotBeEmptyMsg = "Password must not be empty";
        final String unknownScramMechanismMsg = "Unknown SCRAM mechanism";
        alterations.stream().filter(a -> a instanceof UserScramCredentialDeletion).forEach(alteration -> {
            final String user = alteration.user();
            if (user == null || user.isEmpty()) {
                userIllegalAlterationExceptions.put(alteration.user(), new UnacceptableCredentialException(usernameMustNotBeEmptyMsg));
            } else {
                UserScramCredentialDeletion deletion = (UserScramCredentialDeletion) alteration;
                ScramMechanism mechanism = deletion.mechanism();
                if (mechanism == null || mechanism == ScramMechanism.UNKNOWN) {
                    userIllegalAlterationExceptions.put(user, new UnsupportedSaslMechanismException(unknownScramMechanismMsg));
                }
            }
        });
        // 创建或更新凭证可能会抛出InvalidKeyException或NoSuchAlgorithmException异常
        // 需要跟踪受这些异常影响的用户，以便后续统一处理失败情况
        // 使用嵌套Map存储用户的凭证更新信息：外层key为用户名，内层key为SCRAM机制类型
        final Map<String, Map<ScramMechanism, AlterUserScramCredentialsRequestData.ScramCredentialUpsertion>> userInsertions = new HashMap<>();
        alterations.stream().filter(a -> a instanceof UserScramCredentialUpsertion)
                .filter(alteration -> !userIllegalAlterationExceptions.containsKey(alteration.user()))
                .forEach(alteration -> {
                    final String user = alteration.user();
                    if (user == null || user.isEmpty()) {
                        userIllegalAlterationExceptions.put(alteration.user(), new UnacceptableCredentialException(usernameMustNotBeEmptyMsg));
                    } else {
                        UserScramCredentialUpsertion upsertion = (UserScramCredentialUpsertion) alteration;
                        try {
                            byte[] password = upsertion.password();
                            if (password == null || password.length == 0) {
                                userIllegalAlterationExceptions.put(user, new UnacceptableCredentialException(passwordMustNotBeEmptyMsg));
                            } else {
                                ScramMechanism mechanism = upsertion.credentialInfo().mechanism();
                                if (mechanism == null || mechanism == ScramMechanism.UNKNOWN) {
                                    userIllegalAlterationExceptions.put(user, new UnsupportedSaslMechanismException(unknownScramMechanismMsg));
                                } else {
                                    userInsertions.putIfAbsent(user, new HashMap<>());
                                    userInsertions.get(user).put(mechanism, getScramCredentialUpsertion(upsertion));
                                }
                            }
                        } catch (NoSuchAlgorithmException e) {
                            // we might overwrite an exception from a previous alteration, but we don't really care
                            // since we just need to mark this user as having at least one illegal alteration
                            // and make an exception instance available for completing the corresponding future exceptionally
                            userIllegalAlterationExceptions.put(user, new UnsupportedSaslMechanismException(unknownScramMechanismMsg));
                        } catch (InvalidKeyException e) {
                            // generally shouldn't happen since we deal with the empty password case above,
                            // but we still need to catch/handle it
                            userIllegalAlterationExceptions.put(user, new UnacceptableCredentialException(e.getMessage(), e));
                        }
                    }
                });

        // 只为没有非法操作的用户提交凭证变更请求
        // 创建一个新的Call对象处理请求，设置超时时间和Controller节点提供者
        Call call = new Call("alterUserScramCredentials", calcDeadlineMs(now, options.timeoutMs()),
                new ControllerNodeProvider()) {
            @Override
            public AlterUserScramCredentialsRequest.Builder createRequest(int timeoutMs) {
                return new AlterUserScramCredentialsRequest.Builder(
                        new AlterUserScramCredentialsRequestData().setUpsertions(alterations.stream()
                                .filter(a -> a instanceof UserScramCredentialUpsertion)
                                .filter(a -> !userIllegalAlterationExceptions.containsKey(a.user()))
                                .map(a -> userInsertions.get(a.user()).get(((UserScramCredentialUpsertion) a).credentialInfo().mechanism()))
                                .collect(Collectors.toList()))
                        .setDeletions(alterations.stream()
                                .filter(a -> a instanceof UserScramCredentialDeletion)
                                .filter(a -> !userIllegalAlterationExceptions.containsKey(a.user()))
                                .map(d -> getScramCredentialDeletion((UserScramCredentialDeletion) d))
                                .collect(Collectors.toList())));
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                AlterUserScramCredentialsResponse response = (AlterUserScramCredentialsResponse) abstractResponse;
                // Check for controller change
                for (Errors error : response.errorCounts().keySet()) {
                    if (error == Errors.NOT_CONTROLLER) {
                        handleNotControllerError(error);
                    }
                }
                /* Now that we have the results for the ones we sent,
                 * fail any users that have an illegal alteration as identified above.
                 * Be sure to do this after the NOT_CONTROLLER error check above
                 * so that all errors are consistent in that case.
                 */
                userIllegalAlterationExceptions.entrySet().stream().forEach(entry ->
                    futures.get(entry.getKey()).completeExceptionally(entry.getValue())
                );
                response.data().results().forEach(result -> {
                    KafkaFutureImpl<Void> future = futures.get(result.user());
                    if (future == null) {
                        log.warn("Server response mentioned unknown user {}", result.user());
                    } else {
                        Errors error = Errors.forCode(result.errorCode());
                        if (error != Errors.NONE) {
                            future.completeExceptionally(error.exception(result.errorMessage()));
                        } else {
                            future.complete(null);
                        }
                    }
                });
                completeUnrealizedFutures(
                    futures.entrySet().stream(),
                    user -> "The broker response did not contain a result for user " + user);
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        };
        runnable.call(call, now);
        return new AlterUserScramCredentialsResult(new HashMap<>(futures));
    }

    /**
     * 将用户的SCRAM凭证更新操作转换为请求数据对象
     * 
     * @param u 用户SCRAM凭证更新操作对象
     * @return 转换后的请求数据对象
     * @throws InvalidKeyException 当密码无效时抛出
     * @throws NoSuchAlgorithmException 当指定的加密算法不可用时抛出
     */
    private static AlterUserScramCredentialsRequestData.ScramCredentialUpsertion getScramCredentialUpsertion(UserScramCredentialUpsertion u) throws InvalidKeyException, NoSuchAlgorithmException {
        // 创建新的凭证更新请求数据对象
        AlterUserScramCredentialsRequestData.ScramCredentialUpsertion retval = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion();
        // 设置用户名、SCRAM机制类型、迭代次数、盐值和加密后的密码
        return retval.setName(u.user())
                .setMechanism(u.credentialInfo().mechanism().type())
                .setIterations(u.credentialInfo().iterations())
                .setSalt(u.salt())
                .setSaltedPassword(getSaltedPassword(u.credentialInfo().mechanism(), u.password(), u.salt(), u.credentialInfo().iterations()));
    }

    /**
     * 将用户的SCRAM凭证删除操作转换为请求数据对象
     * 
     * @param d 用户SCRAM凭证删除操作对象
     * @return 转换后的请求数据对象
     */
    private static AlterUserScramCredentialsRequestData.ScramCredentialDeletion getScramCredentialDeletion(UserScramCredentialDeletion d) {
        // 创建新的凭证删除请求数据对象，设置用户名和SCRAM机制类型
        return new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName(d.user()).setMechanism(d.mechanism().type());
    }

    /**
     * 使用SCRAM算法计算加盐密码
     * 
     * @param publicScramMechanism SCRAM机制类型(如SCRAM-SHA-256)
     * @param password 原始密码字节数组
     * @param salt 盐值字节数组
     * @param iterations PBKDF2算法的迭代次数
     * @return 经过SCRAM算法处理的加盐密码字节数组
     * @throws NoSuchAlgorithmException 当指定的加密算法不可用时抛出
     * @throws InvalidKeyException 当密码无效时抛出
     */
    private static byte[] getSaltedPassword(ScramMechanism publicScramMechanism, byte[] password, byte[] salt, int iterations) throws NoSuchAlgorithmException, InvalidKeyException {
        // 创建SCRAM格式化器并计算加盐密码
        return new ScramFormatter(org.apache.kafka.common.security.scram.internals.ScramMechanism.forMechanismName(publicScramMechanism.mechanismName()))
                .hi(password, salt, iterations);
    }

    /**
     * 描述Kafka集群中的功能特性配置信息
     * 该方法通过发送ApiVersionsRequest请求来获取集群中的功能特性元数据，包括已完成版本范围和支持的版本范围
     *
     * @param options 描述功能特性的选项，包含超时时间等配置
     * @return DescribeFeaturesResult 包含功能特性元数据的异步结果
     */
    @Override
    public DescribeFeaturesResult describeFeatures(final DescribeFeaturesOptions options) {
        // 创建一个Future对象用于存储功能特性元数据的异步结果
        final KafkaFutureImpl<FeatureMetadata> future = new KafkaFutureImpl<>();
        // 获取当前时间戳
        final long now = time.milliseconds();
        // 创建一个Call对象，用于发送请求到负载最小的broker或活跃的KRaft控制器
        final Call call = new Call(
            "describeFeatures", calcDeadlineMs(now, options.timeoutMs()), new LeastLoadedBrokerOrActiveKController()) {

            /**
             * 从ApiVersions响应中创建功能特性元数据
             * 
             * @param response ApiVersions响应对象
             * @return FeatureMetadata 包含已完成和支持的功能特性版本范围信息
             */
            private FeatureMetadata createFeatureMetadata(final ApiVersionsResponse response) {
                // 创建已完成功能特性的版本范围映射
                final Map<String, FinalizedVersionRange> finalizedFeatures = new HashMap<>();
                for (final FinalizedFeatureKey key : response.data().finalizedFeatures().valuesSet()) {
                    // 将每个已完成功能特性的名称和版本范围添加到映射中
                    finalizedFeatures.put(key.name(), new FinalizedVersionRange(key.minVersionLevel(), key.maxVersionLevel()));
                }

                // 获取已完成功能特性的纪元（epoch）
                Optional<Long> finalizedFeaturesEpoch;
                if (response.data().finalizedFeaturesEpoch() >= 0L) {
                    finalizedFeaturesEpoch = Optional.of(response.data().finalizedFeaturesEpoch());
                } else {
                    finalizedFeaturesEpoch = Optional.empty();
                }

                // 创建支持的功能特性的版本范围映射
                final Map<String, SupportedVersionRange> supportedFeatures = new HashMap<>();
                for (final SupportedFeatureKey key : response.data().supportedFeatures().valuesSet()) {
                    // 将每个支持的功能特性的名称和版本范围添加到映射中
                    supportedFeatures.put(key.name(), new SupportedVersionRange(key.minVersion(), key.maxVersion()));
                }

                // 创建并返回功能特性元数据对象
                return new FeatureMetadata(finalizedFeatures, finalizedFeaturesEpoch, supportedFeatures);
            }

            /**
             * 创建ApiVersions请求构建器
             * 
             * @param timeoutMs 请求超时时间（毫秒）
             * @return ApiVersionsRequest.Builder 请求构建器实例
             */
            @Override
            ApiVersionsRequest.Builder createRequest(int timeoutMs) {
                // 创建一个新的ApiVersions请求构建器
                return new ApiVersionsRequest.Builder();
            }

            /**
             * 处理ApiVersions响应
             * 
             * @param response 服务器返回的响应
             */
            @Override
            void handleResponse(AbstractResponse response) {
                // 将响应转换为ApiVersions响应类型
                final ApiVersionsResponse apiVersionsResponse = (ApiVersionsResponse) response;
                // 检查响应中的错误码
                if (apiVersionsResponse.data().errorCode() == Errors.NONE.code()) {
                    // 如果没有错误，创建功能特性元数据并完成Future
                    future.complete(createFeatureMetadata(apiVersionsResponse));
                } else {
                    // 如果有错误，使用相应的异常完成Future
                    future.completeExceptionally(Errors.forCode(apiVersionsResponse.data().errorCode()).exception());
                }
            }

            /**
             * 处理请求失败的情况
             * 
             * @param throwable 失败原因的异常
             */
            @Override
            void handleFailure(Throwable throwable) {
                // 使用异常完成Future
                completeAllExceptionally(Collections.singletonList(future), throwable);
            }
        };

        runnable.call(call, now);
        return new DescribeFeaturesResult(future);
    }

    /**
     * 更新Kafka集群中的功能特性配置
     * 该方法用于更新功能特性的版本级别和升级类型，支持批量更新多个功能特性
     *
     * @param featureUpdates 要更新的功能特性映射，键为功能特性名称，值为更新信息
     * @param options 更新功能特性的选项，包含超时时间和验证模式等配置
     * @return UpdateFeaturesResult 包含更新结果的异步结果
     * @throws IllegalArgumentException 当功能特性更新列表为空或包含空名称时
     */
    @Override
    public UpdateFeaturesResult updateFeatures(final Map<String, FeatureUpdate> featureUpdates,
                                               final UpdateFeaturesOptions options) {
        // 检查更新列表是否为空
        if (featureUpdates.isEmpty()) {
            throw new IllegalArgumentException("Feature updates can not be null or empty.");
        }

        // 为每个功能特性创建对应的Future对象
        final Map<String, KafkaFutureImpl<Void>> updateFutures = new HashMap<>();
        for (final Map.Entry<String, FeatureUpdate> entry : featureUpdates.entrySet()) {
            final String feature = entry.getKey();
            // 检查功能特性名称是否为空
            if (Utils.isBlank(feature)) {
                throw new IllegalArgumentException("Provided feature can not be empty.");
            }
            // 为每个功能特性创建一个Future对象
            updateFutures.put(entry.getKey(), new KafkaFutureImpl<>());
        }

        final long now = time.milliseconds();
        final Call call = new Call("updateFeatures", calcDeadlineMs(now, options.timeoutMs()),
            new ControllerNodeProvider(true)) {

            /**
             * 创建更新功能特性的请求构建器
             * 
             * @param timeoutMs 请求超时时间（毫秒）
             * @return UpdateFeaturesRequest.Builder 请求构建器实例
             */
            @Override
            UpdateFeaturesRequest.Builder createRequest(int timeoutMs) {
                // 创建功能特性更新集合
                final UpdateFeaturesRequestData.FeatureUpdateKeyCollection featureUpdatesRequestData
                    = new UpdateFeaturesRequestData.FeatureUpdateKeyCollection();
                // 遍历所有需要更新的功能特性
                for (Map.Entry<String, FeatureUpdate> entry : featureUpdates.entrySet()) {
                    final String feature = entry.getKey();
                    final FeatureUpdate update = entry.getValue();
                    // 创建功能特性更新请求项
                    final UpdateFeaturesRequestData.FeatureUpdateKey requestItem =
                        new UpdateFeaturesRequestData.FeatureUpdateKey();
                    requestItem.setFeature(feature);
                    requestItem.setMaxVersionLevel(update.maxVersionLevel());
                    requestItem.setUpgradeType(update.upgradeType().code());
                    // 将请求项添加到更新集合中
                    featureUpdatesRequestData.add(requestItem);
                }
                // 创建并返回请求构建器
                return new UpdateFeaturesRequest.Builder(
                    new UpdateFeaturesRequestData()
                        .setTimeoutMs(timeoutMs)
                        .setValidateOnly(options.validateOnly())
                        .setFeatureUpdates(featureUpdatesRequestData));
            }

            /**
             * 处理更新功能特性的响应
             * 
             * @param abstractResponse 服务器返回的响应
             */
            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                // 将响应转换为UpdateFeatures响应类型
                final UpdateFeaturesResponse response =
                    (UpdateFeaturesResponse) abstractResponse;

                // 获取顶层错误信息
                ApiError topLevelError = response.topLevelError();
                switch (topLevelError.error()) {
                    case NONE:
                        // 对于V2及以上版本，无错误响应只会有一个顶层NONE错误 - 标记所有Future为完成
                        if (response.data().results().isEmpty()) {
                            // 如果结果为空，完成所有Future
                            for (final KafkaFutureImpl<Void> future : updateFutures.values()) {
                                future.complete(null);
                            }
                        } else {
                            // 处理每个功能特性的更新结果
                            for (final UpdatableFeatureResult result : response.data().results()) {
                                final KafkaFutureImpl<Void> future = updateFutures.get(result.feature());
                                if (future == null) {
                                    // 如果发现未知的功能特性，记录警告日志
                                    log.warn("Server response mentioned unknown feature {}", result.feature());
                                } else {
                                    // 检查每个功能特性的错误码
                                    final Errors error = Errors.forCode(result.errorCode());
                                    if (error == Errors.NONE) {
                                        future.complete(null);
                                    } else {
                                        future.completeExceptionally(error.exception(result.errorMessage()));
                                    }
                                }
                            }
                            // 服务器应该为每个功能特性返回结果，这里进行完整性检查
                            completeUnrealizedFutures(updateFutures.entrySet().stream(),
                                    feature -> "The controller response did not contain a result for feature " + feature);
                        }
                        break;
                    case NOT_CONTROLLER:
                        // 如果不是控制器节点，处理相应错误
                        handleNotControllerError(topLevelError.error());
                        break;
                    default:
                        // 处理其他错误情况，使所有Future异常完成
                        for (final Map.Entry<String, KafkaFutureImpl<Void>> entry : updateFutures.entrySet()) {
                            entry.getValue().completeExceptionally(topLevelError.exception());
                        }
                        break;
                }
            }

            /**
             * 处理请求失败的情况
             * 
             * @param throwable 失败原因的异常
             */
            @Override
            void handleFailure(Throwable throwable) {
                // 使用异常完成所有Future
                completeAllExceptionally(updateFutures.values(), throwable);
            }
        };

        runnable.call(call, now);
        return new UpdateFeaturesResult(new HashMap<>(updateFutures));
    }

    /**
     * 描述Kafka集群的元数据仲裁信息
     * 此方法用于获取Kafka集群中元数据主题的仲裁状态信息，包括leader、投票者和观察者等信息
     *
     * @param options 描述元数据仲裁的选项，包含超时时间等配置
     * @return DescribeMetadataQuorumResult 包含仲裁信息的异步结果
     */
    @Override
    public DescribeMetadataQuorumResult describeMetadataQuorum(DescribeMetadataQuorumOptions options) {
        // 创建节点提供者，用于选择负载最小的broker或活跃的KRaft控制器
        NodeProvider provider = new LeastLoadedBrokerOrActiveKController();

        // 创建用于存储异步操作结果的Future对象
        final KafkaFutureImpl<QuorumInfo> future = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        // 创建RPC调用对象
        final Call call = new Call(
                "describeMetadataQuorum", calcDeadlineMs(now, options.timeoutMs()), provider) {

            /**
             * 将服务器响应中的副本状态转换为客户端使用的QuorumInfo.ReplicaState对象
             * 
             * @param replica 服务器返回的副本状态数据
             * @return 转换后的副本状态对象
             */
            private QuorumInfo.ReplicaState translateReplicaState(DescribeQuorumResponseData.ReplicaState replica) {
                return new QuorumInfo.ReplicaState(
                        replica.replicaId(), // 副本ID
                        replica.replicaDirectoryId() == null ? Uuid.ZERO_UUID : replica.replicaDirectoryId(), // 副本目录ID，如果为空则使用ZERO_UUID
                        replica.logEndOffset(), // 日志末端偏移量
                        replica.lastFetchTimestamp() == -1 ? OptionalLong.empty() : OptionalLong.of(replica.lastFetchTimestamp()), // 最后一次拉取时间戳
                        replica.lastCaughtUpTimestamp() == -1 ? OptionalLong.empty() : OptionalLong.of(replica.lastCaughtUpTimestamp())); // 最后一次追赶上的时间戳
            }

            /**
             * 根据服务器响应创建仲裁信息结果对象
             * 
             * @param partition 分区数据，包含leader、epoch和投票者等信息
             * @param nodeCollection 节点集合，包含所有参与仲裁的节点信息
             * @return 封装后的仲裁信息对象
             */
            private QuorumInfo createQuorumResult(final DescribeQuorumResponseData.PartitionData partition, DescribeQuorumResponseData.NodeCollection nodeCollection) {
                // 转换当前的投票者列表
                List<QuorumInfo.ReplicaState> voters = partition.currentVoters().stream()
                    .map(this::translateReplicaState)
                    .collect(Collectors.toList());

                // 转换观察者列表
                List<QuorumInfo.ReplicaState> observers = partition.observers().stream()
                    .map(this::translateReplicaState)
                    .collect(Collectors.toList());

                // 转换节点信息，包括节点ID和监听器端点
                Map<Integer, QuorumInfo.Node> nodes = nodeCollection.stream().map(n -> {
                    List<RaftVoterEndpoint> endpoints = n.listeners().stream()
                        .map(l -> new RaftVoterEndpoint(l.name(), l.host(), l.port()))
                        .collect(Collectors.toList());

                    return new QuorumInfo.Node(n.nodeId(), endpoints);
                }).collect(Collectors.toMap(QuorumInfo.Node::nodeId, Function.identity()));

                // 创建并返回仲裁信息对象
                return new QuorumInfo(
                    partition.leaderId(), // leader节点ID
                    partition.leaderEpoch(), // leader的任期号
                    partition.highWatermark(), // 高水位线
                    voters, // 投票者列表
                    observers, // 观察者列表
                    nodes // 节点信息映射
                );
            }

            @Override
            DescribeQuorumRequest.Builder createRequest(int timeoutMs) {
                return new Builder(DescribeQuorumRequest.singletonRequest(
                        new TopicPartition(CLUSTER_METADATA_TOPIC_NAME, CLUSTER_METADATA_TOPIC_PARTITION.partition())));
            }

            @Override
            void handleResponse(AbstractResponse response) {
                handleNotControllerError(response);
                final DescribeQuorumResponse quorumResponse = (DescribeQuorumResponse) response;
                if (quorumResponse.data().errorCode() != Errors.NONE.code()) {
                    throw Errors.forCode(quorumResponse.data().errorCode()).exception(quorumResponse.data().errorMessage());
                }
                if (quorumResponse.data().topics().size() != 1) {
                    String msg = String.format("DescribeMetadataQuorum received %d topics when 1 was expected",
                            quorumResponse.data().topics().size());
                    log.debug(msg);
                    throw new UnknownServerException(msg);
                }
                DescribeQuorumResponseData.TopicData topic = quorumResponse.data().topics().get(0);
                if (!topic.topicName().equals(CLUSTER_METADATA_TOPIC_NAME)) {
                    String msg = String.format("DescribeMetadataQuorum received a topic with name %s when %s was expected",
                            topic.topicName(), CLUSTER_METADATA_TOPIC_NAME);
                    log.debug(msg);
                    throw new UnknownServerException(msg);
                }
                if (topic.partitions().size() != 1) {
                    String msg = String.format("DescribeMetadataQuorum received a topic %s with %d partitions when 1 was expected",
                            topic.topicName(), topic.partitions().size());
                    log.debug(msg);
                    throw new UnknownServerException(msg);
                }
                DescribeQuorumResponseData.PartitionData partition = topic.partitions().get(0);
                if (partition.partitionIndex() != CLUSTER_METADATA_TOPIC_PARTITION.partition()) {
                    String msg = String.format("DescribeMetadataQuorum received a single partition with index %d when %d was expected",
                            partition.partitionIndex(), CLUSTER_METADATA_TOPIC_PARTITION.partition());
                    log.debug(msg);
                    throw new UnknownServerException(msg);
                }
                if (partition.errorCode() != Errors.NONE.code()) {
                    throw Errors.forCode(partition.errorCode()).exception(partition.errorMessage());
                }
                future.complete(createQuorumResult(partition, quorumResponse.data().nodes()));
            }

            @Override
            void handleFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        };

        runnable.call(call, now);
        return new DescribeMetadataQuorumResult(future);
    }

    /**
     * 注销指定的Broker
     * 此方法用于从Kafka集群中注销一个Broker，使其不再参与集群的工作
     *
     * @param brokerId 要注销的Broker的ID
     * @param options 注销操作的选项，包含超时时间等配置
     * @return UnregisterBrokerResult 注销操作的异步结果
     */
    @Override
    public UnregisterBrokerResult unregisterBroker(int brokerId, UnregisterBrokerOptions options) {
        // 创建用于存储异步操作结果的Future对象
        final KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        // 创建RPC调用对象，使用负载最小的节点处理请求
        final Call call = new Call("unregisterBroker", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {

            @Override
            UnregisterBrokerRequest.Builder createRequest(int timeoutMs) {
                UnregisterBrokerRequestData data =
                        new UnregisterBrokerRequestData().setBrokerId(brokerId);
                return new UnregisterBrokerRequest.Builder(data);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                final UnregisterBrokerResponse response =
                        (UnregisterBrokerResponse) abstractResponse;
                Errors error = Errors.forCode(response.data().errorCode());
                switch (error) {
                    case NONE:
                        future.complete(null);
                        break;
                    case REQUEST_TIMED_OUT:
                        throw error.exception();
                    default:
                        log.error("Unregister broker request for broker ID {} failed: {}",
                            brokerId, error.message());
                        future.completeExceptionally(error.exception());
                        break;
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        };
        runnable.call(call, now);
        return new UnregisterBrokerResult(future);
    }

    /**
     * 描述指定主题分区上的生产者信息
     * 此方法用于获取当前活跃的生产者的详细信息，包括生产者ID、事务状态等
     *
     * @param topicPartitions 要查询的主题分区集合
     * @param options 描述生产者的选项，包含超时时间等配置
     * @return DescribeProducersResult 包含生产者信息的异步结果
     */
    @Override
    public DescribeProducersResult describeProducers(Collection<TopicPartition> topicPartitions, DescribeProducersOptions options) {
        // 创建分区leader策略的Future对象，用于存储每个分区的生产者状态
        PartitionLeaderStrategy.PartitionLeaderFuture<DescribeProducersResult.PartitionProducerState> future =
            DescribeProducersHandler.newFuture(topicPartitions, partitionLeaderCache);
        // 创建处理器对象
        DescribeProducersHandler handler = new DescribeProducersHandler(options, logContext);
        // 调用驱动程序执行请求
        invokeDriver(handler, future, options.timeoutMs);
        // 返回包含所有分区生产者信息的结果
        return new DescribeProducersResult(future.all());
    }

    /**
     * 描述指定事务ID的事务状态信息
     * 此方法用于获取事务的详细信息，包括事务状态、生产者ID、超时时间等
     *
     * @param transactionalIds 要查询的事务ID集合
     * @param options 描述事务的选项，包含超时时间等配置
     * @return DescribeTransactionsResult 包含事务信息的异步结果
     */
    @Override
    public DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds, DescribeTransactionsOptions options) {
        // 创建管理API的Future对象，用于存储事务描述信息
        AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, TransactionDescription> future =
            DescribeTransactionsHandler.newFuture(transactionalIds);
        // 创建事务描述处理器
        DescribeTransactionsHandler handler = new DescribeTransactionsHandler(logContext);
        // 调用驱动程序执行请求
        invokeDriver(handler, future, options.timeoutMs);
        // 返回包含所有事务信息的结果
        return new DescribeTransactionsResult(future.all());
    }

    /**
     * 中止(回滚)指定事务的操作
     * 
     * @param spec 事务中止规范,包含要中止的事务的主题分区和事务ID等信息
     * @param options 中止事务的选项配置,如超时时间等
     * @return AbortTransactionResult 中止事务的结果
     */
    @Override
    public AbortTransactionResult abortTransaction(AbortTransactionSpec spec, AbortTransactionOptions options) {
        // 创建一个分区leader策略的Future,用于跟踪事务中止操作的完成状态
        // 这里只处理单个主题分区的事务中止
        PartitionLeaderStrategy.PartitionLeaderFuture<Void> future =
            AbortTransactionHandler.newFuture(Collections.singleton(spec.topicPartition()), partitionLeaderCache);
        
        // 创建事务中止处理器,负责具体的事务中止逻辑
        AbortTransactionHandler handler = new AbortTransactionHandler(spec, logContext);
        
        // 调用驱动程序执行事务中止操作
        invokeDriver(handler, future, options.timeoutMs);
        
        // 返回事务中止结果
        return new AbortTransactionResult(future.all());
    }

    /**
     * 列出集群中所有正在进行的事务
     * 
     * @param options 列出事务的选项配置,如超时时间等
     * @return ListTransactionsResult 包含所有正在进行的事务列表的结果
     */
    @Override
    public ListTransactionsResult listTransactions(ListTransactionsOptions options) {
        // 创建一个面向所有broker的Future,用于收集所有broker上的事务信息
        AllBrokersStrategy.AllBrokersFuture<Collection<TransactionListing>> future =
            ListTransactionsHandler.newFuture();
        
        // 创建列出事务的处理器
        ListTransactionsHandler handler = new ListTransactionsHandler(options, logContext);
        
        // 调用驱动程序执行列出事务的操作
        invokeDriver(handler, future, options.timeoutMs);
        
        // 返回事务列表结果
        return new ListTransactionsResult(future.all());
    }

    /**
     * 将指定事务ID的生产者设置为已中止状态(fence),防止这些生产者继续进行事务操作
     * 
     * @param transactionalIds 要设置fence状态的事务ID集合
     * @param options fence操作的选项配置
     * @return FenceProducersResult fence操作的结果
     */
    @Override
    public FenceProducersResult fenceProducers(Collection<String> transactionalIds, FenceProducersOptions options) {
        // 创建一个简单的AdminAPI Future,用于跟踪fence操作的完成状态
        // 每个事务ID都会映射到对应的ProducerId和Epoch
        AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, ProducerIdAndEpoch> future =
            FenceProducersHandler.newFuture(transactionalIds);
        
        // 创建fence处理器,负责执行具体的fence逻辑
        FenceProducersHandler handler = new FenceProducersHandler(options, logContext, requestTimeoutMs);
        
        // 调用驱动程序执行fence操作
        invokeDriver(handler, future, options.timeoutMs);
        
        // 返回fence操作结果
        return new FenceProducersResult(future.all());
    }

    /**
     * 列出集群中所有客户端的度量资源信息
     * 
     * @param options 列出客户端度量资源的选项配置
     * @return ListClientMetricsResourcesResult 包含所有客户端度量资源列表的结果
     */
    @Override
    public ListClientMetricsResourcesResult listClientMetricsResources(ListClientMetricsResourcesOptions options) {
        // 获取当前时间戳
        final long now = time.milliseconds();
        
        // 创建一个Future用于异步获取结果
        final KafkaFutureImpl<Collection<ClientMetricsResourceListing>> future = new KafkaFutureImpl<>();
        
        // 创建并执行一个异步调用
        runnable.call(new Call("listClientMetricsResources", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            // 创建列出客户端度量资源的请求
            @Override
            ListClientMetricsResourcesRequest.Builder createRequest(int timeoutMs) {
                return new ListClientMetricsResourcesRequest.Builder(new ListClientMetricsResourcesRequestData());
            }

            // 处理服务器的响应
            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                ListClientMetricsResourcesResponse response = (ListClientMetricsResourcesResponse) abstractResponse;
                if (response.error().isFailure()) {
                    // 如果响应包含错误,则完成Future并附带异常
                    future.completeExceptionally(response.error().exception());
                } else {
                    // 成功获取度量资源列表,完成Future
                    future.complete(response.clientMetricsResources());
                }
            }

            // 处理请求失败的情况
            @Override
            void handleFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        }, now);
        
        // 返回包含Future的结果对象
        return new ListClientMetricsResourcesResult(future);
    }

    /**
     * 向Kafka集群添加一个新的Raft投票者节点
     * 
     * @param voterId 新投票者的唯一标识ID
     * @param voterDirectoryId 投票者的目录ID，用于标识投票者在集群中的位置
     * @param endpoints 投票者节点的网络端点集合，包含名称、主机地址和端口信息
     * @param options 添加投票者的配置选项，如超时时间和集群ID等
     * @return AddRaftVoterResult 异步操作结果，包含操作是否成功的Future对象
     */
    @Override
    public AddRaftVoterResult addRaftVoter(
        int voterId,
        Uuid voterDirectoryId,
        Set<RaftVoterEndpoint> endpoints,
        AddRaftVoterOptions options
    ) {
        // 创建节点提供者，使用负载最小的broker或活跃的KRaft控制器
        NodeProvider provider = new LeastLoadedBrokerOrActiveKController();

        // 创建异步操作的Future对象
        final KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        // 创建RPC调用对象
        final Call call = new Call(
                "addRaftVoter", calcDeadlineMs(now, options.timeoutMs()), provider) {

            @Override
            AddRaftVoterRequest.Builder createRequest(int timeoutMs) {
                // 构建投票者的监听器集合
                AddRaftVoterRequestData.ListenerCollection listeners =
                    new AddRaftVoterRequestData.ListenerCollection();
                // 将每个端点信息添加到监听器集合中
                endpoints.forEach(endpoint ->
                    listeners.add(new AddRaftVoterRequestData.Listener().
                        setName(endpoint.name()).
                        setHost(endpoint.host()).
                        setPort(endpoint.port())));
                // 创建添加投票者请求
                return new AddRaftVoterRequest.Builder(
                        new AddRaftVoterRequestData().
                            setClusterId(options.clusterId().orElse(null)).
                            setTimeoutMs(timeoutMs).
                            setVoterId(voterId) .
                            setVoterDirectoryId(voterDirectoryId).
                            setListeners(listeners));
            }

            @Override
            void handleResponse(AbstractResponse response) {
                // 处理非控制器错误
                handleNotControllerError(response);
                AddRaftVoterResponse addResponse = (AddRaftVoterResponse) response;
                // 检查响应中的错误码
                if (addResponse.data().errorCode() != Errors.NONE.code()) {
                    // 如果存在错误，将Future标记为异常完成
                    ApiError error = new ApiError(
                        addResponse.data().errorCode(),
                        addResponse.data().errorMessage());
                    future.completeExceptionally(error.exception());
                } else {
                    // 操作成功，完成Future
                    future.complete(null);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理调用失败的情况
                future.completeExceptionally(throwable);
            }
        };
        // 执行RPC调用
        runnable.call(call, now);
        return new AddRaftVoterResult(future);
    }

    /**
     * 从Kafka集群中移除一个Raft投票者节点
     * 
     * @param voterId 要移除的投票者的唯一标识ID
     * @param voterDirectoryId 投票者的目录ID
     * @param options 移除投票者的配置选项
     * @return RemoveRaftVoterResult 异步操作结果
     */
    @Override
    public RemoveRaftVoterResult removeRaftVoter(
        int voterId,
        Uuid voterDirectoryId,
        RemoveRaftVoterOptions options
    ) {
        // 创建节点提供者，使用负载最小的broker或活跃的KRaft控制器
        NodeProvider provider = new LeastLoadedBrokerOrActiveKController();

        // 创建异步操作的Future对象
        final KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        // 创建RPC调用对象
        final Call call = new Call(
                "removeRaftVoter", calcDeadlineMs(now, options.timeoutMs()), provider) {

            @Override
            RemoveRaftVoterRequest.Builder createRequest(int timeoutMs) {
                // 创建移除投票者请求
                return new RemoveRaftVoterRequest.Builder(
                    new RemoveRaftVoterRequestData().
                        setClusterId(options.clusterId().orElse(null)).
                        setVoterId(voterId) .
                        setVoterDirectoryId(voterDirectoryId));
            }

            @Override
            void handleResponse(AbstractResponse response) {
                // 处理非控制器错误
                handleNotControllerError(response);
                RemoveRaftVoterResponse addResponse = (RemoveRaftVoterResponse) response;
                // 检查响应中的错误码
                if (addResponse.data().errorCode() != Errors.NONE.code()) {
                    // 如果存在错误，将Future标记为异常完成
                    ApiError error = new ApiError(
                            addResponse.data().errorCode(),
                            addResponse.data().errorMessage());
                    future.completeExceptionally(error.exception());
                } else {
                    // 操作成功，完成Future
                    future.complete(null);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 处理调用失败的情况
                future.completeExceptionally(throwable);
            }
        };
        // 执行RPC调用
        runnable.call(call, now);
        return new RemoveRaftVoterResult(future);
    }

    /**
     * 获取客户端实例的唯一标识ID
     * 
     * @param timeout 获取实例ID的超时时间
     * @return Uuid 客户端实例的唯一标识ID
     * @throws IllegalArgumentException 如果超时时间为负数
     * @throws IllegalStateException 如果遥测功能未启用
     */
    @Override
    public Uuid clientInstanceId(Duration timeout) {
        // 检查超时时间是否为负数
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("The timeout cannot be negative.");
        }

        // 检查遥测报告器是否可用
        if (clientTelemetryReporter.isEmpty()) {
            throw new IllegalStateException("Telemetry is not enabled. Set config `" + AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");

        }

        // 如果已经有实例ID，直接返回
        if (clientInstanceId != null) {
            return clientInstanceId;
        }

        // 通过遥测工具获取客户端实例ID
        clientInstanceId = ClientTelemetryUtils.fetchClientInstanceId(clientTelemetryReporter.get(), timeout);
        return clientInstanceId;
    }

    /**
     * 调用AdminApiDriver来处理管理API请求
     * 
     * @param handler 处理具体API请求的处理器
     * @param future 用于获取API调用结果的Future对象
     * @param timeoutMs 请求超时时间(毫秒)
     * @param <K> 请求键的类型
     * @param <V> 响应值的类型
     */
    private <K, V> void invokeDriver(
        AdminApiHandler<K, V> handler,
        AdminApiFuture<K, V> future,
        Integer timeoutMs
    ) {
        // 获取当前时间戳
        long currentTimeMs = time.milliseconds();
        // 计算请求的截止时间
        long deadlineMs = calcDeadlineMs(currentTimeMs, timeoutMs);

        // 创建AdminApiDriver实例来处理请求
        AdminApiDriver<K, V> driver = new AdminApiDriver<>(
            handler,            // API请求处理器
            future,             // 用于获取结果的Future
            deadlineMs,         // 请求截止时间
            retryBackoffMs,     // 重试等待时间
            retryBackoffMaxMs,  // 最大重试等待时间
            logContext          // 日志上下文
        );

        // 尝试发送请求
        maybeSendRequests(driver, currentTimeMs);
    }

    /**
     * 尝试发送管理API请求
     * 
     * @param driver 管理API请求的驱动器
     * @param currentTimeMs 当前时间戳
     * @param <K> 请求键的类型
     * @param <V> 响应值的类型
     */
    private <K, V> void maybeSendRequests(AdminApiDriver<K, V> driver, long currentTimeMs) {
        // 轮询获取所有待发送的请求
        for (AdminApiDriver.RequestSpec<K> spec : driver.poll()) {
            // 为每个请求创建新的Call对象并执行
            runnable.call(newCall(driver, spec), currentTimeMs);
        }
    }

    /**
     * 创建新的Call对象来处理具体的请求
     * 
     * @param driver 管理API请求的驱动器
     * @param spec 请求的具体规格说明
     * @param <K> 请求键的类型
     * @param <V> 响应值的类型
     * @return 新创建的Call对象
     */
    private <K, V> Call newCall(AdminApiDriver<K, V> driver, AdminApiDriver.RequestSpec<K> spec) {
        // 根据请求规格选择节点提供器
        // 如果指定了目标broker ID，使用固定节点提供器
        // 否则使用负载最小的节点提供器
        NodeProvider nodeProvider = spec.scope.destinationBrokerId().isPresent() ?
            new ConstantNodeIdProvider(spec.scope.destinationBrokerId().getAsInt()) :
            new LeastLoadedNodeProvider();
            
        // 创建新的Call对象
        return new Call(spec.name, spec.nextAllowedTryMs, spec.tries, spec.deadlineMs, nodeProvider) {
            @Override
            AbstractRequest.Builder<?> createRequest(int timeoutMs) {
                // 返回请求构建器
                return spec.request;
            }

            @Override
            void handleResponse(AbstractResponse response) {
                // 获取当前时间戳
                long currentTimeMs = time.milliseconds();
                // 处理响应
                driver.onResponse(currentTimeMs, spec, response, this.curNode());
                // 继续发送其他待处理的请求
                maybeSendRequests(driver, currentTimeMs);
            }

            @Override
            void handleFailure(Throwable throwable) {
                // 获取当前时间戳
                long currentTimeMs = time.milliseconds();
                // 处理失败情况
                driver.onFailure(currentTimeMs, spec, throwable);
                // 继续发送其他待处理的请求
                maybeSendRequests(driver, currentTimeMs);
            }

            @Override
            void maybeRetry(long currentTimeMs, Throwable throwable) {
                if (throwable instanceof DisconnectException) {
                    // 断开连接是特殊情况，我们希望给驱动器一个重试查找的机会
                    // 而不是停留在已经宕机的节点上
                    // 例如，如果分区leader在我们的元数据查询之后关闭
                    // 那么我们可能会遇到断开连接的情况
                    // 我们希望尝试找到新的分区leader而不是在同一个节点上重试
                    driver.onFailure(currentTimeMs, spec, throwable);
                    maybeSendRequests(driver, currentTimeMs);
                } else {
                    // 对于其他异常，使用父类的重试逻辑
                    super.maybeRetry(currentTimeMs, throwable);
                }
            }
        };
    }

    /**
     * 从OffsetSpec对象获取对应的时间戳值
     * 
     * @param offsetSpec 偏移量规格说明对象
     * @return 对应的时间戳值
     */
    private static long getOffsetFromSpec(OffsetSpec offsetSpec) {
        // 根据不同的OffsetSpec类型返回相应的时间戳
        if (offsetSpec instanceof TimestampSpec) {
            // 如果是指定时间戳，直接返回该时间戳
            return ((TimestampSpec) offsetSpec).timestamp();
        } else if (offsetSpec instanceof OffsetSpec.EarliestSpec) {
            // 如果是最早偏移量，返回最早时间戳
            return ListOffsetsRequest.EARLIEST_TIMESTAMP;
        } else if (offsetSpec instanceof OffsetSpec.MaxTimestampSpec) {
            // 如果是最大时间戳，返回最大时间戳
            return ListOffsetsRequest.MAX_TIMESTAMP;
        } else if (offsetSpec instanceof OffsetSpec.EarliestLocalSpec) {
            // 如果是最早本地偏移量，返回最早本地时间戳
            return ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP;
        } else if (offsetSpec instanceof OffsetSpec.LatestTieredSpec) {
            // 如果是最新分层偏移量，返回最新分层时间戳
            return ListOffsetsRequest.LATEST_TIERED_TIMESTAMP;
        }
        // 默认返回最新时间戳
        return ListOffsetsRequest.LATEST_TIMESTAMP;
    }

    /**
     * 获取批量请求中的子级错误
     * 
     * @param subLevelErrors 子级错误映射
     * @param subKey 子级键
     * @param keyNotFoundMsg 键未找到时的错误消息
     * @param <K> 键的类型
     * @return 如果找到对应的错误则返回该错误，否则返回IllegalArgumentException
     */
    static <K> Throwable getSubLevelError(Map<K, Errors> subLevelErrors, K subKey, String keyNotFoundMsg) {
        // 检查子级错误映射中是否包含指定的键
        if (!subLevelErrors.containsKey(subKey)) {
            // 如果键不存在，返回IllegalArgumentException
            return new IllegalArgumentException(keyNotFoundMsg);
        } else {
            // 如果键存在，返回对应的错误
            return subLevelErrors.get(subKey).exception();
        }
    }
}
