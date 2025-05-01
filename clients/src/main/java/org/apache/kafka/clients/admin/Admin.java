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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.FeatureUpdateFailedException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.requests.LeaveGroupResponse;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Kafka的管理客户端，支持管理和检查主题(topics)、代理(brokers)、配置(configurations)和访问控制列表(ACLs)。
 * <p>
 * 通过{@code create}方法创建的实例保证是线程安全的。
 * 但是，请求方法返回的{@link KafkaFuture KafkaFutures}由单个线程执行，因此在完成时
 * 在该线程上执行的任何代码（例如使用{@link KafkaFuture#thenApply(KafkaFuture.BaseFunction)}）
 * 不应阻塞太长时间。如有必要，应将结果的处理传递给另一个线程。
 * <p>
 * Admin暴露的操作遵循一致的模式：
 * <ul>
 *     <li>Admin实例应使用{@link Admin#create(Properties)}或{@link Admin#create(Map)}创建</li>
 *     <li>每个操作通常有两个重载方法，一个使用默认选项集，另一个重载方法的最后一个参数是显式选项对象</li>
 *     <li>操作方法的第一个参数是要执行操作的项目的{@code Collection}。将多个请求批处理到单个调用中
 *     比多次调用同一方法更有效率</li>
 *     <li>操作方法异步执行</li>
 *     <li>每个{@code xxx}操作方法返回一个{@code XxxResult}类，该类提供访问操作结果的{@link KafkaFuture}方法</li>
 *     <li>通常提供{@code all()}方法来获取批处理的整体成功/失败状态，以及{@code values()}方法来访问
 *     请求批处理中的每个项目。可能还提供其他方法</li>
 *     <li>对于同步行为，使用{@link KafkaFuture#get()}</li>
 * </ul>
 * <p>
 * 以下是使用Admin客户端实例创建新主题的简单示例：
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 *
 * try (Admin admin = Admin.create(props)) {
 *   String topicName = "my-topic";
 *   int partitions = 12;
 *   short replicationFactor = 3;
 *   // 创建一个压缩主题
 *   CreateTopicsResult result = admin.createTopics(Collections.singleton(
 *     new NewTopic(topicName, partitions, replicationFactor)
 *       .configs(Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT))));
 *
 *   // 调用values()获取特定主题的结果
 *   KafkaFuture<Void> future = result.values().get(topicName);
 *
 *   // 调用get()阻塞直到主题创建完成或失败
 *   // 如果创建失败，ExecutionException将包装底层原因
 *   future.get();
 * }
 * }
 * </pre>
 *
 * <h3>引导和负载均衡</h3>
 * <p>
 * 传递给{@link Admin#create(Properties)}的{@code Map}或{@code Properties}中的{@code bootstrap.servers}配置
 * 仅用于发现集群中的代理，客户端将根据需要连接到这些代理。
 * 因此，只需包含两到三个代理地址就足以应对代理不可用的可能性。
 * <p>
 * 不同的操作需要将请求发送到集群中的不同节点。例如，
 * {@link #createTopics(Collection)}与控制器通信，但{@link #describeTopics(Collection)}
 * 可以与任何代理通信。当接收者无关紧要时，实例将尝试使用未处理请求最少的代理。
 * <p>
 * 客户端将透明地重试通常是暂时性的某些错误。
 * 例如，如果{@code createTopics()}的请求发送到不是控制器的节点，
 * 元数据将被刷新，请求将重新发送到控制器。
 *
 * <h3>代理兼容性</h3>
 * <p>
 * 所需的最低代理版本是0.10.0.0。具有更严格要求的方法将指定所需的最低代理版本。
 * <p>
 * 该客户端在0.11.0.0中引入，API仍在演进。我们将尝试以兼容的方式发展API，
 * 但如有必要，我们保留在次要版本中进行重大更改的权利。一旦API被认为稳定，
 * 我们将更新{@code InterfaceStability}注解和此通知。
 * <p>
 */
@InterfaceStability.Evolving
public interface Admin extends AutoCloseable {

    /**
     * 使用给定的配置创建一个新的Admin实例。
     * 这个方法是创建Kafka管理客户端的主要入口点之一。
     * 配置参数通过Properties对象传入，包括必要的集群连接信息等。
     *
     * @param props 配置参数，包含如bootstrap.servers等关键配置
     * @return 返回新创建的KafkaAdminClient实例
     */
    static Admin create(Properties props) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(props, true), null);
    }

    /**
     * 使用给定的配置创建一个新的Admin实例。
     * 这是一个重载方法，允许使用Map来提供配置参数。
     * 这种方式更灵活，可以直接使用键值对形式的配置。
     *
     * @param conf 配置参数Map，包含客户端配置信息
     * @return 返回新创建的KafkaAdminClient实例
     */
    static Admin create(Map<String, Object> conf) {
        return KafkaAdminClient.createInternal(new AdminClientConfig(conf, true), null, null);
    }

    /**
     * 关闭Admin客户端并释放所有相关资源。
     * 这是一个默认实现，使用最大可能的超时时间。
     * <p>
     * 参见 {@link #close(Duration)}
     */
    @Override
    default void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * 关闭Admin客户端并释放所有相关资源。
     * <p>
     * 关闭操作包含一个宽限期，在此期间允许当前操作完成，
     * 宽限期由给定的持续时间指定。
     * 在宽限期间不会接受新的操作。一旦宽限期结束，
     * 所有尚未完成的操作都将被中止，并抛出{@link org.apache.kafka.common.errors.TimeoutException}。
     *
     * @param timeout 等待时间，指定关闭操作的最大等待时间
     */
    void close(Duration timeout);

    /**
     * 使用默认选项批量创建新主题。
     * <p>
     * 这是{@link #createTopics(Collection, CreateTopicsOptions)}方法的便捷版本，使用默认选项。
     * 详细信息请参见重载方法。
     * <p>
     * 此操作要求broker版本不低于0.10.1.0。
     * 
     * 实现细节：
     * 1. 内部调用重载方法，使用默认的CreateTopicsOptions配置
     * 2. 主题创建请求会发送到集群的控制器节点
     * 3. 如果控制器发生变更，客户端会自动重试
     *
     * @param newTopics 要创建的新主题集合，每个主题可以指定分区数、副本因子和配置
     * @return CreateTopicsResult 异步操作结果，包含每个主题的创建状态
     */
    default CreateTopicsResult createTopics(Collection<NewTopic> newTopics) {
        return createTopics(newTopics, new CreateTopicsOptions());
    }

    /**
     * 批量创建新主题，支持自定义选项。
     * <p>
     * 此操作不是事务性的，这意味着：
     * 1. 部分主题可能创建成功而其他失败
     * 2. 失败的主题不会回滚成功的主题
     * 3. 建议检查返回结果中每个主题的状态
     * <p>
     * 主题创建的异步特性：
     * 1. {@link CreateTopicsResult}返回成功后，可能需要几秒钟时间所有broker才能感知到新主题
     * 2. 在这个时间窗口内，{@link #listTopics()}和{@link #describeTopics(Collection)}
     * 可能无法返回新创建主题的信息
     * 3. 建议在创建后添加适当的重试机制来确认主题可用
     * <p>
     * 版本兼容性：
     * 1. 基本功能要求broker版本不低于0.10.1.0
     * 2. validateOnly选项从0.10.2.0版本开始支持
     * 3. 不同版本的broker可能支持不同的主题配置选项
     *
     * @param newTopics 要创建的新主题集合，每个主题可以指定分区数、副本因子和配置
     * @param options 创建主题时使用的选项，如超时时间、是否仅验证等
     * @return CreateTopicsResult 异步操作结果，包含每个主题的创建状态
     */
    CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options);

    /**
     * 使用默认选项批量删除主题。
     * <p>
     * 这是{@link #deleteTopics(TopicCollection, DeleteTopicsOptions)}方法的便捷版本，使用默认选项。
     * 详细信息请参见重载方法。
     * <p>
     * 此操作要求broker版本不低于0.10.1.0。
     * 
     * 实现细节：
     * 1. 将主题名称集合转换为TopicCollection
     * 2. 使用默认的DeleteTopicsOptions配置
     * 3. 删除请求发送到集群的控制器节点
     *
     * @param topics 要删除的主题名称集合
     * @return DeleteTopicsResult 异步操作结果，包含每个主题的删除状态
     */
    default DeleteTopicsResult deleteTopics(Collection<String> topics) {
        return deleteTopics(TopicCollection.ofTopicNames(topics), new DeleteTopicsOptions());
    }

    /**
     * 使用指定选项批量删除主题（基于主题名称）。
     * <p>
     * 这是{@link #deleteTopics(TopicCollection, DeleteTopicsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * <p>
     * 此操作要求broker版本不低于0.10.1.0。
     * 
     * 实现细节：
     * 1. 将主题名称和选项转换为相应的内部格式
     * 2. 支持自定义删除选项，如超时设置
     *
     * @param topics 要删除的主题名称集合
     * @param options 删除主题时使用的选项
     * @return DeleteTopicsResult 异步操作结果
     */
    default DeleteTopicsResult deleteTopics(Collection<String> topics, DeleteTopicsOptions options) {
        return deleteTopics(TopicCollection.ofTopicNames(topics), options);
    }

    /**
     * 使用默认选项批量删除主题（支持主题ID）。
     * <p>
     * 这是{@link #deleteTopics(TopicCollection, DeleteTopicsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * <p>
     * 版本兼容性：
     * 1. 使用主题ID时，要求broker间协议版本不低于2.8
     * 2. 使用主题名称时，要求broker版本不低于0.10.1.0
     * 
     * 实现细节：
     * 1. 支持通过主题ID或名称删除主题
     * 2. 使用默认的删除选项
     *
     * @param topics 要删除的主题集合（可以是ID或名称）
     * @return DeleteTopicsResult 异步操作结果
     */
    default DeleteTopicsResult deleteTopics(TopicCollection topics) {
        return deleteTopics(topics, new DeleteTopicsOptions());
    }

    /**
     * 批量删除主题，支持自定义选项和主题ID。
     * <p>
     * 此操作不是事务性的，这意味着：
     * 1. 部分主题可能删除成功而其他失败
     * 2. 失败的删除操作不会影响已成功删除的主题
     * 3. 建议检查返回结果中每个主题的状态
     * <p>
     * 删除操作的异步特性：
     * 1. {@link DeleteTopicsResult}返回成功后，可能需要几秒钟所有broker才能感知到主题已被删除
     * 2. 在这个时间窗口内，{@link #listTopics()}和{@link #describeTopics(Collection)}
     * 可能仍会返回已删除主题的信息
     * 3. 建议在删除后添加适当的重试机制来确认主题确实被删除
     * <p>
     * 特殊情况处理：
     * 1. 如果broker的delete.topic.enable设置为false：
     *    - 主题只会被标记为待删除，但不会实际删除
     *    - 操作仍会返回成功
     *    - 这种情况下主题仍然可见，但标记为待删除状态
     * <p>
     * 版本兼容性：
     * 1. 使用主题ID时，要求broker间协议版本不低于2.8
     * 2. 使用主题名称时，要求broker版本不低于0.10.1.0
     *
     * @param topics 要删除的主题集合（支持ID或名称）
     * @param options 删除主题时使用的选项，如超时设置等
     * @return DeleteTopicsResult 异步操作结果，包含每个主题的删除状态
     */
    DeleteTopicsResult deleteTopics(TopicCollection topics, DeleteTopicsOptions options);

    /**
     * 使用默认选项列出集群中可用的所有主题。
     * <p>
     * 这是{@link #listTopics(ListTopicsOptions)}方法的便捷版本，使用默认选项。
     * 详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的ListTopicsOptions配置
     * 2. 可以连接到任何broker获取主题列表
     * 3. 返回的主题列表可能包含：
     *    - 正在创建的主题
     *    - 已标记删除但尚未完全删除的主题
     *    - 所有当前可用的主题
     *
     * @return ListTopicsResult 异步操作结果，包含主题列表和其他元数据
     */
    default ListTopicsResult listTopics() {
        return listTopics(new ListTopicsOptions());
    }

    /**
     * 列出集群中所有可用的主题。
     * <p>
     * 此方法用于获取集群中所有主题的列表，支持自定义选项。
     * 实现细节：
     * 1. 可以连接到任何broker获取主题列表
     * 2. 返回的主题列表包含所有当前可见的主题，包括：
     *    - 正在创建的主题
     *    - 已标记删除但尚未完全删除的主题
     *    - 所有当前可用的主题
     * 3. 支持内部主题的过滤（通过options.listInternal()控制）
     *
     * @param options 列出主题时使用的选项，如是否包含内部主题等
     * @return ListTopicsResult 异步操作结果，包含主题列表
     */
    ListTopicsResult listTopics(ListTopicsOptions options);

    /**
     * 使用默认选项获取指定主题的详细信息。
     * <p>
     * 这是{@link #describeTopics(Collection, DescribeTopicsOptions)}方法的便捷版本。
     * 使用默认选项描述主题，详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 内部创建默认的DescribeTopicsOptions配置
     * 2. 可以连接到任何broker获取主题信息
     * 3. 返回的信息包括分区数、副本分配等元数据
     *
     * @param topicNames 要描述的主题名称集合
     * @return DescribeTopicsResult 异步操作结果，包含主题详细信息
     */
    default DescribeTopicsResult describeTopics(Collection<String> topicNames) {
        return describeTopics(topicNames, new DescribeTopicsOptions());
    }

    /**
     * 使用指定选项获取主题的详细信息。
     * <p>
     * 此方法允许通过主题名称和自定义选项获取主题的详细元数据。
     * 
     * 实现细节：
     * 1. 将主题名称转换为TopicCollection
     * 2. 支持自定义描述选项
     * 3. 返回的信息包括主题配置、分区状态等
     *
     * @param topicNames 要描述的主题名称集合
     * @param options 描述主题时使用的选项
     * @return DescribeTopicsResult 异步操作结果
     */
    default DescribeTopicsResult describeTopics(Collection<String> topicNames, DescribeTopicsOptions options) {
        return describeTopics(TopicCollection.ofTopicNames(topicNames), options);
    }

    /**
     * 使用默认选项获取主题详细信息（支持主题ID）。
     * <p>
     * 这是{@link #describeTopics(TopicCollection, DescribeTopicsOptions)}方法的便捷版本。
     * <p>
     * 版本兼容性：
     * - 使用主题ID时，要求broker版本不低于3.1.0
     * 
     * 实现细节：
     * 1. 支持通过主题ID或名称查询
     * 2. 使用默认的描述选项
     * 3. 自动处理不同版本broker的兼容性
     *
     * @param topics 要描述的主题集合（支持ID或名称）
     * @return DescribeTopicsResult 异步操作结果
     */
    default DescribeTopicsResult describeTopics(TopicCollection topics) {
        return describeTopics(topics, new DescribeTopicsOptions());
    }

    /**
     * 使用指定选项获取主题详细信息（支持主题ID）。
     * <p>
     * 此方法提供了最灵活的主题描述功能，支持通过ID或名称查询，并可自定义查询选项。
     * 
     * 版本兼容性：
     * - 使用主题ID时，要求broker版本不低于3.1.0
     * 
     * 实现细节：
     * 1. 支持混合使用主题ID和名称
     * 2. 可以自定义描述选项
     * 3. 返回详细的主题元数据信息
     *
     * @param topics 要描述的主题集合（支持ID或名称）
     * @param options 描述主题时使用的选项
     * @return DescribeTopicsResult 异步操作结果
     */
    DescribeTopicsResult describeTopics(TopicCollection topics, DescribeTopicsOptions options);

    /**
     * 使用默认选项获取集群节点信息。
     * <p>
     * 这是{@link #describeCluster(DescribeClusterOptions)}方法的便捷版本。
     * 
     * 实现细节：
     * 1. 使用默认的DescribeClusterOptions配置
     * 2. 返回集群的基本信息，如节点列表、控制器ID等
     * 3. 可以连接到任何broker获取信息
     *
     * @return DescribeClusterResult 异步操作结果，包含集群信息
     */
    default DescribeClusterResult describeCluster() {
        return describeCluster(new DescribeClusterOptions());
    }

    /**
     * 使用指定选项获取集群节点信息。
     * <p>
     * 此方法用于获取Kafka集群的详细信息，包括：
     * - 集群中的节点列表
     * - 当前控制器节点
     * - 集群ID
     * - 其他集群级别的元数据
     * 
     * 实现细节：
     * 1. 支持自定义描述选项
     * 2. 返回完整的集群拓扑信息
     * 3. 适用于监控和管理场景
     *
     * @param options 获取集群信息时使用的选项
     * @return DescribeClusterResult 异步操作结果
     */
    DescribeClusterResult describeCluster(DescribeClusterOptions options);

    /**
     * 使用默认选项获取ACL信息。
     * <p>
     * 这是{@link #describeAcls(AclBindingFilter, DescribeAclsOptions)}方法的便捷版本。
     * <p>
     * 版本兼容性：
     * - 此操作要求broker版本不低于0.11.0.0
     * 
     * 实现细节：
     * 1. 使用默认的DescribeAclsOptions配置
     * 2. 根据过滤器匹配ACL规则
     * 3. 返回匹配的访问控制规则列表
     *
     * @param filter 用于过滤ACL的条件
     * @return DescribeAclsResult 异步操作结果
     */
    default DescribeAclsResult describeAcls(AclBindingFilter filter) {
        return describeAcls(filter, new DescribeAclsOptions());
    }

    /**
     * 根据提供的过滤器列出访问控制列表（ACLs）。
     * <p>
     * 注意：由{@code createAcls}或{@code deleteAcls}所做的更改可能需要一些时间才能在
     * {@code describeAcls}的输出中反映出来。
     * <p>
     * 此操作要求broker版本不低于0.11.0.0。
     * 
     * 实现细节：
     * 1. 支持复杂的ACL过滤条件
     * 2. 返回的ACL信息包括权限类型、资源类型、主体等
     * 3. 异步操作，返回Future对象
     *
     * @param filter  用于过滤ACL的条件
     * @param options 列出ACL时使用的选项
     * @return DescribeAclsResult 包含匹配的ACL列表
     */
    DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options);

    /**
     * 使用默认选项创建访问控制列表（ACLs）。
     * <p>
     * 这是{@link #createAcls(Collection, CreateAclsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * <p>
     * 此操作要求broker版本不低于0.11.0.0。
     * 
     * 实现细节：
     * 1. 使用默认的CreateAclsOptions配置
     * 2. 支持批量创建多个ACL规则
     *
     * @param acls 要创建的ACL集合
     * @return CreateAclsResult 异步操作结果
     */
    default CreateAclsResult createAcls(Collection<AclBinding> acls) {
        return createAcls(acls, new CreateAclsOptions());
    }

    /**
     * 创建与特定资源绑定的访问控制列表（ACLs）。
     * <p>
     * 此操作不是事务性的，这意味着：
     * 1. 部分ACL可能创建成功而其他失败
     * 2. 失败的ACL不会影响已成功创建的ACL
     * <p>
     * 如果尝试添加一个与现有ACL重复的规则：
     * 1. 不会报错
     * 2. 也不会进行任何更改
     * <p>
     * 此操作要求broker版本不低于0.11.0.0。
     * 
     * 实现细节：
     * 1. 支持批量创建多个ACL规则
     * 2. 每个ACL规则包含：资源类型、资源名称、主体、主机、操作类型和权限类型
     * 3. 自动处理重复规则
     *
     * @param acls    要创建的ACL集合
     * @param options 创建ACL时使用的选项
     * @return CreateAclsResult 异步操作结果
     */
    CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options);

    /**
     * 使用默认选项删除访问控制列表（ACLs）。
     * <p>
     * 这是{@link #deleteAcls(Collection, DeleteAclsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * <p>
     * 此操作要求broker版本不低于0.11.0.0。
     * 
     * 实现细节：
     * 1. 使用默认的DeleteAclsOptions配置
     * 2. 支持通过过滤器批量删除ACL
     *
     * @param filters 用于匹配要删除的ACL的过滤器集合
     * @return DeleteAclsResult 异步操作结果
     */
    default DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters) {
        return deleteAcls(filters, new DeleteAclsOptions());
    }

    /**
     * 根据提供的过滤器删除访问控制列表（ACLs）。
     * <p>
     * 此操作不是事务性的，这意味着：
     * 1. 部分ACL可能删除成功而其他失败
     * 2. 失败的删除操作不会影响已成功删除的ACL
     * <p>
     * 此操作要求broker版本不低于0.11.0.0。
     * 
     * 实现细节：
     * 1. 支持通过过滤器批量删除ACL
     * 2. 过滤器可以匹配多个维度：资源类型、资源名称、主体、主机等
     * 3. 返回删除的ACL数量和任何发生的错误
     *
     * @param filters 用于匹配要删除的ACL的过滤器集合
     * @param options 删除ACL时使用的选项
     * @return DeleteAclsResult 异步操作结果
     */
    DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options);


    /**
     * 使用默认选项获取指定资源的配置。
     * <p>
     * 这是{@link #describeConfigs(Collection, DescribeConfigsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * <p>
     * 此操作要求broker版本不低于0.11.0.0。
     * 
     * 实现细节：
     * 1. 使用默认的DescribeConfigsOptions配置
     * 2. 支持查询多种资源类型的配置
     * 3. 返回包括默认值和用户设置值的完整配置
     *
     * @param resources 要查询配置的资源集合，参见{@link ConfigResource.Type}
     * @return DescribeConfigsResult 异步操作结果
     */
    default DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources) {
        return describeConfigs(resources, new DescribeConfigsOptions());
    }

    /**
     * 获取指定资源的配置信息。
     * <p>
     * 返回的配置包括默认值，可以通过isDefault()方法区分默认值和用户设置值。
     * <p>
     * 对于isSensitive()为true的配置项，其值始终为{@code null}，以防止敏感信息泄露。
     * <p>
     * isReadOnly()为true的配置项不能被更新。
     * <p>
     * 不同类型资源在不存在时的行为：
     * <ul>
     *     <li>{@link ConfigResource.Type#BROKER}：
     *     将抛出{@link org.apache.kafka.common.errors.TimeoutException}异常</li>
     *     <li>{@link ConfigResource.Type#TOPIC}：
     *     将抛出{@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException}异常</li>
     *     <li>{@link ConfigResource.Type#GROUP}：
     *     即使目标组不存在也会返回默认配置</li>
     *     <li>{@link ConfigResource.Type#BROKER_LOGGER}：
     *     将抛出{@link org.apache.kafka.common.errors.TimeoutException}异常</li>
     *     <li>{@link ConfigResource.Type#CLIENT_METRICS}：将返回空配置</li>
     * </ul>
     * <p>
     * 此操作要求broker版本不低于0.11.0.0。
     * 
     * 实现细节：
     * 1. 支持批量查询多个资源的配置
     * 2. 自动处理敏感配置项
     * 3. 区分只读配置和可修改配置
     * 4. 提供完整的配置元数据（来源、是否动态等）
     *
     * @param resources 要查询配置的资源集合
     * @param options   查询配置时使用的选项
     * @return DescribeConfigsResult 异步操作结果
     */
    DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options);

    /**
     * 使用默认选项增量更新指定资源的配置。
     * <p>
     * 这是{@link #incrementalAlterConfigs(Map, AlterConfigsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * <p>
     * 此操作要求broker版本不低于2.3.0。
     * 
     * 实现细节：
     * 1. 使用默认的AlterConfigsOptions配置
     * 2. 支持增量修改配置（添加、删除、更新）
     * 3. 可以同时修改多个资源的配置
     *
     * @param configs 要修改的资源及其配置变更操作
     * @return AlterConfigsResult 异步操作结果
     */
    default AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs) {
        return incrementalAlterConfigs(configs, new AlterConfigsOptions());
    }

    /**
     * 增量更新指定资源的配置。
     * <p>
     * 更新操作不是事务性的，这意味着：
     * 1. 某些资源可能更新成功而其他失败
     * 2. 对于单个资源的配置更新是原子性的
     * <p>
     * 当调用返回的{@link AlterConfigsResult}中的futures的{@code get()}方法时，可能会遇到以下异常：
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     * 如果认证用户没有修改集群的权限</li>
     * <li>{@link org.apache.kafka.common.errors.TopicAuthorizationException}
     * 如果认证用户没有修改主题的权限</li>
     * <li>{@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException}
     * 如果主题不存在</li>
     * <li>{@link org.apache.kafka.common.errors.InvalidRequestException}
     * 如果请求详情无效，例如，对同一资源多次指定了相同的配置键</li>
     * </ul>
     * <p>
     * 此操作要求broker版本不低于2.3.0。
     * 
     * 实现细节：
     * 1. 支持增量更新多个资源的配置
     * 2. 每个资源的配置更新是原子性的
     * 3. 更新请求发送到集群的控制器节点
     *
     * @param configs 要更新的资源及其配置
     * @param options 更新配置时使用的选项
     * @return AlterConfigsResult 异步操作结果
     */
    AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource,
        Collection<AlterConfigOp>> configs, AlterConfigsOptions options);

    /**
     * 更改指定副本的日志目录。
     * <p>
     * 处理逻辑：
     * 1. 如果副本不存在：
     *    - 结果显示REPLICA_NOT_AVAILABLE
     *    - 副本将在后续创建时使用指定的日志目录
     * 2. 如果副本已存在：
     *    - 如果不在指定目录，则移动到新目录
     *    - 如果已在指定目录，则不进行操作
     * <p>
     * 这是{@link #alterReplicaLogDirs(Map, AlterReplicaLogDirsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * <p>
     * 此操作要求broker版本不低于1.1.0。
     * 
     * 实现细节：
     * 1. 使用默认的AlterReplicaLogDirsOptions配置
     * 2. 支持批量更改多个副本的日志目录
     * 3. 操作不是事务性的，部分副本可能成功而其他失败
     *
     * @param replicaAssignment 副本及其对应的日志目录绝对路径
     * @return AlterReplicaLogDirsResult 异步操作结果
     */
    default AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment) {
        return alterReplicaLogDirs(replicaAssignment, new AlterReplicaLogDirsOptions());
    }

    /**
     * 更改指定副本的日志目录。
     * <p>
     * 处理逻辑：
     * 1. 如果副本不存在：
     *    - 结果显示REPLICA_NOT_AVAILABLE
     *    - 副本将在后续创建时使用指定的日志目录
     * 2. 如果副本已存在：
     *    - 如果不在指定目录，则移动到新目录
     *    - 如果已在指定目录，则不进行操作
     * <p>
     * 此操作不是事务性的，可能部分副本成功而其他失败。
     * <p>
     * 此操作要求broker版本不低于1.1.0。
     * 
     * 实现细节：
     * 1. 支持自定义选项进行更细粒度的控制
     * 2. 可以同时移动多个副本的日志目录
     * 3. 返回详细的操作结果供检查
     *
     * @param replicaAssignment 副本及其对应的日志目录绝对路径
     * @param options 更改副本目录时使用的选项
     * @return AlterReplicaLogDirsResult 异步操作结果
     */
    AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment,
                                                  AlterReplicaLogDirsOptions options);

    /**
     * 查询指定broker集合的所有日志目录信息。
     * <p>
     * 这是{@link #describeLogDirs(Collection, DescribeLogDirsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * <p>
     * 此操作要求broker版本不低于1.0.0。
     * 
     * 实现细节：
     * 1. 使用默认的DescribeLogDirsOptions配置
     * 2. 可以查询多个broker的日志目录信息
     * 3. 返回每个broker上所有日志目录的详细信息
     *
     * @param brokers 要查询的broker列表
     * @return DescribeLogDirsResult 异步操作结果
     */
    default DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers) {
        return describeLogDirs(brokers, new DescribeLogDirsOptions());
    }

    /**
     * 查询指定broker集合的所有日志目录信息。
     * <p>
     * 此操作要求broker版本不低于1.0.0。
     * 
     * 实现细节：
     * 1. 支持自定义查询选项
     * 2. 返回每个broker的日志目录详细信息
     * 3. 可用于监控和管理日志存储
     *
     * @param brokers 要查询的broker列表
     * @param options 查询日志目录信息时使用的选项
     * @return DescribeLogDirsResult 异步操作结果
     */
    DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options);

    /**
     * 查询指定副本的日志目录信息。
     * <p>
     * 这是{@link #describeReplicaLogDirs(Collection, DescribeReplicaLogDirsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * <p>
     * 此操作要求broker版本不低于1.0.0。
     * 
     * 实现细节：
     * 1. 使用默认的DescribeReplicaLogDirsOptions配置
     * 2. 可以查询多个副本的日志目录信息
     * 3. 返回每个副本的存储位置和状态
     *
     * @param replicas 要查询的副本集合
     * @return DescribeReplicaLogDirsResult 异步操作结果
     */
    default DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas) {
        return describeReplicaLogDirs(replicas, new DescribeReplicaLogDirsOptions());
    }

    /**
     * 查询指定副本的日志目录信息。
     * <p>
     * 此操作要求broker版本不低于1.0.0。
     * 
     * 实现细节：
     * 1. 支持自定义查询选项
     * 2. 返回副本的详细存储信息
     * 3. 可用于监控副本的存储状态
     *
     * @param replicas 要查询的副本集合
     * @param options 查询副本日志目录信息时使用的选项
     * @return DescribeReplicaLogDirsResult 异步操作结果
     */
    DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options);

    /**
     * 增加指定主题的分区数。
     * <p>
     * <strong>注意：如果增加带有key的主题的分区数，消息的分区逻辑和顺序将受到影响。</strong>
     * <p>
     * 这是{@link #createPartitions(Map, CreatePartitionsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的CreatePartitionsOptions配置
     * 2. 支持同时增加多个主题的分区
     * 3. 分区增加操作是不可逆的
     *
     * @param newPartitions 要增加分区的主题及其对应的参数
     * @return CreatePartitionsResult 异步操作结果
     */
    default CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions) {
        return createPartitions(newPartitions, new CreatePartitionsOptions());
    }

    /**
     * 根据{@code newPartitions}中指定的值增加主题的分区数。
     * <strong>注意：如果为带有键的主题增加分区，将会影响分区逻辑和消息的顺序。</strong>
     * <p>
     * 此操作的特点：
     * 1. 非事务性：部分主题可能成功而其他失败
     * 2. 异步性：方法返回成功后，可能需要几秒钟时间所有broker才能感知到新分区的创建
     * 3. 在这个时间窗口内，{@link #describeTopics(Collection)}可能无法返回新分区的信息
     * <p>
     * 版本要求：此操作要求broker版本不低于1.0.0
     * <p>
     * 可能的异常情况：
     * 当调用返回的{@link CreatePartitionsResult}中{@link CreatePartitionsResult#values() values()}方法获取的futures的{@code get()}时，
     * 可能会遇到以下异常：
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.AuthorizationException}
     * - 当认证用户没有修改主题的权限时</li>
     * <li>{@link org.apache.kafka.common.errors.TimeoutException}
     * - 当请求在{@link CreatePartitionsOptions#timeoutMs()}指定的时间内未完成时</li>
     * <li>{@link org.apache.kafka.common.errors.ReassignmentInProgressException}
     * - 当分区重分配正在进行时</li>
     * <li>{@link org.apache.kafka.common.errors.BrokerNotAvailableException}
     * - 当{@link NewPartitions#assignments()}中指定的broker当前不可用时</li>
     * <li>{@link org.apache.kafka.common.errors.InvalidReplicationFactorException}
     * - 当未提供{@link NewPartitions#assignments()}且broker无法按主题的复制因子分配副本时</li>
     * <li>其他{@link org.apache.kafka.common.KafkaException}的子类
     * - 当请求以某种方式无效时</li>
     * </ul>
     *
     * @param newPartitions 需要创建新分区的主题及其对应的分区参数
     * @param options 创建新分区时使用的选项
     * @return CreatePartitionsResult 异步操作结果
     */
    CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions,
                                            CreatePartitionsOptions options);

    /**
     * 删除指定分区中偏移量小于给定偏移量的记录。
     * <p>
     * 这是{@link #deleteRecords(Map, DeleteRecordsOptions)}方法的便捷版本，使用默认选项。
     * 详细信息请参见重载方法。
     * <p>
     * 版本要求：此操作要求broker版本不低于0.11.0.0
     * 
     * 实现细节：
     * 1. 内部调用重载方法，使用默认的DeleteRecordsOptions配置
     * 2. 删除请求会发送到相应分区的leader副本所在的broker
     * 3. 删除操作是异步的，需要等待所有副本完成同步
     *
     * @param recordsToDelete 要删除记录的主题分区及其对应的起始偏移量
     * @return DeleteRecordsResult 异步操作结果
     */
    default DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete) {
        return deleteRecords(recordsToDelete, new DeleteRecordsOptions());
    }

    /**
     * 删除指定分区中偏移量小于给定偏移量的记录。
     * <p>
     * 此操作的特点：
     * 1. 删除是不可逆的，一旦执行无法恢复
     * 2. 删除是异步的，可能需要一段时间才能在所有副本上完成
     * 3. 删除不会影响分区的高水位线（high watermark）
     * <p>
     * 版本要求：此操作要求broker版本不低于0.11.0.0
     * <p>
     * 注意事项：
     * 1. 删除操作可能会影响正在消费的消费者
     * 2. 建议在执行删除前确保没有活跃的消费者
     * 3. 删除后的偏移量仍然保持不变，不会重新编号
     *
     * @param recordsToDelete 要删除记录的主题分区及其对应的起始偏移量
     * @param options 删除记录时使用的选项，如超时设置等
     * @return DeleteRecordsResult 异步操作结果
     */
    DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete,
                                      DeleteRecordsOptions options);

    /**
     * 创建委派令牌（Delegation Token）。
     * <p>
     * 这是{@link #createDelegationToken(CreateDelegationTokenOptions)}方法的便捷版本，使用默认选项。
     * 详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 内部调用重载方法，使用默认的CreateDelegationTokenOptions配置
     * 2. 令牌创建请求会发送到集群的控制器节点
     * 3. 创建的令牌可用于后续的身份验证
     *
     * @return CreateDelegationTokenResult 异步操作结果
     */
    default CreateDelegationTokenResult createDelegationToken() {
        return createDelegationToken(new CreateDelegationTokenOptions());
    }


    /**
     * 创建委派令牌（Delegation Token）。
     * <p>
     * 委派令牌的主要用途：
     * 1. 允许服务代表用户进行身份验证
     * 2. 避免在多个应用程序间共享实际凭据
     * 3. 支持临时授权和访问控制
     * <p>
     * 安全注意事项：
     * 1. 令牌应妥善保管，防止泄露
     * 2. 建议设置合适的过期时间
     * 3. 仅在必要时启用令牌功能
     * <p>
     * 版本要求：此操作要求broker版本不低于1.1.0
     * <p>
     * 可能的异常情况：
     * 当调用返回的{@link CreateDelegationTokenResult}中{@link CreateDelegationTokenResult#delegationToken() delegationToken()}方法获取的futures的{@code get()}时，
     * 可能会遇到以下异常：
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.UnsupportedByAuthenticationException}
     * - 当请求通过PLAINTEXT/单向SSL通道或使用委派令牌认证的通道发送时</li>
     * <li>{@link org.apache.kafka.common.errors.InvalidPrincipalTypeException}
     * - 当更新者的主体类型不受支持时</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenDisabledException}
     * - 当委派令牌功能被禁用时</li>
     * <li>{@link org.apache.kafka.common.errors.TimeoutException}
     * - 当请求在{@link CreateDelegationTokenOptions#timeoutMs()}指定的时间内未完成时</li>
     * </ul>
     *
     * @param options 创建委派令牌时使用的选项
     * @return CreateDelegationTokenResult 异步操作结果
     */
    CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options);


    /**
     * 续期委派令牌（Delegation Token）。
     * <p>
     * 这是{@link #renewDelegationToken(byte[], RenewDelegationTokenOptions)}方法的便捷版本，使用默认选项。
     * 详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 内部调用重载方法，使用默认的RenewDelegationTokenOptions配置
     * 2. 续期请求会发送到集群的控制器节点
     * 3. 成功续期后令牌的有效期会被延长
     *
     * @param hmac 委派令牌的HMAC值
     * @return RenewDelegationTokenResult 异步操作结果
     */
    default RenewDelegationTokenResult renewDelegationToken(byte[] hmac) {
        return renewDelegationToken(hmac, new RenewDelegationTokenOptions());
    }

    /**
     * 续期委派令牌（Delegation Token）。
     * <p>
     * 令牌续期的主要用途：
     * 1. 延长现有令牌的有效期
     * 2. 避免令牌过期导致的服务中断
     * 3. 维持长期运行服务的连续性
     * <p>
     * 安全注意事项：
     * 1. 只有令牌的所有者或指定的续期者可以执行续期操作
     * 2. 续期操作应在令牌过期前执行
     * 3. 建议实现自动续期机制以防止意外过期
     * <p>
     * 版本要求：此操作要求broker版本不低于1.1.0
     * <p>
     * 可能的异常情况：
     * 当调用返回的{@link RenewDelegationTokenResult}中{@link RenewDelegationTokenResult#expiryTimestamp() expiryTimestamp()}方法获取的futures的{@code get()}时，
     * 可能会遇到以下异常：
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.UnsupportedByAuthenticationException}
     * - 当请求通过PLAINTEXT/单向SSL通道或使用委派令牌认证的通道发送时</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenDisabledException}
     * - 当委派令牌功能被禁用时</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenNotFoundException}
     * - 当服务器上找不到指定的委派令牌时</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException}
     * - 当认证用户不是令牌的所有者或续期者时</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenExpiredException}
     * - 当委派令牌已过期时</li>
     * <li>{@link org.apache.kafka.common.errors.TimeoutException}
     * - 当请求在{@link RenewDelegationTokenOptions#timeoutMs()}指定的时间内未完成时</li>
     * </ul>
     *
     * @param hmac 委派令牌的HMAC值
     * @param options 续期令牌时使用的选项
     * @return RenewDelegationTokenResult 异步操作结果
     */
    RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options);

    /**
     * 使用默认选项使委派令牌过期。
     * <p>
     * 这是{@link #expireDelegationToken(byte[], ExpireDelegationTokenOptions)}方法的便捷版本，使用默认选项。
     * 此方法将立即使令牌过期。详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的ExpireDelegationTokenOptions配置
     * 2. 令牌过期请求发送到集群的控制器节点
     * 3. 过期操作是即时的，不可撤销
     *
     * @param hmac 委派令牌的HMAC值
     * @return ExpireDelegationTokenResult 异步操作结果
     */
    default ExpireDelegationTokenResult expireDelegationToken(byte[] hmac) {
        return expireDelegationToken(hmac, new ExpireDelegationTokenOptions());
    }

    /**
     * 使委派令牌过期，支持自定义选项。
     * <p>
     * 此操作要求broker版本不低于1.1.0。
     * <p>
     * 当调用返回的{@link ExpireDelegationTokenResult}中{@link ExpireDelegationTokenResult#expiryTimestamp() expiryTimestamp()}
     * 方法获取的futures的{@code get()}方法时，可能会遇到以下异常：
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.UnsupportedByAuthenticationException}
     * 如果请求通过PLAINTEXT/单向SSL通道或委派令牌认证通道发送。</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenDisabledException}
     * 如果委派令牌功能被禁用。</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenNotFoundException}
     * 如果在服务器上找不到委派令牌。</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException}
     * 如果认证用户不是请求令牌的所有者/更新者。</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenExpiredException}
     * 如果委派令牌已过期。</li>
     * <li>{@link org.apache.kafka.common.errors.TimeoutException}
     * 如果请求未在给定的{@link ExpireDelegationTokenOptions#timeoutMs()}时间内完成。</li>
     * </ul>
     * 
     * 实现细节：
     * 1. 支持自定义过期选项，如超时设置
     * 2. 过期操作是原子的
     * 3. 过期后的令牌不能被恢复
     *
     * @param hmac    委派令牌的HMAC值
     * @param options 过期令牌时使用的选项
     * @return ExpireDelegationTokenResult 异步操作结果
     */
    ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options);

    /**
     * 使用默认选项描述委派令牌。
     * <p>
     * 这是{@link #describeDelegationToken(DescribeDelegationTokenOptions)}方法的便捷版本，使用默认选项。
     * 此方法将返回用户拥有的所有令牌和用户具有Describe权限的令牌。详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的DescribeDelegationTokenOptions配置
     * 2. 返回当前用户可见的所有令牌信息
     * 3. 包括令牌的元数据和状态信息
     *
     * @return DescribeDelegationTokenResult 异步操作结果
     */
    default DescribeDelegationTokenResult describeDelegationToken() {
        return describeDelegationToken(new DescribeDelegationTokenOptions());
    }

    /**
     * 描述委派令牌，支持自定义选项。
     * <p>
     * 此操作要求broker版本不低于1.1.0。
     * <p>
     * 当调用返回的{@link DescribeDelegationTokenResult}中{@link DescribeDelegationTokenResult#delegationTokens() delegationTokens()}
     * 方法获取的futures的{@code get()}方法时，可能会遇到以下异常：
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.UnsupportedByAuthenticationException}
     * 如果请求通过PLAINTEXT/单向SSL通道或委派令牌认证通道发送。</li>
     * <li>{@link org.apache.kafka.common.errors.DelegationTokenDisabledException}
     * 如果委派令牌功能被禁用。</li>
     * <li>{@link org.apache.kafka.common.errors.TimeoutException}
     * 如果请求未在给定的{@link DescribeDelegationTokenOptions#timeoutMs()}时间内完成。</li>
     * </ul>
     * 
     * 实现细节：
     * 1. 支持自定义描述选项
     * 2. 返回详细的令牌信息，包括：
     *    - 令牌所有者
     *    - 创建时间
     *    - 过期时间
     *    - 最大有效期
     *    - 更新者列表
     *
     * @param options 描述令牌时使用的选项
     * @return DescribeDelegationTokenResult 异步操作结果
     */
    DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options);

    /**
     * 描述集群中的一些消费者组。
     * <p>
     * 此方法用于获取指定消费者组的详细信息，包括：
     * - 组的当前状态
     * - 组的协调器信息
     * - 组成员列表
     * - 分区分配信息
     * 
     * 实现细节：
     * 1. 支持批量查询多个消费者组
     * 2. 返回每个组的详细状态信息
     * 3. 可以连接到任何broker获取信息
     *
     * @param groupIds 要描述的消费者组ID集合
     * @param options  描述消费者组时使用的选项
     * @return DescribeConsumerGroupsResult 异步操作结果
     */
    DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds,
                                                        DescribeConsumerGroupsOptions options);

    /**
     * 使用默认选项描述集群中的一些消费者组。
     * <p>
     * 这是{@link #describeConsumerGroups(Collection, DescribeConsumerGroupsOptions)}方法的便捷版本，
     * 使用默认选项。详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的DescribeConsumerGroupsOptions配置
     * 2. 适用于快速查询消费者组状态的场景
     *
     * @param groupIds 要描述的消费者组ID集合
     * @return DescribeConsumerGroupsResult 异步操作结果
     */
    default DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds) {
        return describeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions());
    }

    /**
     * 列出集群中可用的消费者组。
     * <p>
     * 此方法用于获取集群中所有活跃的消费者组列表。
     * 
     * 实现细节：
     * 1. 支持自定义列表选项
     * 2. 返回所有消费者组的基本信息
     * 3. 可以连接到任何broker获取信息
     *
     * @param options 列出消费者组时使用的选项
     * @return ListConsumerGroupsResult 异步操作结果
     */
    ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options);

    /**
     * 使用默认选项列出集群中可用的消费者组。
     * <p>
     * 这是{@link #listConsumerGroups(ListConsumerGroupsOptions)}方法的便捷版本，使用默认选项。
     * 详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的ListConsumerGroupsOptions配置
     * 2. 返回所有活跃的消费者组
     *
     * @return ListConsumerGroupsResult 异步操作结果
     */
    default ListConsumerGroupsResult listConsumerGroups() {
        return listConsumerGroups(new ListConsumerGroupsOptions());
    }

    /**
     * 列出集群中的消费者组位移信息。
     * <p>
     * 此方法用于获取指定消费者组的位移信息。
     * 
     * 实现细节：
     * 1. 创建一个空的ListConsumerGroupOffsetsSpec
     * 2. 将单个组ID转换为Map格式
     * 3. 使用批量API获取位移信息
     *
     * @param groupId 消费者组ID
     * @param options 列出消费者组位移时使用的选项
     * @return ListConsumerGroupOffsetsResult 异步操作结果
     */
    default ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId, ListConsumerGroupOffsetsOptions options) {
        ListConsumerGroupOffsetsSpec groupSpec = new ListConsumerGroupOffsetsSpec();

        // 使用批量API，该API使用组规范中的主题分区，忽略选项中设置的主题分区
        return listConsumerGroupOffsets(Collections.singletonMap(groupId, groupSpec), options);
    }

    /**
     * 使用默认选项列出集群中的消费者组位移信息。
     * <p>
     * 这是{@link #listConsumerGroupOffsets(Map, ListConsumerGroupOffsetsOptions)}方法的便捷版本，
     * 用于列出单个组的所有分区的位移信息，使用默认选项。
     * 
     * 实现细节：
     * 1. 使用默认的ListConsumerGroupOffsetsOptions配置
     * 2. 获取指定组的所有分区位移
     *
     * @param groupId 消费者组ID
     * @return ListConsumerGroupOffsetsResult 异步操作结果
     */
    default ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId) {
        return listConsumerGroupOffsets(groupId, new ListConsumerGroupOffsetsOptions());
    }

    /**
     * 列出指定消费者组的位移信息。
     * <p>
     * 此方法支持同时查询多个消费者组的位移信息，并且可以指定每个组感兴趣的主题分区。
     * 
     * 实现细节：
     * 1. 支持批量查询多个组的位移
     * 2. 可以为每个组指定不同的分区集合
     * 3. 返回每个组的分区位移信息
     *
     * @param groupSpecs 消费者组ID到其位移查询规范的映射，规范指定了要列出位移的主题分区
     * @param options 列出消费者组位移时使用的选项
     * @return ListConsumerGroupOffsetsResult 异步操作结果
     */
    ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs, ListConsumerGroupOffsetsOptions options);

    /**
     * 使用默认选项列出指定消费者组的位移信息。
     * <p>
     * 这是{@link #listConsumerGroupOffsets(Map, ListConsumerGroupOffsetsOptions)}方法的便捷版本，
     * 使用默认选项。
     * 
     * 实现细节：
     * 1. 使用默认的ListConsumerGroupOffsetsOptions配置
     * 2. 支持批量查询多个组的位移信息
     *
     * @param groupSpecs 消费者组ID到其位移查询规范的映射，规范指定了要列出位移的主题分区
     * @return ListConsumerGroupOffsetsResult 异步操作结果
     */
    default ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs) {
        return listConsumerGroupOffsets(groupSpecs, new ListConsumerGroupOffsetsOptions());
    }

    /**
     * 从集群中删除消费者组。
     * <p>
     * 此操作用于删除一个或多个消费者组及其相关资源。
     * 
     * 实现细节：
     * 1. 删除操作是异步的，返回Future对象
     * 2. 删除请求发送到组协调器所在的broker
     * 3. 如果组协调器发生变更，客户端会自动重试
     * 
     * 可能的异常：
     * - GroupAuthorizationException：如果客户端没有删除组的权限
     * - GroupIdNotFoundException：如果指定的消费者组不存在
     * - InvalidGroupIdException：如果组ID格式无效
     *
     * @param groupIds 要删除的消费者组ID集合
     * @param options 删除消费者组时使用的选项
     * @return DeleteConsumerGroupsResult 异步操作结果，包含每个组的删除状态
     */
    DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options);

    /**
     * 使用默认选项从集群中删除消费者组。
     * <p>
     * 这是{@link #deleteConsumerGroups(Collection, DeleteConsumerGroupsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的DeleteConsumerGroupsOptions配置
     * 2. 内部调用完整版本的deleteConsumerGroups方法
     *
     * @param groupIds 要删除的消费者组ID集合
     * @return DeleteConsumerGroupsResult 异步操作结果
     */
    default DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds) {
        return deleteConsumerGroups(groupIds, new DeleteConsumerGroupsOptions());
    }

    /**
     * 删除消费者组中特定分区的已提交位移。
     * <p>
     * 此操作只有在消费者组没有活跃订阅相应主题时才会在分区级别成功。
     * 
     * 实现细节：
     * 1. 删除操作是异步的，返回Future对象
     * 2. 删除请求发送到组协调器所在的broker
     * 3. 每个分区的删除操作是独立的
     * 
     * 使用场景：
     * - 清理不再需要的位移记录
     * - 重置消费者组的消费位置
     * - 修复位移相关的问题
     * 
     * 可能的异常：
     * - GroupAuthorizationException：如果客户端没有管理组的权限
     * - GroupIdNotFoundException：如果指定的消费者组不存在
     * - InvalidGroupIdException：如果组ID格式无效
     *
     * @param groupId 消费者组ID
     * @param partitions 要删除位移的主题分区集合
     * @param options 删除位移时使用的选项
     * @return DeleteConsumerGroupOffsetsResult 异步操作结果
     */
    DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId,
        Set<TopicPartition> partitions,
        DeleteConsumerGroupOffsetsOptions options);

    /**
     * 使用默认选项删除消费者组中特定分区的已提交位移。
     * <p>
     * 这是{@link #deleteConsumerGroupOffsets(String, Set, DeleteConsumerGroupOffsetsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的DeleteConsumerGroupOffsetsOptions配置
     * 2. 内部调用完整版本的deleteConsumerGroupOffsets方法
     *
     * @param groupId 消费者组ID
     * @param partitions 要删除位移的主题分区集合
     * @return DeleteConsumerGroupOffsetsResult 异步操作结果
     */
    default DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(String groupId, Set<TopicPartition> partitions) {
        return deleteConsumerGroupOffsets(groupId, partitions, new DeleteConsumerGroupOffsetsOptions());
    }

    /**
     * 使用默认选项列出集群中可用的所有消费者组。
     * <p>
     * 这是{@link #listGroups(ListGroupsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的ListGroupsOptions配置
     * 2. 可以连接到任何broker获取组列表
     * 3. 返回的组列表包括所有状态的消费者组
     *
     * @return ListGroupsResult 异步操作结果，包含消费者组列表
     */
    default ListGroupsResult listGroups() {
        return listGroups(new ListGroupsOptions());
    }

    /**
     * 列出集群中可用的所有消费者组。
     * <p>
     * 此方法返回集群中所有消费者组的详细信息，包括：
     * - 组ID
     * - 组状态（Empty、Stable、PreparingRebalance等）
     * - 组协调器信息
     * - 组成员数量等
     * 
     * 实现细节：
     * 1. 列表操作是异步的，返回Future对象
     * 2. 可以连接到任何broker获取信息
     * 3. 支持通过选项过滤特定状态的组
     *
     * @param options 列出组时使用的选项
     * @return ListGroupsResult 异步操作结果
     */
    ListGroupsResult listGroups(ListGroupsOptions options);

    /**
     * 使用默认选项为主题分区选举新的leader副本。
     * <p>
     * 这是{@link #electLeaders(ElectionType, Set, ElectLeadersOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的ElectLeadersOptions配置
     * 2. 选举请求发送到集群的控制器节点
     * 3. 如果控制器发生变更，客户端会自动重试
     *
     * @param electionType 选举类型，决定如何选择新的leader
     * @param partitions 需要进行leader选举的主题分区集合
     * @return ElectLeadersResult 异步操作结果
     */
    default ElectLeadersResult electLeaders(ElectionType electionType, Set<TopicPartition> partitions) {
        return electLeaders(electionType, partitions, new ElectLeadersOptions());
    }

    /**
     * 为指定的分区选举新的leader副本。
     * 如果partitions参数为null，则对所有分区进行leader选举。
     * <p>
     * 此操作不是事务性的，这意味着：
     * 1. 部分分区可能选举成功而其他失败
     * 2. 失败的选举不会影响已成功的选举
     * 3. 建议检查返回结果中每个分区的状态
     * <p>
     * 选举的异步特性：
     * 1. {@link ElectLeadersResult}返回成功后，可能需要几秒钟所有broker才能感知到新的leader
     * 2. 在这个时间窗口内，{@link #describeTopics(Collection)}可能无法返回分区新leader的信息
     * <p>
     * 版本兼容性：
     * 1. 如果使用优先选举（preferred election），要求broker版本不低于2.2.0
     * 2. 其他类型的选举要求broker版本不低于2.4.0
     * <p>
     * 可能的异常：
     * <ul>
     * <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     * - 如果客户端没有修改集群的权限</li>
     * <li>{@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException}
     * - 如果指定的主题或分区不存在</li>
     * <li>{@link org.apache.kafka.common.errors.InvalidTopicException}
     * - 如果主题已被标记为待删除</li>
     * <li>{@link org.apache.kafka.common.errors.NotControllerException}
     * - 如果请求发送到非控制器节点</li>
     * <li>{@link org.apache.kafka.common.errors.TimeoutException}
     * - 如果选举操作超时</li>
     * <li>{@link org.apache.kafka.common.errors.LeaderNotAvailableException}
     * - 如果首选的leader不可用或不在ISR集合中</li>
     * </ul>
     *
     * @param electionType 选举类型，决定如何选择新的leader
     * @param partitions 需要进行leader选举的主题分区集合，如果为null则选举所有分区
     * @param options 选举时使用的选项
     * @return ElectLeadersResult 异步操作结果
     */
    ElectLeadersResult electLeaders(
        ElectionType electionType,
        Set<TopicPartition> partitions,
        ElectLeadersOptions options);

    /**
     * 修改一个或多个分区的副本分配。
     * 提供空的Optional（例如通过{@link Optional#empty()}）将<bold>撤销</bold>对应分区的重分配。
     * <p>
     * 这是{@link #alterPartitionReassignments(Map, AlterPartitionReassignmentsOptions)}方法的便捷版本，
     * 使用默认选项。详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的AlterPartitionReassignmentsOptions配置
     * 2. 重分配请求发送到集群的控制器节点
     * 3. 支持撤销正在进行的重分配
     *
     * @param reassignments 分区到新副本分配的映射，空Optional表示撤销重分配
     * @return AlterPartitionReassignmentsResult 异步操作结果
     */
    default AlterPartitionReassignmentsResult alterPartitionReassignments(
        Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments) {
        return alterPartitionReassignments(reassignments, new AlterPartitionReassignmentsOptions());
    }

    /**
     * 修改一个或多个分区的副本分配。
     * 提供一个空的Optional（例如通过{@link Optional#empty()}）将<bold>撤销</bold>对应分区的重分配。
     * <p>
     * 应用场景：
     * 1. 负载均衡：将分区重新分配到负载较轻的broker
     * 2. 硬件升级：将分区从旧硬件迁移到新硬件
     * 3. 故障恢复：在broker故障后重新分配分区
     * 4. 集群扩容：在添加新broker后重新平衡分区
     * <p>
     * 实现细节：
     * 1. 支持批量修改多个分区的分配
     * 2. 可以添加、修改或删除分区的重分配
     * 3. 通过空Optional撤销正在进行的重分配
     * 4. 操作需要发送到集群控制器节点
     * <p>
     * 当在返回的{@code AlterPartitionReassignmentsResult}的futures上调用{@code get()}时，
     * 可能会遇到以下异常：
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   如果认证用户没有修改集群的权限。</li>
     *   <li>{@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException}
     *   如果指定的主题或分区在集群中不存在。</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   如果在控制器记录新分配之前请求超时。</li>
     *   <li>{@link org.apache.kafka.common.errors.InvalidReplicaAssignmentException}
     *   如果指定的分配方案无效。</li>
     *   <li>{@link org.apache.kafka.common.errors.NoReassignmentInProgressException}
     *   如果尝试取消一个没有正在进行重分配的分区的重分配。</li>
     * </ul>
     *
     * @param reassignments 要添加、修改或删除的重分配。参见{@link NewPartitionReassignment}
     * @param options 使用的选项
     * @return 异步操作结果
     */
    AlterPartitionReassignmentsResult alterPartitionReassignments(
        Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments,
        AlterPartitionReassignmentsOptions options);


    /**
     * 列出所有当前正在进行的分区重分配。
     * <p>
     * 这是{@link #listPartitionReassignments(ListPartitionReassignmentsOptions)}方法的便捷版本，
     * 使用默认选项。详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的ListPartitionReassignmentsOptions配置
     * 2. 查询所有分区的重分配状态
     * 3. 操作需要发送到集群控制器节点
     */
    default ListPartitionReassignmentsResult listPartitionReassignments() {
        return listPartitionReassignments(new ListPartitionReassignmentsOptions());
    }

    /**
     * 列出指定分区的当前重分配状态。
     * <p>
     * 这是{@link #listPartitionReassignments(Set, ListPartitionReassignmentsOptions)}方法的便捷版本，
     * 使用默认选项。详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的ListPartitionReassignmentsOptions配置
     * 2. 只查询指定分区的重分配状态
     * 3. 支持批量查询多个分区
     */
    default ListPartitionReassignmentsResult listPartitionReassignments(Set<TopicPartition> partitions) {
        return listPartitionReassignments(partitions, new ListPartitionReassignmentsOptions());
    }

    /**
     * 列出指定分区的当前重分配状态，支持自定义选项。
     * <p>
     * 应用场景：
     * 1. 监控分区迁移进度
     * 2. 验证重分配操作是否成功
     * 3. 故障诊断和问题排查
     * 4. 集群运维和管理
     * <p>
     * 当在返回的{@code ListPartitionReassignmentsResult}的futures上调用{@code get()}时，
     * 可能会遇到以下异常：
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   如果认证用户没有查看集群状态的权限。</li>
     *   <li>{@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException}
     *   如果指定的主题或分区不存在。</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   如果在控制器返回当前重分配列表之前请求超时。</li>
     * </ul>
     *
     * @param partitions 要查询重分配状态的主题分区集合
     * @param options 使用的选项
     * @return 异步操作结果
     */
    default ListPartitionReassignmentsResult listPartitionReassignments(
        Set<TopicPartition> partitions,
        ListPartitionReassignmentsOptions options) {
        return listPartitionReassignments(Optional.of(partitions), options);
    }

    /**
     * 列出所有当前正在进行的分区重分配，支持自定义选项。
     * <p>
     * 实现细节：
     * 1. 查询集群中所有正在进行重分配的分区
     * 2. 返回每个分区的当前重分配状态
     * 3. 支持通过选项自定义查询行为
     * <p>
     * 当在返回的{@code ListPartitionReassignmentsResult}的futures上调用{@code get()}时，
     * 可能会遇到以下异常：
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   如果认证用户没有查看集群状态的权限。</li>
     *   <li>{@link org.apache.kafka.common.errors.UnknownTopicOrPartitionException}
     *   如果指定的主题或分区不存在。</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   如果在控制器返回当前重分配列表之前请求超时。</li>
     * </ul>
     *
     * @param options 使用的选项
     * @return 异步操作结果
     */
    default ListPartitionReassignmentsResult listPartitionReassignments(ListPartitionReassignmentsOptions options) {
        return listPartitionReassignments(Optional.empty(), options);
    }

    /**
     * 列出指定分区或所有分区的重分配状态。
     * <p>
     * 实现细节：
     * 1. 如果partitions为空Optional，则查询所有分区
     * 2. 如果partitions不为空，则只查询指定的分区
     * 3. 支持通过选项自定义查询行为
     * 4. 操作需要发送到集群控制器节点
     *
     * @param partitions 要查询重分配状态的分区集合，如果为空Optional则查询所有分区
     * @param options 使用的选项
     * @return 异步操作结果
     */
    ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions,
                                                                ListPartitionReassignmentsOptions options);

    /**
     * 从消费者组中移除指定的成员。
     * <p>
     * 应用场景：
     * 1. 主动移除不活跃的消费者
     * 2. 消费者组成员管理和维护
     * 3. 故障恢复和问题排查
     * 4. 集群扩缩容时的成员调整
     * <p>
     * 实现细节：
     * 1. 支持批量移除多个成员
     * 2. 触发消费者组的重平衡
     * 3. 可能导致分区重新分配
     * 4. 操作需要发送到组协调器
     * <p>
     * 可能的错误码请参考 {@link LeaveGroupResponse}。
     *
     * @param groupId 要移除成员的消费者组ID
     * @param options 包含要移除成员信息的选项
     * @return 成员变更结果
     */
    RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId, RemoveMembersFromConsumerGroupOptions options);

    /**
     * 修改指定消费者组的位移。要成功执行此操作，消费者组必须为空。
     * <p>
     * 这是{@link #alterConsumerGroupOffsets(String, Map, AlterConsumerGroupOffsetsOptions)}方法的便捷版本，
     * 使用默认选项。详细信息请参见重载方法。
     * <p>
     * 实现细节：
     * 1. 使用默认的AlterConsumerGroupOffsetsOptions配置
     * 2. 支持批量修改多个分区的位移
     * 3. 要求消费者组中没有活跃成员
     *
     * @param groupId 要修改位移的消费者组
     * @param offsets 分区到位移的映射，包含相关元数据
     * @return 位移修改结果
     */
    default AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets) {
        return alterConsumerGroupOffsets(groupId, offsets, new AlterConsumerGroupOffsetsOptions());
    }

    /**
     * 修改指定消费者组的位移。要成功执行此操作，消费者组必须为空。
     * <p>
     * 应用场景：
     * 1. 重置消费位置（如从头开始消费）
     * 2. 跳过损坏的消息
     * 3. 消费进度管理
     * 4. 问题恢复和调试
     * <p>
     * 实现细节：
     * 1. 此操作不是事务性的，可能部分分区成功而其他失败
     * 2. 只修改指定分区的位移，未指定的分区保持不变
     * 3. 要求消费者组中没有活跃成员
     * 4. 操作需要发送到组协调器
     *
     * @param groupId 要修改位移的消费者组
     * @param offsets 分区到位移的映射，包含相关元数据。未在映射中指定的分区将被忽略
     * @param options 修改位移时使用的选项
     * @return 位移修改结果
     */
    AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsets, AlterConsumerGroupOffsetsOptions options);

    /**
     * 查找指定分区的位移信息。此操作可以查找分区的起始位移、结束位移以及匹配指定时间戳的位移。
     * <p>
     * 这是{@link #listOffsets(Map, ListOffsetsOptions)}方法的便捷版本，使用默认选项。
     * <p>
     * 应用场景：
     * 1. 查找特定时间点的消息位置
     * 2. 确定分区的数据范围
     * 3. 监控消息堆积情况
     * 4. 数据清理和维护
     * 
     * 实现细节：
     * 1. 使用默认的ListOffsetsOptions配置
     * 2. 支持批量查询多个分区
     * 3. 可以查询不同类型的位移（开始、结束、时间戳）
     *
     * @param topicPartitionOffsets 分区到OffsetSpec的映射，指定每个分区要查找的位移类型
     * @return ListOffsetsResult 包含查询到的位移信息
     */
    default ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets) {
        return listOffsets(topicPartitionOffsets, new ListOffsetsOptions());
    }

    /**
     * 列出指定分区的位移信息。此操作可以查找分区的起始位移、结束位移以及与时间戳匹配的位移。
     * <p>
     * 应用场景：
     * 1. 消费者需要从特定时间点开始消费消息
     * 2. 监控工具需要计算分区的消息积压量
     * 3. 数据清理时需要确定位移范围
     * 
     * 实现细节：
     * 1. 支持批量查询多个分区的位移信息
     * 2. 可以指定不同类型的位移查询（起始、结束、时间戳）
     * 3. 异步操作，返回Future对象
     *
     * @param topicPartitionOffsets 分区到位移规范的映射，指定每个分区要查询的位移类型
     * @param options 查询位移时使用的选项，如超时设置等
     * @return ListOffsetsResult 包含查询结果的异步操作对象
     */
    ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, ListOffsetsOptions options);

    /**
     * 查询所有匹配指定过滤器且至少定义了一个客户端配额配置值的实体。
     * <p>
     * 这是{@link #describeClientQuotas(ClientQuotaFilter, DescribeClientQuotasOptions)}方法的便捷版本，
     * 使用默认选项。详细信息请参见重载方法。
     * <p>
     * 应用场景：
     * 1. 管理工具需要查看当前的配额设置
     * 2. 监控系统需要收集配额信息
     * 3. 运维人员需要审计配额配置
     * <p>
     * 此操作要求broker版本不低于2.6.0。
     *
     * @param filter 用于匹配实体的过滤器
     * @return DescribeClientQuotasResult 包含查询结果的异步操作对象
     */
    default DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter) {
        return describeClientQuotas(filter, new DescribeClientQuotasOptions());
    }

    /**
     * 查询所有匹配指定过滤器且至少定义了一个客户端配额配置值的实体。
     * <p>
     * 实现细节：
     * 1. 支持复杂的过滤条件组合
     * 2. 返回匹配实体的所有配额设置
     * 3. 异步操作，支持超时控制
     * <p>
     * 调用{@code get()}方法获取结果时可能抛出以下异常：
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   如果认证用户没有集群的describe权限</li>
     *   <li>{@link org.apache.kafka.common.errors.InvalidRequestException}
     *   如果请求详情无效，例如指定了无效的实体类型</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   如果请求在完成前超时</li>
     * </ul>
     * <p>
     * 此操作要求broker版本不低于2.6.0。
     *
     * @param filter 用于匹配实体的过滤器
     * @param options 查询时使用的选项
     * @return DescribeClientQuotasResult 包含查询结果的异步操作对象
     */
    DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter, DescribeClientQuotasOptions options);

    /**
     * 修改客户端配额配置。
     * <p>
     * 这是{@link #alterClientQuotas(Collection, AlterClientQuotasOptions)}方法的便捷版本，
     * 使用默认选项。详细信息请参见重载方法。
     * <p>
     * 应用场景：
     * 1. 动态调整客户端的资源使用限制
     * 2. 实施流量控制策略
     * 3. 针对特定客户端进行限流
     * <p>
     * 此操作要求broker版本不低于2.6.0。
     *
     * @param entries 要执行的配额修改操作集合
     * @return AlterClientQuotasResult 包含修改结果的异步操作对象
     */
    default AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries) {
        return alterClientQuotas(entries, new AlterClientQuotasOptions());
    }

    /**
     * 修改客户端配额配置。
     * <p>
     * 实现细节：
     * 1. 单个实体的修改是原子的，但跨实体的修改不保证原子性
     * 2. 需要检查每个实体的错误码来确定更新的成功或失败
     * 3. 支持批量修改多个实体的配额
     * <p>
     * 调用{@code get()}方法获取结果时可能抛出以下异常：
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   如果认证用户没有集群的alter权限</li>
     *   <li>{@link org.apache.kafka.common.errors.InvalidRequestException}
     *   如果请求详情无效，例如为同一实体多次指定配置键</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   如果请求在完成前超时，无法确定更新是否成功</li>
     * </ul>
     * <p>
     * 此操作要求broker版本不低于2.6.0。
     *
     * @param entries 要执行的配额修改操作集合
     * @param options 修改配额时使用的选项
     * @return AlterClientQuotasResult 包含修改结果的异步操作对象
     */
    AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries, AlterClientQuotasOptions options);

    /**
     * 查询所有用户的SASL/SCRAM凭证信息。
     * <p>
     * 这是{@link #describeUserScramCredentials(List, DescribeUserScramCredentialsOptions)}方法的便捷版本。
     * <p>
     * 应用场景：
     * 1. 安全审计
     * 2. 用户认证管理
     * 3. 凭证状态检查
     *
     * @return DescribeUserScramCredentialsResult 包含查询结果的异步操作对象
     */
    default DescribeUserScramCredentialsResult describeUserScramCredentials() {
        return describeUserScramCredentials(null, new DescribeUserScramCredentialsOptions());
    }

    /**
     * 查询指定用户的SASL/SCRAM凭证信息。
     * <p>
     * 这是{@link #describeUserScramCredentials(List, DescribeUserScramCredentialsOptions)}方法的便捷版本。
     * <p>
     * 实现细节：
     * 1. 支持批量查询多个用户的凭证
     * 2. 当users为null或空时查询所有用户
     * 3. 异步操作，返回Future对象
     *
     * @param users 要查询凭证的用户列表；如果为null或空，则查询所有用户的凭证
     * @return DescribeUserScramCredentialsResult 包含查询结果的异步操作对象
     */
    default DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users) {
        return describeUserScramCredentials(users, new DescribeUserScramCredentialsOptions());
    }

    /**
     * 查询SASL/SCRAM凭证信息。
     * <p>
     * 实现细节：
     * 1. 支持查询特定SCRAM机制的凭证
     * 2. 返回凭证的详细信息，包括迭代次数和盐值
     * 3. 适用于安全审计和用户管理场景
     * <p>
     * 调用{@code get()}方法获取结果时可能抛出以下异常：
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   如果认证用户没有集群的describe权限</li>
     *   <li>{@link org.apache.kafka.common.errors.ResourceNotFoundException}
     *   如果用户不存在或没有SCRAM凭证</li>
     *   <li>{@link org.apache.kafka.common.errors.DuplicateResourceException}
     *   如果在原始请求中多次请求描述同一个用户</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   如果请求在完成前超时</li>
     * </ul>
     * <p>
     * 此操作要求broker版本不低于2.7.0。
     *
     * @param users 要查询凭证的用户列表；如果为null或空，则查询所有用户的凭证
     * @param options 查询凭证时使用的选项
     * @return DescribeUserScramCredentialsResult 包含查询结果的异步操作对象
     */
    DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users, DescribeUserScramCredentialsOptions options);

    /**
     * 修改指定用户的SASL/SCRAM凭证。
     * <p>
     * 这是{@link #alterUserScramCredentials(List, AlterUserScramCredentialsOptions)}方法的便捷版本，
     * 使用默认选项。
     * 
     * 实现细节：
     * 1. 使用默认的AlterUserScramCredentialsOptions配置
     * 2. 请求发送到集群的控制器节点
     * 3. 支持批量修改多个用户的凭证
     *
     * @param alterations 要应用的凭证修改列表
     * @return AlterUserScramCredentialsResult 异步操作结果
     */
    default AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations) {
        return alterUserScramCredentials(alterations, new AlterUserScramCredentialsOptions());
    }

    /**
     * 修改SASL/SCRAM凭证，支持自定义选项。
     * <p>
     * 当调用返回的{@link AlterUserScramCredentialsResult}中的futures的{@code get()}方法时，
     * 可能会遇到以下异常：
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.NotControllerException}
     *   如果请求未发送到控制器broker。</li>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   如果认证用户没有修改集群的权限。</li>
     *   <li>{@link org.apache.kafka.common.errors.UnsupportedByAuthenticationException}
     *   如果用户使用委派令牌进行认证。</li>
     *   <li>{@link org.apache.kafka.common.errors.UnsupportedSaslMechanismException}
     *   如果请求的SCRAM机制未被识别或不受支持。</li>
     *   <li>{@link org.apache.kafka.common.errors.UnacceptableCredentialException}
     *   如果用户名为空或请求的迭代次数过小或过大。</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   如果请求在完成前超时。</li>
     * </ul>
     * <p>
     * 此操作要求broker版本不低于2.7.0。
     * 
     * 实现细节：
     * 1. 支持批量修改多个用户的凭证
     * 2. 可以指定自定义选项，如超时设置
     * 3. 操作结果通过异步方式返回
     *
     * @param alterations 要应用的凭证修改列表
     * @param options 修改凭证时使用的选项
     * @return AlterUserScramCredentialsResult 异步操作结果
     */
    AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations,
                                                              AlterUserScramCredentialsOptions options);
    /**
     * 描述已完成确定的和支持的功能特性。
     * <p>
     * 这是{@link #describeFeatures(DescribeFeaturesOptions)}方法的便捷版本，使用默认选项。
     * 详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的DescribeFeaturesOptions配置
     * 2. 可以连接到任何broker获取功能特性信息
     * 3. 返回集群支持的所有功能特性列表
     *
     * @return DescribeFeaturesResult 包含查询结果的对象
     */
    default DescribeFeaturesResult describeFeatures() {
        return describeFeatures(new DescribeFeaturesOptions());
    }

    /**
     * 描述已完成确定的和支持的功能特性。请求会发送到随机选择的broker。
     * <p>
     * 当调用返回的{@link DescribeFeaturesResult}中的future的{@code get()}方法时，
     * 可能会遇到以下异常：
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   如果请求在描述操作完成前超时。</li>
     * </ul>
     * 
     * 实现细节：
     * 1. 支持自定义查询选项
     * 2. 返回详细的功能特性信息
     * 3. 可以查询特定版本的功能支持情况
     *
     * @param options 查询时使用的选项
     * @return DescribeFeaturesResult 包含查询结果的对象
     */
    DescribeFeaturesResult describeFeatures(DescribeFeaturesOptions options);

    /**
     * 应用指定的功能特性更新。此操作不是事务性的，部分更新可能成功而其他失败。
     * <p>
     * API接收一个映射，将功能特性名称映射到{@link FeatureUpdate}。每个条目指定要添加、更新或
     * 删除的功能特性，以及新的最大功能版本级别值。此请求仅发送到控制器，因为该API仅由控制器提供服务。
     * 返回值包含每个提供的{@link FeatureUpdate}的错误代码，表明更新在控制器中是否成功。
     * <ul>
     * <li>降级功能版本级别并不常见，仅在必要时执行。只有当{@link FeatureUpdate}将
     * {@code upgradeType}指定为{@link FeatureUpdate.UpgradeType#SAFE_DOWNGRADE}或
     * {@link FeatureUpdate.UpgradeType#UNSAFE_DOWNGRADE}时才允许。
     * <ul>
     * <li>{@code SAFE_DOWNGRADE}：允许不会导致元数据丢失的降级。</li>
     * <li>{@code UNSAFE_DOWNGRADE}：允许可能导致元数据丢失的降级。</li>
     * </ul>
     * 注意，即使使用这些设置，如果控制器认为某些降级不安全或不可能，仍可能拒绝这些降级。</li>
     * <li>删除已确定的功能特性版本也不是常见操作。要删除功能特性，
     * 将{@code maxVersionLevel}设置为零，并将{@code upgradeType}指定为
     * {@link FeatureUpdate.UpgradeType#SAFE_DOWNGRADE}或
     * {@link FeatureUpdate.UpgradeType#UNSAFE_DOWNGRADE}。</li>
     * <li>当{@code maxVersionLevel}为零时，不能使用{@link FeatureUpdate.UpgradeType#UPGRADE}类型。
     * 尝试这样做将导致{@link IllegalArgumentException}。</li>
     * </ul>
     * <p>
     * 当调用返回的{@link UpdateFeaturesResult}中的futures的{@code get()}方法时，
     * 可能会遇到以下异常：
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   如果认证用户没有修改集群的权限。</li>
     *   <li>{@link org.apache.kafka.common.errors.InvalidRequestException}
     *   如果请求详情无效。例如，尝试删除或降级不存在的已确定功能特性。</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   如果请求在更新完成前超时。无法保证更新是否成功。</li>
     *   <li>{@link FeatureUpdateFailedException}
     *   表示在控制器应用更新时遇到意外错误。无法保证更新是否成功。
     *   最好的方法是发出{@link Admin#describeFeatures(DescribeFeaturesOptions)}请求来确认。</li>
     * </ul>
     * <p>
     * 此操作要求broker版本不低于2.7.0。
     * 
     * 实现细节：
     * 1. 支持批量更新多个功能特性
     * 2. 提供安全和不安全的降级选项
     * 3. 包含详细的错误处理机制
     *
     * @param featureUpdates 功能特性名称到{@link FeatureUpdate}的映射
     * @param options 更新时使用的选项
     * @return UpdateFeaturesResult 包含更新结果的对象
     */
    UpdateFeaturesResult updateFeatures(Map<String, FeatureUpdate> featureUpdates, UpdateFeaturesOptions options);

    /**
     * 描述元数据仲裁的状态。
     * <p>
     * 这是{@link #describeMetadataQuorum(DescribeMetadataQuorumOptions)}方法的便捷版本，
     * 使用默认选项。详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的DescribeMetadataQuorumOptions配置
     * 2. 返回集群元数据仲裁的当前状态
     * 3. 适用于监控和诊断场景
     *
     * @return DescribeMetadataQuorumResult 包含查询结果的对象
     */
    default DescribeMetadataQuorumResult describeMetadataQuorum() {
        return describeMetadataQuorum(new DescribeMetadataQuorumOptions());
    }

    /**
     * 描述元数据仲裁（Metadata Quorum）的状态。
     * <p>
     * 元数据仲裁是Kafka的一个关键组件，负责管理集群的元数据信息。
     * 此方法用于获取元数据仲裁的当前状态，包括：
     * - 仲裁成员信息
     * - 当前的leader和follower状态
     * - 复制和同步状态
     * <p>
     * 在调用返回的{@code DescribeMetadataQuorumResult}的{@code get()}方法时，
     * 可能会遇到以下异常：
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.ClusterAuthorizationException}
     *   如果认证用户没有集群的{@code DESCRIBE}访问权限</li>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   如果在控制器能够列出集群链接之前请求超时</li>
     * </ul>
     * 
     * 应用场景：
     * 1. 监控元数据仲裁的健康状态
     * 2. 诊断元数据复制问题
     * 3. 进行集群运维和故障排查
     *
     * @param options 描述仲裁时使用的{@link DescribeMetadataQuorumOptions}选项
     * @return {@link DescribeMetadataQuorumResult} 包含查询结果的对象
     */
    DescribeMetadataQuorumResult describeMetadataQuorum(DescribeMetadataQuorumOptions options);

    /**
     * 注销一个broker。
     * <p>
     * 这是{@link #unregisterBroker(int, UnregisterBrokerOptions)}方法的便捷版本。
     * <p>
     * 重要说明：
     * 1. 此操作不会影响分区的分配状态
     * 2. 注销操作只是将broker从集群中移除，不会影响数据
     * 3. 适用于需要临时或永久移除broker的场景
     * 
     * 应用场景：
     * - 集群缩容时安全移除broker
     * - 维护期间临时移除broker
     * - broker硬件升级或替换
     *
     * @param brokerId 要注销的broker的ID
     * @return {@link UnregisterBrokerResult} 包含操作结果的对象
     */
    @InterfaceStability.Unstable
    default UnregisterBrokerResult unregisterBroker(int brokerId) {
        return unregisterBroker(brokerId, new UnregisterBrokerOptions());
    }

    /**
     * 注销一个broker，支持自定义选项。
     * <p>
     * 此操作用于从Kafka集群中安全地移除一个broker。
     * 重要说明：
     * 1. 此操作不会影响现有的分区分配
     * 2. broker被注销后，需要手动处理其上的数据
     * 3. 建议在执行此操作前先将数据迁移到其他broker
     * <p>
     * 在调用返回的{@link UnregisterBrokerResult}的{@code get()}方法时，
     * 可能会遇到以下异常：
     * <ul>
     *   <li>{@link org.apache.kafka.common.errors.TimeoutException}
     *   如果在描述操作完成之前请求超时</li>
     *   <li>{@link org.apache.kafka.common.errors.UnsupportedVersionException}
     *   如果软件版本过旧，不支持注销API</li>
     * </ul>
     * <p>
     * 执行步骤建议：
     * 1. 先将broker上的数据迁移到其他节点
     * 2. 确保没有生产者和消费者正在使用该broker
     * 3. 执行注销操作
     * 4. 验证注销结果
     * 5. 根据需要进行清理工作
     *
     * @param brokerId 要注销的broker的ID
     * @param options 注销操作的选项配置
     * @return {@link UnregisterBrokerResult} 包含操作结果的对象
     */
    @InterfaceStability.Unstable
    UnregisterBrokerResult unregisterBroker(int brokerId, UnregisterBrokerOptions options);

    /**
     * 查询一组主题分区上的生产者状态。
     * <p>
     * 这是{@link #describeProducers(Collection, DescribeProducersOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * 
     * 应用场景：
     * 1. 监控生产者活动状态
     * 2. 排查生产者问题
     * 3. 识别活跃的事务
     *
     * @param partitions 要查询的分区集合
     * @return DescribeProducersResult 包含生产者状态信息的结果对象
     */
    default DescribeProducersResult describeProducers(Collection<TopicPartition> partitions) {
        return describeProducers(partitions, new DescribeProducersOptions());
    }

    /**
     * 查询一组主题分区上的活跃生产者状态。
     * <p>
     * 此方法提供了详细的生产者状态信息，包括：
     * 1. 活跃的生产者ID列表
     * 2. 生产者的事务状态
     * 3. 最后操作的时间戳
     * <p>
     * 除非通过{@link DescribeProducersOptions#brokerId(int)}指定特定的broker，
     * 否则将查询分区leader以获取生产者状态。
     * 
     * 实现细节：
     * 1. 默认查询分区leader节点
     * 2. 支持指定特定broker进行查询
     * 3. 返回每个分区的活跃生产者信息
     * 
     * 使用建议：
     * 1. 在排查生产者问题时使用
     * 2. 监控特定分区的生产者活动
     * 3. 识别可能的生产者故障或卡住的事务
     *
     * @param partitions 要查询的分区集合
     * @param options 控制查询行为的选项
     * @return DescribeProducersResult 包含生产者状态信息的结果对象
     */
    DescribeProducersResult describeProducers(Collection<TopicPartition> partitions, DescribeProducersOptions options);

    /**
     * 查询一组事务ID的状态。
     * <p>
     * 这是{@link #describeTransactions(Collection, DescribeTransactionsOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * 
     * 应用场景：
     * 1. 监控事务的执行状态
     * 2. 排查事务相关问题
     * 3. 识别长时间运行或卡住的事务
     *
     * @param transactionalIds 要查询的事务ID集合
     * @return DescribeTransactionsResult 包含事务状态信息的结果对象
     */
    default DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds) {
        return describeTransactions(transactionalIds, new DescribeTransactionsOptions());
    }

    /**
     * 从相应的事务协调器查询一组事务ID的状态。
     * <p>
     * 此方法提供了详细的事务状态信息，包括：
     * 1. 事务当前状态（进行中、已提交、已中止等）
     * 2. 事务涉及的主题分区
     * 3. 生产者ID和事务开始时间
     * <p>
     * 实现细节：
     * 1. 动态发现事务协调器
     * 2. 并行查询多个事务的状态
     * 3. 聚合来自不同协调器的结果
     * 
     * 使用建议：
     * 1. 定期监控重要事务的状态
     * 2. 在发生故障时快速定位问题
     * 3. 识别需要手动干预的事务
     *
     * @param transactionalIds 要查询的事务ID集合
     * @param options 控制查询行为的选项
     * @return DescribeTransactionsResult 包含事务状态信息的结果对象
     */
    DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds, DescribeTransactionsOptions options);

    /**
     * 强制中止主题分区上的开放事务。
     * <p>
     * 这是{@link #abortTransaction(AbortTransactionSpec, AbortTransactionOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * 
     * 应用场景：
     * 1. 清理长时间未完成的事务
     * 2. 处理生产者崩溃导致的悬挂事务
     * 3. 系统恢复过程中的事务清理
     *
     * @param spec 事务规范，包含主题分区和生产者详细信息
     * @return AbortTransactionResult 包含事务中止结果的对象
     */
    default AbortTransactionResult abortTransaction(AbortTransactionSpec spec) {
        return abortTransaction(spec, new AbortTransactionOptions());
    }

    /**
     * 强制中止主题分区上的开放事务。
     * <p>
     * 此方法将向分区leader发送`WriteTxnMarkers`请求以中止事务。
     * 执行此操作需要管理员权限。
     * <p>
     * 实现细节：
     * 1. 向分区leader发送中止标记
     * 2. 更新事务元数据
     * 3. 清理相关资源
     * 
     * 重要说明：
     * 1. 此操作不可逆，请谨慎使用
     * 2. 建议在确认事务确实需要中止时才使用
     * 3. 可能影响正在进行的生产者操作
     * 
     * 使用建议：
     * 1. 在进行系统恢复时使用
     * 2. 处理检测到的死锁事务
     * 3. 作为运维工具处理异常情况
     *
     * @param spec 事务规范，包含主题分区和生产者详细信息
     * @param options 控制方法行为的选项（包括过滤器）
     * @return AbortTransactionResult 包含事务中止结果的对象
     */
    AbortTransactionResult abortTransaction(AbortTransactionSpec spec, AbortTransactionOptions options);

    /**
     * 列出集群中所有活跃的事务。
     * 这是{@link #listTransactions(ListTransactionsOptions)}方法的便捷版本，使用默认选项。
     * 详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的ListTransactionsOptions配置
     * 2. 查询所有潜在的事务协调器节点
     * 3. 收集所有事务的状态信息
     *
     * @return ListTransactionsResult 异步操作结果，包含活跃事务列表
     */
    default ListTransactionsResult listTransactions() {
        return listTransactions(new ListTransactionsOptions());
    }

    /**
     * 列出集群中所有活跃的事务，支持自定义选项。
     * 此方法会查询集群中所有潜在的事务协调器，并收集所有事务的状态。
     * <p>
     * 为了减少结果集大小，用户通常应该使用以下过滤选项：
     * 1. {@link ListTransactionsOptions#filterProducerIds(Collection)} - 按生产者ID过滤
     * 2. {@link ListTransactionsOptions#filterStates(Collection)} - 按事务状态过滤
     * 3. {@link ListTransactionsOptions#filterOnDuration(long)} - 按事务持续时间过滤
     * 
     * 实现细节：
     * 1. 支持多种过滤条件组合
     * 2. 异步收集所有事务协调器的响应
     * 3. 合并过滤后的结果集
     *
     * @param options 控制方法行为的选项（包括过滤器）
     * @return ListTransactionsResult 异步操作结果，包含经过过滤的活跃事务列表
     */
    ListTransactionsResult listTransactions(ListTransactionsOptions options);

    /**
     * 使用默认选项隔离（fence out）指定事务ID的所有活跃生产者。
     * <p>
     * 这是{@link #fenceProducers(Collection, FenceProducersOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的FenceProducersOptions配置
     * 2. 对每个事务ID执行隔离操作
     * 3. 防止旧的生产者实例继续写入
     *
     * @param transactionalIds 要隔离的生产者的事务ID集合
     * @return FenceProducersResult 异步操作结果，包含每个事务ID的隔离状态
     */
    default FenceProducersResult fenceProducers(Collection<String> transactionalIds) {
        return fenceProducers(transactionalIds, new FenceProducersOptions());
    }

    /**
     * 隔离（fence out）指定事务ID的所有活跃生产者，支持自定义选项。
     * <p>
     * 隔离操作的作用：
     * 1. 阻止使用这些事务ID的所有当前活跃的生产者继续发送消息
     * 2. 确保这些生产者的未完成事务被终止
     * 3. 允许新的生产者使用这些事务ID开始新的事务
     * 
     * 实现细节：
     * 1. 向事务协调器发送隔离请求
     * 2. 等待协调器确认隔离完成
     * 3. 支持批量操作多个事务ID
     *
     * @param transactionalIds 要隔离的生产者的事务ID集合
     * @param options 隔离生产者时使用的选项
     * @return FenceProducersResult 异步操作结果
     */
    FenceProducersResult fenceProducers(Collection<String> transactionalIds,
                                        FenceProducersOptions options);

    /**
     * 列出集群中可用的客户端度量配置资源。
     * <p>
     * 此方法用于获取集群级别的客户端监控配置信息，包括：
     * 1. 可用的度量指标类型
     * 2. 度量采集配置
     * 3. 监控资源限制
     * 
     * 实现细节：
     * 1. 查询集群中的度量配置
     * 2. 收集所有可用的监控资源
     * 3. 支持自定义查询选项
     *
     * @param options 列出客户端度量资源时使用的选项
     * @return ListClientMetricsResourcesResult 异步操作结果，包含可用的度量配置资源
     */
    ListClientMetricsResourcesResult listClientMetricsResources(ListClientMetricsResourcesOptions options);

    /**
     * 使用默认选项列出集群中可用的客户端度量配置资源。
     * <p>
     * 这是{@link #listClientMetricsResources(ListClientMetricsResourcesOptions)}方法的便捷版本。
     * 详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的ListClientMetricsResourcesOptions配置
     * 2. 返回所有可用的度量配置资源
     *
     * @return ListClientMetricsResourcesResult 异步操作结果
     */
    default ListClientMetricsResourcesResult listClientMetricsResources() {
        return listClientMetricsResources(new ListClientMetricsResourcesOptions());
    }

    /**
     * 获取用于遥测的客户端唯一实例ID。
     * 此ID对于特定的客户端实例是唯一的，一旦生成就不会改变。
     * ID用于关联客户端操作与发送到broker及其最终监控目标的遥测数据。
     * <p>
     * 工作机制：
     * 1. 如果启用了遥测，首次调用需要连接集群生成唯一客户端实例ID
     * 2. 方法会等待最多timeout时间让admin客户端完成请求
     * 3. 遥测功能由{@link AdminClientConfig#ENABLE_METRICS_PUSH_CONFIG}配置项控制
     * 
     * 实现细节：
     * 1. 首次调用时生成全局唯一的实例ID
     * 2. 后续调用返回相同的ID
     * 3. 支持超时控制
     *
     * @param timeout 等待admin客户端确定其客户端实例ID的最大时间。
     *                值必须非负。指定0表示如果请求尚未完成则不等待。
     * @throws InterruptException 如果线程在阻塞时被中断
     * @throws KafkaException 如果在确定客户端实例ID时发生意外错误
     *                        （注意：此错误不一定表示admin客户端不可用）
     * @throws IllegalArgumentException 如果timeout为负数
     * @throws IllegalStateException 如果遥测未启用，即配置`{@code enable.metrics.push}`
     *                               设置为`{@code false}`
     * @return 用于度量收集的客户端分配的实例ID
     */
    Uuid clientInstanceId(Duration timeout);

    /**
     * 向KRaft元数据仲裁组添加新的投票节点。
     * <p>
     * 这是{@link #addRaftVoter(int, Uuid, Set, AddRaftVoterOptions)}方法的便捷版本。
     * 使用默认选项添加投票节点。
     * 
     * 实现细节：
     * 1. 使用默认的AddRaftVoterOptions配置
     * 2. 验证新节点的有效性
     * 3. 将节点加入仲裁组
     *
     * @param voterId 投票节点的节点ID
     * @param voterDirectoryId 投票节点的目录ID
     * @param endpoints 新投票节点的网络端点集合
     * @return AddRaftVoterResult 异步操作结果
     */
    default AddRaftVoterResult addRaftVoter(
        int voterId,
        Uuid voterDirectoryId,
        Set<RaftVoterEndpoint> endpoints
    ) {
        return addRaftVoter(voterId, voterDirectoryId, endpoints, new AddRaftVoterOptions());
    }

    /**
     * 向KRaft元数据仲裁组添加新的投票节点，支持自定义选项。
     * <p>
     * 此操作用于扩展KRaft集群的投票成员。新节点将参与：
     * 1. 领导者选举
     * 2. 元数据的复制和同步
     * 3. 集群配置的决策
     * 
     * 实现细节：
     * 1. 验证新节点配置
     * 2. 更新仲裁组成员关系
     * 3. 等待新节点加入完成
     *
     * @param voterId 投票节点的节点ID
     * @param voterDirectoryId 投票节点的目录ID
     * @param endpoints 新投票节点的网络端点集合
     * @param options 添加新投票节点时使用的选项
     * @return AddRaftVoterResult 异步操作结果
     */
    AddRaftVoterResult addRaftVoter(
        int voterId,
        Uuid voterDirectoryId,
        Set<RaftVoterEndpoint> endpoints,
        AddRaftVoterOptions options
    );

    /**
     * 从KRaft元数据仲裁组中移除投票节点。
     * <p>
     * 这是{@link #removeRaftVoter(int, Uuid, RemoveRaftVoterOptions)}方法的便捷版本。
     * 使用默认选项移除投票节点。
     * 
     * 实现细节：
     * 1. 使用默认的RemoveRaftVoterOptions配置
     * 2. 验证移除操作的安全性
     * 3. 更新仲裁组成员关系
     *
     * @param voterId 要移除的投票节点的节点ID
     * @param voterDirectoryId 要移除的投票节点的目录ID
     * @return RemoveRaftVoterResult 异步操作结果
     */
    default RemoveRaftVoterResult removeRaftVoter(
        int voterId,
        Uuid voterDirectoryId
    ) {
        return removeRaftVoter(voterId, voterDirectoryId, new RemoveRaftVoterOptions());
    }

    /**
     * 从KRaft元数据仲裁中移除一个投票节点。
     * <p>
     * 此操作用于管理KRaft集群的成员关系，允许动态调整投票成员。
     * <p>
     * 版本兼容性：
     * - 此操作仅在KRaft（Kafka Raft）模式下可用
     * - 要求broker版本不低于3.0
     * 
     * 实现细节：
     * 1. 操作会发送到当前的控制器节点
     * 2. 移除操作是异步的，可能需要一段时间才能完成
     * 3. 如果被移除的节点是当前控制器，可能触发新的控制器选举
     *
     * @param voterId 要移除的投票节点的ID
     * @param voterDirectoryId 投票节点的目录ID
     * @param options 移除投票节点时使用的选项
     * @return RemoveRaftVoterResult 异步操作结果
     */
    RemoveRaftVoterResult removeRaftVoter(
        int voterId,
        Uuid voterDirectoryId,
        RemoveRaftVoterOptions options
    );

    /**
     * 描述集群中的共享消费者组信息。
     * <p>
     * 此操作用于获取共享消费者组的详细信息，包括：
     * - 组的成员列表
     * - 分区分配信息
     * - 消费进度
     * - 其他组级别的元数据
     * 
     * 实现细节：
     * 1. 请求会发送到组协调器所在的broker
     * 2. 如果组协调器发生变更，客户端会自动重试
     * 3. 返回的信息反映查询时刻的状态快照
     *
     * @param groupIds 要描述的消费者组ID集合
     * @param options 描述共享消费者组时使用的选项
     * @return DescribeShareGroupsResult 异步操作结果
     */
    DescribeShareGroupsResult describeShareGroups(Collection<String> groupIds,
                                                  DescribeShareGroupsOptions options);

    /**
     * 使用默认选项描述集群中的共享消费者组信息。
     * <p>
     * 这是{@link #describeShareGroups(Collection, DescribeShareGroupsOptions)}方法的便捷版本。
     * 使用默认选项描述共享消费者组，详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的DescribeShareGroupsOptions配置
     * 2. 适用于不需要特殊选项的常规查询场景
     *
     * @param groupIds 要描述的消费者组ID集合
     * @return DescribeShareGroupsResult 异步操作结果
     */
    default DescribeShareGroupsResult describeShareGroups(Collection<String> groupIds) {
        return describeShareGroups(groupIds, new DescribeShareGroupsOptions());
    }

    /**
     * 列出指定共享消费者组的位移信息。
     * <p>
     * 此操作用于获取共享消费者组在各个主题分区上的消费位移，包括：
     * - 当前提交的位移
     * - 主题分区的元数据
     * - 位移相关的时间戳信息
     * 
     * 实现细节：
     * 1. 请求发送到组协调器所在的broker
     * 2. 支持按主题分区过滤要查询的位移
     * 3. 返回的位移信息反映查询时刻的状态
     *
     * @param groupSpecs 共享消费者组ID到主题分区规格的映射，指定要列出位移的分区
     * @param options 列出共享消费者组位移时使用的选项
     * @return ListShareGroupOffsetsResult 异步操作结果
     */
    ListShareGroupOffsetsResult listShareGroupOffsets(Map<String, ListShareGroupOffsetsSpec> groupSpecs, ListShareGroupOffsetsOptions options);

    /**
     * 使用默认选项列出指定共享消费者组的位移信息。
     * <p>
     * 这是{@link #listShareGroupOffsets(Map, ListShareGroupOffsetsOptions)}方法的便捷版本。
     * 使用默认选项列出所有分区的位移信息。
     * 
     * 实现细节：
     * 1. 使用默认的ListShareGroupOffsetsOptions配置
     * 2. 返回指定消费者组所有分区的位移信息
     *
     * @param groupSpecs 共享消费者组ID到主题分区规格的映射
     * @return ListShareGroupOffsetsResult 异步操作结果
     */
    default ListShareGroupOffsetsResult listShareGroupOffsets(Map<String, ListShareGroupOffsetsSpec> groupSpecs) {
        return listShareGroupOffsets(groupSpecs, new ListShareGroupOffsetsOptions());
    }

    /**
     * 描述集群中的经典消费者组信息。
     * <p>
     * 此操作用于获取经典消费者组的详细信息，包括：
     * - 组的成员列表和成员元数据
     * - 分区分配方案
     * - 组的状态信息
     * - 协议类型和协议集
     * 
     * 实现细节：
     * 1. 请求发送到组协调器所在的broker
     * 2. 支持批量查询多个消费者组
     * 3. 返回的信息包含组的完整状态
     *
     * @param groupIds 要描述的消费者组ID集合
     * @param options 描述经典消费者组时使用的选项
     * @return DescribeClassicGroupsResult 异步操作结果
     */
    DescribeClassicGroupsResult describeClassicGroups(Collection<String> groupIds,
                                                      DescribeClassicGroupsOptions options);

    /**
     * 使用默认选项描述集群中的经典消费者组信息。
     * <p>
     * 这是{@link #describeClassicGroups(Collection, DescribeClassicGroupsOptions)}方法的便捷版本。
     * 使用默认选项描述经典消费者组，详细信息请参见重载方法。
     * 
     * 实现细节：
     * 1. 使用默认的DescribeClassicGroupsOptions配置
     * 2. 适用于不需要特殊选项的标准查询场景
     *
     * @param groupIds 要描述的消费者组ID集合
     * @return DescribeClassicGroupsResult 异步操作结果
     */
    default DescribeClassicGroupsResult describeClassicGroups(Collection<String> groupIds) {
        return describeClassicGroups(groupIds, new DescribeClassicGroupsOptions());
    }

    /**
     * 添加应用程序指标以进行订阅。
     * <p>
     * 此方法将指标添加到客户端的指标集合中，这些指标可用于：
     * 1. 监控订阅
     * 2. 作为遥测数据发送到broker
     * <p>
     * 指标要求：
     * - 必须符合OpenTelemetry v1指标protobuf消息类型
     * - 支持以下两种类型：
     * <ul>
     *   <li>
     *     `Sum`：单调递增计数器，适用于累计值，如总发送字节数
     *   </li>
     *   <li>
     *     `Gauge`：非单调当前值计数器，适用于瞬时值，如当前队列长度
     *   </li>
     * </ul>
     * 
     * 实现细节：
     * 1. 不匹配支持类型的指标会被静默忽略
     * 2. 重复注册同一指标会更新该指标的条目
     * 3. 注册的指标可用于监控和性能分析
     *
     * @param metric 要注册的应用程序指标
     */
    void registerMetricForSubscription(KafkaMetric metric);

    /**
     * 取消订阅并移除应用程序指标。
     * <p>
     * 此方法将指标从客户端的指标集合中移除，导致：
     * 1. 指标不再可用于订阅
     * 2. 停止向broker发送该指标的遥测数据
     * 
     * 实现细节：
     * 1. 移除未注册的指标是无害操作，不会产生任何效果
     * 2. 成功移除后，该指标将不再参与监控和报告
     *
     * @param metric 要移除的应用程序指标
     */
    void unregisterMetricFromSubscription(KafkaMetric metric);

    /**
     * 获取Admin客户端维护的所有指标。
     * 
     * 实现细节：
     * 1. 返回当前活跃的所有指标及其最新值
     * 2. 指标可用于监控客户端的性能和健康状况
     * 3. 返回的Map中，key为指标名称，value为指标对象
     *
     * @return 包含所有活跃指标的Map
     */
    Map<MetricName, ? extends Metric> metrics();
}
