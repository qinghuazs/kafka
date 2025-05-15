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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.LogTruncationException;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 用于获取偏移量、验证和重置位置的工具函数。
 * 应用场景：在 Kafka 消费者客户端内部，当需要与 Broker 交互以获取分区的最新偏移量、
 *          根据时间戳查找偏移量，或者在消费者启动/重新平衡后验证和重置消费位置时，会使用此类中的方法。
 * 设计考虑：此类将偏移量获取相关的逻辑集中起来，便于管理和复用。
 *          它处理了与 Broker 通信的细节，包括错误处理和重试逻辑，
 *          并提供了对不同版本 Kafka Broker API 的兼容性支持。
 */
class OffsetFetcherUtils {
    // 消费者元数据，用于获取集群和主题分区的信息
    private final ConsumerMetadata metadata;
    // 消费者的订阅状态，用于跟踪消费者订阅的主题和分区，以及它们的位置信息
    private final SubscriptionState subscriptionState;
    // 时间工具类，用于获取当前时间，主要用于超时控制和时间戳相关的操作
    private final Time time;
    // 重试退避时间（毫秒），当请求失败需要重试时，等待的时间间隔
    private final long retryBackoffMs;
    // API 版本信息，用于确定与 Broker 通信时可以使用的 API 版本
    private final ApiVersions apiVersions;
    // 日志记录器，用于记录此类中的日志信息
    private final Logger log;

    /**
     * 验证位置时发生的异常，将在下一次调用验证位置时传播。
     * 这可能是在 OffsetsForLeaderEpoch 响应中收到的错误，
     * 或者是在使用成功响应验证位置时检测到的 LogTruncationException。
     * 抛出时将被清除。
     * 应用场景：当消费者需要验证其当前消费位置的有效性时（例如，检查日志是否被截断），
     *          如果验证过程中发生可恢复的错误或需要延迟处理的错误，此字段会缓存该异常。
     * 设计考虑：使用 AtomicReference 确保线程安全地缓存和传播异常，避免在多次调用间丢失重要的错误信息。
     */
    private final AtomicReference<RuntimeException> cachedValidatePositionsException = new AtomicReference<>();
    /**
     * 重置位置时发生的异常，将在下一次调用重置位置时传播。
     * 这将包含在 ListOffsets 请求响应中收到的错误。
     * 在下一次调用重置时抛出时将被清除。
     * 应用场景：当消费者需要根据特定策略（如最早、最新、特定时间戳）重置其消费位置时，
     *          如果重置过程中发生错误，此字段会缓存该异常。
     * 设计考虑：与 cachedValidatePositionsException 类似，使用 AtomicReference 保证线程安全，
     *          并确保在后续操作中能够处理之前发生的错误。
     */
    private final AtomicReference<RuntimeException> cachedResetPositionsException = new AtomicReference<>();
    // 元数据更新版本号，用于跟踪元数据的变化，以便在元数据更新时重新验证位置
    private final AtomicInteger metadataUpdateVersion = new AtomicInteger(-1);

    /**
     * OffsetFetcherUtils 的构造函数。
     *
     * @param logContext 日志上下文，用于创建日志记录器。
     * @param metadata 消费者元数据，提供集群和主题分区的信息。
     * @param subscriptionState 消费者的订阅状态，跟踪订阅的主题、分区及其位置。
     * @param time 时间工具类，用于获取当前时间和处理时间相关的操作。
     * @param retryBackoffMs 重试退避时间（毫秒），用于请求失败时的重试间隔。
     * @param apiVersions API 版本信息，用于确定与 Broker 通信时可用的 API 版本。
     * 应用场景：在创建 KafkaConsumer 实例或其内部组件（如 Fetcher）时，会实例化此类，
     *          用于后续的偏移量获取和管理操作。
     * 设计考虑：通过构造函数注入所有依赖项，符合依赖倒置原则，便于测试和管理。
     *          参数涵盖了执行偏移量相关操作所需的所有核心组件和配置。
     */
    OffsetFetcherUtils(LogContext logContext,
                       ConsumerMetadata metadata,
                       SubscriptionState subscriptionState,
                       Time time,
                       long retryBackoffMs,
                       ApiVersions apiVersions) {
        // 使用提供的 logContext 初始化日志记录器
        this.log = logContext.logger(getClass());
        // 初始化消费者元数据
        this.metadata = metadata;
        // 初始化消费者订阅状态
        this.subscriptionState = subscriptionState;
        // 初始化时间工具类
        this.time = time;
        // 初始化重试退避时间
        this.retryBackoffMs = retryBackoffMs;
        // 初始化 API 版本信息
        this.apiVersions = apiVersions;
    }

    /**
     * 处理 ListOffsets 请求响应的回调方法。
     *
     * @param listOffsetsResponse 从服务器接收到的 ListOffsets 响应。
     * @return 从响应中提取的 {@link OffsetFetcherUtils.ListOffsetResult}，包含获取到的偏移量和需要重试的分区。
     * 应用场景：当消费者发送 ListOffsets 请求（例如，为了根据时间戳查找偏移量，或者获取最早/最新的偏移量）并收到 Broker 的响应后，
     *          此方法被调用来解析响应内容，处理各种成功和失败的情况。
     * 设计考虑：此方法集中处理 ListOffsets 响应的逻辑，包括错误码的判断、成功数据的提取以及需要重试的分区的识别。
     *          通过返回一个包含成功获取的偏移量和待重试分区的结构化对象 (ListOffsetResult)，简化了调用方的处理逻辑。
     *          对于不同的错误类型，采取了不同的处理策略，如记录日志、标记分区以便重试，或抛出授权异常。
     */
    OffsetFetcherUtils.ListOffsetResult handleListOffsetResponse(ListOffsetsResponse listOffsetsResponse) {
        // 创建一个 HashMap 用于存储成功获取到的偏移量数据，键为 TopicPartition，值为 ListOffsetData
        Map<TopicPartition, OffsetFetcherUtils.ListOffsetData> fetchedOffsets = new HashMap<>();
        // 创建一个 HashSet 用于存储需要重试的分区
        Set<TopicPartition> partitionsToRetry = new HashSet<>();
        // 创建一个 HashSet 用于存储未授权访问的主题名称
        Set<String> unauthorizedTopics = new HashSet<>();

        // 遍历 ListOffsetsResponse 中的每个主题的响应
        for (ListOffsetsResponseData.ListOffsetsTopicResponse topic : listOffsetsResponse.topics()) {
            // 遍历当前主题响应中的每个分区的响应
            for (ListOffsetsResponseData.ListOffsetsPartitionResponse partition : topic.partitions()) {
                // 根据主题名称和分区索引创建 TopicPartition 对象
                TopicPartition topicPartition = new TopicPartition(topic.name(), partition.partitionIndex());
                // 根据分区响应中的错误码获取对应的 Errors 枚举实例
                Errors error = Errors.forCode(partition.errorCode());
                // 使用 switch 语句处理不同的错误类型
                switch (error) {
                    case NONE:
                        // 如果没有错误 (NONE)
                        // 记录调试日志，包含分区信息、获取到的偏移量和时间戳
                        log.debug("Handling ListOffsetResponse response for {}. Fetched offset {}, timestamp {}",
                                topicPartition, partition.offset(), partition.timestamp());
                        // 检查获取到的偏移量是否不是未知的偏移量 (ListOffsetsResponse.UNKNOWN_OFFSET)
                        if (partition.offset() != ListOffsetsResponse.UNKNOWN_OFFSET) {
                            // 如果 leader epoch 不是未知的 (ListOffsetsResponse.UNKNOWN_EPOCH)，则包装成 Optional<Integer>，否则为空 Optional
                            Optional<Integer> leaderEpoch = (partition.leaderEpoch() == ListOffsetsResponse.UNKNOWN_EPOCH)
                                    ? Optional.empty()
                                    : Optional.of(partition.leaderEpoch());
                            // 创建 ListOffsetData 对象，包含偏移量、时间戳和 leader epoch
                            OffsetFetcherUtils.ListOffsetData offsetData = new OffsetFetcherUtils.ListOffsetData(partition.offset(), partition.timestamp(),
                                    leaderEpoch);
                            // 将获取到的偏移量数据存入 fetchedOffsets 中
                            fetchedOffsets.put(topicPartition, offsetData);
                        }
                        // 跳出 switch 语句
                        break;
                    case UNSUPPORTED_FOR_MESSAGE_FORMAT:
                        // 如果错误是 UNSUPPORTED_FOR_MESSAGE_FORMAT (Broker 端的消息格式版本低于 0.10.0，不支持时间戳查询)
                        // 我们将这种情况视为无法找到与请求时间戳对应的偏移量，并将其从结果中排除。
                        // 记录调试日志，说明由于消息格式版本过低，无法按时间戳搜索该分区的偏移量
                        log.debug("Cannot search by timestamp for partition {} because the message format version " +
                                "is before 0.10.0", topicPartition);
                        // 跳出 switch 语句
                        break;
                    case NOT_LEADER_OR_FOLLOWER: // 当前 Broker 不是该分区的 Leader 或 Follower
                    case REPLICA_NOT_AVAILABLE:  // 分区副本不可用
                    case KAFKA_STORAGE_ERROR:    // Kafka 存储错误
                    case OFFSET_NOT_AVAILABLE:   // 请求的偏移量不可用 (例如，请求时间戳超出范围)
                    case LEADER_NOT_AVAILABLE:   // Leader 不可用
                    case FENCED_LEADER_EPOCH:    // Leader epoch 被隔离
                    case UNKNOWN_LEADER_EPOCH:   // 未知的 Leader epoch
                        // 对于以上可重试的错误
                        // 记录调试日志，说明获取偏移量失败的原因，并将进行重试
                        log.debug("Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                                topicPartition, error);
                        // 将该分区添加到需要重试的集合中
                        partitionsToRetry.add(topicPartition);
                        // 跳出 switch 语句
                        break;
                    case UNKNOWN_TOPIC_OR_PARTITION:
                        // 如果错误是 UNKNOWN_TOPIC_OR_PARTITION (未知的主题或分区)
                        // 记录警告日志，说明在 ListOffset 请求中收到了未知主题或分区的错误
                        log.warn("Received unknown topic or partition error in ListOffset request for partition {}", topicPartition);
                        // 将该分区添加到需要重试的集合中 (通常需要元数据更新后重试)
                        partitionsToRetry.add(topicPartition);
                        // 跳出 switch 语句
                        break;
                    case TOPIC_AUTHORIZATION_FAILED:
                        // 如果错误是 TOPIC_AUTHORIZATION_FAILED (主题授权失败)
                        // 将该主题的名称添加到未授权主题集合中
                        unauthorizedTopics.add(topicPartition.topic());
                        // 跳出 switch 语句
                        break;
                    default:
                        // 对于其他未明确处理的错误
                        // 记录警告日志，说明获取偏移量失败是由于意外的异常，并将进行重试
                        log.warn("Attempt to fetch offsets for partition {} failed due to unexpected exception: {}, retrying.",
                                topicPartition, error.message());
                        // 将该分区添加到需要重试的集合中
                        partitionsToRetry.add(topicPartition);
                }
            }
        }

        // 检查是否存在未授权访问的主题
        if (!unauthorizedTopics.isEmpty())
            // 如果存在未授权的主题，则抛出 TopicAuthorizationException 异常
            throw new TopicAuthorizationException(unauthorizedTopics);
        else
            // 如果没有未授权的主题，则创建并返回 ListOffsetResult 对象，包含成功获取的偏移量和需要重试的分区
            return new OffsetFetcherUtils.ListOffsetResult(fetchedOffsets, partitionsToRetry);
    }




    /**
     * 将按分区组织的数据重新组合成按节点组织的数据。
     * 应用场景：当需要将按分区存储的信息（例如，要获取偏移量的分区列表）按照它们所属的 Leader 节点进行分组时，
     *          可以使用此方法。这通常是为了优化网络请求，将发往同一节点的请求合并处理。
     * 实现细节：使用 Java Stream API 的 `groupingBy` 操作，根据每个分区的 Leader 节点进行分组。
     *          `metadata.fetch().leaderFor(entry.getKey())` 用于获取分区的 Leader 节点。
     * 设计考虑：此方法提供了一种通用的方式来重组数据，使其更适合按节点批量处理的场景。
     *          泛型 `<T>` 使得此方法可以处理不同类型的值。
     *
     * @param partitionMap 按 {@link TopicPartition} 为键，类型为 {@code T} 的值为值的映射。
     * @param <T>          映射中值的类型。
     * @return 按 {@link Node} 为键，其值为一个映射（键为 {@link TopicPartition}，值为 {@code T}）的映射。
     */
    <T> Map<Node, Map<TopicPartition, T>> regroupPartitionMapByNode(Map<TopicPartition, T> partitionMap) {
        // 将输入的 partitionMap 转换为 EntrySet 流
        return partitionMap.entrySet()
                .stream()
                // 使用 Collectors.groupingBy 按 Leader 节点对条目进行分组
                .collect(Collectors.groupingBy(entry -> metadata.fetch().leaderFor(entry.getKey()),
                        // 对于每个分组（即每个 Leader 节点），将属于该节点的分区和对应的值收集到一个新的 Map 中
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * 获取需要验证其位置（offset 和 leader epoch）的分区列表。
     * 应用场景：在消费者启动、分区重新分配或检测到元数据变更后，需要验证当前消费位置的有效性，
     *          例如，检查日志是否被截断，或者 Leader 是否发生变化。
     * 实现细节：
     * 1. 首先检查并抛出之前缓存的验证位置时发生的异常。
     * 2. 调用 `validatePositionsOnMetadataChange` 方法，如果元数据有更新，则标记所有已分配分区需要验证。
     * 3. 从 `subscriptionState` 中获取当前时间点需要验证位置的分区（考虑了退避逻辑）。
     * 4. 过滤掉那些没有设置消费位置的分区。
     * 5. 将需要验证的分区及其当前的 `FetchPosition` 收集到一个 Map 中返回。
     * 设计考虑：此方法封装了确定哪些分区需要验证位置的逻辑，包括异常处理、元数据变更检测和退避机制，
     *          使得调用方可以简单地获取需要处理的分区列表。
     *
     * @return 一个映射，键是需要验证位置的 {@link TopicPartition}，值是对应的 {@link SubscriptionState.FetchPosition}。
     */
    Map<TopicPartition, SubscriptionState.FetchPosition> getPartitionsToValidate() {
        // 获取并清除缓存的验证位置时发生的异常
        RuntimeException exception = cachedValidatePositionsException.getAndSet(null);
        // 如果存在缓存的异常，则立即抛出
        if (exception != null)
            throw exception;

        // 验证每个分区与当前的 leader 和 epoch
        // 如果我们看到新的元数据版本，检查所有分区
        validatePositionsOnMetadataChange();

        // 收集需要验证的位置，带有退避机制
        // 从订阅状态中获取在当前时间需要进行位置验证的分区集合
        return subscriptionState
                .partitionsNeedingValidation(time.milliseconds()) // 获取需要验证的分区，考虑了退避时间
                .stream()
                // 过滤掉那些在订阅状态中没有记录位置信息的分区
                .filter(tp -> subscriptionState.position(tp) != null)
                // 将过滤后的分区收集到一个 Map 中，键是分区本身，值是该分区的当前 FetchPosition
                .collect(Collectors.toMap(Function.identity(), subscriptionState::position));
    }

    /**
     * 尝试设置一个在验证位置过程中发生的运行时异常。
     * 应用场景：当异步的验证操作（如 OffsetsForLeaderEpoch 请求）失败时，其回调函数会调用此方法来缓存异常。
     *          这个异常会在下一次调用 `getPartitionsToValidate` 时被抛出。
     * 实现细节：使用 `AtomicReference.compareAndSet` 来原子地设置异常。
     *          如果 `cachedValidatePositionsException` 当前为 `null`，则将其设置为传入的异常 `e`。
     *          如果已经存在一个待处理的异常，则新的异常 `e` 会被丢弃，并记录一条错误日志。
     * 设计考虑：这种机制确保了只有一个验证相关的异常会被缓存和传播，避免了异常信息的覆盖或混淆。
     *          通过原子操作保证了线程安全。
     *
     * @param e 在验证位置时发生的运行时异常。
     */
    void maybeSetValidatePositionsException(RuntimeException e) {
        // 尝试原子地将 cachedValidatePositionsException 从 null 设置为 e
        // 如果设置失败（即 cachedValidatePositionsException 原本不为 null），说明已有待处理的异常
        if (!cachedValidatePositionsException.compareAndSet(null, e)) {
            // 记录错误日志，说明由于已存在待处理的错误，当前错误被丢弃
            log.error("Discarding error validating positions because another error is pending", e);
        }
    }

    /**
     * 如果我们检测到新的元数据（通过 {@link org.apache.kafka.clients.Metadata#updateVersion()} 跟踪），
     * 那么我们应该检查所有已分配的分区是否都有一个有效的位置。
     * 应用场景：当 Kafka 集群的元数据发生变化（例如 Leader 切换、分区增减等）时，消费者需要重新验证其当前消费位置的有效性。
     *          此方法用于在检测到元数据更新后，触发对所有已分配分区的验证逻辑。
     * 实现细节：
     * 1. 获取当前元数据的更新版本号 `newMetadataUpdateVersion`。
     * 2. 使用 `AtomicInteger.getAndSet` 原子地更新内部记录的元数据版本号 `metadataUpdateVersion`，并获取旧的版本号。
     * 3. 如果旧版本号与新版本号不同，说明元数据确实发生了更新。
     * 4. 在这种情况下，遍历所有已分配给当前消费者的分区。
     * 5. 对于每个分区，获取其当前的 Leader 和 Epoch 信息。
     * 6. 调用 `subscriptionState.maybeValidatePositionForCurrentLeader` 方法，
     *    该方法会根据 Leader 和 Epoch 信息来决定是否需要将该分区标记为需要位置验证。
     * 设计考虑：通过比较元数据版本号来检测变更，可以有效地避免不必要的验证操作。
     *          使用原子操作更新版本号保证了线程安全。
     *          将实际的验证逻辑委托给 `SubscriptionState`，保持了职责分离。
     */
    void validatePositionsOnMetadataChange() {
        // 获取元数据的当前更新版本号
        int newMetadataUpdateVersion = metadata.updateVersion();
        // 原子地将内部记录的元数据版本号设置为新的版本号，并获取旧的版本号
        // 如果旧版本号与新版本号不同，说明元数据发生了更新
        if (metadataUpdateVersion.getAndSet(newMetadataUpdateVersion) != newMetadataUpdateVersion) {
            // 遍历所有已分配给当前消费者的分区
            subscriptionState.assignedPartitions().forEach(topicPartition -> {
                // 获取该分区的当前 Leader 和 Epoch 信息
                ConsumerMetadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(topicPartition);
                // 根据当前的 Leader 和 Epoch，尝试验证该分区的位置
                // 这通常会将该分区标记为需要通过 OffsetsForLeaderEpoch 请求进行验证
                subscriptionState.maybeValidatePositionForCurrentLeader(apiVersions, topicPartition, leaderAndEpoch);
            });
        }
    }

    /**
     * 获取所有已分配分区中需要重置偏移量的分区的 {@link AutoOffsetResetStrategy}（自动偏移量重置策略）。
     * 应用场景：当消费者启动时，如果某些分区没有有效的已提交偏移量，或者消费者配置了从特定位置开始消费，
     *          就需要根据配置的重置策略（如 earliest, latest, none，或特定时间戳）来确定起始偏移量。
     * 实现细节：
     * 1. 首先检查并抛出之前缓存的重置位置时发生的异常。
     * 2. 从 `subscriptionState` 中获取当前时间点需要重置偏移量的分区集合（考虑了退避逻辑）。
     * 3. 遍历这些需要重置的分区。
     * 4. 对于每个分区，调用 `offsetResetStrategyWithValidTimestamp` 方法获取其有效的重置策略。
     *    （`offsetResetStrategyWithValidTimestamp` 会处理 `SubscriptionState` 中为该分区指定的具体策略，
     *     如果是基于时间戳的策略，它会确保时间戳有效）。
     * 5. 将分区和对应的重置策略存入一个 Map 中返回。
     * 设计考虑：此方法封装了确定哪些分区需要重置以及它们各自应采用何种重置策略的逻辑。
     *          它处理了异常传播和退避机制，并确保了时间戳策略的有效性。
     *
     * @return 一个映射，键是需要重置偏移量的 {@link TopicPartition}，值是对应的 {@link AutoOffsetResetStrategy}。
     */
    Map<TopicPartition, AutoOffsetResetStrategy> getOffsetResetStrategyForPartitions() {
        // 如果存在上一次偏移量获取操作中引发的异常，则抛出该异常
        // 获取并清除缓存的重置位置时发生的异常
        RuntimeException exception = cachedResetPositionsException.getAndSet(null);
        // 如果存在缓存的异常，则立即抛出
        if (exception != null)
            throw exception;

        // 从订阅状态中获取在当前时间需要进行偏移量重置的分区集合
        Set<TopicPartition> partitions = subscriptionState.partitionsNeedingReset(time.milliseconds());
        // 创建一个 HashMap 用于存储分区及其对应的自动偏移量重置策略
        final Map<TopicPartition, AutoOffsetResetStrategy> partitionAutoOffsetResetStrategyMap = new HashMap<>();
        // 遍历所有需要重置偏移量的分区
        for (final TopicPartition partition : partitions) {
            // 为每个分区获取其有效的偏移量重置策略（如果策略是基于时间戳的，会进行验证）
            // 并将其存入 partitionAutoOffsetResetStrategyMap 中
            partitionAutoOffsetResetStrategyMap.put(partition, offsetResetStrategyWithValidTimestamp(partition));
        }
        // 返回包含所有需要重置的分区及其对应重置策略的 Map
        return partitionAutoOffsetResetStrategyMap;
    }

    /**
     * 根据搜索的时间戳和获取到的偏移量数据，构建 ListOffsets 请求的结果。
     * 应用场景：在处理 ListOffsets 请求的响应后，需要将 Broker 返回的原始偏移量数据 (`ListOffsetData`)
     *          转换为消费者 API 更易于使用的 `OffsetAndTimestamp` 对象，并与请求的分区对应起来。
     * 实现细节：
     * 1. 初始化一个结果 Map `offsetsResults`，其大小与请求的分区数相同，并将所有分区的值初始化为 `null`。
     *    这样做是为了确保即使某些分区没有成功获取到偏移量，结果中也会包含这些分区的条目（值为 `null`）。
     * 2. 遍历成功获取到的偏移量数据 `fetchedOffsets`。
     * 3. 对于每个成功获取到偏移量的分区，使用提供的 `resultMapper` 函数将 `ListOffsetData` 转换为 `OffsetAndTimestamp`。
     * 4. 将转换后的结果更新到 `offsetsResults` 中对应分区的条目。
     * 设计考虑：此方法提供了一个通用的构建 ListOffsets 结果的框架，通过 `resultMapper` 函数的参数化，
     *          可以灵活地处理不同类型的 ListOffsets 请求（例如，按时间戳查找 vs. 获取最早/最新偏移量）。
     *          预先用 `null`填充结果确保了结果的完整性。
     *
     * @param timestampsToSearch 一个映射，键是请求查找偏移量的 {@link TopicPartition}，值是对应的时间戳。
     * @param fetchedOffsets 一个映射，键是成功获取到偏移量的 {@link TopicPartition}，值是对应的 {@link ListOffsetData}。
     * @param resultMapper 一个双参数函数，用于将 {@link TopicPartition} 和 {@link ListOffsetData} 转换为 {@link OffsetAndTimestamp}。
     * @return 一个映射，键是 {@link TopicPartition}，值是对应的 {@link OffsetAndTimestamp}；如果某个分区未找到偏移量，则值为 `null`。
     */
    static Map<TopicPartition, OffsetAndTimestamp> buildListOffsetsResult(
        final Map<TopicPartition, Long> timestampsToSearch, // 需要搜索时间戳的分区映射
        final Map<TopicPartition, ListOffsetData> fetchedOffsets, // 已获取的偏移量数据
        BiFunction<TopicPartition, ListOffsetData, OffsetAndTimestamp> resultMapper) { // 结果映射函数

        // 初始化结果 Map，大小为搜索时间戳的分区数量
        HashMap<TopicPartition, OffsetAndTimestamp> offsetsResults = new HashMap<>(timestampsToSearch.size());
        // 遍历所有请求搜索的分区，并将它们在结果 Map 中的初始值设为 null
        // 这是为了确保即使某些分区没有成功获取偏移量，结果中也会有它们的条目
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet())
            offsetsResults.put(entry.getKey(), null);

        // 遍历已成功获取到的偏移量数据
        for (Map.Entry<TopicPartition, ListOffsetData> entry : fetchedOffsets.entrySet()) {
            // 获取当前分区的 ListOffsetData
            ListOffsetData offsetData = entry.getValue();
            // 使用提供的 resultMapper 函数，将 TopicPartition 和 ListOffsetData 转换为 OffsetAndTimestamp，并更新到结果 Map 中
            offsetsResults.put(entry.getKey(), resultMapper.apply(entry.getKey(), offsetData));
        }
        // 返回构建好的结果 Map
        return offsetsResults;
    }

    /**
     * 为按时间戳查找偏移量（OffsetsForTimes）的场景构建结果。
     * 这是 {@link #buildListOffsetsResult} 的一个特化版本。
     * 应用场景：当消费者调用 `offsetsForTimes()` API 时，此方法用于将底层的 ListOffsets 请求结果
     *          （以 `ListOffsetData` 形式表示）转换为 API 返回的 `OffsetAndTimestamp` 对象。
     * 实现细节：直接调用 {@link #buildListOffsetsResult} 方法，并提供一个特定的 `resultMapper` 函数。
     *          这个 `resultMapper` 函数简单地从 `ListOffsetData` 中提取 `offset`、`timestamp` 和 `leaderEpoch`
     *          来创建一个新的 `OffsetAndTimestamp` 对象。
     * 设计考虑：通过复用 `buildListOffsetsResult` 并提供一个具体的映射逻辑，简化了代码，
     *          并保持了与通用构建逻辑的一致性。
     *
     * @param timestampsToSearch 一个映射，键是请求查找偏移量的 {@link TopicPartition}，值是对应的时间戳。
     * @param fetchedOffsets 一个映射，键是成功获取到偏移量的 {@link TopicPartition}，值是对应的 {@link ListOffsetData}。
     * @return 一个映射，键是 {@link TopicPartition}，值是对应的 {@link OffsetAndTimestamp}；如果某个分区未找到偏移量，则值为 `null`。
     */
    static Map<TopicPartition, OffsetAndTimestamp> buildOffsetsForTimesResult(
        final Map<TopicPartition, Long> timestampsToSearch, // 需要搜索时间戳的分区映射
        final Map<TopicPartition, ListOffsetData> fetchedOffsets) { // 已获取的偏移量数据
        // 调用通用的 buildListOffsetsResult 方法
        return buildListOffsetsResult(timestampsToSearch, fetchedOffsets,
            // 提供一个具体的 resultMapper 函数，用于将 ListOffsetData 转换为 OffsetAndTimestamp
            (topicPartition, offsetData) -> new OffsetAndTimestamp(
                offsetData.offset, // 偏移量
                offsetData.timestamp, // 时间戳
                offsetData.leaderEpoch)); // Leader Epoch
    }

    /**
     * 根据搜索的时间戳和获取到的偏移量数据构建内部使用的 OffsetAndTimestampInternal 结果映射。
     * 应用场景：当消费者通过 ListOffsets 请求根据时间戳查询偏移量后，此方法用于将查询结果和原始请求的时间戳关联起来，
     *          形成一个包含每个分区及其对应偏移量和时间戳（如果找到）的映射。
     * 设计考虑：此方法将原始的 ListOffsets 响应数据转换为更易于内部使用的 OffsetAndTimestampInternal 格式，
     *          并确保即使某些分区没有找到对应的偏移量，结果映射中也会包含这些分区（值为 null）。
     *
     * @param timestampsToSearch 一个映射，键是 TopicPartition，值是用于搜索偏移量的时间戳。
     * @param fetchedOffsets 一个映射，键是 TopicPartition，值是从 Broker 获取到的 ListOffsetData（包含偏移量、时间戳和 leader epoch）。
     * @return 一个映射，键是 TopicPartition，值是 OffsetAndTimestampInternal 对象，如果某个分区没有找到偏移量，则对应的值为 null。
     */
    static Map<TopicPartition, OffsetAndTimestampInternal> buildOffsetsForTimeInternalResult(
            final Map<TopicPartition, Long> timestampsToSearch, // 需要搜索时间戳的分区到时间戳的映射
            final Map<TopicPartition, ListOffsetData> fetchedOffsets) { // 从 Broker 获取到的分区到偏移量数据的映射
        // 初始化结果映射，大小与请求搜索的分区数量相同
        HashMap<TopicPartition, OffsetAndTimestampInternal> offsetsResults = new HashMap<>(timestampsToSearch.size());
        // 遍历所有请求搜索时间戳的分区
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            // 对于每个请求的分区，首先在结果映射中放入一个 null 值，表示尚未找到或可能找不到偏移量
            offsetsResults.put(entry.getKey(), null);
        }
        // 遍历从 Broker 获取到的所有分区的偏移量数据
        for (Map.Entry<TopicPartition, ListOffsetData> entry : fetchedOffsets.entrySet()) {
            // 获取当前分区的 ListOffsetData
            ListOffsetData offsetData = entry.getValue();
            // 使用获取到的偏移量、时间戳和 leader epoch 创建一个新的 OffsetAndTimestampInternal 对象
            // 并将其放入结果映射中，替换之前可能存在的 null 值
            offsetsResults.put(entry.getKey(), new OffsetAndTimestampInternal(
                    offsetData.offset, // 偏移量
                    offsetData.timestamp, // 时间戳
                    offsetData.leaderEpoch)); // Leader Epoch
        }
        // 返回构建好的结果映射
        return offsetsResults;
    }

    /**
     * 获取指定分区的偏移量重置策略，并确保该策略包含有效的时间戳。
     * 应用场景：当需要根据特定时间戳重置分区的消费位置时，此方法用于获取配置的重置策略，
     *          并验证该策略是否确实提供了时间戳。如果策略不提供时间戳（例如，策略是 LATEST 或 EARLIEST，
     *          但当前场景需要基于时间戳的重置），则会抛出异常。
     * 设计考虑：此方法强制要求重置策略必须提供时间戳，以防止在需要时间戳的场景下错误地使用了不提供时间戳的策略。
     *
     * @param partition 需要获取重置策略并验证时间戳的主题分区。
     * @return 如果分区的重置策略包含有效的时间戳，则返回该 AutoOffsetResetStrategy。
     * @throws NoOffsetForPartitionException 如果分区的重置策略不包含时间戳。
     */
    private AutoOffsetResetStrategy offsetResetStrategyWithValidTimestamp(final TopicPartition partition) {
        // 从订阅状态中获取指定分区的重置策略
        AutoOffsetResetStrategy strategy = subscriptionState.resetStrategy(partition);
        // 检查策略是否包含时间戳
        if (strategy.timestamp().isPresent()) {
            // 如果策略包含时间戳，则返回该策略
            return strategy;
        } else {
            // 如果策略不包含时间戳，则抛出 NoOffsetForPartitionException 异常
            throw new NoOffsetForPartitionException(partition);
        }
    }

    /**
     * 从给定的 TopicPartition 集合中提取所有唯一的主题名称。
     * 应用场景：当需要知道一组分区涉及到哪些主题时，例如在请求元数据更新或进行日志记录时，可以使用此方法。
     * 设计考虑：使用 Java Stream API 可以简洁高效地完成此操作，通过 map 和 collect(Collectors.toSet()) 自动处理了去重。
     *
     * @param partitions TopicPartition 的集合。
     * @return 包含所有唯一主题名称的集合 (Set)。
     */
    static Set<String> topicsForPartitions(Collection<TopicPartition> partitions) {
        // 使用流处理 partitions 集合
        return partitions.stream()
                // 将每个 TopicPartition 对象映射为其主题名称 (String)
                .map(TopicPartition::topic)
                // 将所有主题名称收集到一个 Set 中，自动去重
                .collect(Collectors.toSet());
    }

    /**
     * 根据获取到的偏移量数据和隔离级别更新订阅状态中的 LSO (Last Stable Offset) 或 HW (High Watermark)。
     * 应用场景：当通过 ListOffsets 请求获取到分区的 LSO 或 HW 后，此方法用于将这些信息更新到消费者的内部订阅状态中。
     *          这对于消费者了解可安全读取到的消息位置非常重要，特别是在事务性消费或读取已提交消息的场景下。
     * 设计考虑：此方法根据隔离级别区分更新 LSO 还是 HW。如果隔离级别是 READ_COMMITTED，则更新 LSO；
     *          否则（通常是 READ_UNCOMMITTED），更新 HW。只对消费者已分配的分区进行更新。
     *
     * @param fetchedOffsets 一个映射，键是 TopicPartition，值是从 Broker 获取到的 ListOffsetData。
     * @param isolationLevel 当前消费者的隔离级别。
     */
    void updateSubscriptionState(Map<TopicPartition, OffsetFetcherUtils.ListOffsetData> fetchedOffsets, // 获取到的偏移量数据
                                 IsolationLevel isolationLevel) { // 隔离级别
        // 遍历所有获取到偏移量的分区
        for (final Map.Entry<TopicPartition, ListOffsetData> entry : fetchedOffsets.entrySet()) {
            // 获取当前处理的分区
            final TopicPartition partition = entry.getKey();

            // 如果感兴趣的分区是订阅的一部分，则使用返回的偏移量也更新订阅状态：
            //   * 对于 read-committed，返回的偏移量将是 LSO；
            //   * 对于 read-uncommitted，返回的偏移量将是 HW；
            // 检查当前分区是否是消费者已分配的分区
            if (subscriptionState.isAssigned(partition)) {
                // 获取该分区的偏移量值
                final long offset = entry.getValue().offset;
                // 根据隔离级别进行不同的处理
                if (isolationLevel == IsolationLevel.READ_COMMITTED) {
                    // 如果隔离级别是 READ_COMMITTED
                    // 记录追踪日志，说明正在更新分区的 LSO
                    log.trace("Updating last stable offset for partition {} to {}", partition, offset);
                    // 更新订阅状态中该分区的 LSO
                    subscriptionState.updateLastStableOffset(partition, offset);
                } else {
                    // 如果隔离级别不是 READ_COMMITTED (即 READ_UNCOMMITTED)
                    // 记录追踪日志，说明正在更新分区的高水位标记 (HW)
                    log.trace("Updating high watermark for partition {} to {}", partition, offset);
                    // 更新订阅状态中该分区的高水位标记 (HW)
                    subscriptionState.updateHighWatermark(partition, offset);
                }
            }
        }
    }

    /**
     * 处理用于重置位置的 ListOffsets 请求成功响应。
     * 应用场景：当消费者发送 ListOffsets 请求以重置某些分区的消费位置（例如，根据最早、最新或特定时间戳）并收到成功响应后，
     *          此方法被调用来处理响应结果。它会根据获取到的偏移量数据和每个分区配置的重置策略来实际更新消费位置。
     *          如果部分分区需要重试，则会标记这些分区并在下次请求元数据更新。
     * 设计考虑：此方法将成功响应的处理逻辑封装起来。对于成功获取到偏移量的分区，会调用 `resetPositionIfNeeded` 来更新位置；
     *          对于需要重试的分区，会更新其状态并请求元数据更新，以便后续重试。
     *
     * @param result ListOffsets 请求的结果，包含成功获取的偏移量和需要重试的分区。
     * @param partitionAutoOffsetResetStrategyMap 一个映射，键是 TopicPartition，值是该分区对应的自动偏移量重置策略。
     */
    void onSuccessfulResponseForResettingPositions(
            final ListOffsetResult result, // ListOffsets 请求的结果
            final Map<TopicPartition, AutoOffsetResetStrategy> partitionAutoOffsetResetStrategyMap) { // 分区到自动偏移量重置策略的映射
        // 检查结果中是否有需要重试的分区
        if (!result.partitionsToRetry.isEmpty()) {
            // 如果有需要重试的分区，则在订阅状态中标记这些分区请求失败，并设置下次重试时间
            subscriptionState.requestFailed(result.partitionsToRetry, time.milliseconds() + retryBackoffMs);
            // 请求元数据更新，因为分区 Leader 可能发生变化或其他原因导致需要重试
            metadata.requestUpdate(false);
        }

        // 遍历所有成功获取到偏移量的分区
        for (Map.Entry<TopicPartition, ListOffsetData> fetchedOffset : result.fetchedOffsets.entrySet()) {
            // 获取当前处理的分区
            TopicPartition partition = fetchedOffset.getKey();
            // 获取该分区的偏移量数据
            ListOffsetData offsetData = fetchedOffset.getValue();
            // 根据获取到的偏移量数据和该分区的重置策略，按需重置消费位置
            resetPositionIfNeeded(
                    partition, // 当前分区
                    partitionAutoOffsetResetStrategyMap.get(partition), // 该分区的自动偏移量重置策略
                    offsetData); // 获取到的偏移量数据
        }
    }

    /**
     * 处理用于重置位置的 ListOffsets 请求失败响应。
     * 应用场景：当消费者发送 ListOffsets 请求以重置某些分区的消费位置，但请求失败（例如，网络超时、Broker 错误）时，
     *          此方法被调用来处理失败情况。它会将所有请求的分区标记为失败，并请求元数据更新以便后续重试。
     *          如果错误不是可重试的，并且当前没有缓存其他重置位置的错误，则会缓存此错误。
     * 设计考虑：此方法统一处理 ListOffsets 请求失败的情况。所有相关的分区都会被标记为需要重试。
     *          对于不可重试的错误，会尝试缓存它，以便在后续操作中抛出，但如果已有缓存的错误，则会丢弃当前错误并记录日志。
     *
     * @param resetTimestamps 一个映射，键是 TopicPartition，值是该分区请求重置时使用的 ListOffsetsPartition 数据（通常包含时间戳）。
     * @param error 导致请求失败的运行时异常。
     */
    void onFailedResponseForResettingPositions(
            final Map<TopicPartition, ListOffsetsRequestData.ListOffsetsPartition> resetTimestamps, // 请求重置时间戳的分区映射
            final RuntimeException error) { // 发生的运行时异常
        // 将所有请求重置时间戳的分区标记为请求失败，并设置下次重试时间
        subscriptionState.requestFailed(resetTimestamps.keySet(), time.milliseconds() + retryBackoffMs);
        // 请求元数据更新
        metadata.requestUpdate(false);

        // 检查错误是否不是可重试异常，并且 cachedResetPositionsException 当前为 null (即没有缓存其他错误)
        // compareAndSet 会原子地设置值，如果当前值为 null，则设置为 error，并返回 true；否则返回 false。
        if (!(error instanceof RetriableException) && !cachedResetPositionsException.compareAndSet(null,
                error))
            // 如果错误不可重试，但已经有一个待处理的错误被缓存，则丢弃当前错误并记录一条错误日志
            log.error("Discarding error resetting positions because another error is pending",
                    error);
    }


    /**
     * 处理用于验证位置的 OffsetsForLeaderEpoch 请求的成功响应。
     * 应用场景：当消费者需要验证其当前消费位置是否仍然有效（例如，在重新平衡后或检测到可能的日志截断时），
     *          会发送 OffsetsForLeaderEpoch 请求。此方法在收到成功响应后被调用，用于检查是否存在日志截断，
     *          并相应地更新分区的状态或准备抛出 LogTruncationException。
     * 设计考虑：此方法的核心逻辑是遍历响应中的每个分区，并调用 `subscriptionState.maybeCompleteValidation`
     *          来完成验证过程。如果检测到任何日志截断，会收集这些截断信息并构建一个 LogTruncationException，
     *          然后通过 `maybeSetValidatePositionsException` 缓存该异常，以便在后续操作中抛出。
     *          对于需要重试的分区，会更新其状态并请求元数据更新。
     *
     * @param fetchPositions 一个映射，键是 TopicPartition，值是该分区在发送请求时的 FetchPosition（包含当前偏移量和 epoch）。
     * @param offsetsResult OffsetsForLeaderEpoch 请求的结果，包含每个分区的末端偏移量 (end offset) 和需要重试的分区。
     */
    void onSuccessfulResponseForValidatingPositions(
            final Map<TopicPartition, SubscriptionState.FetchPosition> fetchPositions, // 分区到其拉取位置的映射
            final OffsetsForLeaderEpochUtils.OffsetForEpochResult offsetsResult) { // OffsetsForLeaderEpoch 请求的结果
        // 创建一个列表用于存储检测到的日志截断信息
        List<SubscriptionState.LogTruncation> truncations = new ArrayList<>();
        // 检查结果中是否有需要重试的分区
        if (!offsetsResult.partitionsToRetry().isEmpty()) {
            // 如果有需要重试的分区，则在订阅状态中设置这些分区的下次允许重试时间
            subscriptionState.setNextAllowedRetry(offsetsResult.partitionsToRetry(),
                    time.milliseconds() + retryBackoffMs);
            // 请求元数据更新
            metadata.requestUpdate(false);
        }

        // 对于每个 OffsetsForLeader 响应，检查分区的末端偏移量是否低于我们当前的偏移量。
        // 如果是，则意味着我们经历了日志截断，需要重新定位该分区的偏移量。
        // 此外，检查返回的偏移量和 epoch 是否有效。如果无效，则如果配置了重置策略，我们应该重置其偏移量，
        // 否则抛出范围超出异常。
        // 遍历 OffsetsForLeaderEpoch 响应中返回的每个分区的末端偏移量
        offsetsResult.endOffsets().forEach((topicPartition, respEndOffset) -> {
            // 获取该分区在请求时的 FetchPosition
            SubscriptionState.FetchPosition requestPosition = fetchPositions.get(topicPartition);
            // 调用 subscriptionState 的方法来尝试完成验证，这会检查日志截断等情况
            Optional<SubscriptionState.LogTruncation> truncationOpt =
                    subscriptionState.maybeCompleteValidation(topicPartition, requestPosition,
                            respEndOffset);
            // 如果检测到日志截断，则将其添加到 truncations 列表中
            truncationOpt.ifPresent(truncations::add);
        });

        // 如果检测到了任何日志截断
        if (!truncations.isEmpty()) {
            // 构建一个 LogTruncationException 并尝试设置到 cachedValidatePositionsException
            maybeSetValidatePositionsException(buildLogTruncationException(truncations));
        }
    }

    /**
     * 当验证位置的请求收到失败响应时的处理方法。
     * 应用场景：当消费者尝试验证其当前消费位置（例如，通过 OffsetsForLeaderEpoch 请求）但请求失败时，此方法被调用。
     * 设计考虑：此方法统一处理验证位置失败的情况，包括标记相关分区需要重试，请求元数据更新，以及在错误不可重试时缓存异常。
     * @param fetchPositions 验证失败的分区及其对应的拉取位置信息。
     * @param error 导致请求失败的运行时异常。
     */
    void onFailedResponseForValidatingPositions(final Map<TopicPartition, SubscriptionState.FetchPosition> fetchPositions,
                                                final RuntimeException error) {
        // 标记这些分区的请求失败，并设置下次重试的时间为当前时间加上重试退避间隔
        subscriptionState.requestFailed(fetchPositions.keySet(), time.milliseconds() + retryBackoffMs);
        // 请求更新元数据，参数 false 表示非阻塞更新
        metadata.requestUpdate(false);

        // 如果错误不是可重试异常 (RetriableException)
        if (!(error instanceof RetriableException)) {
            // 则将此错误设置为验证位置时发生的异常，以便后续处理
            maybeSetValidatePositionsException(error);
        }
    }

    /**
     * 构建一个 {@link LogTruncationException} 异常实例。
     * 应用场景：当检测到日志被截断（即消费者当前的消费位置指向了一个不再存在的偏移量）时，此方法用于创建一个具体的异常对象，
     *          其中包含了关于截断的详细信息，如分歧点偏移量和被截断的拉取偏移量。
     * 设计考虑：将构建 LogTruncationException 的逻辑封装在此方法中，使得代码更清晰，并集中处理异常信息的收集。
     * @param truncations 检测到的日志截断信息列表。
     * @return 构建好的 {@link LogTruncationException} 实例。
     */
    private LogTruncationException buildLogTruncationException(List<SubscriptionState.LogTruncation> truncations) {
        // 创建一个 Map 用于存储分歧点的偏移量和元数据，键为 TopicPartition
        Map<TopicPartition, OffsetAndMetadata> divergentOffsets = new HashMap<>();
        // 创建一个 Map 用于存储被截断的拉取偏移量，键为 TopicPartition，值为偏移量
        Map<TopicPartition, Long> truncatedFetchOffsets = new HashMap<>();
        // 遍历所有的日志截断信息
        for (SubscriptionState.LogTruncation truncation : truncations) {
            // 如果存在分歧点偏移量 (divergentOffsetOpt)
            truncation.divergentOffsetOpt.ifPresent(divergentOffset ->
                    // 则将其添加到 divergentOffsets 映射中
                    divergentOffsets.put(truncation.topicPartition, divergentOffset));
            // 将被截断的拉取偏移量添加到 truncatedFetchOffsets 映射中
            truncatedFetchOffsets.put(truncation.topicPartition, truncation.fetchPosition.offset);
        }
        // 使用收集到的信息创建并返回一个新的 LogTruncationException 实例
        return new LogTruncationException(truncatedFetchOffsets, divergentOffsets);
    }

    // 仅用于测试，因此可见性为包级私有
    /**
     * 如果需要，重置指定分区的消费位置。
     * 应用场景：当消费者需要根据指定的重置策略（例如，最早、最新或特定时间戳）来更新其在某个分区上的消费位置时，此方法被调用。
     *          通常在 ListOffsets 请求成功获取到目标偏移量后使用。
     * 设计考虑：此方法封装了重置消费位置的核心逻辑，包括创建新的 FetchPosition 对象（跳过验证），
     *          更新元数据中的 lastSeenEpoch，以及最终调用 SubscriptionState 的方法来实际更新位置。
     * @param partition 需要重置位置的主题分区。
     * @param requestedResetStrategy 请求的自动偏移量重置策略。
     * @param offsetData 从 ListOffsets 请求获取到的偏移量数据，包含偏移量和可能的 leader epoch。
     */
    void resetPositionIfNeeded(TopicPartition partition, AutoOffsetResetStrategy requestedResetStrategy,
                               ListOffsetData offsetData) {
        // 根据获取到的偏移量数据创建一个新的 FetchPosition 对象
        SubscriptionState.FetchPosition position = new SubscriptionState.FetchPosition(
                offsetData.offset, // 使用获取到的偏移量
                Optional.empty(), // 设置 leader epoch 为空，这将确保跳过位置验证
                metadata.currentLeader(partition)); // 获取当前分区的 leader 信息
        // 如果 offsetData 中包含 leaderEpoch
        offsetData.leaderEpoch.ifPresent(epoch -> 
                // 则更新元数据中该分区的 lastSeenEpoch（如果新的 epoch 更大）
                metadata.updateLastSeenEpochIfNewer(partition, epoch));
        // 尝试使用新的位置和请求的重置策略来更新订阅状态中该分区的消费位置（不进行验证）
        subscriptionState.maybeSeekUnvalidated(partition, position, requestedResetStrategy);
    }

    /**
     * 将拉取位置按其当前的 leader 节点进行重新分组。
     * 应用场景：在发送需要按 leader 节点组织的请求（如 OffsetsForLeaderEpoch 请求）之前，
     *          此方法用于将按 TopicPartition 组织的位置信息转换为按 Node 组织的结构。
     * 设计考虑：使用 Java Stream API 进行转换，代码简洁且易于理解。过滤掉没有 leader 的分区，确保只处理有效的分区。
     * @param partitionMap 一个映射，键为 {@link TopicPartition}，值为其对应的 {@link SubscriptionState.FetchPosition}。
     * @return 一个映射，键为 {@link Node} (leader 节点)，值为另一个映射，
     *         该内部映射的键为 {@link TopicPartition}，值为其对应的 {@link SubscriptionState.FetchPosition}。
     */
    static Map<Node, Map<TopicPartition, SubscriptionState.FetchPosition>> regroupFetchPositionsByLeader(
            Map<TopicPartition, SubscriptionState.FetchPosition> partitionMap) {
        // 将 partitionMap 的条目集转换为流
        return partitionMap.entrySet()
                .stream()
                // 过滤掉那些 FetchPosition 中 currentLeader 的 leader 为空（即没有 leader）的条目
                .filter(entry -> entry.getValue().currentLeader.leader.isPresent())
                // 按照 FetchPosition 中的 leader 节点进行分组
                .collect(Collectors.groupingBy(entry -> entry.getValue().currentLeader.leader.get(),
                        // 对于每个分组（即每个 leader 节点），将原始条目的键（TopicPartition）和值（FetchPosition）收集到一个新的 Map 中
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * 检查节点是否支持可用的 OffsetForLeaderEpoch API 版本，该版本支持主题权限。
     * 应用场景：在决定是否可以向某个 Broker 节点发送 OffsetForLeaderEpoch 请求（特别是需要考虑主题权限的版本）时，
     *          会调用此方法来检查节点的 API 版本兼容性。
     * 设计考虑：此方法封装了检查 API 版本兼容性的逻辑，使得调用方无需关心具体的版本号判断细节。
     * @param nodeApiVersions 目标节点的 API 版本信息。
     * @return 如果节点支持可用的、支持主题权限的 OffsetForLeaderEpoch API 版本，则返回 true；否则返回 false。
     */
    static boolean hasUsableOffsetForLeaderEpochVersion(NodeApiVersions nodeApiVersions) {
        // 获取节点支持的 OFFSET_FOR_LEADER_EPOCH API 的版本信息
        ApiVersionsResponseData.ApiVersion apiVersion = nodeApiVersions.apiVersion(ApiKeys.OFFSET_FOR_LEADER_EPOCH);
        // 如果节点不支持 OFFSET_FOR_LEADER_EPOCH API (apiVersion 为 null)
        if (apiVersion == null)
            // 则返回 false
            return false;

        // 检查节点支持的 OFFSET_FOR_LEADER_EPOCH API 的最大版本是否支持主题权限
        return OffsetsForLeaderEpochRequest.supportsTopicPermission(apiVersion.maxVersion());
    }

    /**
     * 表示 ListOffsets 请求的结果的静态内部类。
     * 应用场景：当 {@link OffsetFetcherUtils#handleListOffsetResponse(ListOffsetsResponse)} 方法处理完 Broker 返回的 ListOffsets 响应后，
     *          会创建一个此类的实例来封装成功获取到的偏移量信息以及需要重试的分区列表。
     * 设计考虑：将 ListOffsets 请求的结果组织成一个专门的类，可以清晰地分离成功数据和失败/重试信息，方便调用方处理。
     */
    static class ListOffsetResult {
        // 存储成功获取到的偏移量数据，键为 TopicPartition，值为 ListOffsetData
        final Map<TopicPartition, OffsetFetcherUtils.ListOffsetData> fetchedOffsets;
        // 存储需要重试的分区集合
        final Set<TopicPartition> partitionsToRetry;

        /**
         * ListOffsetResult 的构造函数。
         * @param fetchedOffsets 成功获取到的偏移量数据。
         * @param partitionsNeedingRetry 需要重试的分区集合。
         */
        ListOffsetResult(Map<TopicPartition, OffsetFetcherUtils.ListOffsetData> fetchedOffsets,
                         Set<TopicPartition> partitionsNeedingRetry) {
            // 初始化成功获取到的偏移量数据
            this.fetchedOffsets = fetchedOffsets;
            // 初始化需要重试的分区集合
            this.partitionsToRetry = partitionsNeedingRetry;
        }

        /**
         * ListOffsetResult 的无参构造函数，用于创建一个空的结果对象。
         */
        ListOffsetResult() {
            // 初始化一个空的 HashMap 用于存储偏移量数据
            this.fetchedOffsets = new HashMap<>();
            // 初始化一个空的 HashSet 用于存储需要重试的分区
            this.partitionsToRetry = new HashSet<>();
        }
    }

    /**
     * 表示由 Broker 返回的关于偏移量的数据的静态内部类。
     * 应用场景：当 ListOffsets 请求成功获取到某个分区的偏移量信息时，这些信息（偏移量、时间戳、leader epoch）会被封装到此类的实例中。
     * 设计考虑：将单个分区的偏移量相关数据聚合到一个类中，提高了代码的可读性和组织性。
     *          使用 Optional<Integer> 表示 leaderEpoch，可以清晰地处理 leader epoch 可能不存在的情况。
     */
    static class ListOffsetData {
        // 获取到的偏移量值
        final long offset;
        // 获取到的时间戳；如果 Broker 不支持返回时间戳，则为 null
        final Long timestamp; // 如果 broker 不支持返回时间戳，则为 null
        // 获取到的 leader epoch；如果 leader epoch 未知，则为空 Optional
        final Optional<Integer> leaderEpoch; // 如果 leader epoch 未知，则为空

        /**
         * ListOffsetData 的构造函数。
         * @param offset 偏移量值。
         * @param timestamp 时间戳；如果 Broker 不支持，则为 null。
         * @param leaderEpoch leader epoch；如果未知，则为空 Optional。
         */
        ListOffsetData(long offset, Long timestamp, Optional<Integer> leaderEpoch) {
            // 初始化偏移量
            this.offset = offset;
            // 初始化时间戳
            this.timestamp = timestamp;
            // 初始化 leader epoch
            this.leaderEpoch = leaderEpoch;
        }
    }
}
