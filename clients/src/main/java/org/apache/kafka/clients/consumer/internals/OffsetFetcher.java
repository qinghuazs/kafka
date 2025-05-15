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
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetData;
import org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.ListOffsetResult;
import org.apache.kafka.clients.consumer.internals.OffsetsForLeaderEpochUtils.OffsetForEpochResult;
import org.apache.kafka.clients.consumer.internals.SubscriptionState.FetchPosition;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.buildOffsetsForTimesResult;
import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.hasUsableOffsetForLeaderEpochVersion;
import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.regroupFetchPositionsByLeader;
import static org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils.topicsForPartitions;

/**
 * {@link OffsetFetcher} 负责为一组给定的 {@link TopicPartition 主题和分区对} 获取 {@link OffsetAndTimestamp偏移量和时间戳}，
 * 并在需要时进行位置的验证和重置。
 */
public class OffsetFetcher {

    // 日志记录器
    private final Logger log;
    // 消费者元数据，用于获取集群信息，如leader节点等
    private final ConsumerMetadata metadata;
    // 订阅状态，维护消费者订阅的主题和分区信息，以及它们的消费位置
    private final SubscriptionState subscriptions;
    // 消费者网络客户端，用于向Kafka broker发送请求
    private final ConsumerNetworkClient client;
    // 时间工具类，用于获取当前时间等
    private final Time time;
    // 请求超时时间（毫秒）
    private final int requestTimeoutMs;
    // 事务隔离级别
    private final IsolationLevel isolationLevel;
    // 用于根据leader epoch获取偏移量的客户端
    private final OffsetsForLeaderEpochClient offsetsForLeaderEpochClient;
    // API版本信息，用于确定与broker通信时使用的API版本
    private final ApiVersions apiVersions;
    // OffsetFetcher工具类，提供一些辅助方法
    private final OffsetFetcherUtils offsetFetcherUtils;


    /**
     * OffsetFetcher的构造函数。
     *
     * @param logContext 日志上下文，用于创建日志记录器
     * @param client 消费者网络客户端，用于与Kafka broker通信
     * @param metadata 消费者元数据，包含集群和主题分区的信息
     * @param subscriptions 订阅状态，维护消费者订阅信息和消费位置
     * @param time 时间工具类
     * @param retryBackoffMs 重试退避时间（毫秒）
     * @param requestTimeoutMs 请求超时时间（毫秒）
     * @param isolationLevel 事务隔离级别
     * @param apiVersions API版本信息
     */
    public OffsetFetcher(LogContext logContext,
                         ConsumerNetworkClient client,
                         ConsumerMetadata metadata,
                         SubscriptionState subscriptions,
                         Time time,
                         long retryBackoffMs,
                         int requestTimeoutMs,
                         IsolationLevel isolationLevel,
                         ApiVersions apiVersions) {
        // 初始化日志记录器
        this.log = logContext.logger(getClass());
        // 初始化时间工具类
        this.time = time;
        // 初始化消费者网络客户端
        this.client = client;
        // 初始化消费者元数据
        this.metadata = metadata;
        // 初始化订阅状态
        this.subscriptions = subscriptions;
        // 初始化请求超时时间
        this.requestTimeoutMs = requestTimeoutMs;
        // 初始化事务隔离级别
        this.isolationLevel = isolationLevel;
        // 初始化API版本信息
        this.apiVersions = apiVersions;
        // 初始化OffsetsForLeaderEpochClient，用于根据leader epoch获取偏移量
        this.offsetsForLeaderEpochClient = new OffsetsForLeaderEpochClient(client, logContext);
        // 初始化OffsetFetcherUtils，提供偏移量获取相关的辅助功能
        this.offsetFetcherUtils = new OffsetFetcherUtils(logContext, metadata, subscriptions,
                time, retryBackoffMs, apiVersions);
    }

    /**
     * 为所有需要重置偏移量的已分配分区重置偏移量。
     * 应用场景：当消费者启动时，如果某些分区没有有效的已提交偏移量，或者根据配置的重置策略（如earliest, latest）需要重置时调用。
     * 设计考虑：此方法首先检查哪些分区需要重置，然后异步地为这些分区执行重置操作，以避免阻塞主消费线程。
     *
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException 如果没有定义偏移量重置策略，
     *                                                                         并且一个或多个分区没有等待 seekToBeginning() 或 seekToEnd()。
     */
    public void resetPositionsIfNeeded() {
        // 获取需要重置偏移量的分区及其对应的自动偏移量重置策略
        Map<TopicPartition, AutoOffsetResetStrategy> partitionAutoOffsetResetStrategyMap =
                offsetFetcherUtils.getOffsetResetStrategyForPartitions();

        // 如果没有分区需要重置偏移量，则直接返回
        if (partitionAutoOffsetResetStrategyMap.isEmpty())
            return;

        // 异步重置这些分区的偏移量
        resetPositionsAsync(partitionAutoOffsetResetStrategyMap);
    }

    /**
     * 为所有检测到leader发生变化的已分配分区验证偏移量。
     * 应用场景：在消费者感知到分区leader发生切换后，需要验证当前持有的消费位置是否仍然有效，以防止数据丢失或重复消费。
     * 设计考虑：此方法识别出需要验证的分区，并异步地执行验证操作。验证通常涉及到获取分区的leader epoch对应的偏移量。
     */
    public void validatePositionsIfNeeded() {
        // 获取需要验证偏移量的分区及其当前的拉取位置信息
        Map<TopicPartition, SubscriptionState.FetchPosition> partitionsToValidate =
                offsetFetcherUtils.getPartitionsToValidate();

        // 异步验证这些分区的偏移量
        validatePositionsAsync(partitionsToValidate);
    }

    /**
     * 根据给定的时间戳查找对应分区的偏移量和时间戳。
     * 应用场景：用户可能希望从某个特定的时间点开始消费消息，此方法允许根据时间戳定位到相应的偏移量。
     * 设计考虑：该方法会向broker发送ListOffsets请求。为了处理元数据可能过时的情况，它会暂时将查询的topic加入到元数据的瞬态主题列表中，
     * 并在操作完成后清除。如果查找超时，会抛出TimeoutException。
     *
     * @param timestampsToSearch 一个映射，键是主题分区，值是要搜索的目标时间戳（毫秒）
     * @param timer 用于控制操作超时的计时器
     * @return 一个映射，键是主题分区，值是找到的偏移量和时间戳 (OffsetAndTimestamp)。如果某个分区找不到对应时间戳的偏移量，则结果中可能不包含该分区，或者对应的值为null。
     */
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                                   Timer timer) {
        // 将待查询偏移量的主题添加到元数据的瞬态主题集合中，确保元数据包含这些主题的信息
        // 这样做是为了在元数据更新时能够包含这些可能尚未被订阅的主题
        metadata.addTransientTopics(topicsForPartitions(timestampsToSearch.keySet()));

        try {
            // 调用内部方法根据时间戳批量获取偏移量数据
            // 第三个参数 true 表示如果broker不支持精确时间戳查找，则应抛出异常
            Map<TopicPartition, ListOffsetData> fetchedOffsets = fetchOffsetsByTimes(timestampsToSearch,
                    timer, true).fetchedOffsets;

            // 将获取到的 ListOffsetData 转换为用户期望的 OffsetAndTimestamp 格式
            return buildOffsetsForTimesResult(timestampsToSearch, fetchedOffsets);
        } finally {
            // 操作完成后，从元数据中清除这些瞬态主题，避免不必要的元数据维护
            metadata.clearTransientTopics();
        }
    }

    /**
     * 根据给定的时间戳批量获取分区的偏移量。
     * 应用场景：当需要根据特定的时间点（例如，从昨天某个时刻开始消费）来查找消息的起始偏移量时使用。
     * 实现细节：
     * 1. 初始化一个空的 ListOffsetResult 用于存储结果。
     * 2. 如果待查询的时间戳映射为空，则直接返回空结果。
     * 3. 创建一个 `remainingToSearch` 映射，用于跟踪还需要查询的分区和时间戳，初始值为传入的 `timestampsToSearch`。
     * 4. 进入一个 do-while 循环，该循环会持续到所有分区的偏移量都被获取，或者计时器超时。
     * 5. 在循环内部，调用 `sendListOffsetsRequests` 方法向 broker 发送 ListOffsets 请求，获取 `RequestFuture` 对象。
     * 6. 为 `future` 添加监听器，处理请求成功和失败的情况：
     *    - onSuccess：同步块内，将成功获取到的偏移量 `value.fetchedOffsets` 添加到 `result.fetchedOffsets` 中。
     *                 然后，更新 `remainingToSearch`，只保留那些需要重试的分区 (`value.partitionsToRetry`)。
     *                 最后，调用 `offsetFetcherUtils.updateSubscriptionState` 更新订阅状态中这些分区的偏移量信息。
     *    - onFailure：如果发生的异常不是可重试异常 (`RetriableException`)，则直接抛出 `future` 中的异常。
     * 7. 检查计时器 `timer` 的超时设置：
     *    - 如果超时时间为 0，表示不应尝试轮询网络客户端，立即返回当前的 `result`（可能为空）。
     *    - 否则，调用 `client.poll(future, timer)` 同步等待请求结果，直到 `future` 完成或 `timer` 超时。
     * 8. 检查 `future` 的状态：
     *    - 如果 `future` 尚未完成（通常意味着 `client.poll` 超时），则跳出循环。
     *    - 如果 `future` 已完成且 `remainingToSearch` 为空（所有分区的偏移量都已获取），则返回 `result`。
     *    - 如果 `future` 已完成但 `remainingToSearch` 不为空（部分分区需要重试），则调用 `client.awaitMetadataUpdate(timer)` 等待元数据更新，然后继续循环。
     * 9. 如果循环结束（通常是因为计时器超时）但仍有未获取偏移量的分区，则抛出 `TimeoutException`。
     * 设计考虑：
     * - 使用 `RequestFuture` 和监听器模式处理异步请求的结果。
     * - 通过 `remainingToSearch` 和循环重试机制来处理部分分区获取失败或需要元数据更新的情况。
     * - 考虑了超时时间为0的特殊情况，避免不必要的网络操作。
     * - 在操作成功后，会更新 `SubscriptionState`，确保消费者内部状态与获取到的偏移量一致。
     *
     * @param timestampsToSearch 一个映射，键是 TopicPartition (主题分区)，值是 Long 类型的目标时间戳 (毫秒)。
     * @param timer Timer 对象，用于控制整个操作的超时。
     * @param requireTimestamps 布尔值，指示如果 broker 不支持精确时间戳查找，是否应抛出异常。
     * @return ListOffsetResult 包含获取到的偏移量信息 (fetchedOffsets) 和需要重试的分区列表 (partitionsToRetry)。
     * @throws TimeoutException 如果在指定时间内未能获取所有请求分区的偏移量。
     */
    private ListOffsetResult fetchOffsetsByTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                 Timer timer,
                                                 boolean requireTimestamps) {
        // 初始化一个 ListOffsetResult 对象，用于存储最终的查询结果
        ListOffsetResult result = new ListOffsetResult();
        // 如果待查询的时间戳映射为空，则无需进行任何操作，直接返回空的 result
        if (timestampsToSearch.isEmpty())
            return result;

        // 创建一个 HashMap 用于存储还需要查询偏移量的分区及其对应的时间戳，初始值为传入的 timestampsToSearch
        Map<TopicPartition, Long> remainingToSearch = new HashMap<>(timestampsToSearch);
        // 进入一个 do-while 循环，该循环会持续执行，直到所有分区的偏移量都被成功获取，或者计时器超时
        do {
            // 调用 sendListOffsetsRequests 方法，向 Kafka broker 发送 ListOffsets 请求
            // 这个方法会为 remainingToSearch 中的每个分区构建请求，并返回一个 RequestFuture 对象，代表异步请求的结果
            RequestFuture<ListOffsetResult> future = sendListOffsetsRequests(remainingToSearch, requireTimestamps);

            // 为 future 添加一个监听器，用于处理请求成功或失败后的回调
            // 为异步请求添加监听器，以处理请求成功或失败的情况
            // 为该异步请求添加监听器，以处理成功或失败的响应
            future.addListener(new RequestFutureListener<>() {
                @Override
                public void onSuccess(ListOffsetResult value) {
                    // 当请求成功时执行此方法
                    // 使用 synchronized 关键字确保对 future 对象的同步访问，避免并发问题
                    synchronized (future) {
                        // 将成功获取到的偏移量 (value.fetchedOffsets) 添加到最终结果 result.fetchedOffsets 中
                        result.fetchedOffsets.putAll(value.fetchedOffsets);
                        // 更新 remainingToSearch 集合，只保留那些在本次请求中需要重试的分区 (value.partitionsToRetry)
                        // retainAll 方法会移除 remainingToSearch.keySet() 中不包含在 value.partitionsToRetry 中的所有元素
                        remainingToSearch.keySet().retainAll(value.partitionsToRetry);

                        // 调用 offsetFetcherUtils 的 updateSubscriptionState 方法，
                        // 使用获取到的偏移量 (value.fetchedOffsets) 和当前的隔离级别 (isolationLevel) 来更新订阅状态
                        offsetFetcherUtils.updateSubscriptionState(value.fetchedOffsets, isolationLevel);
                    }
                }

                // 请求失败时的回调方法
                // 当来自单个节点的 ListOffsets 请求失败时调用
                @Override
                public void onFailure(RuntimeException e) {
                    // 当请求失败时执行此方法
                    // 检查抛出的异常 e 是否是 RetriableException (可重试异常) 的实例
                    if (!(e instanceof RetriableException)) {
                        // 如果不是可重试异常，则直接抛出 future 中封装的原始异常
                        throw future.exception();
                    }
                    // 如果是可重试异常，则不在此处处理，通常会在外层循环中进行重试
                }
            });

            // 如果超时时间 timer.timeoutMs() 设置为 0，表示不应尝试轮询网络客户端，
            // 并且应立即返回当前的 result (可能只包含了部分成功获取的偏移量，或者为空)
            // 否则，尝试同步获取结果，如果无法在规定时间内完成，则抛出超时异常
            if (timer.timeoutMs() == 0L)
                return result;

            // 调用 client.poll 方法，阻塞等待 future 完成，或者直到 timer 超时
            // 这个方法会处理网络通信，发送请求并接收响应
            client.poll(future, timer);

            // 检查 future 是否已经完成 (即请求是否已经收到响应或失败)
            if (!future.isDone()) {
                // 如果 future 尚未完成 (通常意味着 client.poll 因 timer 超时而返回)，则跳出 do-while 循环
                break;
            } else if (remainingToSearch.isEmpty()) {
                // 如果 future 已完成，并且 remainingToSearch 为空 (表示所有分区的偏移量都已成功获取，没有需要重试的了)
                // 则返回最终的 result
                return result;
            // 如果分区的leader已知
            // 如果leader节点可用
                } else {
                // 如果 future 已完成，但 remainingToSearch 不为空 (表示部分分区获取失败，需要重试，或者需要等待元数据更新)
                // 则调用 client.awaitMetadataUpdate(timer) 方法，阻塞等待 Kafka 集群元数据的更新，或者直到 timer 超时
                // 元数据更新后，可能会有新的 leader 信息，有助于下一次重试成功
                client.awaitMetadataUpdate(timer);
            }
        } while (timer.notExpired()); // 循环条件：只要计时器 timer 尚未过期，就继续尝试

        // 如果循环结束 (通常是因为计时器超时)，但仍未能获取所有分区的偏移量，
        // 则抛出一个 TimeoutException，提示获取偏移量操作超时
        throw new TimeoutException("Failed to get offsets by times in " + timer.elapsedMs() + "ms");
    }

    /**
     * 获取指定分区的起始偏移量 (最早的可用偏移量)。
     * 应用场景：当消费者需要从分区的最开始位置消费消息时调用，例如冷启动或数据回溯场景。
     * 实现细节：内部调用 `beginningOrEndOffset` 方法，并将时间戳参数设置为 `ListOffsetsRequest.EARLIEST_TIMESTAMP`。
     * 设计考虑：提供一个便捷的API来获取起始偏移量，封装了底层ListOffsets请求的复杂性。
     *
     * @param partitions 一个包含 TopicPartition (主题分区) 的集合，表示需要获取起始偏移量的分区。
     * @param timer Timer 对象，用于控制整个操作的超时。
     * @return 一个映射，键是 TopicPartition (主题分区)，值是 Long 类型的起始偏移量。
     */
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Timer timer) {
        // 调用 beginningOrEndOffset 方法，传入 ListOffsetsRequest.EARLIEST_TIMESTAMP 表示获取最早的偏移量
        return beginningOrEndOffset(partitions, ListOffsetsRequest.EARLIEST_TIMESTAMP, timer);
    }

    /**
     * 获取指定分区的结束偏移量 (最新的可用偏移量，即下一条待写入消息的偏移量)。
     * 应用场景：当消费者需要从分区的最新位置开始消费消息（跳过历史消息），或者需要检查分区当前的消息末尾时调用。
     * 实现细节：内部调用 `beginningOrEndOffset` 方法，并将时间戳参数设置为 `ListOffsetsRequest.LATEST_TIMESTAMP`。
     * 设计考虑：提供一个便捷的API来获取结束偏移量，封装了底层ListOffsets请求的复杂性。
     *
     * @param partitions 一个包含 TopicPartition (主题分区) 的集合，表示需要获取结束偏移量的分区。
     * @param timer Timer 对象，用于控制整个操作的超时。
     * @return 一个映射，键是 TopicPartition (主题分区)，值是 Long 类型的结束偏移量。
     */
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Timer timer) {
        // 调用 beginningOrEndOffset 方法，传入 ListOffsetsRequest.LATEST_TIMESTAMP 表示获取最新的偏移量
        return beginningOrEndOffset(partitions, ListOffsetsRequest.LATEST_TIMESTAMP, timer);
    }

    /**
     * 获取指定分区集合在特定时间戳（最早或最新）的偏移量。
     * 应用场景：作为 `beginningOffsets` 和 `endOffsets` 方法的内部实现，用于获取分区的起始或结束偏移量。
     * 实现细节：
     * 1. 将待查询的分区对应的主题添加到元数据的瞬态主题列表中，以确保元数据包含这些主题的信息。
     * 2. 使用 Stream API 将输入的分区集合转换为一个 `Map<TopicPartition, Long>`，其中每个分区都映射到传入的 `timestamp` (EARLIEST_TIMESTAMP 或 LATEST_TIMESTAMP)。
     * 3. 调用 `fetchOffsetsByTimes` 方法，传入构建好的时间戳映射、计时器和 `false` (表示不强制要求支持精确时间戳)。
     * 4. 从 `fetchOffsetsByTimes` 返回的 `ListOffsetResult` 中提取 `fetchedOffsets`。
     * 5. 使用 Stream API 将 `fetchedOffsets` (类型为 `Map<TopicPartition, ListOffsetData>`) 转换为 `Map<TopicPartition, Long>`，只保留偏移量值。
     * 6. 在 finally 块中，从元数据中清除之前添加的瞬态主题。
     * 设计考虑：
     * - 通过 `addTransientTopics` 和 `clearTransientTopics` 确保元数据的临时一致性，处理可能未被订阅的主题查询。
     * - 利用 Stream API 简化数据转换逻辑。
     * - 复用 `fetchOffsetsByTimes` 方法来执行实际的偏移量获取操作。
     *
     * @param partitions 一个包含 TopicPartition (主题分区) 的集合。
     * @param timestamp 特殊时间戳，通常是 `ListOffsetsRequest.EARLIEST_TIMESTAMP` (-2L) 或 `ListOffsetsRequest.LATEST_TIMESTAMP` (-1L)。
     * @param timer Timer 对象，用于控制整个操作的超时。
     * @return 一个映射，键是 TopicPartition (主题分区)，值是 Long 类型的对应偏移量。
     */
    private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions,
                                                           long timestamp, // 特殊时间戳，如 EARLIEST_TIMESTAMP 或 LATEST_TIMESTAMP
                                                           Timer timer) {
        // 将待查询偏移量的分区对应的主题添加到元数据的瞬态主题列表中
        // 这样做是为了确保在元数据更新时能够包含这些可能尚未被当前消费者订阅的主题的信息
        metadata.addTransientTopics(topicsForPartitions(partitions));
        try {
            // 将传入的分区集合转换为一个 Map<TopicPartition, Long> 的形式
            // 其中每个分区都映射到给定的 timestamp (例如，ListOffsetsRequest.EARLIEST_TIMESTAMP)
            // 使用 distinct() 确保分区不重复
            Map<TopicPartition, Long> timestampsToSearch = partitions.stream()
                    .distinct() // 去除重复的分区
                    .collect(Collectors.toMap(Function.identity(), tp -> timestamp)); // 将分区映射到指定的时间戳

            // 调用 fetchOffsetsByTimes 方法，根据构建的 timestampsToSearch、timer 和 requireTimestamps=false 来获取偏移量
            // requireTimestamps=false 表示即使 broker 不支持精确时间戳查找，也不会抛出异常 (对于最早/最新偏移量查询通常是这样)
            ListOffsetResult result = fetchOffsetsByTimes(timestampsToSearch, timer, false);

            // 从 fetchOffsetsByTimes 返回的 ListOffsetResult 中提取 fetchedOffsets (类型为 Map<TopicPartition, ListOffsetData>)
            // 然后将其转换为 Map<TopicPartition, Long>，只保留每个分区的偏移量值
            return result.fetchedOffsets.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset));
        } finally {
            // 无论操作成功还是失败，在 finally 块中清除之前添加到元数据中的瞬态主题
            // 这是为了避免这些临时添加的主题影响后续的元数据管理
            metadata.clearTransientTopics();
        }
    }

    /**
     * 异步重置指定分区的消费位置。
     * 应用场景：当消费者启动时，如果某些分区没有有效的已提交偏移量，或者根据配置的重置策略（如 earliest, latest, 或特定时间戳）需要重置时调用。
     * 实现细节：
     * 1. 将输入的 `partitionAutoOffsetResetStrategyMap` (分区到自动重置策略的映射) 转换为 `partitionResetTimestamps` (分区到目标重置时间戳的映射)。
     *    这里会调用每个 `AutoOffsetResetStrategy` 的 `timestamp().get()` 方法来获取具体的时间戳 (可能是 EARLIEST, LATEST, 或用户指定的时间)。
     * 2. 调用 `groupListOffsetRequests` 方法，将 `partitionResetTimestamps` 按目标 broker 节点 (Node) 分组，并转换为 `ListOffsetsPartition` 对象，
     *    得到 `timestampsToSearchByNode` (节点到其负责的分区及对应 ListOffsetsPartition 的映射)。第二个参数 `new HashSet<>()` 可能是用于传递需要包含 leader epoch 的分区集合，此处为空集合。
     * 3. 遍历 `timestampsToSearchByNode` 中的每一个条目 (即每个 broker 节点及其对应的分区列表)。
     * 4. 对于每个节点：
     *    a. 获取节点对象 `node` 和该节点负责的 `resetTimestamps` (分区到 ListOffsetsPartition 的映射)。
     *    b. 调用 `subscriptions.setNextAllowedRetry` 方法，为这些分区设置下一次允许重试的时间，通常是当前时间加上请求超时时间，以避免过于频繁的重试。
     *    c. 调用 `sendListOffsetRequest` 方法向该 `node` 发送 ListOffsets 请求，参数包括 `resetTimestamps` 和 `false` (表示不强制要求时间戳)。返回一个 `RequestFuture`。
     *    d. 为 `future` 添加监听器：
     *       - onSuccess：调用 `offsetFetcherUtils.onSuccessfulResponseForResettingPositions` 处理成功的响应，更新订阅状态中的分区位置。
     *       - onFailure：调用 `offsetFetcherUtils.onFailedResponseForResettingPositions` 处理失败的响应，记录错误或准备重试。
     * 设计考虑：
     * - 异步执行：通过发送异步请求并使用回调处理结果，避免阻塞调用线程。
     * - 按节点分组：将请求按目标 broker 节点分组，可以减少网络连接数，提高效率。
     * - 重试控制：通过 `setNextAllowedRetry` 控制重试频率。
     * - 职责分离：将请求发送、响应处理的逻辑委托给 `sendListOffsetRequest` 和 `OffsetFetcherUtils` 中的相应方法。
     *
     * @param partitionAutoOffsetResetStrategyMap 一个映射，键是 TopicPartition (主题分区)，值是 AutoOffsetResetStrategy (自动偏移量重置策略)。
     */
    private void resetPositionsAsync(Map<TopicPartition, AutoOffsetResetStrategy> partitionAutoOffsetResetStrategyMap) {
        // 将分区到自动重置策略的映射转换为分区到具体重置时间戳的映射
        // AutoOffsetResetStrategy.timestamp() 会返回一个 Optional<Long>，这里假设它总是有值的 (通过 .get() 获取)
        // 这个时间戳可能是 EARLIEST_TIMESTAMP, LATEST_TIMESTAMP, 或者用户配置的特定时间戳
        Map<TopicPartition, Long> partitionResetTimestamps = partitionAutoOffsetResetStrategyMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().timestamp().get()));

        // 将按分区组织的时间戳请求，按其 leader 所在的 broker 节点进行分组
        // groupListOffsetRequests 方法会返回一个 Map<Node, Map<TopicPartition, ListOffsetsPartition>>
        // 其中 ListOffsetsPartition 是 ListOffsetsRequest 中用于表示单个分区请求的数据结构
        // 第二个参数 new HashSet<>() 可能是用于指定哪些分区需要包含 currentLeaderEpoch，这里为空，表示默认行为
        Map<Node, Map<TopicPartition, ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(partitionResetTimestamps, new HashSet<>());

        // 遍历按节点分组后的请求
        // 遍历按节点分组后的请求
        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
            // 获取目标 broker 节点
            Node node = entry.getKey();
            // 获取该节点需要处理的分区及其对应的 ListOffsetsPartition 数据
            final Map<TopicPartition, ListOffsetsPartition> resetTimestamps = entry.getValue();

            // 为这些即将发送请求的分区设置下一次允许重试的时间
            // 这是为了避免在请求失败时过于频繁地重试，增加一个退避时间 (requestTimeoutMs)
            subscriptions.setNextAllowedRetry(resetTimestamps.keySet(), time.milliseconds() + requestTimeoutMs);

            // 向指定的 broker 节点 (node) 发送 ListOffsets 请求
            // resetTimestamps 包含了请求的具体内容 (分区、目标时间戳等)
            // false 参数 (requireTimestamp) 表示不强制要求 broker 返回时间戳 (对于重置操作，主要关心偏移量)
            RequestFuture<ListOffsetResult> future = sendListOffsetRequest(node, resetTimestamps, false);
            // 为异步请求 future 添加监听器，以处理成功或失败的响应
            // 为异步请求添加监听器，以处理请求成功或失败的情况
            // 为该异步请求添加监听器，以处理成功或失败的响应
            future.addListener(new RequestFutureListener<>() {
                @Override
                public void onSuccess(ListOffsetResult result) {
                    // 当请求成功时，调用 offsetFetcherUtils 的方法来处理成功的响应结果
                    // 这个方法会根据返回的偏移量更新订阅状态中相应分区的位置
                    // partitionAutoOffsetResetStrategyMap 也被传入，可能用于某些策略相关的逻辑
                    offsetFetcherUtils.onSuccessfulResponseForResettingPositions(result, partitionAutoOffsetResetStrategyMap);
                }

                // 请求失败时的回调方法
                // 当来自单个节点的 ListOffsets 请求失败时调用
                @Override
                public void onFailure(RuntimeException e) {
                    // 当请求失败时，调用 offsetFetcherUtils 的方法来处理失败的响应
                    // resetTimestamps (原始请求的分区信息) 和异常 e 被传入，用于记录错误或进行后续的重试决策
                    offsetFetcherUtils.onFailedResponseForResettingPositions(resetTimestamps, e);
                }
            });
        }
    }

    /**
     * 异步验证分区位置。
     * 对于每个需要验证的分区，异步发送请求以获取该分区的末尾偏移量，
     * 请求中会附带该分区最后一次观察到的 epoch 或更小的 epoch。
     *
     * <p/>
     *
     * 为了提高效率，请求会按节点进行分组。
     * 应用场景：当消费者感知到分区leader发生切换，或者需要确认当前消费位置的有效性时调用。
     * 例如，在重新分配分区或从故障中恢复后，此方法有助于确保消费者从正确的位置开始消费，防止数据丢失或重复。
     * 实现细节：
     * 1. 将待验证的分区按其leader节点进行重新分组。
     * 2. 遍历每个节点及其对应的分区列表。
     * 3. 如果节点未知，则请求更新元数据。
     * 4. 如果节点API版本未知，则尝试连接该节点。
     * 5. 如果broker不支持基于leader epoch的偏移量获取（Kafka 2.3引入），则跳过验证并将这些分区的验证标记为完成。
     * 6. 设置下一次允许重试的时间。
     * 7. 向节点异步发送 `OffsetsForLeaderEpoch` 请求。
     * 8. 为请求添加监听器，在成功时调用 `onSuccessfulResponseForValidatingPositions` 处理结果，失败时调用 `onFailedResponseForValidatingPositions` 处理异常。
     * 设计考虑：
     * - 异步处理：避免阻塞主消费线程，提高响应性。
     * - 按节点分组请求：减少网络开销，提高请求效率。
     * - 版本兼容性检查：确保只向支持相应功能的broker发送请求。
     * - 重试机制：通过 `setNextAllowedRetry` 控制重试频率，避免频繁无效请求。
     *
     * @param partitionsToValidate 一个映射，键是 TopicPartition (主题分区)，值是 FetchPosition (拉取位置信息)，表示需要验证位置的分区。
     */
    private void validatePositionsAsync(Map<TopicPartition, FetchPosition> partitionsToValidate) {
        // 将需要验证的分区按其当前的leader节点进行重新分组，方便后续按节点批量发送请求
        final Map<Node, Map<TopicPartition, FetchPosition>> regrouped = regroupFetchPositionsByLeader(partitionsToValidate);

        // 计算下一次允许重试的时间戳，基于当前时间和请求超时时间
        long nextResetTimeMs = time.milliseconds() + requestTimeoutMs;
        // 遍历按节点分组后的验证请求
        regrouped.forEach((node, fetchPositions) -> {
            // 如果节点信息为空 (例如，leader未知)
            if (node.isEmpty()) {
                // 请求更新元数据，以便获取最新的leader信息
                // 请求更新元数据，期望下次能获取到leader信息
                metadata.requestUpdate(true);
                // 当前节点无法处理，跳过后续操作
                // 当前无法获取API版本，跳过后续操作
                // 跳过后续操作
                return;
            }

            // 获取该节点的API版本信息
            NodeApiVersions nodeApiVersions = apiVersions.get(node.idString());
            // 如果本地没有缓存该节点的API版本信息
            if (nodeApiVersions == null) {
                // 尝试连接该节点，连接成功后会获取其API版本信息
                client.tryConnect(node);
                // 当前节点无法处理，跳过后续操作
                // 当前无法获取API版本，跳过后续操作
                // 跳过后续操作
                return;
            }

            // 检查该节点是否支持可用的 `OffsetsForLeaderEpoch` API版本 (Kafka 2.3引入)
            if (!hasUsableOffsetForLeaderEpochVersion(nodeApiVersions)) {
                // 如果broker不支持所需协议版本，则记录调试日志并跳过这些分区的验证
                log.debug("由于broker不支持所需的协议版本（Kafka 2.3中引入），跳过对分区 {} 的拉取偏移量验证",
                        fetchPositions.keySet());
                // 遍历这些无法验证的分区
                for (TopicPartition partition : fetchPositions.keySet()) {
                    // 将这些分区的验证状态标记为完成，因为无法通过broker进行验证
                    subscriptions.completeValidation(partition);
                }
                // 当前节点无法处理，跳过后续操作
                // 当前无法获取API版本，跳过后续操作
                // 跳过后续操作
                return;
            }

            // 为这些分区设置下一次允许重试的时间，防止在短时间内对同一节点进行过多请求
            subscriptions.setNextAllowedRetry(fetchPositions.keySet(), nextResetTimeMs);

            // 异步发送 `OffsetsForLeaderEpoch` 请求到目标节点，以获取这些分区的偏移量信息
            RequestFuture<OffsetForEpochResult> future =
                    offsetsForLeaderEpochClient.sendAsyncRequest(node, fetchPositions);

            // 为异步请求添加监听器，以处理请求成功或失败的情况
            // 为该异步请求添加监听器，以处理成功或失败的响应
            future.addListener(new RequestFutureListener<>() {
                // 请求成功时的回调方法
                @Override
                public void onSuccess(OffsetForEpochResult offsetsResult) {
                    // 调用工具类方法处理成功的响应结果，更新分区的验证状态和位置信息
                    offsetFetcherUtils.onSuccessfulResponseForValidatingPositions(fetchPositions,
                            offsetsResult);
                }

                // 请求失败时的回调方法
                // 当来自单个节点的 ListOffsets 请求失败时调用
                @Override
                public void onFailure(RuntimeException e) {
                    // 调用工具类方法处理失败的响应，记录错误并可能触发重试逻辑
                    offsetFetcherUtils.onFailedResponseForValidatingPositions(fetchPositions, e);
                }
            });
        });
    }

    /**
     * 根据指定分区的目标时间搜索偏移量。
     * 应用场景：当需要根据特定的时间戳（例如，从某个历史时刻开始消费）来查找消息的起始偏移量时调用。
     * 例如，用户希望从昨天下午3点开始回溯消费某个主题分区的数据。
     * 实现细节：
     * 1. 初始化一个用于存储需要重试的分区的集合。
     * 2. 调用 `groupListOffsetRequests` 方法，将按时间戳搜索的请求按leader节点分组，并将无leader或leader不可用的分区加入重试集合。
     * 3. 如果分组后没有可发送请求的节点（例如，所有分区的leader都未知或不可用），则返回一个表示元数据过期的失败 `RequestFuture`。
     * 4. 创建一个 `RequestFuture` 用于聚合所有子请求的结果。
     * 5. 初始化一个映射用于存储已获取的时间戳和偏移量。
     * 6. 初始化一个原子计数器，用于跟踪剩余待响应的请求数量，初始值为节点数量。
     * 7. 遍历按节点分组的请求：
     *    a. 对每个节点，调用 `sendListOffsetRequest` 方法异步发送 `ListOffsets` 请求。
     *    b. 为每个子请求的 `RequestFuture` 添加监听器：
     *       i.  成功时：将获取到的偏移量存入 `fetchedTimestampOffsets`，将需要重试的分区加入 `partitionsToRetry`。如果所有子请求都已完成，则用聚合结果完成主 `listOffsetRequestsFuture`。
     *       ii. 失败时：如果主 `listOffsetRequestsFuture` 尚未完成，则用当前异常使其失败。
     * 8. 返回主 `listOffsetRequestsFuture`。
     * 设计考虑：
     * - 异步聚合：通过 `RequestFuture` 和原子计数器实现对多个异步 `ListOffsets` 请求结果的聚合，避免阻塞。
     * - 错误处理：能够处理部分请求成功、部分失败的情况，并将需要重试的分区信息传递出去。
     * - 元数据依赖：依赖 `groupListOffsetRequests` 来处理leader查找和节点可用性问题。
     *
     * @param timestampsToSearch 一个映射，键是 TopicPartition (主题分区)，值是目标时间戳 (ms)。
     * @param requireTimestamps  如果为 true，并且 broker 不支持精确时间戳获取，则会失败并抛出 UnsupportedVersionException。
     * @return 一个 RequestFuture，可以轮询以获取相应的时间戳和偏移量。
     */
    private RequestFuture<ListOffsetResult> sendListOffsetsRequests(final Map<TopicPartition, Long> timestampsToSearch,
                                                                    final boolean requireTimestamps) {
        // 初始化一个集合，用于存储那些因为leader未知、节点不可用等原因需要稍后重试的分区
        final Set<TopicPartition> partitionsToRetry = new HashSet<>();
        // 将按时间戳搜索偏移量的请求按目标leader节点进行分组
        // partitionsToRetry 集合会被填充那些暂时无法处理的分区（例如leader未知）
        Map<Node, Map<TopicPartition, ListOffsetsPartition>> timestampsToSearchByNode =
                groupListOffsetRequests(timestampsToSearch, partitionsToRetry);
        // 如果分组后没有任何有效的节点可以发送请求 (例如，所有分区的leader都未知或不可用)
        if (timestampsToSearchByNode.isEmpty())
            // 返回一个立即失败的 RequestFuture，并附带 StaleMetadataException，表示元数据可能已过期
            return RequestFuture.failure(new StaleMetadataException());

        // 创建一个 RequestFuture，用于最终返回聚合后的 ListOffset 请求结果
        final RequestFuture<ListOffsetResult> listOffsetRequestsFuture = new RequestFuture<>();
        // 初始化一个Map，用于存储从各个broker成功获取到的分区及其对应的偏移量和时间戳数据
        final Map<TopicPartition, ListOffsetData> fetchedTimestampOffsets = new HashMap<>();
        // 初始化一个原子整数，用于跟踪还剩多少个节点的响应没有收到，初始值为节点数量
        final AtomicInteger remainingResponses = new AtomicInteger(timestampsToSearchByNode.size());

        // 遍历按节点分组后的请求
        for (Map.Entry<Node, Map<TopicPartition, ListOffsetsPartition>> entry : timestampsToSearchByNode.entrySet()) {
            // 向当前节点(entry.getKey())发送 ListOffsets 请求，获取其负责的分区(entry.getValue())的偏移量信息
            // requireTimestamps 参数指示是否要求broker支持精确时间戳查找
            RequestFuture<ListOffsetResult> future = sendListOffsetRequest(entry.getKey(), entry.getValue(), requireTimestamps);
            // 为异步请求添加监听器，以处理请求成功或失败的情况
            // 为该异步请求添加监听器，以处理成功或失败的响应
            future.addListener(new RequestFutureListener<>() {
                // 当来自单个节点的 ListOffsets 请求成功返回时调用
                @Override
                public void onSuccess(ListOffsetResult partialResult) {
                    // 同步操作，以确保线程安全地更新共享的结果和状态
                    // 同步操作，确保线程安全地处理失败
                    synchronized (listOffsetRequestsFuture) {
                        // 将从该节点获取到的偏移量数据合并到总的 fetchedTimestampOffsets 中
                        fetchedTimestampOffsets.putAll(partialResult.fetchedOffsets);
                        // 将该节点响应中指示需要重试的分区合并到总的 partitionsToRetry 集合中
                        partitionsToRetry.addAll(partialResult.partitionsToRetry);

                        // 将剩余待响应的节点数减一，如果所有节点的响应都已收到 (remainingResponses变为0)
                        // 并且最终的 listOffsetRequestsFuture 尚未完成 (避免重复完成)
                        if (remainingResponses.decrementAndGet() == 0 && !listOffsetRequestsFuture.isDone()) {
                            // 构建最终的 ListOffsetResult，包含所有成功获取的偏移量和所有需要重试的分区
                            ListOffsetResult result = new ListOffsetResult(fetchedTimestampOffsets, partitionsToRetry);
                            // 使用聚合结果完成 listOffsetRequestsFuture
                            listOffsetRequestsFuture.complete(result);
                        }
                    }
                }

                // 请求失败时的回调方法
                // 当来自单个节点的 ListOffsets 请求失败时调用
                @Override
                public void onFailure(RuntimeException e) {
                    // 同步操作，以确保线程安全地更新共享的结果和状态
                    // 同步操作，确保线程安全地处理失败
                    synchronized (listOffsetRequestsFuture) {
                        // 如果最终的 listOffsetRequestsFuture 尚未完成 (即尚未被其他失败或成功所完成)
                        if (!listOffsetRequestsFuture.isDone())
                            // 使用当前遇到的异常使 listOffsetRequestsFuture 失败
                            // 这意味着只要有一个节点的请求失败，整个批量请求就会失败
                            listOffsetRequestsFuture.raise(e);
                    }
                }
            });
        }
        // 返回聚合了所有节点 ListOffsets 请求结果的 RequestFuture
        return listOffsetRequestsFuture;
    }

    /**
     * 为 `timestampsToSearch` 中的主题分区（这些分区具有可用的leader）按节点对要搜索的时间戳进行分组。
     * `timestampsToSearch` 中没有可用leader的主题分区将被添加到 `partitionsToRetry` 中。
     * 应用场景：在发送 `ListOffsets` 请求之前，需要确定每个分区的leader节点，并将请求按节点组织，以便批量发送。
     * 如果某些分区的leader未知或节点不可达，则将这些分区标记为需要重试。
     * 实现细节：
     * 1. 初始化一个空的 `partitionDataMap` 用于存储按分区组织的 `ListOffsetsPartition` 数据。
     * 2. 遍历 `timestampsToSearch` 中的每个分区及其目标时间戳：
     *    a. 获取当前分区的leader和epoch信息。
     *    b. 如果leader未知（`leaderAndEpoch.leader.isEmpty()`）：
     *       i.  记录调试日志。
     *       ii. 请求更新元数据（`metadata.requestUpdate(true)`）。
     *       iii.将该分区添加到 `partitionsToRetry` 集合中。
     *    c. 如果leader已知：
     *       i.  获取leader节点。
     *       ii. 如果客户端与该leader节点连接不可用（`client.isUnavailable(leader)`）：
     *           - 尝试抛出认证失败异常（`client.maybeThrowAuthFailure(leader)`）。
     *           - 记录调试日志，说明leader在重新连接退避期结束前不可用。
     *           - 将该分区添加到 `partitionsToRetry` 集合中。
     *       iii.如果leader可用：
     *           - 获取当前leader的epoch，如果不存在则使用 `ListOffsetsResponse.UNKNOWN_EPOCH`。
     *           - 创建一个 `ListOffsetsPartition` 对象，设置分区索引、目标时间戳和当前leader epoch。
     *           - 将该 `ListOffsetsPartition` 对象存入 `partitionDataMap`。
     * 3. 调用 `offsetFetcherUtils.regroupPartitionMapByNode(partitionDataMap)` 方法，将 `partitionDataMap` 中的数据按leader节点重新组织成 `Map<Node, Map<TopicPartition, ListOffsetsPartition>>` 格式并返回。
     * 设计考虑：
     * - 元数据依赖：强依赖 `ConsumerMetadata` 来获取分区的leader信息。
     * - 错误处理与重试：通过 `partitionsToRetry` 集合将无法立即处理的分区传递出去，供上层逻辑进行重试或元数据更新。
     * - 节点可用性检查：在组织请求前检查目标leader节点是否可用，避免向不可达节点发送请求。
     *
     * @param timestampsToSearch 按分区映射的目标时间戳
     * @param partitionsToRetry  一个主题分区集合，将使用需要元数据更新或重新连接到leader的分区进行扩展。
     * @return 按节点分组的 `ListOffsetsPartition` 请求数据，键是 Node，值是该节点负责处理的分区及其对应的 `ListOffsetsPartition` 信息。
     */
    private Map<Node, Map<TopicPartition, ListOffsetsPartition>> groupListOffsetRequests(
            Map<TopicPartition, Long> timestampsToSearch,
            Set<TopicPartition> partitionsToRetry) {
        // 初始化一个Map，用于临时存储 TopicPartition 到 ListOffsetsPartition 的映射，之后会按Node分组
        final Map<TopicPartition, ListOffsetsPartition> partitionDataMap = new HashMap<>();
        // 遍历所有需要按时间戳查找偏移量的分区
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            // 当前处理的主题分区
            TopicPartition tp = entry.getKey();
            // 目标时间戳 (在ListOffsets请求中，这个字段实际上是timestamp，变量名offset可能有些误导，但遵循了原有参数名)
            Long offset = entry.getValue();
            // 从元数据中获取当前分区的leader节点及其epoch信息
            Metadata.LeaderAndEpoch leaderAndEpoch = metadata.currentLeader(tp);

            // 如果当前分区的leader未知
            if (leaderAndEpoch.leader.isEmpty()) {
                // 记录调试日志，说明找不到分区的leader
                log.debug("分区 {} 的Leader未知，无法获取偏移量 {}", tp, offset);
                // 请求更新元数据，以便获取最新的leader信息
                // 请求更新元数据，期望下次能获取到leader信息
                metadata.requestUpdate(true);
                // 将该分区添加到需要重试的集合中
                // 将该分区添加到需要重试的集合中
                    partitionsToRetry.add(tp);
            // 如果分区的leader已知
            // 如果leader节点可用
                } else {
                // 获取leader节点对象
                Node leader = leaderAndEpoch.leader.get();
                // 检查客户端与该leader节点的连接是否不可用 (例如，连接失败、正在退避等)
                if (client.isUnavailable(leader)) {
                    // 如果连接不可用是由于认证失败，则尝试抛出相应的认证异常
                    client.maybeThrowAuthFailure(leader);

                    // 连接已失败，我们需要等待退避期结束后才能重试。
                    // 无需请求元数据更新，因为断开连接操作通常已经触发了元数据更新。
                    log.debug("分区 {} 的Leader {} 在重新连接退避期结束前不可用，无法获取偏移量",
                            leader, tp);
                    // 将该分区添加到需要重试的集合中
                // 将该分区添加到需要重试的集合中
                    partitionsToRetry.add(tp);
                // 如果分区的leader已知
            // 如果leader节点可用
                } else {
                    // 获取当前leader的epoch；如果epoch不存在（例如旧版本的broker），则使用 UNKNOWN_EPOCH
                    int currentLeaderEpoch = leaderAndEpoch.epoch.orElse(ListOffsetsResponse.UNKNOWN_EPOCH);
                    // 创建一个 ListOffsetsPartition 对象，用于构建 ListOffsets 请求
                    // 设置分区索引、目标时间戳和当前leader的epoch
                    partitionDataMap.put(tp, new ListOffsetsPartition()
                            .setPartitionIndex(tp.partition()) // 设置分区号
                            .setTimestamp(offset) // 设置目标时间戳
                            .setCurrentLeaderEpoch(currentLeaderEpoch)); // 设置当前leader的epoch
                }
            }
        }
        // 调用工具类方法，将按分区组织的请求数据 (partitionDataMap) 重新按leader节点进行分组，并返回
        return offsetFetcherUtils.regroupPartitionMapByNode(partitionDataMap);
    }

    /**
     * 向特定 broker 发送 ListOffsetRequest 请求，用于获取指定分区在目标时间戳的偏移量。
     * 应用场景：当需要根据时间戳查找偏移量时，例如 `offsetsForTimes` 方法内部会调用此方法。
     * 实现细节：
     * 1. 构建 ListOffsetsRequest 请求，设置消费者标识、是否需要时间戳、隔离级别、目标时间戳和超时时间。
     * 2. 使用 ConsumerNetworkClient 发送请求到指定的 broker 节点。
     * 3. 对返回的 Future 进行转换，在成功回调中处理 ListOffsetsResponse。
     * 设计考虑：
     * - 封装了 ListOffsetsRequest 的构建和发送逻辑。
     * - 使用 RequestFuture 和回调机制处理异步响应。
     *
     * @param node               目标 Kafka broker 节点。
     * @param timestampsToSearch 分区到目标时间戳的映射 (Map<TopicPartition, ListOffsetsPartition>)。
     * @param requireTimestamp   布尔值，指示响应中是否需要包含时间戳。
     * @return RequestFuture<ListOffsetResult> 一个可以轮询以获取相应时间戳和偏移量的响应 Future。
     */
    private RequestFuture<ListOffsetResult> sendListOffsetRequest(final Node node,
                                                                  final Map<TopicPartition, ListOffsetsPartition> timestampsToSearch,
                                                                  boolean requireTimestamp) {
        // 创建 ListOffsetsRequest.Builder 对象，用于构建 ListOffsetsRequest
        ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
                // 设置为消费者请求，并指定是否需要时间戳以及当前的隔离级别
                .forConsumer(requireTimestamp, isolationLevel)
                // 设置目标时间戳，将用户提供的 Map<TopicPartition, ListOffsetsPartition> 转换为请求所需的格式
                .setTargetTimes(ListOffsetsRequest.toListOffsetsTopics(timestampsToSearch))
                // 设置请求的超时时间
                .setTimeoutMs(requestTimeoutMs);

        // 记录调试日志，说明正在向哪个 broker 发送哪个 ListOffsetRequest
        log.debug("Sending ListOffsetRequest {} to broker {}", builder, node);
        // 使用 client (ConsumerNetworkClient) 向指定的 node 发送构建好的 builder (ListOffsetsRequest)
        // send 方法返回一个 RequestFuture<ClientResponse>
        return client.send(node, builder)
                // 使用 compose 方法对 RequestFuture<ClientResponse> 进行转换，将其转换为 RequestFuture<ListOffsetResult>
                // 当原始的 Future 成功完成时，会调用 RequestFutureAdapter 的 onSuccess 方法
                .compose(new RequestFutureAdapter<>() {
                    // 当从 broker 收到响应并成功解析后调用此方法
                    @Override
                    public void onSuccess(ClientResponse response, RequestFuture<ListOffsetResult> future) {
                        // 将 ClientResponse 中的响应体 (responseBody) 强制转换为 ListOffsetsResponse
                        ListOffsetsResponse lor = (ListOffsetsResponse) response.responseBody();
                        // 记录追踪日志，说明从哪个 broker 收到了哪个 ListOffsetResponse
                        log.trace("Received ListOffsetResponse {} from broker {}", lor, node);
                        // 调用 handleListOffsetResponse 方法处理接收到的 ListOffsetsResponse，并更新传入的 future
                        handleListOffsetResponse(lor, future);
                    }
                });
    }

    /**
     * 上述 list offset 调用的响应回调方法。
     * 应用场景：在 `sendListOffsetRequest` 方法中，当从 broker 收到 ListOffsetsResponse 后，此方法被调用来处理响应。
     * 实现细节：
     * 1. 调用 `offsetFetcherUtils.handleListOffsetResponse` 处理原始的 `ListOffsetsResponse`，将其转换为 `ListOffsetResult`。
     * 2. 如果处理成功，则使用结果完成 `future`。
     * 3. 如果处理过程中发生运行时异常，则将异常传递给 `future`。
     * 设计考虑：
     * - 将响应处理逻辑委托给 `OffsetFetcherUtils`，保持当前类的职责单一。
     * - 统一处理成功和异常情况，并更新 `RequestFuture` 的状态。
     *
     * @param listOffsetsResponse 从服务器返回的 ListOffsetsResponse。
     * @param future              当响应返回时需要完成的 RequestFuture。注意，任何分区级别的错误通常会导致整个 future 失败。
     *                            一个例外是 UNSUPPORTED_FOR_MESSAGE_FORMAT，它表示 broker 不支持 v1 消息格式。
     *                            具有此特定错误的分区将简单地从 future 映射中排除。注意，每个分区的相应时间戳值
     *                            可能仅对于 v0 为 null。在 v1 及更高版本中，ListOffset API 不会返回 null 时间戳（必要时返回 -1）。
     */
    private void handleListOffsetResponse(ListOffsetsResponse listOffsetsResponse,
                                          RequestFuture<ListOffsetResult> future) {
        // 使用 try-catch 块来捕获处理响应过程中可能发生的异常
        try {
            // 调用 offsetFetcherUtils 的 handleListOffsetResponse 方法处理从 broker 返回的 ListOffsetsResponse
            // 该方法会解析响应，提取每个分区的偏移量、时间戳等信息，并处理各种错误情况
            ListOffsetResult result = offsetFetcherUtils.handleListOffsetResponse(listOffsetsResponse);
            // 如果处理成功，使用解析得到的 result (ListOffsetResult) 来完成传入的 future
            // 这会通知所有等待此 future 的监听器请求已成功完成
            future.complete(result);
        } catch (RuntimeException e) {
            // 如果在处理响应过程中捕获到 RuntimeException
            // 则调用 future.raise(e) 方法，将此异常传递给 future
            // 这会通知所有等待此 future 的监听器请求失败，并附带该异常信息
            future.raise(e);
        }
    }

    /**
     * 如果我们检测到新的元数据（由 {@link org.apache.kafka.clients.Metadata#updateVersion()} 跟踪），
     * 那么我们应该检查所有分配的分区是否都有一个有效的位置。
     * 应用场景：当消费者元数据更新后（例如，leader 切换或分区重新分配），需要调用此方法来验证当前消费位置的有效性。
     * 实现细节：直接调用 `offsetFetcherUtils.validatePositionsOnMetadataChange()` 来执行实际的验证逻辑。
     * 设计考虑：将具体的验证逻辑委托给 `OffsetFetcherUtils`，使得 `OffsetFetcher` 更侧重于协调和流程控制。
     */
    public void validatePositionsOnMetadataChange() {
        // 调用 offsetFetcherUtils 的 validatePositionsOnMetadataChange 方法
        // 该方法会检查元数据版本是否有变化，如果有变化，则会对所有已分配的分区进行位置验证
        // 位置验证的目的是确保消费者在 leader 切换等元数据变更后，其当前的消费位置仍然有效
        offsetFetcherUtils.validatePositionsOnMetadataChange();
    }
}