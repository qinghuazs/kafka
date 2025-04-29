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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.DescribeProducersOptions;
import org.apache.kafka.clients.admin.DescribeProducersResult.PartitionProducerState;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.DescribeProducersRequest;
import org.apache.kafka.common.requests.DescribeProducersResponse;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DescribeProducersHandler类用于处理描述生产者状态的请求。
 * 该类继承自AdminApiHandler.Batched,专门处理TopicPartition到PartitionProducerState的批量操作。
 * 
 * 应用场景:
 * 1. 监控活跃生产者 - 查询特定分区上当前活跃的生产者及其状态
 * 2. 事务管理 - 获取生产者的事务状态,包括事务偏移量和协调器纪元
 * 3. 故障诊断 - 通过生产者状态信息排查生产问题
 */
public class DescribeProducersHandler extends AdminApiHandler.Batched<TopicPartition, PartitionProducerState> {
    // 日志记录器
    private final Logger log;
    // 描述生产者的配置选项
    private final DescribeProducersOptions options;
    // 用于查找分区所在broker的策略
    private final AdminApiLookupStrategy<TopicPartition> lookupStrategy;

    /**
     * 构造函数
     * @param options 描述生产者的配置选项,可以指定特定的broker ID
     * @param logContext 日志上下文
     */
    public DescribeProducersHandler(
        DescribeProducersOptions options,
        LogContext logContext
    ) {
        this.options = options;
        this.log = logContext.logger(DescribeProducersHandler.class);

        // 根据是否指定broker ID选择不同的查找策略
        if (options.brokerId().isPresent()) {
            // 如果指定了broker ID,使用静态broker策略
            this.lookupStrategy = new StaticBrokerStrategy<>(options.brokerId().getAsInt());
        } else {
            // 否则使用分区leader策略,动态查找分区的leader broker
            this.lookupStrategy = new PartitionLeaderStrategy(logContext);
        }
    }

    /**
     * 创建新的PartitionLeaderFuture实例
     * @param topicPartitions 需要查询的主题分区集合
     * @param partitionLeaderCache 分区leader的缓存映射
     * @return 返回一个新的PartitionLeaderFuture实例
     */
    public static PartitionLeaderStrategy.PartitionLeaderFuture<PartitionProducerState> newFuture(
        Collection<TopicPartition> topicPartitions,
        Map<TopicPartition, Integer> partitionLeaderCache
    ) {
        return new PartitionLeaderStrategy.PartitionLeaderFuture<>(new HashSet<>(topicPartitions), partitionLeaderCache);
    }

    /**
     * 获取API名称
     * @return 返回"describeProducers"作为API名称
     */
    @Override
    public String apiName() {
        return "describeProducers";
    }

    /**
     * 获取查找策略
     * @return 返回当前使用的broker查找策略
     */
    @Override
    public AdminApiLookupStrategy<TopicPartition> lookupStrategy() {
        return lookupStrategy;
    }

    /**
     * 构建批量请求
     * 将多个主题分区的请求合并成一个批量请求,提高处理效率
     *
     * @param brokerId 目标broker的ID
     * @param topicPartitions 需要查询的主题分区集合
     * @return 返回构建好的DescribeProducersRequest.Builder实例
     */
    @Override
    public DescribeProducersRequest.Builder buildBatchedRequest(
        int brokerId,
        Set<TopicPartition> topicPartitions
    ) {
        // 创建请求数据对象
        DescribeProducersRequestData request = new DescribeProducersRequestData();
        DescribeProducersRequest.Builder builder = new DescribeProducersRequest.Builder(request);

        // 按主题对分区进行分组,优化请求结构
        CollectionUtils.groupPartitionsByTopic(
            topicPartitions,
            builder::addTopic,  // 添加主题的处理函数
            (topicRequest, partitionId) -> topicRequest.partitionIndexes().add(partitionId)  // 添加分区ID的处理函数
        );

        return builder;
    }

    /**
     * 处理分区级别的错误
     * 根据不同的错误类型采取相应的处理策略
     *
     * @param topicPartition 发生错误的主题分区
     * @param apiError API错误信息
     * @param failed 存储失败的分区及其异常信息
     * @param unmapped 存储需要重新查找leader的分区
     */
    private void handlePartitionError(
        TopicPartition topicPartition,
        ApiError apiError,
        Map<TopicPartition, Throwable> failed,
        List<TopicPartition> unmapped
    ) {
        switch (apiError.error()) {
            case NOT_LEADER_OR_FOLLOWER:
                if (options.brokerId().isPresent()) {
                    // 如果用户指定了broker ID,这类错误就是致命的
                    // 因为我们不能重试其他broker
                    int brokerId = options.brokerId().getAsInt();
                    log.error("Not leader error in `DescribeProducers` response for partition {} " +
                        "for brokerId {} set in options", topicPartition, brokerId, apiError.exception());
                    failed.put(topicPartition, apiError.error().exception("Failed to describe active producers " +
                        "for partition " + topicPartition + " on brokerId " + brokerId));
                } else {
                    // 如果没有指定broker ID,将分区标记为未映射
                    // 这样可以重新查找分区的leader
                    log.debug("Not leader error in `DescribeProducers` response for partition {}. " +
                        "Will retry later.", topicPartition);
                    unmapped.add(topicPartition);
                }
                break;

            case UNKNOWN_TOPIC_OR_PARTITION:
                // 主题或分区不存在,记录日志后等待重试
                log.debug("Unknown topic/partition error in `DescribeProducers` response for partition {}. " +
                    "Will retry later.", topicPartition);
                break;

            case INVALID_TOPIC_EXCEPTION:
                // 主题名称无效,这是不可恢复的错误
                log.error("Invalid topic in `DescribeProducers` response for partition {}",
                    topicPartition, apiError.exception());
                failed.put(topicPartition, new InvalidTopicException(
                    "Failed to fetch metadata for partition " + topicPartition
                        + " due to invalid topic error: " + apiError.messageWithFallback(),
                    Collections.singleton(topicPartition.topic())));
                break;

            case TOPIC_AUTHORIZATION_FAILED:
                // 没有主题的访问权限,这是不可恢复的错误
                log.error("Authorization failed in `DescribeProducers` response for partition {}",
                    topicPartition, apiError.exception());
                failed.put(topicPartition, new TopicAuthorizationException("Failed to describe " +
                    "active producers for partition " + topicPartition + " due to authorization failure on topic" +
                    " `" + topicPartition.topic() + "`", Collections.singleton(topicPartition.topic())));
                break;

            default:
                // 其他未预期的错误
                log.error("Unexpected error in `DescribeProducers` response for partition {}",
                    topicPartition, apiError.exception());
                failed.put(topicPartition, apiError.error().exception("Failed to describe active " +
                    "producers for partition " + topicPartition + " due to unexpected error"));
                break;
        }
    }

    /**
     * 处理来自broker的响应
     * 解析响应数据,提取生产者状态信息,并处理可能出现的错误
     *
     * @param broker 返回响应的broker节点
     * @param keys 请求的主题分区集合
     * @param abstractResponse broker返回的原始响应
     * @return 返回API调用结果,包含成功、失败和需要重新映射的分区信息
     */
    @Override
    public ApiResult<TopicPartition, PartitionProducerState> handleResponse(
        Node broker,
        Set<TopicPartition> keys,
        AbstractResponse abstractResponse
    ) {
        // 将抽象响应转换为具体的DescribeProducersResponse
        DescribeProducersResponse response = (DescribeProducersResponse) abstractResponse;
        // 存储处理成功的分区及其生产者状态
        Map<TopicPartition, PartitionProducerState> completed = new HashMap<>();
        // 存储处理失败的分区及其异常信息
        Map<TopicPartition, Throwable> failed = new HashMap<>();
        // 存储需要重新查找leader的分区
        List<TopicPartition> unmapped = new ArrayList<>();

        // 遍历响应中的每个主题
        for (DescribeProducersResponseData.TopicResponse topicResponse : response.data().topics()) {
            // 遍历主题中的每个分区
            for (DescribeProducersResponseData.PartitionResponse partitionResponse : topicResponse.partitions()) {
                // 构建主题分区对象
                TopicPartition topicPartition = new TopicPartition(
                    topicResponse.name(), partitionResponse.partitionIndex());

                // 检查是否有错误发生
                Errors error = Errors.forCode(partitionResponse.errorCode());
                if (error != Errors.NONE) {
                    // 如果有错误,交给错误处理器处理
                    ApiError apiError = new ApiError(error, partitionResponse.errorMessage());
                    handlePartitionError(topicPartition, apiError, failed, unmapped);
                    continue;
                }

                // 处理成功的情况,转换活跃生产者信息
                List<ProducerState> activeProducers = partitionResponse.activeProducers().stream()
                    .map(activeProducer -> {
                        // 处理事务起始偏移量,如果小于0表示没有事务
                        OptionalLong currentTransactionFirstOffset =
                            activeProducer.currentTxnStartOffset() < 0 ?
                                OptionalLong.empty() :
                                OptionalLong.of(activeProducer.currentTxnStartOffset());
                        // 处理协调器纪元,如果小于0表示没有协调器
                        OptionalInt coordinatorEpoch =
                            activeProducer.coordinatorEpoch() < 0 ?
                                OptionalInt.empty() :
                                OptionalInt.of(activeProducer.coordinatorEpoch());

                        // 创建生产者状态对象
                        return new ProducerState(
                            activeProducer.producerId(),      // 生产者ID
                            activeProducer.producerEpoch(),   // 生产者纪元
                            activeProducer.lastSequence(),    // 最后的序列号
                            activeProducer.lastTimestamp(),   // 最后的时间戳
                            coordinatorEpoch,                 // 协调器纪元
                            currentTransactionFirstOffset     // 事务起始偏移量
                        );
                    }).collect(Collectors.toList());

                // 将处理结果添加到完成映射中
                completed.put(topicPartition, new PartitionProducerState(activeProducers));
            }
        }
        // 返回包含成功、失败和未映射分区信息的结果
        return new ApiResult<>(completed, failed, unmapped);
    }

}
