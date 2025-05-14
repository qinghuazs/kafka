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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * {@link CompletedFetch} 表示通过 {@link FetchRequest} 从 broker 返回的 {@link RecordBatch 一批} {@link Record 记录}。
 * 它包含了在多次调用 {@link #fetchRecords(FetchConfig, Deserializers, int)} 之间维护状态的逻辑。
 * 应用场景：当消费者从 Kafka broker 获取数据后，需要一个对象来封装这些数据以及相关的元信息和处理状态。
 * 实现细节：此类封装了分区数据、批次迭代器、中止的事务信息等，并提供了方法来迭代记录和管理消费状态。
 * 设计考虑：将拉取到的数据和其处理逻辑封装在一起，便于管理和传递。通过维护内部状态（如已读记录数、下一个拉取偏移量），
 *          可以支持分批次处理拉取到的数据，而不是一次性加载所有记录。
 */
public class CompletedFetch { // CompletedFetch 类定义：表示一次已完成的拉取操作的结果。

    // final TopicPartition partition; // 主题分区，表示此拉取结果属于哪个分区。
    final TopicPartition partition; // 主题分区，表示此拉取结果属于哪个分区。设计考虑：final确保一旦设置不可更改，保证数据一致性。
    // final FetchResponseData.PartitionData partitionData; // 从 FetchResponse 中获取的原始分区数据。
    final FetchResponseData.PartitionData partitionData; // 从 FetchResponse 中获取的原始分区数据。设计考虑：存储原始响应数据，可能用于后续的复杂处理或调试。
    // final short requestVersion; // 用于获取此拉取的 FetchRequest 的版本。
    final short requestVersion; // 用于获取此拉取的 FetchRequest 的版本。设计考虑：请求版本可能影响响应的解析方式或行为。

    // private final Logger log; // 日志记录器。
    private final Logger log; // 日志记录器。设计考虑：用于记录此类操作过程中的信息和错误。
    // private final SubscriptionState subscriptions; // 消费者的订阅状态。
    private final SubscriptionState subscriptions; // 消费者的订阅状态。设计考虑：可能需要访问订阅状态以进行某些决策，例如更新分区的消费位置。
    // private final BufferSupplier decompressionBufferSupplier; // 用于解压缩记录的缓冲区供应器。
    private final BufferSupplier decompressionBufferSupplier; // 用于解压缩记录的缓冲区供应器。设计考虑：提供缓冲区以支持高效的解压缩操作。
    // private final Iterator<? extends RecordBatch> batches; // 从 FetchResponse 获取的 RecordBatch 迭代器。
    private final Iterator<? extends RecordBatch> batches; // 从 FetchResponse 获取的 RecordBatch 迭代器。设计考虑：使用迭代器可以按需处理批次，避免一次性加载所有数据到内存。
    // private final Set<Long> abortedProducerIds; // 已中止事务的生产者 ID 集合。
    private final Set<Long> abortedProducerIds; // 已中止事务的生产者 ID 集合。设计考虑：用于在读取提交（READ_COMMITTED）隔离级别下过滤掉已中止事务的消息。
    // private final PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions; // 已中止事务的优先队列。
    private final PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions; // 已中止事务的优先队列，按起始偏移量排序。设计考虑：优先队列便于高效地检查记录是否属于已中止的事务。
    // private final FetchMetricsAggregator metricAggregator; // 拉取指标聚合器。
    private final FetchMetricsAggregator metricAggregator; // 拉取指标聚合器。设计考虑：用于收集和报告与此拉取相关的指标。

    // private int recordsRead; // 已从此 CompletedFetch 中读取的记录数。
    private int recordsRead; // 已从此 CompletedFetch 中读取的记录数。
    // private int bytesRead; // 已从此 CompletedFetch 中读取的字节数。
    private int bytesRead; // 已从此 CompletedFetch 中读取的字节数。
    // private RecordBatch currentBatch; // 当前正在处理的 RecordBatch。
    private RecordBatch currentBatch; // 当前正在处理的 RecordBatch。
    // private Record lastRecord; // 上一条处理的记录。
    private Record lastRecord; // 上一条处理的记录。
    // private CloseableIterator<Record> records; // 当前 RecordBatch 中记录的迭代器。
    private CloseableIterator<Record> records; // 当前 RecordBatch 中记录的可关闭迭代器。
    // private Exception cachedRecordException = null; // 在解析记录时缓存的异常。
    private Exception cachedRecordException = null; // 在解析记录时缓存的异常。
    // private boolean corruptLastRecord = false; // 标记上一条记录是否损坏。
    private boolean corruptLastRecord = false; // 标记上一条记录是否损坏。
    // private long nextFetchOffset; // 下一次拉取请求应该使用的偏移量。
    private long nextFetchOffset; // 下一次拉取请求应该使用的偏移量。
    // private Optional<Integer> lastEpoch; // 拉取到的最后一个记录的 epoch（如果可用）。
    private Optional<Integer> lastEpoch; // 拉取到的最后一个记录的 epoch（如果可用）。
    // private boolean isConsumed = false; // 标记此 CompletedFetch 中的所有记录是否已被消费。
    private boolean isConsumed = false; // 标记此 CompletedFetch 中的所有记录是否已被消费。
    // private boolean initialized = false; // 标记此 CompletedFetch 是否已初始化。
    private boolean initialized = false; // 标记此 CompletedFetch 是否已初始化。

    /**
     * CompletedFetch 的构造函数。
     * 应用场景：在消费者收到 FetchResponse 后，为每个成功获取数据的分区创建一个 CompletedFetch 实例。
     * 实现细节：初始化所有 final 字段，并从 partitionData 中提取批次迭代器和中止的事务信息。
     * 设计考虑：构造函数接收所有必要的依赖项，确保 CompletedFetch 实例在创建时就拥有了处理拉取数据所需的所有信息。
     *
     * @param logContext 日志上下文，用于创建日志记录器。
     * @param subscriptions 消费者的订阅状态。
     * @param decompressionBufferSupplier 用于解压缩的缓冲区供应器。
     * @param partition 当前数据所属的主题分区。
     * @param partitionData 从 broker 返回的特定分区的数据。
     * @param metricAggregator 用于聚合拉取指标的聚合器。
     * @param fetchOffset 本次拉取请求的起始偏移量。
     * @param requestVersion 本次拉取请求的版本号。
     */
    CompletedFetch(LogContext logContext,
                   SubscriptionState subscriptions,
                   BufferSupplier decompressionBufferSupplier,
                   TopicPartition partition,
                   FetchResponseData.PartitionData partitionData,
                   FetchMetricsAggregator metricAggregator,
                   Long fetchOffset,
                   short requestVersion) {
        // 初始化日志记录器, 使用传入的 logContext 为 CompletedFetch 类创建一个特定的 logger 实例
        this.log = logContext.logger(CompletedFetch.class);
        // 初始化订阅状态引用, 保存传入的 subscriptions 对象，以便后续访问消费者的订阅信息
        this.subscriptions = subscriptions;
        // 初始化解压缩缓冲区供应器引用, 保存传入的 decompressionBufferSupplier，用于解压缩消息时获取缓冲区
        this.decompressionBufferSupplier = decompressionBufferSupplier;
        // 初始化主题分区引用, 保存当前 CompletedFetch 实例关联的 TopicPartition
        this.partition = partition;
        // 初始化分区数据引用, 保存从 Broker 拉取到的原始分区数据
        this.partitionData = partitionData;
        // 初始化指标聚合器引用, 保存传入的 metricAggregator，用于记录相关的 Fetch 指标
        this.metricAggregator = metricAggregator;
        // 从分区数据中获取记录批次的迭代器；如果记录为空或获取失败，则抛出异常
        // FetchResponse.recordsOrFail(partitionData) 会检查 partitionData 中的记录是否有效，如果无效（例如有错误码），则抛出异常
        // .batches() 将记录转换为 RecordBatch 的流
        // .iterator() 获取这个流的迭代器
        this.batches = FetchResponse.recordsOrFail(partitionData).batches().iterator();
        // 初始化下一次拉取的偏移量, 使用传入的 fetchOffset，表示这个 CompletedFetch 实例对应的拉取请求的起始偏移量
        this.nextFetchOffset = fetchOffset;
        // 初始化请求版本, 保存发出此 Fetch 请求时使用的 API 版本号
        this.requestVersion = requestVersion;
        // 初始化 lastEpoch 为空, lastEpoch 用于追踪分区 leader 的 epoch，初始时未知
        this.lastEpoch = Optional.empty();
        // 初始化已中止的生产者 ID 集合, 用于存储在 READ_COMMITTED 模式下需要跳过的事务的生产者 ID
        this.abortedProducerIds = new HashSet<>();
        // 初始化已中止的事务列表（从分区数据中提取并排序）
        // abortedTransactions(partitionData) 是一个静态辅助方法（或此类中的方法），用于从 partitionData 中提取已中止事务的信息
        this.abortedTransactions = abortedTransactions(partitionData);
    }

    /**
     * 获取下一次拉取应该使用的偏移量。
     * 应用场景：消费者在处理完当前 CompletedFetch 中的数据后，需要知道下一次应该从哪个偏移量开始拉取。
     * 实现细节：返回 nextFetchOffset 字段的值。该值在处理记录时会被更新。
     * 设计考虑：提供一个明确的方法来获取下一个拉取点，简化消费者的偏移量管理。
     * @return 下一次拉取的偏移量。
     */
    long nextFetchOffset() {
        // 返回 nextFetchOffset 字段的值, 该值代表了当前批次记录处理完毕后，下一次应该从哪个 offset 开始拉取
        return nextFetchOffset;
    }

    /**
     * 获取拉取到的最后一个记录的 leader epoch。
     * 应用场景：用于 leader epoch 的验证，以检测日志截断。
     * 实现细节：返回 lastEpoch 字段的值。
     * 设计考虑：封装 leader epoch 信息，供上层逻辑使用。
     * @return 一个包含最后一个记录的 leader epoch 的 Optional，如果不可用则为空。
     */
    Optional<Integer> lastEpoch() {
        // 返回 lastEpoch 字段的值, 它可能包含最后处理记录的 leader epoch
        return lastEpoch;
    }

    /**
     * 检查此 CompletedFetch 是否已初始化。
     * 应用场景：在某些操作（如获取记录）之前，可能需要确保 CompletedFetch 实例已经过初始化步骤。
     * 实现细节：返回 initialized 字段的布尔值。
     * 设计考虑：提供状态查询方法，允许外部代码根据其初始化状态执行不同逻辑。
     * @return 如果已初始化，则返回 true；否则返回 false。
     */
    boolean isInitialized() {
        // 返回 initialized 字段的值, 表示此 CompletedFetch 是否已经完成了初始化过程
        return initialized;
    }

    /**
     * 将此 CompletedFetch 标记为已初始化。
     * 应用场景：在完成了必要的设置或初步处理后，调用此方法来更新状态。
     * 实现细节：将 initialized 字段设置为 true。
     * 设计考虑：提供一个明确的方法来改变初始化状态。
     */
    void setInitialized() {
        // 将 initialized 字段设置为 true, 标记初始化完成
        this.initialized = true;
    }

    /**
     * 检查此 CompletedFetch 中的所有记录是否已被消费。
     * 应用场景：判断是否可以安全地丢弃或关闭此 CompletedFetch 实例。
     * 实现细节：返回 isConsumed 字段的布尔值。
     * 设计考虑：提供状态查询方法，用于管理 CompletedFetch 实例的生命周期。
     * @return 如果所有记录都已消费，则返回 true；否则返回 false。
     */
    public boolean isConsumed() {
        // 返回 isConsumed 字段的值, 表示此 CompletedFetch 中的数据是否已经全部被消费处理
        return isConsumed;
    }


    /**
     * 在解析完每个分区后，我们使用解析的总字节数和记录数更新当前的指标总数。
     * 在所有分区都报告完毕后，我们写入该指标。
     * 应用场景：用于监控消费者拉取数据的性能和数量。
     * 实现细节：调用 metricAggregator 的 record 方法记录指定分区的字节数和记录数。
     * 设计考虑：将指标聚合的逻辑委托给 FetchMetricsAggregator，使得 CompletedFetch 类本身更关注数据处理。
     * @param bytes 解析的字节数。
     * @param records 解析的记录数。
     */
    void recordAggregatedMetrics(int bytes, int records) {
        // 调用 metricAggregator 的 record 方法，记录当前分区的字节数和记录数
        // partition 是当前 CompletedFetch 关联的主题分区
        // bytes 是本次处理的字节数
        // records 是本次处理的记录数
        metricAggregator.record(partition, bytes, records);
    }

    /**
     * “耗尽”一个 {@link CompletedFetch} 实例将表明其数据已被消费，并且底层资源已关闭。
     * 这有点类似于 {@link Closeable#close() 关闭}，但如果调用者调用 {@link #fetchRecords(FetchConfig, Deserializers, int)}，则不会产生错误；
     * 相反，将返回一个空的 {@link List 列表}。
     * 应用场景：当一个 CompletedFetch 中的所有记录都已处理完毕，或者不再需要其中的数据时，调用此方法来释放资源并标记其为已消费。
     * 实现细节：设置 isConsumed 标志，关闭记录流，记录指标，并在读取到字节时将分区移到订阅列表末尾。
     * 设计考虑：提供一个明确的方法来指示数据消费完成并进行清理，避免资源泄漏。将分区移到末尾是为了优化后续的序列化效率。
     */
    void drain() { // drain 方法定义：标记此 CompletedFetch 已被消费并释放资源。
        // 检查此 CompletedFetch 是否已经被消费
        if (!isConsumed) {
            // 如果尚未消费，则尝试关闭记录流
            maybeCloseRecordStream();
            // 清除缓存的记录异常
            cachedRecordException = null;
            // 将 isConsumed 标志设置为 true，表示此 CompletedFetch 已被消费
            this.isConsumed = true;
            // 记录聚合的指标，包括读取的字节数和记录数
            recordAggregatedMetrics(bytesRead, recordsRead);

            // 如果读取到的字节数大于0，则将该分区移到订阅列表的末尾。
            // 这样做是为了让相同主题的分区更有可能保持在一起（从而实现更高效的序列化）。
            // 设计考虑：这是一种优化手段，旨在提高后续操作（如提交偏移量或序列化分区信息）的效率，
            // 通过将活跃的分区（即有数据读取的分区）聚集在一起，可能减少数据结构调整的开销或提高局部性。
            if (bytesRead > 0)
                // 调用 subscriptions 对象的 movePartitionToEnd 方法，将当前分区移动到末尾
                subscriptions.movePartitionToEnd(partition);
        }
    } // drain 方法结束

    /**
     * 根据 FetchConfig 的配置，可能对 RecordBatch 进行校验。
     * 应用场景：在处理从 broker 拉取到的 RecordBatch 之前，如果配置了 CRC 校验，则执行此校验以确保数据完整性。
     * 实现细节：仅当 fetchConfig.checkCrcs 为 true 且批次的 magic 值大于等于 V2 时才执行校验。
     * 设计考虑：CRC 校验会消耗 CPU 资源，因此提供配置开关。V2 及以上版本的消息格式才支持这种校验方式。
     * @param fetchConfig 拉取配置，包含是否检查 CRC 等信息。
     * @param batch 要校验的记录批次。
     * @throws KafkaException 如果记录批次无效。
     */
    private void maybeEnsureValid(FetchConfig fetchConfig, RecordBatch batch) { // maybeEnsureValid 方法定义 (针对 RecordBatch)
        // 检查是否需要进行 CRC 校验 (fetchConfig.checkCrcs 为 true)
        // 并且记录批次的 magic 值是否大于等于 RecordBatch.MAGIC_VALUE_V2 (消息格式版本支持 CRC 校验)
        if (fetchConfig.checkCrcs && batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
            // 如果满足条件，则尝试校验批次
            try {
                // 调用 RecordBatch 对象的 ensureValid 方法进行校验
                batch.ensureValid();
            } catch (CorruptRecordException e) {
                // 如果校验过程中捕获到 CorruptRecordException (记录损坏异常)
                // 则抛出 KafkaException，包装原始异常信息，并指明分区、偏移量等上下文信息
                throw new KafkaException("Record batch for partition " + partition + " at offset " +
                        batch.baseOffset() + " is invalid, cause: " + e.getMessage());
            }
        }
    } // maybeEnsureValid (RecordBatch) 方法结束

    /**
     * 根据 FetchConfig 的配置，可能对单条 Record 进行校验。
     * 应用场景：在处理从 broker 拉取到的单条 Record 之前，如果配置了 CRC 校验，则执行此校验。
     * 实现细节：仅当 fetchConfig.checkCrcs 为 true 时执行校验。
     * 设计考虑：与批次校验类似，提供单条记录的校验能力。
     * @param fetchConfig 拉取配置。
     * @param record 要校验的记录。
     * @throws KafkaException 如果记录无效。
     */
    private void maybeEnsureValid(FetchConfig fetchConfig, Record record) { // maybeEnsureValid 方法定义 (针对 Record)
        // 检查是否需要进行 CRC 校验 (fetchConfig.checkCrcs 为 true)
        if (fetchConfig.checkCrcs) {
            // 如果需要，则尝试校验记录
            try {
                // 调用 Record 对象的 ensureValid 方法进行校验
                record.ensureValid();
            } catch (CorruptRecordException e) {
                // 如果校验过程中捕获到 CorruptRecordException (记录损坏异常)
                // 则抛出 KafkaException，包装原始异常信息，并指明分区、偏移量等上下文信息
                throw new KafkaException("Record for partition " + partition + " at offset " + record.offset()
                        + " is invalid, cause: " + e.getMessage());
            }
        }
    } // maybeEnsureValid (Record) 方法结束

    /**
     * 如果记录迭代器 (records) 不为 null，则关闭它并将其设置为 null。
     * 应用场景：在处理完一个 RecordBatch 或者 CompletedFetch 被 drain 时，需要关闭底层的记录迭代器以释放资源。
     * 实现细节：检查 records 是否为 null，如果不为 null，则调用其 close() 方法，并将其引用设置为 null。
     * 设计考虑：确保资源被正确释放，避免潜在的资源泄漏。设置为 null 有助于垃圾回收。
     */
    private void maybeCloseRecordStream() { // maybeCloseRecordStream 方法定义
        // 检查当前的记录迭代器 (records) 是否不为 null
        if (records != null) {
            // 如果不为 null，则调用其 close() 方法关闭迭代器，释放相关资源
            records.close();
            // 将 records 引用设置为 null，以便垃圾回收，并表示当前没有活动的记录迭代器
            records = null;
        }
    } // maybeCloseRecordStream 方法结束

    /**
     * 获取下一条要处理的记录。
     * 此方法会处理批次迭代、记录迭代、事务过滤、CRC 校验和控制记录跳过等逻辑。
     * 应用场景：消费者调用 {@link #fetchRecords(FetchConfig, Deserializers, int)} 时，内部会循环调用此方法来获取指定数量的有效记录。
     * 实现细节：
     * 1. 如果当前记录迭代器为空或已耗尽，则尝试获取下一个批次。
     * 2. 如果没有更多批次，则更新 nextFetchOffset，耗尽此 CompletedFetch 并返回 null。
     * 3. 获取新批次后，更新 leader epoch，校验批次（如果需要）。
     * 4. 如果是 READ_COMMITTED 隔离级别且批次包含生产者ID，则处理中止的事务：
     *    a. 消费（即处理并从队列移除）所有在当前批次最后偏移量之前开始的中止事务。
     *    b. 如果当前批次是中止标记，则从 abortedProducerIds 中移除对应的生产者ID。
     *    c. 如果当前批次本身是已中止的，则跳过该批次，更新 nextFetchOffset 并继续循环。
     * 5. 从当前批次获取记录的流式迭代器。
     * 6. 如果当前记录迭代器有效，则获取下一条记录。
     * 7. 跳过偏移量小于 nextFetchOffset 的记录（这些是已经被处理过的或者是上一次拉取请求范围内的）。
     * 8. 对符合条件的记录进行 CRC 校验（如果需要）。
     * 9. 如果记录不是控制批次中的记录，则返回该记录。
     * 10. 如果是控制记录，则更新 nextFetchOffset 为该控制记录的偏移量 + 1，然后继续循环获取下一条记录。
     * 设计考虑：这是一个核心方法，封装了从原始拉取数据中提取并过滤用户可见记录的复杂逻辑。
     *          通过循环和条件判断，确保只返回有效的、符合隔离级别要求的、且未被消费的记录。
     * @param fetchConfig 拉取配置。
     * @return 下一条有效记录；如果没有更多记录，则返回 null。
     */
    private Record nextFetchedRecord(FetchConfig fetchConfig) { // nextFetchedRecord 方法定义
        // 无限循环，直到找到一个有效的记录或确定没有更多记录
        while (true) {
            // 检查当前记录迭代器 (records) 是否为 null 或者已经没有更多记录
            if (records == null || !records.hasNext()) {
                // 如果是，说明当前批次的记录已处理完毕，或者还没有开始处理任何批次
                // 尝试关闭当前的记录流 (如果存在)
                maybeCloseRecordStream();

                // 检查是否还有更多未处理的批次 (batches.hasNext())
                if (!batches.hasNext()) {
                    // 如果没有更多批次了，说明此 CompletedFetch 中的所有数据都已处理或尝试处理完毕
                    // 消息格式 v2 会保留批次中的最后一个偏移量，即使最后一个记录因压缩而被移除。
                    // 通过使用从批次中最后一个偏移量计算出的下一个偏移量，
                    // 我们可以确保下一次拉取的偏移量将指向下一个批次，从而避免不必要地重新拉取相同的批次
                    // (在最坏的情况下，消费者可能会卡住，重复拉取相同的批次)。
                    if (currentBatch != null) // 如果 currentBatch 不是 null (即至少处理过一个批次)
                        // 更新 nextFetchOffset 为当前批次的下一个偏移量
                        nextFetchOffset = currentBatch.nextOffset();
                    // 耗尽此 CompletedFetch，标记为已消费并释放资源
                    drain();
                    // 返回 null，表示没有更多记录可供获取
                    return null;
                }

                // 如果还有更多批次，则获取下一个批次
                currentBatch = batches.next();
                // 更新 lastEpoch，尝试从当前批次的 leader epoch 获取值
                lastEpoch = maybeLeaderEpoch(currentBatch.partitionLeaderEpoch());
                // 对当前批次进行校验 (如果配置了 CRC 校验)
                maybeEnsureValid(fetchConfig, currentBatch);

                // 检查隔离级别是否为 READ_COMMITTED 并且当前批次是否包含生产者 ID (即事务性消息)
                if (fetchConfig.isolationLevel == IsolationLevel.READ_COMMITTED && currentBatch.hasProducerId()) {
                    // 从中止事务队列中移除所有在当前批次最后偏移量之前开始的中止事务，
                    // 并将相关的 producerId 添加到中止生产者集合中。
                    consumeAbortedTransactionsUpTo(currentBatch.lastOffset());

                    // 获取当前批次的生产者 ID
                    long producerId = currentBatch.producerId();
                    // 检查当前批次是否包含中止标记 (ABORT marker)
                    if (containsAbortMarker(currentBatch)) {
                        // 如果是中止标记，说明这个事务被中止了，之前可能将此 producerId 加入了 abortedProducerIds，现在需要移除，
                        // 因为后续具有相同 producerId 的批次（如果事务重新开始并提交）应该是可见的。
                        abortedProducerIds.remove(producerId);
                    } else if (isBatchAborted(currentBatch)) {
                        // 如果当前批次根据 abortedProducerIds 判断是属于一个已中止的事务
                        // 记录调试信息，说明正在跳过这个已中止的记录批次
                        log.debug("Skipping aborted record batch from partition {} with producerId {} and " +
                                        "offsets {} to {}",
                                partition, producerId, currentBatch.baseOffset(), currentBatch.lastOffset());
                        // 更新 nextFetchOffset 为当前批次的下一个偏移量，准备跳过整个批次
                        nextFetchOffset = currentBatch.nextOffset();
                        // 继续外层 while 循环，获取下一个批次或记录
                        continue;
                    }
                }

                // 从当前批次获取记录的流式迭代器，使用提供的解压缩缓冲区供应器
                records = currentBatch.streamingIterator(decompressionBufferSupplier);
            } else {
                // 如果当前记录迭代器 (records) 有效且有下一条记录
                Record record = records.next(); // 获取下一条记录
                // 跳过任何超出范围的记录 (即记录的偏移量小于 nextFetchOffset 的记录)
                // 这些记录可能是上一次拉取请求中已经处理过的，或者因为某些原因需要跳过。
                if (record.offset() >= nextFetchOffset) {
                    // 只有当消息不应被跳过时，我们才进行验证。
                    maybeEnsureValid(fetchConfig, record); // 对记录进行校验 (如果配置了 CRC 校验)

                    // 控制记录不返回给用户
                    if (!currentBatch.isControlBatch()) {
                        // 如果当前批次不是控制批次 (即包含的是数据记录)
                        // 返回这条记录
                        return record;
                    } else {
                        // 如果是控制批次 (例如包含事务标记)
                        // 当我们跳过一个控制批次时，增加下一个拉取偏移量。
                        // 控制记录本身不返回给用户，但其偏移量会影响 nextFetchOffset。
                        nextFetchOffset = record.offset() + 1;
                        // 此处没有 continue，循环会继续尝试从当前 records 迭代器获取下一条记录，
                        // 或者如果 records 耗尽，则进入外层 if 处理下一个批次。
                    }
                }
                // 如果 record.offset() < nextFetchOffset，则这条记录被跳过，循环继续获取下一条记录。
            }
        }
    } // nextFetchedRecord 方法结束

    /**
     * 将 {@link RecordBatch 一批} {@link Record 记录} 转换为 {@link ConsumerRecord 消费者记录} 的 {@link List 列表} 并返回。
     * 在此步骤中执行 {@link Record 记录} 的键和值的 {@link BufferSupplier 解压缩} 和 {@link Deserializer 反序列化}。
     * 应用场景: 当消费者从 Kafka 获取一批原始记录后，此方法负责将这些原始记录转换成应用程序可直接使用的 ConsumerRecord 对象列表。
     * 实现细节: 遍历拉取到的记录，对每条记录进行反序列化处理，并处理可能发生的异常。同时更新已读记录数、字节数和下一个拉取偏移量。
     * 设计考虑: 通过参数 maxRecords 控制一次返回的记录数量，避免一次性处理过多数据导致内存压力。同时，通过缓存异常并在下次调用时重试，增强了容错性。
     *
     * @param fetchConfig 用于的 {@link FetchConfig 配置}
     * @param deserializers 用于将原始字节转换为期望的键和值类型的 {@link Deserializer 反序列化器}
     * @param maxRecords 要返回的记录数；返回的数量可能是 {@code 0 <= maxRecords}
     * @return {@link ConsumerRecord 消费者记录} 列表
     */
    <K, V> List<ConsumerRecord<K, V>> fetchRecords(FetchConfig fetchConfig,
                                                   Deserializers<K, V> deserializers,
                                                   int maxRecords) { // 方法定义开始，这是一个泛型方法，K代表键的类型，V代表值的类型
        // 在反序列化之前获取下一条记录时出错。
        // 检查上一条记录是否已标记为损坏
        if (corruptLastRecord)
            // 如果上一条记录损坏，则抛出 KafkaException，提示用户可能需要跳过该记录以继续消费
            throw new KafkaException("Received exception when fetching the next record from " + partition
                    + ". If needed, please seek past the record to "
                    + "continue consumption.", cachedRecordException);

        // 检查此 CompletedFetch 中的所有记录是否已经被消费
        if (isConsumed)
            // 如果已全部消费，则返回一个空的记录列表
            return Collections.emptyList();

        // 创建一个 ArrayList 用于存储将要返回的 ConsumerRecord 对象
        List<ConsumerRecord<K, V>> records = new ArrayList<>();

        // 开始 try-catch 块，以捕获在处理记录过程中可能发生的异常
        try {
            // 循环处理记录，最多处理 maxRecords 条记录
            for (int i = 0; i < maxRecords; i++) {
                // 仅当上次拉取没有异常时才移动到下一条记录。否则，我们应该
                // 使用上一条记录再次进行反序列化。
                // 检查上次解析记录时是否缓存了异常
                if (cachedRecordException == null) {
                    // 如果没有缓存异常，说明可以尝试获取下一条记录
                    // 将 corruptLastRecord 标记为 true，表示在获取下一条记录的过程中，如果发生异常，则认为这条记录是损坏的
                    corruptLastRecord = true;
                    // 调用 nextFetchedRecord 方法获取下一条有效的记录
                    lastRecord = nextFetchedRecord(fetchConfig);
                    // 成功获取记录后（或没有更多记录），将 corruptLastRecord 标记回 false
                    corruptLastRecord = false;
                }

                // 如果 lastRecord 为 null，说明没有更多可处理的记录了
                if (lastRecord == null)
                    // 跳出循环
                    break;

                // 从当前批次获取 leader epoch，并将其包装在 Optional 中
                Optional<Integer> leaderEpoch = maybeLeaderEpoch(currentBatch.partitionLeaderEpoch());
                // 获取当前批次的时间戳类型
                TimestampType timestampType = currentBatch.timestampType();
                // 调用 parseRecord 方法将原始 Record 对象解析为 ConsumerRecord 对象
                ConsumerRecord<K, V> record = parseRecord(deserializers, partition, leaderEpoch, timestampType, lastRecord);
                // 将解析后的 ConsumerRecord 添加到结果列表中
                records.add(record);
                // 增加已读记录数计数器
                recordsRead++;
                // 增加已读字节数计数器
                bytesRead += lastRecord.sizeInBytes();
                // 更新下一条要拉取的记录的偏移量，为当前记录的偏移量 + 1
                nextFetchOffset = lastRecord.offset() + 1;
                // 在某些情况下，反序列化可能已引发异常，并且重试可能成功，
                // 在这种情况下，我们允许用户继续前进。
                // 清除缓存的异常，因为这条记录已成功处理
                cachedRecordException = null;
            } // for 循环结束
        } catch (SerializationException se) { // 捕获反序列化异常
            // 将捕获到的 SerializationException 缓存起来
            cachedRecordException = se;
            // 如果记录列表为空（即第一条记录就发生反序列化异常），则直接抛出该异常
            if (records.isEmpty())
                throw se;
        } catch (KafkaException e) { // 捕获 Kafka 相关的其他异常
            // 将捕获到的 KafkaException 缓存起来
            cachedRecordException = e;
            // 如果记录列表为空，则包装原始异常并抛出新的 KafkaException，提示用户如何处理
            if (records.isEmpty())
                throw new KafkaException("Received exception when fetching the next record from " + partition
                        + ". If needed, please seek past the record to "
                        + "continue consumption.", e);
        } // try-catch 块结束
        // 返回包含已处理 ConsumerRecord 的列表
        return records;
    }

    /**
     * 解析记录条目，必要时反序列化键/值字段。
     * 应用场景: 将从 Kafka broker 获取的原始 Record 对象转换为应用程序可以使用的 ConsumerRecord 对象，这个过程包括键和值的反序列化。
     * 实现细节: 分别尝试反序列化键和值，如果发生异常，则记录错误并抛出 RecordDeserializationException。
     * 设计考虑: 将键和值的反序列化过程分开处理，可以更精确地定位反序列化失败的来源。使用 RecordDeserializationException 封装了详细的错误信息，便于问题排查。
     * @param deserializers 用于反序列化键和值的反序列化器集合
     * @param partition 当前记录所属的主题分区
     * @param leaderEpoch 当前记录的 leader epoch (如果可用)
     * @param timestampType 当前记录的时间戳类型
     * @param record 要解析的原始 Kafka 记录
     * @return 解析并反序列化后的 ConsumerRecord 对象
     */
    <K, V> ConsumerRecord<K, V> parseRecord(Deserializers<K, V> deserializers,
                                            TopicPartition partition,
                                            Optional<Integer> leaderEpoch,
                                            TimestampType timestampType,
                                            Record record) { // 方法定义开始，这是一个泛型方法
        // 从原始 Record 对象中获取键的字节缓冲区
        ByteBuffer keyBytes = record.key();
        // 从原始 Record 对象中获取值的字节缓冲区
        ByteBuffer valueBytes = record.value();
        // 从原始 Record 对象中获取头部信息，并封装成 RecordHeaders 对象
        Headers headers = new RecordHeaders(record.headers());
        // 声明泛型变量 key，用于存储反序列化后的键
        K key;
        // 声明泛型变量 value，用于存储反序列化后的值
        V value;
        // 开始 try-catch 块，用于捕获键反序列化过程中可能发生的异常
        try {
            // 如果 keyBytes 为 null，则 key 也为 null；否则，使用 keyDeserializer 反序列化 keyBytes
            key = keyBytes == null ? null : deserializers.keyDeserializer().deserialize(partition.topic(), headers, keyBytes);
        } catch (RuntimeException e) { // 捕获键反序列化时发生的运行时异常
            // 记录错误日志，包含反序列化器信息
            log.error("Key Deserializers with error: {}", deserializers);
            // 抛出自定义的 RecordDeserializationException，指明是键反序列化失败
            throw newRecordDeserializationException(DeserializationExceptionOrigin.KEY, partition, timestampType, record, e, headers);
        }
        // 开始 try-catch 块，用于捕获值反序列化过程中可能发生的异常
        try {
            // 如果 valueBytes 为 null，则 value 也为 null；否则，使用 valueDeserializer 反序列化 valueBytes
            value = valueBytes == null ? null : deserializers.valueDeserializer().deserialize(partition.topic(), headers, valueBytes);
        } catch (RuntimeException e) { // 捕获值反序列化时发生的运行时异常
            // 记录错误日志，包含反序列化器信息
            log.error("Value Deserializers with error: {}", deserializers);
            // 抛出自定义的 RecordDeserializationException，指明是值反序列化失败
            throw newRecordDeserializationException(DeserializationExceptionOrigin.VALUE, partition, timestampType, record, e, headers);
        }
        // 使用反序列化后的键、值以及原始记录的其他信息创建并返回一个新的 ConsumerRecord 对象
        return new ConsumerRecord<>(partition.topic(), // 主题名称
                partition.partition(), // 分区号
                record.offset(), // 记录的偏移量
                record.timestamp(), // 记录的时间戳
                timestampType, // 时间戳类型
                keyBytes == null ? ConsumerRecord.NULL_SIZE : keyBytes.remaining(), // 键的序列化后的大小，如果键为null则为-1
                valueBytes == null ? ConsumerRecord.NULL_SIZE : valueBytes.remaining(), // 值的序列化后的大小，如果值为null则为-1
                key, // 反序列化后的键
                value, // 反序列化后的值
                headers, // 记录的头部信息
                leaderEpoch); // leader epoch (如果可用)
    }

    /**
     * 创建一个新的 RecordDeserializationException 实例。
     * 应用场景: 当记录的键或值在反序列化过程中发生错误时，调用此方法来创建一个具体的异常对象。
     * 实现细节: 封装了反序列化失败的来源（键或值）、分区信息、偏移量、时间戳、原始键值字节、头部信息以及原始异常。
     * 设计考虑: 提供一个统一的方法来创建反序列化异常，确保异常信息的一致性和完整性，便于上层代码捕获和处理。
     * @param origin 反序列化异常的来源 (例如，键或值)
     * @param partition 发生异常的主题分区
     * @param timestampType 记录的时间戳类型
     * @param record 发生反序列化异常的原始记录
     * @param e 捕获到的原始运行时异常
     * @param headers 记录的头部信息
     * @return 构造好的 RecordDeserializationException 对象
     */
    private static RecordDeserializationException newRecordDeserializationException(DeserializationExceptionOrigin origin,
                                                                                    TopicPartition partition,
                                                                                    TimestampType timestampType,
                                                                                    Record record,
                                                                                    RuntimeException e,
                                                                                    Headers headers) { // 方法定义开始，这是一个静态私有方法
        // 创建并返回一个新的 RecordDeserializationException 实例
        return new RecordDeserializationException(origin, // 反序列化失败的来源（键或值）
                partition, // 发生异常的主题分区
                record.offset(), // 记录的偏移量
                record.timestamp(), // 记录的时间戳
                timestampType, // 时间戳类型
                record.key(), // 原始记录的键字节
                record.value(), // 原始记录的值字节
                headers, // 记录的头部信息
                // 详细的错误消息，包含来源、分区、偏移量以及建议操作
                "Error deserializing " + origin.name() + " for partition " + partition + " at offset " + record.offset()
                        + ". If needed, please seek past the record to continue consumption.",
                e); // 原始的运行时异常
    }

    /**
     * 根据提供的 leader epoch 值，返回一个 Optional<Integer>。
     * 应用场景: 当需要处理可能不存在的 leader epoch 时，此方法提供了一种安全的方式来包装它。
     * 实现细节: 如果 leaderEpoch 等于 RecordBatch.NO_PARTITION_LEADER_EPOCH (表示无效或不存在)，则返回 Optional.empty()，否则返回 Optional.of(leaderEpoch)。
     * 设计考虑: 使用 Optional 可以明确表示 leader epoch 的存在与否，避免空指针异常，并鼓励调用者处理 epoch 不存在的情况。
     * @param leaderEpoch leader epoch 的整数值
     * @return 如果 leaderEpoch 有效，则返回包含该值的 Optional；否则返回空的 Optional
     */
    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) { // 方法定义开始
        // 检查传入的 leaderEpoch 是否等于 RecordBatch.NO_PARTITION_LEADER_EPOCH (通常表示 leader epoch 未定义或无效)
        // 如果是，则返回一个空的 Optional 对象
        // 否则，将 leaderEpoch 包装在一个 Optional 对象中并返回
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
    } // maybeLeaderEpoch 方法结束

    /**
     * 处理（消费）所有起始偏移量小于或等于给定偏移量的已中止事务。
     * 应用场景: 在读取提交（READ_COMMITTED）隔离级别下，当处理到某个偏移量的记录时，需要将此偏移量之前所有已中止事务的生产者 ID 添加到 abortedProducerIds 集合中，以便后续过滤这些事务的记录。
     * 实现细节: 遍历 abortedTransactions 优先队列，如果队首事务的 firstOffset 小于等于传入的 offset，则将其从队列中移除并将其 producerId 添加到 abortedProducerIds 集合。
     * 设计考虑: 使用优先队列（按 firstOffset 排序）可以高效地找到并移除所有相关已中止事务。此方法确保在处理特定偏移量的记录之前，相关的中止事务信息已经更新。
     * @param offset 当前处理到的记录的偏移量
     */
    private void consumeAbortedTransactionsUpTo(long offset) { // 方法定义开始
        // 检查 abortedTransactions 列表是否为 null (即没有已中止的事务信息)
        if (abortedTransactions == null)
            // 如果为 null，则直接返回，不做任何操作
            return;

        // 当 abortedTransactions 队列不为空，并且队首事务的起始偏移量 (firstOffset) 小于或等于给定的 offset 时，循环处理
        while (!abortedTransactions.isEmpty() && abortedTransactions.peek().firstOffset() <= offset) {
            // 从队列中取出（并移除）队首的已中止事务信息
            FetchResponseData.AbortedTransaction abortedTransaction = abortedTransactions.poll();
            // 将该已中止事务的生产者 ID (producerId) 添加到 abortedProducerIds 集合中
            abortedProducerIds.add(abortedTransaction.producerId());
        }
    } // consumeAbortedTransactionsUpTo 方法结束

    /**
     * 检查给定的 RecordBatch 是否属于一个已中止的事务。
     * 应用场景: 在读取提交（READ_COMMITTED）隔离级别下，判断一个事务性批次是否应该被跳过。
     * 实现细节: 首先检查批次是否是事务性的 (batch.isTransactional())，然后检查该批次的生产者 ID 是否存在于 abortedProducerIds 集合中。
     * 设计考虑: 这是一个辅助方法，用于封装判断批次是否中止的逻辑，使代码更清晰。
     * @param batch 要检查的记录批次
     * @return 如果批次是事务性的并且其生产者 ID 在已中止生产者 ID 列表中，则返回 true；否则返回 false
     */
    private boolean isBatchAborted(RecordBatch batch) { // 方法定义开始
        // 检查批次是否是事务性的 (batch.isTransactional()) 并且
        // abortedProducerIds 集合中是否包含该批次的生产者 ID (batch.producerId())
        // 如果两者都为 true，则说明该批次属于一个已中止的事务
        return batch.isTransactional() && abortedProducerIds.contains(batch.producerId());
    } // isBatchAborted 方法结束

    /**
     * 从分区数据中提取已中止的事务信息，并将其放入一个按 firstOffset 排序的优先队列中。
     * 应用场景: 在 CompletedFetch 初始化时，需要将从 broker 获取的已中止事务列表转换为内部使用的数据结构。
     * 实现细节: 如果分区数据中没有已中止的事务或列表为空，则返回 null。否则，创建一个新的优先队列，使用 AbortedTransaction::firstOffset 作为比较器，并将所有已中止的事务添加到队列中。
     * 设计考虑: 使用优先队列可以方便地按起始偏移量顺序处理已中止的事务。返回 null 表示没有需要处理的中止事务。
     * @param partition 从 broker 拉取的分区数据
     * @return 一个包含已中止事务的优先队列（按 firstOffset 排序），如果分区数据中没有中止事务，则返回 null
     */
    private PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions(FetchResponseData.PartitionData partition) { // 方法定义开始
        // 检查分区数据中的已中止事务列表 (partition.abortedTransactions()) 是否为 null 或为空
        if (partition.abortedTransactions() == null || partition.abortedTransactions().isEmpty())
            // 如果是，则返回 null，表示没有已中止的事务
            return null;

        // 创建一个新的优先队列 (PriorityQueue)，用于存储已中止的事务信息
        // 队列的初始容量设置为分区数据中已中止事务的数量
        // 队列的比较器设置为比较 AbortedTransaction 对象的 firstOffset 字段 (升序)
        PriorityQueue<FetchResponseData.AbortedTransaction> abortedTransactions = new PriorityQueue<>(
                partition.abortedTransactions().size(), Comparator.comparingLong(FetchResponseData.AbortedTransaction::firstOffset)
        );
        // 将分区数据中的所有已中止事务添加到新创建的优先队列中
        abortedTransactions.addAll(partition.abortedTransactions());
        // 返回填充了已中止事务信息的优先队列
        return abortedTransactions;
    } // abortedTransactions 方法结束

    /**
     * 检查给定的 RecordBatch 是否包含事务中止标记 (ABORT marker)。
     * 应用场景: 在处理控制批次时，需要判断该批次是否是一个事务中止标记，以便进行相应的处理（例如，在读取未提交（READ_UNCOMMITTED）模式下也可能需要识别中止标记）。
     * 实现细节: 首先检查批次是否是控制批次 (batch.isControlBatch())。如果是，则获取批次的迭代器，并检查第一条记录的键是否能解析为 ControlRecordType.ABORT。
     * 设计考虑: 这是一个辅助方法，用于封装判断批次是否包含中止标记的逻辑。只检查第一条记录是因为控制批次通常只包含一个控制记录。
     * @param batch 要检查的记录批次
     * @return 如果批次是控制批次并且其第一条记录是 ABORT 类型的控制记录，则返回 true；否则返回 false
     */
    private boolean containsAbortMarker(RecordBatch batch) { // 方法定义开始
        // 检查批次是否是控制批次 (Control Batch)
        if (!batch.isControlBatch())
            // 如果不是控制批次，则它不可能包含中止标记，直接返回 false
            return false;

        // 获取该控制批次中记录的迭代器
        Iterator<Record> batchIterator = batch.iterator();
        // 检查迭代器是否至少有一个记录
        if (!batchIterator.hasNext())
            // 如果没有记录（理论上控制批次应该至少有一条控制记录），则返回 false
            return false;

        // 获取批次中的第一条记录（控制批次通常只有一条控制记录）
        Record firstRecord = batchIterator.next();
        // 解析第一条记录的键 (key) 以获取其控制记录类型 (ControlRecordType)
        // 然后检查该类型是否为 ControlRecordType.ABORT (中止标记)
        // 如果是，则返回 true；否则返回 false
        return ControlRecordType.ABORT == ControlRecordType.parse(firstRecord.key());
    } // containsAbortMarker 方法结束
}
