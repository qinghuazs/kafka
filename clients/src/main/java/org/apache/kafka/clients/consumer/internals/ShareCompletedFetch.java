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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;

/**
 * {@link ShareCompletedFetch} 代表通过 {@link ShareFetchRequest} 从 broker 返回的 {@link RecordBatch 一批} {@link Record 记录}。
 * 它包含了在多次调用 {@link #fetchRecords(Deserializers, int, boolean)} 之间维护状态的逻辑。
 * 虽然它与 {@link CompletedFetch} 有相似之处，但细节却大相径庭，例如不需要跟踪中止的事务，也不需要跟踪获取位置。
 * 应用场景：当共享消费者从 Kafka broker 获取数据后，需要一个对象来封装这些数据以及相关的元信息和处理状态，特别是针对共享消费模式下的记录获取。
 * 实现细节：此类封装了分区信息、分区数据、请求版本号，并管理已获取记录列表、批次迭代器等，以支持共享消费场景下的记录迭代和状态维护。
 * 设计考虑：与常规的 `CompletedFetch` 不同，`ShareCompletedFetch` 专注于共享消费的特定需求，例如处理 `acquiredRecords`（已获取记录）列表，
 *          这表示哪些记录已经被分配给这个特定的共享消费者实例。它不需要处理事务相关的逻辑，因为共享消费通常不涉及事务性读取。
 */
public class ShareCompletedFetch {

    // final TopicIdPartition partition; // 主题分区ID，表示此拉取结果属于哪个主题的哪个分区。设计考虑：final确保一旦设置不可更改，保证数据一致性。
    final TopicIdPartition partition;
    // final ShareFetchResponseData.PartitionData partitionData; // 从 ShareFetchResponse 中获取的原始分区数据。设计考虑：存储原始响应数据，用于后续处理和状态管理。
    final ShareFetchResponseData.PartitionData partitionData;
    // final short requestVersion; // 用于获取此拉取的 ShareFetchRequest 的版本。设计考虑：请求版本可能影响响应的解析方式或行为。
    final short requestVersion;

    // private final Logger log; // 日志记录器。设计考虑：用于记录此类操作过程中的信息和错误。
    private final Logger log;
    // private final BufferSupplier decompressionBufferSupplier; // 用于解压缩记录的缓冲区供应器。设计考虑：提供缓冲区以支持高效的解压缩操作。
    private final BufferSupplier decompressionBufferSupplier;
    // private final Iterator<? extends RecordBatch> batches; // 从 ShareFetchResponse 获取的 RecordBatch 迭代器。设计考虑：使用迭代器可以按需处理批次，避免一次性加载所有数据到内存。
    private final Iterator<? extends RecordBatch> batches;
    // private int recordsRead; // 已从此 ShareCompletedFetch 中读取的记录数。
    private int recordsRead;
    // private int bytesRead; // 已从此 ShareCompletedFetch 中读取的字节数。
    private int bytesRead;
    // private RecordBatch currentBatch; // 当前正在处理的 RecordBatch。
    private RecordBatch currentBatch;
    // private Record lastRecord; // 上一条处理的记录。
    private Record lastRecord;
    // private CloseableIterator<Record> records; // 当前 RecordBatch 中记录的可关闭迭代器。
    private CloseableIterator<Record> records;
    // private KafkaException cachedBatchException = null; // 在处理批次时缓存的异常（例如CRC校验失败）。
    private KafkaException cachedBatchException = null;
    // private KafkaException cachedRecordException = null; // 在解析单个记录时缓存的异常（例如反序列化失败）。
    private KafkaException cachedRecordException = null;
    // private boolean isConsumed = false; // 标记此 ShareCompletedFetch 中的所有记录是否已被消费或处理完毕。
    private boolean isConsumed = false;
    // private boolean initialized = false; // 标记此 ShareCompletedFetch 是否已初始化。在共享消费场景下，可能表示某些前置条件已满足。
    private boolean initialized = false;
    // private final List<OffsetAndDeliveryCount> acquiredRecordList; // 已获取记录的列表，包含偏移量和投递次数。这是共享消费特有的，表示哪些记录被分配给了当前消费者。
    private final List<OffsetAndDeliveryCount> acquiredRecordList;
    // private ListIterator<OffsetAndDeliveryCount> acquiredRecordIterator; // acquiredRecordList 的列表迭代器，用于遍历已获取的记录。
    private ListIterator<OffsetAndDeliveryCount> acquiredRecordIterator;
    // private OffsetAndDeliveryCount nextAcquired; // 下一个待处理的已获取记录。
    private OffsetAndDeliveryCount nextAcquired;
    // private final ShareFetchMetricsAggregator metricAggregator; // 共享拉取指标聚合器。设计考虑：用于收集和报告与此共享拉取相关的指标。
    private final ShareFetchMetricsAggregator metricAggregator;

    /**
     * ShareCompletedFetch 的构造函数。
     * 应用场景：在共享消费者收到 ShareFetchResponse 后，为每个成功获取数据的分区创建一个 ShareCompletedFetch 实例。
     * 实现细节：初始化所有 final 字段，并从 partitionData 中提取批次迭代器和构建 acquiredRecordList。
     * 设计考虑：构造函数接收所有必要的依赖项，确保 ShareCompletedFetch 实例在创建时就拥有了处理共享拉取数据所需的所有信息。
     *
     * @param logContext 日志上下文，用于创建日志记录器。
     * @param decompressionBufferSupplier 用于解压缩的缓冲区供应器。
     * @param partition 当前数据所属的主题分区ID。
     * @param partitionData 从 broker 返回的特定分区的数据。
     * @param metricAggregator 用于聚合共享拉取指标的聚合器。
     * @param requestVersion 本次共享拉取请求的版本号。
     */
    ShareCompletedFetch(final LogContext logContext,
                        final BufferSupplier decompressionBufferSupplier,
                        final TopicIdPartition partition,
                        final ShareFetchResponseData.PartitionData partitionData,
                        final ShareFetchMetricsAggregator metricAggregator,
                        final short requestVersion) {
        // 初始化日志记录器, 使用传入的 logContext 为 ShareCompletedFetch 类创建一个特定的 logger 实例
        this.log = logContext.logger(org.apache.kafka.clients.consumer.internals.ShareCompletedFetch.class);
        // 初始化解压缩缓冲区供应器引用, 保存传入的 decompressionBufferSupplier，用于解压缩消息时获取缓冲区
        this.decompressionBufferSupplier = decompressionBufferSupplier;
        // 初始化主题分区ID引用, 保存当前 ShareCompletedFetch 实例关联的 TopicIdPartition
        this.partition = partition;
        // 初始化分区数据引用, 保存从 Broker 拉取到的原始分区数据
        this.partitionData = partitionData;
        // 初始化共享指标聚合器引用, 保存传入的 metricAggregator，用于记录相关的共享 Fetch 指标
        this.metricAggregator = metricAggregator;
        // 初始化请求版本, 保存发出此 ShareFetch 请求时使用的 API 版本号
        this.requestVersion = requestVersion;
        // 从分区数据中获取记录批次的迭代器；如果记录为空或获取失败，则抛出异常
        // ShareFetchResponse.recordsOrFail(partitionData) 会检查 partitionData 中的记录是否有效，如果无效（例如有错误码），则抛出异常
        // .batches() 将记录转换为 RecordBatch 的流
        // .iterator() 获取这个流的迭代器
        this.batches = ShareFetchResponse.recordsOrFail(partitionData).batches().iterator();
        // 构建已获取记录列表，这是共享消费的核心部分，表示哪些记录范围被分配给了这个消费者
        // partitionData.acquiredRecords() 返回一个列表，其中每个元素代表一个或多个连续的已获取记录段及其投递次数
        this.acquiredRecordList = buildAcquiredRecordList(partitionData.acquiredRecords());
        // 初始化下一个已获取记录为 null，将在首次迭代时填充
        this.nextAcquired = null;
    }

    /**
     * 根据从 broker 返回的已获取记录信息构建一个 {@link OffsetAndDeliveryCount} 列表。
     * 应用场景：在 ShareCompletedFetch 初始化时，需要将 broker 返回的 `AcquiredRecords` 数据结构转换为内部更易于处理的列表形式。
     *           `AcquiredRecords` 可能表示一个偏移量范围，此方法会将其展开为单个偏移量和对应的投递次数。
     * 实现细节：遍历 `partitionAcquiredRecords` 中的每个 `AcquiredRecords` 对象。对于每个对象，
     *           从其 `firstOffset` 到 `lastOffset`（包含两者）的每个偏移量，都创建一个新的 `OffsetAndDeliveryCount`
     *           实例（使用该偏移量和 `AcquiredRecords` 的 `deliveryCount`），并将其添加到结果列表中。
     * 设计考虑：将 broker 返回的紧凑表示（可能是一个范围）转换为扁平化的列表，方便后续按偏移量逐个匹配和处理已获取的记录。
     *           使用 LinkedList 可能考虑到后续会有迭代器操作，但如果列表主要用于构建后随机访问或大小固定，ArrayList 可能更优。
     *           此处选择 LinkedList 可能是因为后续会使用 ListIterator。
     *
     * @param partitionAcquiredRecords 从 {@link ShareFetchResponseData.PartitionData#acquiredRecords()} 获取的已获取记录列表。
     * @return 一个包含所有已获取记录的偏移量和对应投递次数的 {@link OffsetAndDeliveryCount} 列表。
     */
    private List<OffsetAndDeliveryCount> buildAcquiredRecordList(List<ShareFetchResponseData.AcquiredRecords> partitionAcquiredRecords) {
        // 创建一个 LinkedList 用于存储转换后的 OffsetAndDeliveryCount 对象
        List<OffsetAndDeliveryCount> acquiredRecordList = new LinkedList<>();
        // 遍历从 broker 获取的每个 AcquiredRecords 段
        partitionAcquiredRecords.forEach(acquiredRecords -> {
            // 对于每个 AcquiredRecords 段，它代表一个从 firstOffset 到 lastOffset 的连续记录范围
            // 遍历这个范围内的每一个偏移量
            for (long offset = acquiredRecords.firstOffset(); offset <= acquiredRecords.lastOffset(); offset++) {
                // 为当前偏移量创建一个新的 OffsetAndDeliveryCount 对象，
                // 其中包含偏移量本身和该段记录的投递次数 (acquiredRecords.deliveryCount())
                acquiredRecordList.add(new OffsetAndDeliveryCount(offset, acquiredRecords.deliveryCount()));
            }
        });
        // 返回构建好的 OffsetAndDeliveryCount 列表
        return acquiredRecordList;
    }

    /**
     * 检查此 ShareCompletedFetch 是否已初始化。
     * 应用场景：在某些操作（如实际获取记录之前）可能需要确认 ShareCompletedFetch 实例是否已完成其初始化流程。
     *           例如，确保已获取记录列表已构建，或某些内部状态已设置。
     * 实现细节：简单返回 {@code initialized} 字段的布尔值。
     * 设计考虑：提供一个明确的状态查询方法，允许外部代码根据其初始化状态执行不同逻辑。
     *
     * @return 如果已初始化，则返回 true；否则返回 false。
     */
    boolean isInitialized() {
        // 返回 initialized 字段的值，表示此 ShareCompletedFetch 是否已初始化
        return initialized;
    }

    /**
     * 将此 ShareCompletedFetch 标记为已初始化。
     * 应用场景：当 ShareCompletedFetch 完成了所有必要的设置和准备工作后（例如，成功构建了 acquiredRecordList），
     *           可以调用此方法将其状态更新为已初始化。
     * 实现细节：将 {@code initialized} 字段设置为 true。
     * 设计考虑：提供一个明确的方法来更新初始化状态，而不是直接暴露字段。
     */
    void setInitialized() {
        // 将 initialized 字段设置为 true，标记此实例已完成初始化
        this.initialized = true;
    }

    /**
     * 检查此 ShareCompletedFetch 中的数据是否已被完全消费或处理。
     * 应用场景：用于判断是否可以安全地丢弃此 ShareCompletedFetch 实例，或者是否还有待处理的数据。
     *           例如，在 {@link #drain()} 方法被调用后，此方法将返回 true。
     * 实现细节：简单返回 {@code isConsumed} 字段的布尔值。
     * 设计考虑：提供一个清晰的接口来查询消费状态，有助于管理 ShareCompletedFetch 对象的生命周期。
     *
     * @return 如果数据已被消费，则返回 true；否则返回 false。
     */
    public boolean isConsumed() {
        // 返回 isConsumed 字段的值，表示此 ShareCompletedFetch 中的数据是否已处理完毕
        return isConsumed;
    }

    /**
     * 清空（Draining）一个 {@link ShareCompletedFetch} 实例将表明其数据已被消费，并且底层资源已关闭。
     * 这有点类似于 {@link Closeable#close() 关闭}，尽管如果调用者调用 {@link #fetchRecords(Deserializers, int, boolean)} 不会产生错误；
     * 相反，将返回一个空的 {@link List 列表}。
     * 应用场景：当一个 ShareCompletedFetch 中的所有数据都处理完毕，或者需要提前释放资源时调用。
     * 实现细节：如果尚未标记为已消费，则关闭记录流，清除缓存的异常，标记为已消费，并记录聚合指标。
     * 设计考虑：提供一种明确的方式来标记数据已处理完并释放资源，同时处理后续调用 fetchRecords 的情况，避免抛出异常。
     */
    void drain() {
        // 检查此 ShareCompletedFetch 是否已经被消费
        if (!isConsumed) {
            // 如果尚未消费，则尝试关闭记录流（如果存在）
            maybeCloseRecordStream();
            // 清除缓存的记录级别异常
            cachedRecordException = null;
            // 清除缓存的批处理级别异常
            cachedBatchException = null;
            // 将此 ShareCompletedFetch 标记为已消费
            this.isConsumed = true;
            // 记录已读取的字节数和记录数到聚合指标中
            recordAggregatedMetrics(bytesRead, recordsRead);
        }
    }

    /**
     * 在解析完每个分区后，我们使用解析的总字节数和记录数更新当前的指标总数。
     * 在所有分区都报告完毕后，我们写入该指标。
     * 应用场景：在数据处理完成后，需要将处理过程中的统计数据（如读取的字节数和记录数）聚合到监控指标中。
     * 实现细节：调用 metricAggregator 的 record 方法，传入分区信息、字节数和记录数。
     * 设计考虑：将指标记录逻辑封装在此方法中，便于在适当的时候调用，保持 ShareCompletedFetch 类的职责清晰。
     * @param bytes 本次处理的字节数。
     * @param records 本次处理的记录数。
     */
    void recordAggregatedMetrics(int bytes, int records) {
        // 调用指标聚合器的 record 方法，记录指定分区的字节数和记录数
        metricAggregator.record(partition.topicPartition(), bytes, records);
    }

    /**
     * {@link RecordBatch 一批} {@link Record 记录} 被转换为 {@link ConsumerRecord 消费者记录} 的 {@link List 列表} 并返回。
     * {@link Record 记录}的键和值的 {@link BufferSupplier 解压缩} 和 {@link Deserializer 反序列化} 在此步骤中执行。
     * 应用场景：这是共享消费者从 ShareCompletedFetch 实例中获取实际业务数据的核心方法。
     * 实现细节：处理缓存的异常，然后迭代拉取到的记录批次和记录，与已获取的记录（acquired records）进行匹配。
     *           只有匹配上的记录才会被反序列化并添加到返回的 ShareInFlightBatch 中。同时处理各种异常情况。
     * 设计考虑：该方法负责将底层的 RecordBatch 转换为上层应用可直接使用的 ConsumerRecord 列表，并处理共享消费模式下的记录匹配逻辑。
     *           通过 ShareInFlightBatch 返回结果，其中包含了记录以及可能的异常信息和间隙信息。
     *
     * @param deserializers {@link Deserializer} 用于将原始字节转换为预期的键和值类型的反序列化器。
     * @param maxRecords 要返回的记录数；返回的数量可能是 {@code 0 <= maxRecords}。
     * @param checkCrcs 是否检查获取记录的 CRC。
     *
     * @return {@link ShareInFlightBatch ShareInFlightBatch，包含记录及其确认信息}。
     */
    <K, V> ShareInFlightBatch<K, V> fetchRecords(final Deserializers<K, V> deserializers,
                                                 final int maxRecords,
                                                 final boolean checkCrcs) {
        // 创建一个空的 ShareInFlightBatch，用于存放本次获取的记录和相关信息
        ShareInFlightBatch<K, V> inFlightBatch = new ShareInFlightBatch<>(partition);

        // 检查是否有缓存的批处理级别异常（如CRC校验失败）
        if (cachedBatchException != null) {
            // 如果存在批处理异常，说明整个批次已损坏，拒绝该批次中的所有记录
            rejectRecordBatch(inFlightBatch, currentBatch);
            // 将缓存的批处理异常设置到 inFlightBatch 中
            inFlightBatch.setException(cachedBatchException);
            // 清除缓存的批处理异常，因为它已经被处理
            cachedBatchException = null;
            // 返回包含异常信息的 inFlightBatch
            return inFlightBatch;
        }

        // 检查是否有缓存的记录级别异常（如反序列化失败）
        if (cachedRecordException != null) {
            // 如果存在记录异常，为上一条导致异常的记录添加一个 RELEASE 类型的确认
            inFlightBatch.addAcknowledgement(lastRecord.offset(), AcknowledgeType.RELEASE);
            // 将缓存的记录异常设置到 inFlightBatch 中
            inFlightBatch.setException(cachedRecordException);
            // 清除缓存的记录异常，因为它已经被处理
            cachedRecordException = null;
            // 返回包含异常信息的 inFlightBatch
            return inFlightBatch;
        }

        // 如果此 ShareCompletedFetch 中的所有记录已被消费，则直接返回空的 inFlightBatch
        if (isConsumed)
            return inFlightBatch;

        // 初始化 acquiredRecordIterator 和 nextAcquired，如果它们尚未初始化
        initializeNextAcquired();

        try {
            // 初始化当前批次中已添加的记录数
            int recordsInBatch = 0;
            // 标记当前 RecordBatch 是否还有更多记录未处理
            boolean currentBatchHasMoreRecords = false;

            // 循环直到达到 maxRecords 限制，或者当前批次没有更多记录且所有批次已处理完毕
            while (recordsInBatch < maxRecords || currentBatchHasMoreRecords) {
                // 获取下一条已拉取的记录（如果当前批次有），并更新 currentBatchHasMoreRecords 状态
                currentBatchHasMoreRecords = nextFetchedRecord(checkCrcs);
                // 如果 lastRecord 为 null，表示所有已拉取的记录都已处理完毕
                if (lastRecord == null) {
                    // 此时，任何剩余的 acquired records 都被视为空缺（gap）
                    while (nextAcquired != null) {
                        // 将空缺的偏移量添加到 inFlightBatch
                        inFlightBatch.addGap(nextAcquired.offset);
                        // 移动到下一个 acquired record
                        nextAcquired = nextAcquiredRecord();
                    }
                    // 所有记录处理完毕，跳出外层 while 循环
                    break;
                }

                // 内部循环，用于将当前拉取的记录 (lastRecord) 与 acquired records 列表进行匹配
                while (nextAcquired != null) {
                    // 如果当前拉取记录的偏移量与下一个已获取记录的偏移量相同
                    if (lastRecord.offset() == nextAcquired.offset) {
                        // 这条记录是已获取的，因此解析它并将其添加到批处理中
                        // 获取当前批次的 leader epoch (如果存在)
                        Optional<Integer> leaderEpoch = maybeLeaderEpoch(currentBatch.partitionLeaderEpoch());
                        // 获取当前批次的时间戳类型
                        TimestampType timestampType = currentBatch.timestampType();
                        // 解析记录，将其从 Record 转换为 ConsumerRecord
                        ConsumerRecord<K, V> record = parseRecord(deserializers, partition, leaderEpoch,
                                timestampType, lastRecord, nextAcquired.deliveryCount);
                        // 将解析后的 ConsumerRecord 添加到 inFlightBatch
                        inFlightBatch.addRecord(record);
                        // 增加已读记录数计数器
                        recordsRead++;
                        // 增加已读字节数计数器
                        bytesRead += lastRecord.sizeInBytes();
                        // 增加当前批次中已添加的记录数
                        recordsInBatch++;

                        // 移动到下一个 acquired record
                        nextAcquired = nextAcquiredRecord();
                        // 已成功匹配并处理一个记录，跳出内部 while 循环，去获取下一条拉取的记录
                        break;
                    } else if (lastRecord.offset() < nextAcquired.offset) {
                        // 当前拉取的记录偏移量小于下一个已获取记录的偏移量
                        // 这意味着当前拉取的记录未被获取（或者是一个跳过的记录），我们不处理它
                        // 跳出内部 while 循环，去获取下一条拉取的记录
                        break;
                    } else {
                        // 当前拉取的记录偏移量大于下一个已获取记录的偏移量 (lastRecord.offset() > nextAcquired.offset)
                        // 这意味着 nextAcquired.offset 处的记录是一个空缺（没有对应的非控制记录）
                        inFlightBatch.addGap(nextAcquired.offset);
                        // 继续检查下一个 acquired record
                        nextAcquired = nextAcquiredRecord();
                    }
                }
            }
        } catch (SerializationException se) {
            // 捕获序列化/反序列化异常
            // 尝试移动到下一个 acquired record，因为当前这个可能与导致异常的记录有关
            nextAcquired = nextAcquiredRecord();
            // 如果当前 inFlightBatch 为空，说明这是第一个导致异常的记录
            if (inFlightBatch.isEmpty()) {
                // 为导致异常的记录（lastRecord）添加 RELEASE 确认
                inFlightBatch.addAcknowledgement(lastRecord.offset(), AcknowledgeType.RELEASE);
                // 将异常设置到 inFlightBatch 中
                inFlightBatch.setException(se);
            } else {
                // 如果 inFlightBatch 不为空，说明之前已有成功解析的记录
                // 将此异常缓存起来，以便在下一次调用 fetchRecords 时处理
                cachedRecordException = se;
                // 标记 inFlightBatch 包含一个缓存的异常
                inFlightBatch.setHasCachedException(true);
            }
        } catch (CorruptRecordException e) {
            // 捕获记录损坏异常（通常由 CRC 校验失败引起）
            // 如果当前 inFlightBatch 为空，说明这是第一个导致异常的记录（或批次）
            if (inFlightBatch.isEmpty()) {
                // 如果由于CRC校验失败导致事件发生，则拒绝整个记录批次，因为它已损坏。
                rejectRecordBatch(inFlightBatch, currentBatch);
                // 将异常设置到 inFlightBatch 中
                inFlightBatch.setException(e);
            } else {
                // 如果 inFlightBatch 不为空，说明之前已有成功解析的记录
                // 将此批处理级别的异常缓存起来，以便在下一次调用 fetchRecords 时处理
                cachedBatchException = e;
                // 标记 inFlightBatch 包含一个缓存的异常
                inFlightBatch.setHasCachedException(true);
            }
        }

        // 返回包含已获取记录、空缺信息或异常的 inFlightBatch
        return inFlightBatch;
    }

    /**
     * 初始化下一个已获取的记录 (nextAcquired)。
     * 应用场景：在迭代处理已获取记录之前，需要确保 nextAcquired 字段被正确初始化。
     *           如果 nextAcquired 为 null，则尝试从 acquiredRecordIterator 获取下一个元素。
     * 实现细节：首先检查 nextAcquired 是否为 null。如果是，则进一步检查 acquiredRecordIterator 是否已初始化。
     *           如果 acquiredRecordIterator 未初始化，则从 acquiredRecordList 创建一个新的列表迭代器。
     *           然后，如果迭代器有下一个元素，则将其赋给 nextAcquired。
     * 设计考虑：延迟初始化 acquiredRecordIterator 和 nextAcquired，仅在需要时才进行，可以略微提高效率，
     *           特别是在 acquiredRecordList 为空的情况下。此方法确保在访问 nextAcquired 之前它已经被适当地填充。
     */
    private void initializeNextAcquired() {
        // 检查 nextAcquired 是否为 null，即是否需要初始化
        if (nextAcquired == null) {
            // 如果 acquiredRecordIterator 尚未初始化
            if (acquiredRecordIterator == null) {
                // 从 acquiredRecordList 获取列表迭代器并赋给 acquiredRecordIterator
                acquiredRecordIterator = acquiredRecordList.listIterator();
            }
            // 如果 acquiredRecordIterator 还有下一个元素
            if (acquiredRecordIterator.hasNext()) {
                // 获取下一个已获取的记录并赋给 nextAcquired
                nextAcquired = acquiredRecordIterator.next();
            }
        }
    }

    /**
     * 从已获取记录迭代器中获取下一个 {@link OffsetAndDeliveryCount}。
     * 应用场景：当需要按顺序处理已分配给当前消费者的记录时，调用此方法获取下一个待处理的记录信息。
     * 实现细节：检查 {@code acquiredRecordIterator} 是否有下一个元素。如果有，则返回下一个元素；否则返回 null。
     * 设计考虑：封装了迭代器操作，提供一个简洁的方法来获取下一个已获取的记录。返回 null 表示没有更多已获取的记录。
     *           此方法假定 {@code acquiredRecordIterator} 已经被正确初始化（例如通过 {@link #initializeNextAcquired()}）。
     *
     * @return 下一个已获取的记录 (OffsetAndDeliveryCount)，如果不存在则返回 null。
     */
    private OffsetAndDeliveryCount nextAcquiredRecord() {
        // 检查 acquiredRecordIterator 是否还有下一个元素
        if (acquiredRecordIterator.hasNext()) {
            // 如果有，返回迭代器的下一个元素
            return acquiredRecordIterator.next();
        }
        // 如果没有更多元素，返回 null
        return null;
    }

    /**
     * 拒绝当前记录批次中的所有已获取记录。
     * 应用场景：当处理一个记录批次时发生不可恢复的错误（例如 CRC 校验失败），导致整个批次无法处理时，
     *           需要将该批次中所有本应由当前消费者处理的记录标记为“拒绝”。
     * 实现细节：
     * 1. 将 {@code acquiredRecordIterator} 重置到 {@code acquiredRecordList} 的开头，以确保从一个已知的状态开始比较。
     * 2. 获取第一个已获取的记录 {@code nextAcquired}。
     * 3. 遍历 {@code currentBatch} 中的每一个偏移量（从 {@code baseOffset} 到 {@code lastOffset}）。
     * 4. 对于每个偏移量：
     *    a. 如果 {@code nextAcquired} 为 null，表示没有更多已获取的记录需要匹配，循环终止。
     *    b. 如果当前偏移量等于 {@code nextAcquired.offset}，表示这条记录是已获取的，将其添加到 {@code inFlightBatch} 中并标记为 {@link AcknowledgeType#REJECT REJECT}。
     *    c. 如果当前偏移量小于 {@code nextAcquired.offset}，表示这条记录不是已获取的（或者已获取的记录在后面），跳过当前偏移量。
     *    d. 在匹配或跳过之后，获取下一个已获取的记录 {@code nextAcquired} 以便进行下一次比较。
     * 设计考虑：此方法确保在批次级别发生故障时，所有相关的已获取记录都被正确地标记为拒绝，以便 broker 可以重新投递它们。
     *           通过重置迭代器并逐个比较偏移量，可以准确地识别出批次中哪些记录是属于当前消费者的。
     *
     * @param <K> 记录键的类型。
     * @param <V> 记录值的类型。
     * @param inFlightBatch 用于存储确认信息的飞行中批次对象。
     * @param currentBatch 当前正在处理的记录批次，其中的已获取记录将被拒绝。
     */
    private <K, V> void rejectRecordBatch(final ShareInFlightBatch<K, V> inFlightBatch,
                                          final RecordBatch currentBatch) {
        // 将 acquiredRecordIterator 重置到 acquiredRecordList 的开头，确保从一个确定的状态开始
        acquiredRecordIterator = acquiredRecordList.listIterator();

        // 获取下一个（即第一个）已获取的记录
        OffsetAndDeliveryCount localNextAcquired = nextAcquiredRecord(); // 使用局部变量避免修改成员变量 nextAcquired 的状态
        // 遍历当前批次中的所有偏移量
        for (long offset = currentBatch.baseOffset(); offset <= currentBatch.lastOffset(); offset++) {
            // 如果 localNextAcquired 为 null，表示已获取记录列表已经遍历完毕
            if (localNextAcquired == null) {
                // 没有更多已获取的记录了，所以我们完成了
                break;
            // 如果当前记录的偏移量与下一个已获取记录的偏移量匹配
            } else if (offset == localNextAcquired.offset) {
                // 这是已获取的记录，所以我们拒绝它
                inFlightBatch.addAcknowledgement(offset, AcknowledgeType.REJECT);
                // 匹配成功后，获取下一个已获取的记录，为下一次循环做准备
                localNextAcquired = nextAcquiredRecord();
            // 如果当前记录的偏移量小于下一个已获取记录的偏移量
            } else if (offset < localNextAcquired.offset) {
                // 这不是已获取的记录（或者已获取的记录在当前记录之后），所以我们跳过它
                continue;
            } else { // offset > localNextAcquired.offset
                // 当前记录的偏移量大于了下一个已获取记录的偏移量，这意味着我们可能错过了匹配
                // 这通常不应该发生，除非 acquiredRecordList 不是严格按偏移量排序的，或者逻辑有误
                // 为了安全起见，我们尝试获取下一个已获取记录，看看是否能重新同步
                // 如果 acquiredRecordList 是有序的，那么这个分支理论上不应该频繁进入
                localNextAcquired = nextAcquiredRecord();
                // 重新检查当前 offset，因为 localNextAcquired 已经更新
                // 通过将 offset 减 1 并在下一次循环中加 1，可以重新评估当前 offset
                offset--; 
            }
        }
    }

    /**
     * 解析记录条目，必要时反序列化键/值字段。
     * // 解析记录条目，如果需要，反序列化键/值字段
     * 应用场景：当从 broker 获取到原始的 {@link Record} 对象后，需要将其转换为应用程序可用的 {@link ConsumerRecord} 对象。
     *           此过程包括提取元数据（如偏移量、时间戳）和反序列化键、值。
     * 实现细节：
     * 1. 从 {@link Record} 中提取头部信息、键字节和值字节。
     * 2. 使用提供的 {@link Deserializers} 对键字节进行反序列化。如果键字节为 null，则键也为 null。
     * 3. 使用提供的 {@link Deserializers} 对值字节进行反序列化。如果值字节为 null，则值也为 null。
     * 4. 捕获反序列化过程中可能发生的 {@link RuntimeException}，并将其包装为 {@link RecordDeserializationException} 抛出，同时记录错误日志。
     * 5. 使用所有提取和反序列化后的信息（主题、分区、偏移量、时间戳、时间戳类型、键大小、值大小、键、值、头部、leader epoch、投递次数）
     *    创建一个新的 {@link ConsumerRecord} 实例并返回。
     * 设计考虑：将记录解析和反序列化逻辑封装在此方法中，使其可重用。
     *           通过传入 {@link Deserializers}，使得此方法可以处理不同类型的键和值。
     *           错误处理机制确保反序列化失败时能提供详细的上下文信息。
     *           {@code deliveryCount} 是共享消费特有的，表示记录的投递次数。
     *
     * @param <K> 记录键的类型。
     * @param <V> 记录值的类型。
     * @param deserializers 用于反序列化键和值的反序列化器集合。
     * @param partition 记录所属的主题分区ID。
     * @param leaderEpoch 可选的 leader epoch。
     * @param timestampType 记录的时间戳类型。
     * @param record 从 broker 获取的原始记录对象。
     * @param deliveryCount 记录的投递次数（共享消费特定）。
     * @return 解析并反序列化后的 {@link ConsumerRecord}。
     * @throws RecordDeserializationException 如果在反序列化键或值时发生错误。
     */
    <K, V> ConsumerRecord<K, V> parseRecord(final Deserializers<K, V> deserializers,
                                            final TopicIdPartition partition,
                                            final Optional<Integer> leaderEpoch,
                                            final TimestampType timestampType,
                                            final Record record,
                                            final short deliveryCount) {
        // 从原始记录中获取头部信息，并创建一个新的 RecordHeaders 实例
        Headers headers = new RecordHeaders(record.headers());
        // 从原始记录中获取键的字节缓冲区
        ByteBuffer keyBytes = record.key();
        // 从原始记录中获取值的字节缓冲区
        ByteBuffer valueBytes = record.value();
        // 声明泛型键变量
        K key;
        // 声明泛型值变量
        V value;
        try {
            // 如果键字节为 null，则键为 null；否则，使用键反序列化器进行反序列化
            // partition.topic() 提供主题名称，headers 提供头部信息，keyBytes 提供待反序列化的数据
            key = keyBytes == null ? null : deserializers.keyDeserializer().deserialize(partition.topic(), headers, keyBytes);
        } catch (RuntimeException e) {
            // 如果反序列化键时发生运行时异常，记录错误日志
            log.error("Key Deserializers with error: {}", deserializers);
            // 抛出自定义的记录反序列化异常，指明是键反序列化失败
            throw newRecordDeserializationException(RecordDeserializationException.DeserializationExceptionOrigin.KEY, partition.topicPartition(), timestampType, record, e, headers);
        }
        try {
            // 如果值字节为 null，则值为 null；否则，使用值反序列化器进行反序列化
            // partition.topic() 提供主题名称，headers 提供头部信息，valueBytes 提供待反序列化的数据
            value = valueBytes == null ? null : deserializers.valueDeserializer().deserialize(partition.topic(), headers, valueBytes);
        } catch (RuntimeException e) {
            // 如果反序列化值时发生运行时异常，记录错误日志
            log.error("Value Deserializers with error: {}", deserializers);
            // 抛出自定义的记录反序列化异常，指明是值反序列化失败
            throw newRecordDeserializationException(RecordDeserializationException.DeserializationExceptionOrigin.VALUE, partition.topicPartition(), timestampType, record, e, headers);
        }
        // 创建并返回一个新的 ConsumerRecord 实例
        return new ConsumerRecord<>(partition.topic(),            // 主题名称
                partition.partition(),        // 分区号
                record.offset(),              // 记录的偏移量
                record.timestamp(),           // 记录的时间戳
                timestampType,                // 时间戳类型
                keyBytes == null ? ConsumerRecord.NULL_SIZE : keyBytes.remaining(), // 键的序列化后的大小，如果键为null则为-1
                valueBytes == null ? ConsumerRecord.NULL_SIZE : valueBytes.remaining(), // 值的序列化后的大小，如果值为null则为-1
                key,                          // 反序列化后的键
                value,                        // 反序列化后的值
                headers,                      // 记录的头部信息
                leaderEpoch,                  // leader epoch (可选)
                Optional.of(deliveryCount));  // 投递次数 (共享消费特定，包装在Optional中)
    }

    /**
     * 创建一个新的 {@link RecordDeserializationException} 实例。
     * 应用场景：当记录的键或值在反序列化过程中发生错误时，调用此辅助方法来构造一个包含详细上下文信息的异常对象。
     * 实现细节：收集所有相关的上下文信息（反序列化来源、分区、偏移量、时间戳、时间戳类型、原始键字节、原始值字节、头部信息、错误消息和原始异常），
     *           并用这些信息实例化一个新的 {@link RecordDeserializationException}。
     * 设计考虑：将异常创建逻辑封装在一个静态辅助方法中，使得创建具有一致格式和详细信息的反序列化异常更加方便。
     *           错误消息中明确指出是哪个部分（键或值）反序列化失败，以及相关的分区和偏移量，有助于问题排查。
     *
     * @param origin 反序列化失败的来源（例如，键或值）。
     * @param partition 记录所属的主题分区。
     * @param timestampType 记录的时间戳类型。
     * @param record 发生反序列化错误的原始记录。
     * @param e 捕获到的原始运行时异常。
     * @param headers 记录的头部信息。
     * @return 一个新的 {@link RecordDeserializationException} 实例。
     */
    private static RecordDeserializationException newRecordDeserializationException(RecordDeserializationException.DeserializationExceptionOrigin origin,
                                                                                    TopicPartition partition,
                                                                                    TimestampType timestampType,
                                                                                    Record record,
                                                                                    RuntimeException e,
                                                                                    Headers headers) {
        // 构造详细的错误消息字符串
        String errorMessage = "Error deserializing " + origin.name() + // 指明是键(KEY)还是值(VALUE)反序列化出错
                " for partition " + partition +                     // 发生错误的分区
                " at offset " + record.offset() +                   // 发生错误的记录的偏移量
                ". The record has been released.";                  // 附带信息，表明该记录已被释放（通常意味着不会再尝试处理）
        // 创建并返回一个新的 RecordDeserializationException 实例
        return new RecordDeserializationException(origin,                     // 反序列化来源 (KEY 或 VALUE)
                partition,                  // 主题分区
                record.offset(),            // 记录偏移量
                record.timestamp(),         // 记录时间戳
                timestampType,              // 时间戳类型
                record.key(),               // 原始键的 ByteBuffer (可能为 null)
                record.value(),             // 原始值的 ByteBuffer (可能为 null)
                headers,                    // 记录的头部信息
                errorMessage,               // 构造的错误消息
                e);                         // 原始的运行时异常，作为 cause
    }

    /**
     * 扫描可用批次中的下一条记录，跳过控制记录。
     * // 扫描可用批次中的下一条记录，跳过控制记录
     * 应用场景：在 {@link #fetchRecords(Deserializers, int, boolean)} 方法中，当需要从拉取的数据中获取下一条有效的业务记录时调用。
     *           此方法负责处理批次迭代、记录迭代、CRC 校验以及跳过控制记录的逻辑。
     * 实现细节：
     * 1. 进入一个无限循环，直到找到一个有效的业务记录或没有更多记录为止。
     * 2. 检查当前记录迭代器 {@code records} 是否为 null 或是否没有更多记录：
     *    a. 如果是，则首先尝试关闭当前的记录流 ({@link #maybeCloseRecordStream()})。
     *    b. 然后检查批次迭代器 {@code batches} 是否还有更多批次：
     *       i. 如果没有更多批次，则调用 {@link #drain()} 标记此 {@code ShareCompletedFetch} 已耗尽，将 {@code lastRecord} 设为 null，并跳出循环。
     *       ii. 如果有更多批次，则获取下一个批次 {@code currentBatch}，对其进行有效性检查 ({@link #maybeEnsureValid(RecordBatch, boolean)})，
     *           并从该批次创建新的流式记录迭代器 {@code records}。
     * 3. 如果当前记录迭代器 {@code records} 有效且有下一条记录：
     *    a. 获取下一条记录 {@code record}，并对其进行有效性检查 ({@link #maybeEnsureValid(Record, boolean)})。
     *    b. 检查当前批次 {@code currentBatch} 是否是控制批次。控制记录不会返回给用户。
     *       i. 如果不是控制批次，则将当前记录 {@code record} 赋给 {@code lastRecord}，并跳出循环（因为已找到一条业务记录）。
     *       ii. 如果是控制批次，则继续循环以查找下一条记录。
     * 4. 循环结束后，返回 {@code records} 是否不为 null 且仍有下一条记录。这实际上指示了在跳出循环时，是否成功定位到了 {@code lastRecord} 并且其后可能还有记录。
     *    然而，更准确地说，此方法的目的是将 {@code lastRecord} 设置为下一个有效的业务记录，返回值主要用于指示迭代是否可以继续。
     *    (注意：原代码的返回值 `records != null && records.hasNext()` 可能不完全直观地反映是否找到了 `lastRecord`，
     *     因为 `lastRecord` 是在找到非控制记录时被设置并 `break` 的。如果 `break` 发生，`records.hasNext()` 可能为 true 或 false。
     *     如果是因为 `drain()` 而 `break`，则 `records` 会是 null。此返回值主要用于外部循环判断是否继续尝试获取记录。)
     * 设计考虑：此方法将查找下一条有效记录的复杂逻辑（处理批次切换、记录迭代、CRC 校验、跳过控制记录）封装起来，
     *           使得 {@link #fetchRecords(Deserializers, int, boolean)} 的主逻辑更清晰。
     *           通过循环和条件判断，确保只将有效的业务数据记录作为 {@code lastRecord} 暴露出来。
     *
     * @param checkCrcs 是否检查获取记录的 CRC。
     * @return 如果当前批次（在处理后）仍有更多记录，则返回 true；否则返回 false。
     *         更准确地说，如果成功将 {@code lastRecord} 设置为一条业务记录，并且其迭代器 {@code records} 之后可能还有记录，则为 true。
     *         如果所有批次都已耗尽，则为 false。
     */
    private boolean nextFetchedRecord(final boolean checkCrcs) {
        // 无限循环，直到找到一个业务记录或所有数据耗尽
        while (true) {
            // 如果当前记录迭代器 (records) 为空，或者它没有更多记录了
            if (records == null || !records.hasNext()) {
                // 尝试关闭当前的记录流（如果存在）
                maybeCloseRecordStream();

                // 检查是否还有更多未处理的批次 (batches)
                if (!batches.hasNext()) {
                    // 如果没有更多批次了，说明所有数据都已处理完毕
                    drain(); // 调用 drain 方法标记此 ShareCompletedFetch 已耗尽，并记录指标
                    lastRecord = null; // 将 lastRecord 置为 null，因为没有更多记录了
                    break; // 跳出 while 循环
                }

                // 获取下一个记录批次
                currentBatch = batches.next();
                // 对当前批次进行有效性检查（例如 CRC 校验），如果失败可能会抛出异常并缓存
                maybeEnsureValid(currentBatch, checkCrcs);

                // 从当前批次创建流式记录迭代器
                records = currentBatch.streamingIterator(decompressionBufferSupplier);
            } else {
                // 如果当前记录迭代器 (records) 有效且有下一条记录
                Record record = records.next(); // 获取下一条原始记录
                // 对这条记录进行有效性检查（例如 CRC 校验），如果失败可能会抛出异常并缓存
                maybeEnsureValid(record, checkCrcs);

                // 控制记录不返回给用户。检查当前批次是否为控制批次
                if (!currentBatch.isControlBatch()) {
                    // 如果不是控制批次，说明这是一条业务数据记录
                    lastRecord = record; // 将其赋给 lastRecord
                    break; // 成功找到一条业务记录，跳出 while 循环
                }
                // 如果是控制批次，则循环继续，以查找下一条记录
            }
        }
        // 返回 records 是否不为 null 并且 records 中是否还有下一条记录
        // 这个返回值表明在成功找到 lastRecord (如果找到了) 之后，当前的 records 迭代器是否还有更多内容，
        // 或者在所有批次耗尽 (drain 被调用) 的情况下，records 会是 null，此时返回 false。
        return records != null && records.hasNext();
    }

    /**
     * 将整数类型的 leader epoch 转换为 Optional<Integer> 类型。
     * 应用场景：在处理记录批次时，需要将原始的 leader epoch 值转换为可选类型，以便在上层逻辑中统一处理。
     * 实现细节：检查传入的 leaderEpoch 是否等于 RecordBatch.NO_PARTITION_LEADER_EPOCH 常量，
     *           如果是，则返回空的 Optional；否则，将 leaderEpoch 包装在 Optional 中返回。
     * 设计考虑：使用 Optional 可以明确表示 leader epoch 可能不存在的情况，提高代码的可读性和安全性。
     *           这种模式在 Java 8+ 中很常见，用于避免空指针异常。
     *
     * @param leaderEpoch 分区 leader 的 epoch 值
     * @return 包含 leaderEpoch 的 Optional，如果 leaderEpoch 无效则为空
     */
    private Optional<Integer> maybeLeaderEpoch(final int leaderEpoch) {
        // 检查 leaderEpoch 是否等于 NO_PARTITION_LEADER_EPOCH 常量（通常为 -1）
        // 如果是，返回空的 Optional，表示没有有效的 leader epoch
        // 如果不是，则将 leaderEpoch 包装在 Optional 中返回
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
    }

    /**
     * 根据配置验证记录批次的有效性。
     * 应用场景：在处理记录批次之前，需要验证批次数据的完整性，特别是在启用了 CRC 校验的情况下。
     * 实现细节：如果启用了 CRC 校验且批次的魔数大于等于 V2，则调用批次的 ensureValid 方法进行验证。
     *           如果验证失败，会抛出包含详细错误信息的 CorruptRecordException 异常。
     * 设计考虑：将验证逻辑封装在单独的方法中，便于在多处调用。通过参数控制是否执行验证，提高灵活性。
     *           增强异常信息，包含分区和偏移量，便于定位问题。
     *
     * @param batch 要验证的记录批次
     * @param checkCrcs 是否执行 CRC 校验
     * @throws CorruptRecordException 如果批次验证失败
     */
    private void maybeEnsureValid(RecordBatch batch, boolean checkCrcs) {
        // 只有当 checkCrcs 为 true（启用 CRC 校验）且批次的魔数大于等于 V2 时才执行验证
        // MAGIC_VALUE_V2 表示 Kafka 0.11.0 及以上版本的消息格式
        if (checkCrcs && batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
            try {
                // 调用批次的 ensureValid 方法验证批次的完整性
                // 这通常包括 CRC 校验和其他格式验证
                batch.ensureValid();
            } catch (CorruptRecordException e) {
                // 如果验证失败，抛出新的异常，包含更详细的上下文信息
                // 包括分区信息和批次的基础偏移量，便于问题定位
                throw new CorruptRecordException("Record batch for partition " + partition.topicPartition()
                        + " at offset " + batch.baseOffset() + " is invalid, cause: " + e.getMessage());
            }
        }
    }

    /**
     * 根据配置验证单条记录的有效性。
     * 应用场景：在处理单条记录之前，需要验证记录数据的完整性，特别是在启用了 CRC 校验的情况下。
     * 实现细节：如果启用了 CRC 校验，则调用记录的 ensureValid 方法进行验证。
     *           如果验证失败，会抛出包含详细错误信息的 CorruptRecordException 异常。
     * 设计考虑：与批次验证方法类似，但针对单条记录。方法重载提供了统一的接口，简化了调用代码。
     *           增强异常信息，包含分区和偏移量，便于定位问题。
     *
     * @param record 要验证的记录
     * @param checkCrcs 是否执行 CRC 校验
     * @throws CorruptRecordException 如果记录验证失败
     */
    private void maybeEnsureValid(final Record record, final boolean checkCrcs) {
        // 只有当 checkCrcs 为 true（启用 CRC 校验）时才执行验证
        if (checkCrcs) {
            try {
                // 调用记录的 ensureValid 方法验证记录的完整性
                // 这通常包括 CRC 校验和其他格式验证
                record.ensureValid();
            } catch (CorruptRecordException e) {
                // 如果验证失败，抛出新的异常，包含更详细的上下文信息
                // 包括分区信息和记录的偏移量，便于问题定位
                throw new CorruptRecordException("Record for partition " + partition.topicPartition()
                        + " at offset " + record.offset() + " is invalid, cause: " + e.getMessage());
            }
        }
    }

    /**
     * 安全地关闭记录流，并清除引用。
     * 应用场景：在完成记录处理或需要释放资源时，需要安全地关闭记录流。
     * 实现细节：检查记录流是否为 null，如果不是，则关闭它并将引用设置为 null。
     * 设计考虑：封装关闭逻辑，避免空指针异常。将引用设置为 null 有助于垃圾回收。
     *           方法名以 maybe 开头，表示这是一个条件操作，只在必要时执行。
     */
    private void maybeCloseRecordStream() {
        // 检查记录流是否存在（不为 null）
        if (records != null) {
            // 关闭记录流，释放相关资源
            records.close();
            // 将引用设置为 null，帮助垃圾回收并防止后续误用
            records = null;
        }
    }

    /**
     * 表示一个已获取记录的偏移量和投递次数。
     * 应用场景：在共享消费模式下，需要跟踪每条记录的偏移量和已投递次数，以支持消息确认和重试机制。
     * 实现细节：包含两个字段：offset（记录的偏移量）和 deliveryCount（记录的投递次数）。
     * 设计考虑：使用不可变对象模式，所有字段都是 final 的，确保线程安全。
     *           作为内部静态类，不依赖外部类的状态，可以独立使用。
     */
    private static class OffsetAndDeliveryCount {
        // 记录的偏移量，用于唯一标识一条记录
        final long offset;
        // 记录的投递次数，表示该记录已被投递给消费者的次数
        // 在共享消费模式下，用于支持消息确认和重试机制
        final short deliveryCount;

        /**
         * 构造一个新的 OffsetAndDeliveryCount 实例。
         * 
         * @param offset 记录的偏移量
         * @param deliveryCount 记录的投递次数
         */
        OffsetAndDeliveryCount(long offset, short deliveryCount) {
            // 初始化偏移量
            this.offset = offset;
            // 初始化投递次数
            this.deliveryCount = deliveryCount;
        }

        /**
         * 返回此对象的字符串表示。
         * 
         * @return 包含偏移量和投递次数的字符串
         */
        @Override
        public String toString() {
            // 生成包含偏移量和投递次数的字符串表示
            return "OffsetAndDeliveryCount{" +
                    "offset=" + offset +
                    ", deliveryCount=" + deliveryCount +
                    "}";
        }
    }
}
