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
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 此类维护在共享组中传递给消费者的单个主题分区上一组记录的确认和间隙信息。
 * 应用场景：在 Kafka 共享消费模式下，消费者需要确认已处理的消息，以便 Broker 可以更新消费位移。此类用于跟踪这些确认信息。
 * 设计考虑：使用 Map 存储确认信息，键为偏移量，值为确认类型。使用 TreeMap 保证偏移量有序，便于后续处理和优化。
 */
public class Acknowledgements {
    /**
     * 表示间隙的确认类型字节值。
     * 当记录批次中期望的偏移量丢失时（例如被日志压缩器移除），会使用此类型标记一个间隙。
     */
    public static final byte ACKNOWLEDGE_TYPE_GAP = (byte) 0;
    /**
     * 具有相同确认类型的最大记录数。
     * 用于优化确认批次，当连续记录具有相同确认类型且数量超过此值时，可以进行合并优化。
     */
    public static final int MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE = 10;

    // 按偏移量键控的确认信息。如果记录是一个间隙，则 AcknowledgeType 将为 null。
    // 设计考虑：使用 final 修饰，确保 acknowledgements 映射在对象创建后不可更改引用，但其内容可以修改。
    private final Map<Long, AcknowledgeType> acknowledgements;

    // 当代理响应确认时，返回的错误代码。
    // 应用场景：用于记录 Broker 处理确认请求的结果，如果发生错误，则存储相应的错误码。
    private Errors acknowledgeErrorCode;

    /**
     * 创建一个空的 Acknowledgements 实例。
     * 应用场景：在初始化或需要一个不包含任何确认信息的 Acknowledgements 对象时使用。
     * 实现细节：内部使用 TreeMap 来存储确认信息，以保证偏移量的有序性。
     * @return 一个新的、空的 Acknowledgements 对象。
     */
    public static Acknowledgements empty() {
        // 返回一个新的 Acknowledgements 实例，其内部使用 TreeMap 初始化。
        return new Acknowledgements(new TreeMap<>());
    }

    /**
     * Acknowledgements 类的私有构造函数。
     * 设计考虑：设为私有以控制对象的创建方式，通常通过静态工厂方法（如 empty()）创建。
     * @param acknowledgements 用于初始化确认信息的映射。
     */
    private Acknowledgements(Map<Long, AcknowledgeType> acknowledgements) {
        // 将传入的 acknowledgements 映射赋值给类的成员变量。
        this.acknowledgements = acknowledgements;
    }

    /**
     * 为特定偏移量添加确认。如果已存在相同偏移量的确认，则会覆盖。
     * 应用场景：当消费者处理完一条消息后，调用此方法添加确认信息。
     * @param offset 记录的偏移量。
     * @param type   确认类型 (AcknowledgeType)。
     */
    public void add(long offset, AcknowledgeType type) {
        // 将指定的偏移量和确认类型存入 acknowledgements 映射中。
        // Map.put() 方法会覆盖具有相同键的现有条目。
        this.acknowledgements.put(offset, type);
    }

    /**
     * 为特定偏移量添加确认。如果已存在相同偏移量的确认，则<b>不会</b>覆盖。
     * 应用场景：在某些情况下，可能不希望覆盖已有的确认信息，例如，首次确认的优先级更高。
     * @param offset 记录的偏移量。
     * @param type   确认类型 (AcknowledgeType)。
     *
     * @return 如果确认信息被成功添加（即之前不存在该偏移量的确认），则返回 true；否则返回 false。
     */
    public boolean addIfAbsent(long offset, AcknowledgeType type) {
        // 使用 Map.putIfAbsent() 方法尝试添加确认信息。
        // 如果指定的键 (offset) 尚不存在或关联到 null，则将其与给定的值 (type) 关联，并返回 null。
        // 如果键已存在，则返回当前关联的值，并且不更改映射。
        // 因此，当 putIfAbsent 返回 null 时，表示成功添加了新的确认。
        return acknowledgements.putIfAbsent(offset, type) == null;
    }

    /**
     * 为指定的偏移量添加一个间隙。这意味着代理期望在此偏移量处有一条记录，
     * 但是在解析记录批次时，期望的偏移量丢失了。当记录已被日志压缩器移除时，会发生这种情况。
     * 应用场景：当消费者发现消息流中存在间隙（由于日志压缩等原因），需要通知 Broker 这个间隙的存在。
     * @param offset 记录的偏移量，该偏移量处存在间隙。
     */
    public void addGap(long offset) {
        // 将指定的偏移量与 null 值关联，表示这是一个间隙。
        acknowledgements.put(offset, null);
    }

    /**
     * 获取指定偏移量的确认类型。
     * 应用场景：查询特定偏移量消息的确认状态。
     * @param offset 记录的偏移量。
     * @return 确认类型 (AcknowledgeType)，如果不存在该偏移量的确认，则返回 null。
     */
    public AcknowledgeType get(long offset) {
        // 从 acknowledgements 映射中获取指定偏移量对应的确认类型。
        return acknowledgements.get(offset);
    }

    /**
     * 判断确认集合是否为空。
     * 应用场景：在发送确认请求前，检查是否有待确认的消息。
     * @return 如果确认集合为空，则返回 true；否则返回 false。
     */
    public boolean isEmpty() {
        // 调用 acknowledgements 映射的 isEmpty() 方法判断是否为空。
        return acknowledgements.isEmpty();
    }

    /**
     * 返回确认集合的大小。
     * 应用场景：获取当前待确认消息的数量。
     * @return 确认集合的大小。
     */
    public int size() {
        // 调用 acknowledgements 映射的 size() 方法获取其大小。
        return acknowledgements.size();
    }

    /**
     * 判断确认信息是否已发送给代理并收到响应。
     * 应用场景：检查与 Broker 的确认交互是否已完成。
     * @return 如果确认信息已发送给代理并收到响应，则返回 true；否则返回 false。
     */
    public boolean isCompleted() {
        // 通过检查 acknowledgeErrorCode 是否为 null 来判断。
        // 如果 acknowledgeErrorCode 不为 null，表示已收到 Broker 的响应（可能成功也可能失败）。
        return acknowledgeErrorCode != null;
    }

    /**
     * 当从代理收到响应后，设置确认错误代码。
     * 应用场景：在处理完 Broker 对确认请求的响应后，更新此对象的错误状态。
     * @param acknowledgeErrorCode 错误代码。
     */
    public void setAcknowledgeErrorCode(Errors acknowledgeErrorCode) {
        // 将传入的错误代码赋值给成员变量 acknowledgeErrorCode。
        this.acknowledgeErrorCode = acknowledgeErrorCode;
    }

    /**
     * 当从代理收到响应后，获取确认错误代码。
     * 应用场景：获取上次与 Broker 确认交互的结果。
     * @return 错误代码。
     */
    public Errors getAcknowledgeErrorCode() {
        // 返回成员变量 acknowledgeErrorCode 的值。
        return acknowledgeErrorCode;
    }

    /**
     * 合并两个确认集合。如果存在重叠的确认（即同一偏移量的确认），则以“other”集合中的确认为准。
     * 应用场景：当需要将另一个 Acknowledgements 对象的确认信息合并到当前对象时使用，例如在处理来自不同来源的确认时。
     * 设计考虑：直接使用 Map 的 putAll 方法，简洁高效。 “other”集合中的确认会覆盖当前集合中相同偏移量的确认。
     *
     * @param other 要合并的另一个 Acknowledgements 对象。
     * @return 当前 Acknowledgements 对象（合并后的），方便链式调用。
     */
    public Acknowledgements merge(Acknowledgements other) {
        // 调用 acknowledgements 映射的 putAll 方法，将 other 对象中的所有确认信息（偏移量及其对应的确认类型）添加到当前对象的 acknowledgements 映射中。
        // 如果 other.acknowledgements 中包含与当前 acknowledgements 映射中已存在的键（偏移量），则这些键对应的值将被 other.acknowledgements 中的值覆盖。
        acknowledgements.putAll(other.acknowledgements);
        // 返回当前对象的引用，允许进行链式操作。
        return this;
    }

    /**
     * 返回包含偏移量到确认类型映射的 Map。
     * 应用场景：需要直接访问和操作原始确认数据结构时使用。
     * 设计考虑：直接返回内部的 acknowledgements 映射，调用者可以获取所有确认信息。
     * @return 一个包含偏移量 (Long) 到确认类型 (AcknowledgeType) 的 Map。
     */
    public Map<Long, AcknowledgeType> getAcknowledgementsTypeMap() {
        // 直接返回内部存储确认信息的 acknowledgements 映射。
        return acknowledgements;
    }

    /**
     * 将确认信息转换为 {@link AcknowledgementBatch} 列表，这些批次可以轻松转换为 RPC 请求所需的形式。
     * 应用场景：在准备向 Broker 发送确认请求时，需要将离散的确认信息聚合成批次以提高效率。
     * 设计考虑：
     * 1. 遍历有序的确认信息 (TreeMap 保证了偏移量的顺序)。
     * 2. 将连续的或可优化的确认信息合并到同一个 AcknowledgementBatch 中。
     * 3. 对生成的批次进行优化，特别是针对具有单一确认类型的批次。
     * @return 一个 {@link AcknowledgementBatch} 对象的列表。
     */
    public List<AcknowledgementBatch> getAcknowledgementBatches() {
        // 创建一个 ArrayList 用于存储生成的 AcknowledgementBatch 对象。
        List<AcknowledgementBatch> batches = new ArrayList<>();
        // 如果没有任何确认信息，则直接返回空的批次列表。
        if (acknowledgements.isEmpty())
            // 返回空的批次列表。
            return batches;

        // 初始化当前正在构建的 AcknowledgementBatch 为 null。
        AcknowledgementBatch currentBatch = null;
        // 遍历 acknowledgements 映射中的每一个条目（偏移量 -> 确认类型）。
        // 由于 acknowledgements 是 TreeMap，entrySet() 返回的条目将按偏移量升序排列。
        for (Map.Entry<Long, AcknowledgeType> entry : acknowledgements.entrySet()) {
            // 获取当前条目的偏移量。
            long offset = entry.getKey();
            // 获取当前条目的确认类型 (可能为 null，表示间隙)。
            AcknowledgeType type = entry.getValue();

            // 如果 currentBatch 为 null，表示这是第一个确认信息，或者上一个批次已经完成并添加到 batches 列表中。
            if (currentBatch == null) {
                // 创建一个新的 AcknowledgementBatch 实例。
                currentBatch = new AcknowledgementBatch();
                // 设置新批次的起始偏移量为当前确认信息的偏移量。
                currentBatch.setFirstOffset(offset);
            } else {
                // 如果 currentBatch 不为 null，表示当前正在构建一个批次。
                // 调用 maybeCreateNewBatch 方法检查是否需要因为偏移量不连续而创建新的批次。
                // 如果需要创建新批次，maybeCreateNewBatch 会将旧的 currentBatch (优化后) 添加到 batches 列表，并返回一个新的 currentBatch。
                // 如果不需要创建新批次，则返回原始的 currentBatch。
                currentBatch = maybeCreateNewBatch(currentBatch, offset, batches);
            }
            // 更新当前批次的结束偏移量为当前确认信息的偏移量。
            currentBatch.setLastOffset(offset);
            // 如果当前确认信息的确认类型不为 null (即不是一个间隙)。
            if (type != null) {
                // 将确认类型的 ID 添加到当前批次的确认类型列表中。
                currentBatch.acknowledgeTypes().add(type.id);
            } else {
                // 如果确认类型为 null，表示这是一个间隙。
                // 将表示间隙的特殊确认类型 ACKNOWLEDGE_TYPE_GAP 添加到当前批次的确认类型列表中。
                currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
            }
        }
        // 在处理完所有确认信息后，对最后一个构建的 currentBatch (如果存在) 进行优化。
        // maybeOptimiseAcknowledgementTypes 方法可能会将 currentBatch 拆分成多个更优的批次。
        List<AcknowledgementBatch> optimalBatches = maybeOptimiseAcknowledgementTypes(currentBatch);

        // 遍历优化后得到的批次列表。
        optimalBatches.forEach(batch -> {
            // 检查当前批次是否可以针对单一确认类型进行优化。
            // canOptimiseForSingleAcknowledgeType 的逻辑是：如果批次中所有确认类型都相同，或者批次只包含一个确认。
            if (canOptimiseForSingleAcknowledgeType(batch)) {
                // 如果批次具有单一确认类型，则优化确认类型数组，使其只包含一个元素，而不管记录的数量。
                // This comment was: If the batch had a single acknowledgement type, we optimise the array independent of the number of records.
                // 清除批次确认类型列表中从第二个元素到末尾的所有元素，只保留第一个确认类型。
                batch.acknowledgeTypes().subList(1, batch.acknowledgeTypes().size()).clear();
            }
            // 将（可能已优化的）批次添加到最终的批次列表中。
            batches.add(batch);
        });
        // 返回包含所有确认批次的列表。
        // 返回优化后的确认批次列表
        return batches;
    }

    /**
     * 如果下一个偏移量不是当前批次最后一个偏移量加一（即偏移量不连续），则创建并处理新的当前批次。
     * 应用场景：在构建 {@link AcknowledgementBatch} 列表时，用于处理偏移量不连续的情况。
     * 设计考虑：
     * 1. 当检测到偏移量不连续时，意味着当前的 `currentBatch` 已经结束。
     * 2. 在结束 `currentBatch` 之前，会尝试对其进行优化 (通过 `maybeOptimiseAcknowledgementTypes`)。
     * 3. 优化后的批次（可能是一个或多个）会被添加到 `batches` 列表中。
     * 4. 然后创建一个新的 `AcknowledgementBatch` 作为新的 `currentBatch`，并设置其起始偏移量。
     *
     * @param currentBatch 当前正在构建的确认批次。
     * @param nextOffset   下一个待处理的确认信息的偏移量。
     * @param batches      用于存储已完成的确认批次的列表。
     * @return 如果创建了新批次，则返回新的批次对象；否则返回传入的 `currentBatch`。
     */
    private AcknowledgementBatch maybeCreateNewBatch(AcknowledgementBatch currentBatch, Long nextOffset, List<AcknowledgementBatch> batches) {
        // 检查下一个偏移量 (nextOffset) 是否是当前批次最后一个偏移量 (currentBatch.lastOffset()) 加 1。
        // 如果不是，则表示偏移量不连续，当前的 currentBatch 需要结束，并开始一个新的批次。
        if (nextOffset != currentBatch.lastOffset() + 1) {
            // 对当前的 currentBatch 进行优化。maybeOptimiseAcknowledgementTypes 可能会将其拆分为多个更优的批次。
            List<AcknowledgementBatch> optimalBatches = maybeOptimiseAcknowledgementTypes(currentBatch);

            // 遍历优化后得到的批次列表。
            optimalBatches.forEach(batch -> {
                // 检查当前批次是否可以针对单一确认类型进行优化。
                if (canOptimiseForSingleAcknowledgeType(batch)) {
                    // 如果批次具有单一确认类型，则优化确认类型数组，使其只包含一个元素。
                    // This comment was: If the batch had a single acknowledgement type, we optimise the array independent of the number of records.
                    // 清除批次确认类型列表中从第二个元素到末尾的所有元素。
                    batch.acknowledgeTypes().subList(1, batch.acknowledgeTypes().size()).clear();
                }
                // 将（可能已优化的）批次添加到最终的批次列表中。
                batches.add(batch);
            });

            // 因为偏移量不连续，所以创建一个新的 AcknowledgementBatch 实例作为新的 currentBatch。
            currentBatch = new AcknowledgementBatch();
            // 设置新批次的起始偏移量为 nextOffset。
            currentBatch.setFirstOffset(nextOffset);
        }

        // 返回 currentBatch。如果创建了新批次，则这是新批次；否则，这是传入的原始批次。
        return currentBatch;
    }

    /**
     * 遍历 acknowledgementBatch 并尽可能将其拆分为最优的批次。
     * 当一个批次中具有相同确认类型的连续记录数量超过默认值时，就会进行优化。
     * 在这种情况下，批次将被拆分为2个，其中包含连续记录的批次在其数组中只有1种确认类型。
     * 应用场景：当需要发送确认信息给 Broker 时，此方法可以优化确认批次的结构，减少网络传输和 Broker 处理的开销。
     * 设计考虑：通过识别并合并具有大量相同确认类型的连续记录，可以显著减小确认数据的大小。
     * @param currentAcknowledgeBatch 当前的确认批次，可能包含多种确认类型和多个记录。
     * @return 优化后的确认批次列表。如果输入批次为 null 或无需优化，则返回包含原始批次（或其子集）的列表。
     */
    private List<AcknowledgementBatch> maybeOptimiseAcknowledgementTypes(AcknowledgementBatch currentAcknowledgeBatch) {
        // 创建一个列表，用于存储优化后的确认批次
        List<AcknowledgementBatch> batches = new ArrayList<>();
        // 如果当前的确认批次为 null，则直接返回空的批次列表
        if (currentAcknowledgeBatch == null) return batches;

        // 获取当前确认批次的起始偏移量
        long currentOffset = currentAcknowledgeBatch.firstOffset();
        // 初始化当前处理的起始索引
        int currentStartIndex = 0;
        // 初始化具有相同确认类型的连续记录计数器
        int recordsWithSameAcknowledgeType = 1;
        // 从第二个确认类型开始遍历当前确认批次中的所有确认类型
        for (int i = 1; i < currentAcknowledgeBatch.acknowledgeTypes().size(); i++) {
            // 获取当前索引处的确认类型
            byte acknowledgeType = currentAcknowledgeBatch.acknowledgeTypes().get(i);
            // 如果我们有一组连续的记录具有相同的确认类型，并且数量超过了默认计数，
            // 那么我们就优化批次，使其只包含开始和结束偏移量，并且数组中只有一种确认类型。
            // 获取前一个索引处的确认类型
            byte prevAcknowledgeType = currentAcknowledgeBatch.acknowledgeTypes().get(i - 1);
            // 检查当前确认类型是否与前一个相同，并且连续相同类型的记录数是否达到或超过了最大阈值
            if (acknowledgeType == prevAcknowledgeType && recordsWithSameAcknowledgeType >= MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE) {
                // 我们继续遍历，直到遇到不同的确认类型。
                // 继续向后查找，统计所有连续相同的确认类型
                while (i < currentAcknowledgeBatch.acknowledgeTypes().size()) {
                    // 获取当前位置的确认类型
                    byte acknowledgeType2 = currentAcknowledgeBatch.acknowledgeTypes().get(i);
                    // 如果当前确认类型与前一个不同，则跳出循环
                    if (acknowledgeType2 != currentAcknowledgeBatch.acknowledgeTypes().get(i - 1)) break;
                    // 移动到下一个确认类型
                    i++;
                    // 增加相同确认类型的记录计数
                    recordsWithSameAcknowledgeType++;
                }

                // 现在我们准备两个批次，一个从具有单一确认类型的批次之前开始
                // 另一个是具有单一确认类型的批次。
                // 创建第一个批次，包含在连续相同确认类型块之前的部分
                AcknowledgementBatch batch1 = new AcknowledgementBatch();
                // 设置第一个批次的起始偏移量
                batch1.setFirstOffset(currentOffset);
                // 计算并设置第一个批次的结束偏移量
                batch1.setLastOffset(currentOffset + i - recordsWithSameAcknowledgeType - currentStartIndex - 1);
                // 如果第一个批次的结束偏移量大于等于起始偏移量（即批次有效）
                if (batch1.lastOffset() >= batch1.firstOffset()) {
                    // 从原始批次中提取对应范围的确认类型，并设置给第一个批次
                    batch1.setAcknowledgeTypes(new ArrayList<>(currentAcknowledgeBatch.acknowledgeTypes().subList(currentStartIndex,
                            i - recordsWithSameAcknowledgeType)));
                    // 将第一个批次添加到结果列表中
                    batches.add(batch1);
                }

                // 创建第二个批次，专门用于存储连续的相同确认类型
                AcknowledgementBatch batch2 = new AcknowledgementBatch();
                // 计算并设置第二个批次的起始偏移量
                batch2.setFirstOffset(currentOffset + i - recordsWithSameAcknowledgeType - currentStartIndex);
                // 计算并设置第二个批次的结束偏移量
                batch2.setLastOffset(currentOffset + i - currentStartIndex - 1);
                // 将单一的确认类型添加到第二个批次的确认类型列表中（优化点）
                batch2.acknowledgeTypes().add(acknowledgeType);

                // 将第二个批次添加到结果列表中
                batches.add(batch2);
                // 重置相同确认类型的记录计数器
                recordsWithSameAcknowledgeType = 1;

                // 更新偏移量和起始索引以进行后续迭代。
                // 更新当前偏移量，指向下一个未处理记录的起始位置
                currentOffset = currentOffset + i - currentStartIndex;
                // 更新当前处理的起始索引
                currentStartIndex = i;
            // 如果当前确认类型与前一个相同，但尚未达到最大阈值
            } else if (acknowledgeType == prevAcknowledgeType) {
                // 最大限制尚未达到，我们增加计数并继续前进。
                // 增加相同确认类型的记录计数
                recordsWithSameAcknowledgeType++;
            // 如果当前确认类型与前一个不同
            } else {
                // 重置相同确认类型的记录计数器
                recordsWithSameAcknowledgeType = 1;
            }
        }
        // 循环结束后，处理剩余的确认类型（如果存在）
        if (currentStartIndex < currentAcknowledgeBatch.acknowledgeTypes().size()) {
            // 创建一个新的批次用于存储剩余的确认类型
            AcknowledgementBatch batch = new AcknowledgementBatch();
            // 设置剩余批次的起始偏移量
            batch.setFirstOffset(currentOffset);
            // 计算并设置剩余批次的结束偏移量
            batch.setLastOffset(currentOffset + currentAcknowledgeBatch.acknowledgeTypes().size() - currentStartIndex - 1);
            // 从原始批次中提取剩余的确认类型，并设置给新的批次
            batch.setAcknowledgeTypes(new ArrayList<>(currentAcknowledgeBatch.acknowledgeTypes().subList(currentStartIndex,
                    currentAcknowledgeBatch.acknowledgeTypes().size())));
            // 将剩余的批次添加到结果列表中
            batches.add(batch);
        }
        // 返回优化后的确认批次列表
        return batches;
    }

    /**
     * 检查共享获取批次中的确认类型数组是否包含单一确认类型，并且数组大小可以减小到1。
     * 应用场景：在发送确认请求前，判断是否可以将一个批次优化为只包含一个确认类型，以减少数据量。
     * 设计考虑：此方法用于辅助 `maybeOptimiseAcknowledgementTypes` 或其他优化逻辑，判断一个批次是否满足单一确认类型的优化条件。
     * @param acknowledgementBatch 要检查的确认批次。
     * @return 如果共享获取批次中的确认类型数组包含单一确认类型并且数组大小可以减小到1，则返回 true。
     *         当数组具有多种确认类型或已经优化时，返回 false。
     */
    private boolean canOptimiseForSingleAcknowledgeType(AcknowledgementBatch acknowledgementBatch) {
        // 如果确认批次为 null 或者其确认类型列表的大小已经是1（已经优化或只有一个元素），则不能进一步优化
        if (acknowledgementBatch == null || acknowledgementBatch.acknowledgeTypes().size() == 1) return false;
        // 获取确认类型列表中的第一个确认类型，作为比较基准
        int firstAcknowledgeType = acknowledgementBatch.acknowledgeTypes().get(0);
        // 从第二个确认类型开始遍历列表
        for (int i = 1; i < acknowledgementBatch.acknowledgeTypes().size(); i++) {
            // 如果发现任何一个确认类型与第一个不同，则说明该批次包含多种确认类型，不能优化为单一类型
            if (acknowledgementBatch.acknowledgeTypes().get(i) != firstAcknowledgeType) return false;
        }
        // 如果遍历完成都没有发现不同的确认类型，说明所有确认类型都相同，可以优化
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Acknowledgements(");
        sb.append(acknowledgements);
        if (acknowledgeErrorCode != null) {
            sb.append(", errorCode=");
            sb.append(acknowledgeErrorCode.code());
        }
        sb.append(")");
        return sb.toString();
    }
}
