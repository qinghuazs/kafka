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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ConsumerProtocolAssignment;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.SchemaException;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * ConsumerProtocol 包含了用于 Kafka 通用组管理协议的消费者订阅和分配的模式定义。
 * <p>
 * 当前的实现假设未来的版本不会破坏兼容性。当遇到较新版本时，它会使用当前格式进行解析。
 * 这基本上意味着新版本不能删除或重新排序任何现有字段。
 * </p>
 * <p>
 * 应用场景：
 * 该协议用于 Kafka 消费者客户端和 Kafka Broker 之间的通信，特别是在消费者加入组、同步订阅信息、接收分区分配等场景。
 * 它确保了消费者和 Broker 对订阅和分配数据的理解是一致的。
 * </p>
 * <p>
 * 设计考虑：
 * 1.  版本兼容性：协议设计时考虑了向前兼容，允许旧版本的客户端或 Broker 处理新版本的数据（通过忽略未知字段）。
 * 2.  可扩展性：通过版本号和灵活的序列化格式，方便未来添加新的字段和功能。
 * 3.  标准化：定义了统一的订阅和分配信息结构，简化了消费者组管理的复杂性。
 * </p>
 */
public class ConsumerProtocol {
    /**
     * 协议类型常量，标识这是一个消费者协议。
     * <p>
     * 应用场景：在 Kafka 的通用组管理框架中，用于区分不同类型的协议（例如，消费者协议、Connect 协议等）。
     * </p>
     */
    public static final String PROTOCOL_TYPE = "consumer";

    /**
     * 静态初始化块，用于执行协议版本一致性检查。
     * <p>
     * 实现细节：
     * 检查 {@link ConsumerProtocolSubscription} 和 {@link ConsumerProtocolAssignment} 定义的最低和最高支持版本是否一致。
     * 这是为了确保消费者协议的订阅部分和分配部分保持同步，避免因版本不匹配导致的问题。
     * </p>
     * <p>
     * 设计考虑：
     * 通过在类加载时进行此检查，可以及早发现潜在的配置或代码错误，防止在运行时出现更复杂的问题。
     * </p>
     */
    static {
        // 安全检查，确保消费者协议的两个部分（订阅和分配）保持同步。
        // 检查最低支持版本是否一致
        if (ConsumerProtocolSubscription.LOWEST_SUPPORTED_VERSION
                != ConsumerProtocolAssignment.LOWEST_SUPPORTED_VERSION)
            // 如果不一致，则抛出 IllegalStateException 异常
            throw new IllegalStateException("Subscription and Assignment schemas must have the " +
                "same lowest version");

        // 检查最高支持版本是否一致
        if (ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION
                != ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION)
            // 如果不一致，则抛出 IllegalStateException 异常
            throw new IllegalStateException("Subscription and Assignment schemas must have the " +
                "same highest version");
    }

    /**
     * 从给定的 ByteBuffer 中反序列化协议版本号。
     * <p>
     * 应用场景：在解析消费者协议数据（订阅或分配信息）之前，首先需要读取版本号以确定后续的解析方式。
     * </p>
     *
     * @param buffer 包含协议数据的 ByteBuffer，其当前位置应指向版本号的起始处。
     * @return 反序列化得到的协议版本号 (short类型)。
     * @throws SchemaException 如果在解析过程中发生 ByteBuffer 下溢（即缓冲区数据不足）。
     */
    public static short deserializeVersion(final ByteBuffer buffer) {
        try {
            // 从 ByteBuffer 中读取一个 short 类型的值作为版本号
            return buffer.getShort();
        } catch (BufferUnderflowException e) {
            // 如果读取时发生 BufferUnderflowException (例如，buffer中剩余字节不够一个short)，
            // 则抛出 SchemaException，指示解析消费者协议头部时发生缓冲区下溢。
            throw new SchemaException("Buffer underflow while parsing consumer protocol's header", e);
        }
    }

    /**
     * 使用最高支持的版本序列化消费者的订阅信息。
     * <p>
     * 应用场景：当消费者加入消费者组或更新其订阅时，需要将订阅信息序列化后发送给 Kafka Broker。
     * 此方法默认使用当前代码支持的最高协议版本进行序列化。
     * </p>
     *
     * @param subscription 包含消费者订阅信息的 {@link Subscription} 对象。
     * @return 包含序列化后的订阅信息的 ByteBuffer。
     * @see #serializeSubscription(Subscription, short)
     */
    public static ByteBuffer serializeSubscription(final Subscription subscription) {
        // 调用重载方法，使用 ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION 作为序列化版本
        return serializeSubscription(subscription, ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION);
    }

    /**
     * 使用指定的版本序列化消费者的订阅信息。
     * <p>
     * 实现细节：
     * 1.  检查并规范化指定的协议版本。
     * 2.  创建一个 {@link ConsumerProtocolSubscription} 对象来存储待序列化的数据。
     * 3.  对订阅的主题列表进行排序，以确保序列化结果的确定性。
     * 4.  复制用户自定义数据 (userData)。
     * 5.  对消费者拥有的分区列表 (ownedPartitions) 进行排序（先按主题名，再按分区号），并将其转换为协议定义的格式。
     *     为了节省空间，相同主题的分区会聚合到同一个 {@link ConsumerProtocolSubscription.TopicPartition} 对象中。
     * 6.  设置机架 ID (rackId)，如果存在的话。
     * 7.  设置消费者的年代信息 (generationId)，如果不存在则默认为 -1。
     * 8.  使用 {@link MessageUtil#toVersionPrefixedByteBuffer(short, Object)} 方法将版本号和序列化后的数据组装成最终的 ByteBuffer。
     * </p>
     *
     * @param subscription 包含消费者订阅信息的 {@link Subscription} 对象。
     * @param version      用于序列化的协议版本号。
     * @return 包含序列化后的订阅信息的 ByteBuffer。
     */
    public static ByteBuffer serializeSubscription(final Subscription subscription, short version) {
        // 检查并可能调整传入的 version，确保其在支持的范围内 (此方法未在当前代码片段中提供，假定存在)
        version = checkSubscriptionVersion(version);

        // 创建 ConsumerProtocolSubscription 对象，用于承载序列化数据
        ConsumerProtocolSubscription data = new ConsumerProtocolSubscription();

        // 获取订阅的主题列表，并创建一个新的 ArrayList 进行排序，避免修改原始列表
        List<String> topics = new ArrayList<>(subscription.topics());
        // 对主题列表进行字典序排序，以保证序列化结果的一致性
        Collections.sort(topics);
        // 将排序后的主题列表设置到 data 对象中
        data.setTopics(topics);

        // 设置用户数据。如果 subscription.userData() 不为 null，则复制其内容；否则设置为 null。
        // .duplicate() 方法确保了 ByteBuffer 的独立性，避免后续操作影响原始数据。
        data.setUserData(subscription.userData() != null ? subscription.userData().duplicate() : null);

        // 获取消费者拥有的分区列表
        List<TopicPartition> ownedPartitions = new ArrayList<>(subscription.ownedPartitions());
        // 对拥有的分区进行排序：首先按主题名称排序，然后按分区号排序。
        // 这是为了确保序列化输出的确定性，并可能优化存储结构。
        ownedPartitions.sort(Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
        // 用于临时存储当前正在处理的主题的分区信息
        ConsumerProtocolSubscription.TopicPartition partition = null;
        // 遍历排序后的已拥有分区列表
        for (TopicPartition tp : ownedPartitions) {
            // 如果是第一个分区，或者当前分区的主题与上一个分区的主题不同
            if (partition == null || !partition.topic().equals(tp.topic())) {
                // 创建一个新的 ConsumerProtocolSubscription.TopicPartition 对象，并设置其主题名称
                partition = new ConsumerProtocolSubscription.TopicPartition().setTopic(tp.topic());
                // 将这个新的主题分区对象添加到 data 的 ownedPartitions 列表中
                data.ownedPartitions().add(partition);
            }
            // 将当前分区的分区号添加到当前主题的分区列表中
            partition.partitions().add(tp.partition());
        }
        // 如果订阅信息中包含 rackId，则将其设置到 data 对象中
        subscription.rackId().ifPresent(data::setRackId);

        // 设置 generationId。如果 subscription.generationId() 存在，则使用其值；否则使用 -1。
        data.setGenerationId(subscription.generationId().orElse(-1));
        // 使用 MessageUtil 工具类将版本号和 data 对象序列化为 ByteBuffer
        // toVersionPrefixedByteBuffer 会在 ByteBuffer 的开头写入版本号，然后是 data 对象的序列化内容。
        return MessageUtil.toVersionPrefixedByteBuffer(version, data);
    }

    /**
     * 使用指定的版本从 ByteBuffer 中反序列化消费者的订阅信息。
     * <p>
     * 实现细节：
     * 1.  检查并规范化指定的协议版本。
     * 2.  使用 {@link ConsumerProtocolSubscription} 的构造函数，配合 {@link ByteBufferAccessor} 和版本号，从 ByteBuffer 中解析数据。
     * 3.  将解析得到的协议格式的拥有分区列表 (ownedPartitions) 转换回 {@link List<TopicPartition>}。
     * 4.  构造并返回一个新的 {@link Subscription} 对象，填充从 data 对象中获取的各个字段（主题、用户数据、拥有分区、年代ID、机架ID）。
     * </p>
     *
     * @param buffer  包含序列化订阅信息的 ByteBuffer。
     * @param version 用于反序列化的协议版本号。
     * @return 反序列化得到的 {@link Subscription} 对象。
     * @throws SchemaException 如果在解析过程中发生 ByteBuffer 下溢或其他模式相关错误。
     */
    public static Subscription deserializeSubscription(final ByteBuffer buffer, short version) {
        // 检查并可能调整传入的 version，确保其在支持的范围内 (此方法未在当前代码片段中提供，假定存在)
        version = checkSubscriptionVersion(version);

        try {
            // 使用 ConsumerProtocolSubscription 的构造函数从 ByteBuffer 中反序列化数据。
            // ByteBufferAccessor 用于以特定版本读取 ByteBuffer。
            ConsumerProtocolSubscription data =
                new ConsumerProtocolSubscription(new ByteBufferAccessor(buffer), version);

            // 创建一个列表用于存储反序列化后的 TopicPartition 对象
            List<TopicPartition> ownedPartitions = new ArrayList<>();
            // 遍历从 data 对象中获取的已拥有分区信息 (ConsumerProtocolSubscription.TopicPartition 格式)
            for (ConsumerProtocolSubscription.TopicPartition tp : data.ownedPartitions()) {
                // 对于每个主题，遍历其下的所有分区号
                for (Integer partition : tp.partitions()) {
                    // 创建新的 TopicPartition 对象并添加到列表中
                    ownedPartitions.add(new TopicPartition(tp.topic(), partition));
                }
            }

            // 构建并返回一个新的 Subscription 对象
            return new Subscription(
                data.topics(), // 设置主题列表
                data.userData() != null ? data.userData().duplicate() : null, // 设置用户数据，如果存在则复制
                ownedPartitions, // 设置已拥有的分区列表
                data.generationId(), // 设置 generationId
                // 设置 rackId。如果 data.rackId() 为 null 或为空字符串，则为 Optional.empty()；否则为 Optional.of(data.rackId())
                data.rackId() == null || data.rackId().isEmpty() ? Optional.empty() : Optional.of(data.rackId()));
        } catch (BufferUnderflowException e) {
            // 如果反序列化过程中发生 BufferUnderflowException，则抛出 SchemaException
            throw new SchemaException("Buffer underflow while parsing consumer protocol's subscription", e);
        }
    }

    /**
     * 从 ByteBuffer 中反序列化消费者的订阅信息，版本号会从 ByteBuffer 自身头部读取。
     * <p>
     * 应用场景：当接收到包含订阅信息的 ByteBuffer 时，此方法先解析出版本号，然后使用该版本号进行后续的完整反序列化。
     * </p>
     *
     * @param buffer 包含序列化订阅信息的 ByteBuffer，其起始位置应包含版本号。
     * @return 反序列化得到的 {@link Subscription} 对象。
     * @see #deserializeVersion(ByteBuffer)
     * @see #deserializeSubscription(ByteBuffer, short)
     */
    public static Subscription deserializeSubscription(final ByteBuffer buffer) {
        // 首先调用 deserializeVersion 从 buffer 中读取版本号，
        // 然后以读取到的版本号和原始 buffer 调用重载的 deserializeSubscription 方法。
        return deserializeSubscription(buffer, deserializeVersion(buffer));
    }

    /**
     * 使用指定的版本从 ByteBuffer 中反序列化 {@link ConsumerProtocolSubscription} 对象。
     * <p>
     * 与 {@link #deserializeSubscription(ByteBuffer, short)} 不同，此方法直接返回协议层级的 {@link ConsumerProtocolSubscription} 对象，
     * 而不是更高级别的 {@link Subscription} 抽象。
     * </p>
     * <p>
     * 应用场景：可能用于需要直接操作底层协议数据结构的场景，或者在协议内部的不同模块间传递数据。
     * </p>
     *
     * @param buffer  包含序列化订阅信息的 ByteBuffer。
     * @param version 用于反序列化的协议版本号。
     * @return 反序列化得到的 {@link ConsumerProtocolSubscription} 对象。
     * @throws SchemaException 如果在解析过程中发生 ByteBuffer 下溢或其他模式相关错误。
     */
    public static ConsumerProtocolSubscription deserializeConsumerProtocolSubscription(
        final ByteBuffer buffer,
        short version
    ) {
        // 检查并可能调整传入的 version，确保其在支持的范围内 (此方法未在当前代码片段中提供，假定存在)
        version = checkSubscriptionVersion(version);

        try {
            // 直接构造并返回 ConsumerProtocolSubscription 对象，使用 ByteBufferAccessor 和指定版本进行解析
            return new ConsumerProtocolSubscription(new ByteBufferAccessor(buffer), version);
        } catch (BufferUnderflowException e) {
            // 如果反序列化过程中发生 BufferUnderflowException，则抛出 SchemaException
            throw new SchemaException("Buffer underflow while parsing consumer protocol's subscription", e);
        }
    }

    /**
     * 从 ByteBuffer 中反序列化 ConsumerProtocolSubscription 对象。
     * <p>
     * 应用场景：当 Broker 端收到消费者的订阅信息时，需要将其从字节流反序列化为结构化对象进行处理。
     * 此方法首先从 buffer 中解析出协议版本，然后调用另一个重载方法进行实际的反序列化。
     * </p>
     *
     * @param buffer 包含序列化后的 ConsumerProtocolSubscription 数据的 ByteBuffer。
     * @return 反序列化得到的 {@link ConsumerProtocolSubscription} 对象。
     */
    public static ConsumerProtocolSubscription deserializeConsumerProtocolSubscription(
        final ByteBuffer buffer
    ) {
        // 调用另一个 deserializeConsumerProtocolSubscription 方法，传入 buffer 和从 buffer 中解析出的版本号
        return deserializeConsumerProtocolSubscription(buffer, deserializeVersion(buffer));
    }


    /**
     * 使用最高支持的版本序列化消费者的分配信息。
     * <p>
     * 应用场景：当 Coordinator 计算出分区分配结果后，需要将分配信息序列化后发送给消费者。
     * 此方法默认使用当前代码支持的最高协议版本进行序列化。
     * </p>
     *
     * @param assignment 包含消费者分区分配信息的 {@link Assignment} 对象。
     * @return 包含序列化后的分配信息的 ByteBuffer。
     */
    public static ByteBuffer serializeAssignment(final Assignment assignment) {
        // 调用另一个 serializeAssignment 方法，使用 ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION 作为序列化版本
        return serializeAssignment(assignment, ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION);
    }


    /**
     * 使用指定的版本序列化消费者的分配信息。
     * <p>
     * 实现细节：
     * 1.  检查并规范化指定的协议版本。
     * 2.  创建一个 {@link ConsumerProtocolAssignment} 对象来存储待序列化的数据。
     * 3.  复制用户自定义数据 (userData)。
     * 4.  遍历 {@link Assignment} 中的分区列表，将其转换为协议定义的格式。
     *     为了节省空间，相同主题的分区会聚合到同一个 {@link ConsumerProtocolAssignment.TopicPartition} 对象中。
     * 5.  使用 {@link MessageUtil#toVersionPrefixedByteBuffer(short, Object)} 方法将版本号和序列化后的数据组装成最终的 ByteBuffer。
     * </p>
     * <p>
     * 应用场景：在消费者组重平衡后，Leader 消费者或 Coordinator 将计算出的分区分配方案序列化，以便分发给组内其他成员。
     * </p>
     * <p>
     * 设计考虑：
     * - 版本控制：通过 `version` 参数和 `checkAssignmentVersion` 方法确保协议的兼容性。
     * - 数据结构优化：将同一主题的分区聚合存储，可以减少序列化后数据的大小。
     * - ByteBuffer复用：`userData().duplicate()` 创建了一个共享内容但独立位置、限制和标记的 ByteBuffer，避免了不必要的拷贝，同时保证了原始数据的安全。
     * </p>
     *
     * @param assignment 包含消费者分区分配信息的 {@link Assignment} 对象。
     * @param version    用于序列化的协议版本号。
     * @return 包含序列化后的分配信息的 ByteBuffer。
     */
    public static ByteBuffer serializeAssignment(final Assignment assignment, short version) {
        // 检查并可能调整传入的 version，确保其在支持的范围内
        version = checkAssignmentVersion(version);

        // 创建 ConsumerProtocolAssignment 对象，用于承载序列化数据
        ConsumerProtocolAssignment data = new ConsumerProtocolAssignment();
        // 设置用户数据。如果 assignment.userData() 不为 null，则复制其内容；否则设置为 null。
        // .duplicate() 方法确保了 ByteBuffer 的独立性，避免后续操作影响原始数据。
        data.setUserData(assignment.userData() != null ? assignment.userData().duplicate() : null);
        // 遍历 assignment 对象中的所有分区信息
        assignment.partitions().forEach(tp -> {
            // 尝试从 data 的 assignedPartitions 列表中查找是否已存在该主题的分区条目
            ConsumerProtocolAssignment.TopicPartition partition = data.assignedPartitions().find(tp.topic());
            // 如果未找到对应主题的条目
            if (partition == null) {
                // 创建一个新的 ConsumerProtocolAssignment.TopicPartition 对象，并设置其主题名称
                partition = new ConsumerProtocolAssignment.TopicPartition().setTopic(tp.topic());
                // 将这个新的主题分区对象添加到 data 的 assignedPartitions 列表中
                data.assignedPartitions().add(partition);
            }
            // 将当前分区的分区号添加到对应主题的分区列表中
            partition.partitions().add(tp.partition());
        });
        // 使用 MessageUtil 工具类将版本号和 data 对象序列化为 ByteBuffer
        // toVersionPrefixedByteBuffer 会在 ByteBuffer 的开头写入版本号，然后是 data 对象的序列化内容。
        return MessageUtil.toVersionPrefixedByteBuffer(version, data);
    }


    /**
     * 使用指定的版本序列化 {@link ConsumerProtocolAssignment} 对象。
     * <p>
     * 应用场景：这是一个更底层的序列化方法，当已经拥有一个 {@link ConsumerProtocolAssignment} 对象（而不是通用的 {@link Assignment} 对象）时，
     * 可以直接调用此方法进行序列化。例如，在某些内部处理流程中可能直接操作协议对象。
     * </p>
     * <p>
     * 实现细节：
     * 1. 检查并规范化指定的协议版本。
     * 2. 直接使用 {@link MessageUtil#toVersionPrefixedByteBuffer(short, Object)} 进行序列化。
     * </p>
     *
     * @param assignment 要序列化的 {@link ConsumerProtocolAssignment} 对象。
     * @param version    用于序列化的协议版本号。
     * @return 包含序列化后的分配信息的 ByteBuffer。
     */
    public static ByteBuffer serializeAssignment(final ConsumerProtocolAssignment assignment, short version) {
        // 检查并可能调整传入的 version，确保其在支持的范围内
        version = checkAssignmentVersion(version);
        // 使用 MessageUtil 工具类将版本号和 assignment 对象序列化为 ByteBuffer
        return MessageUtil.toVersionPrefixedByteBuffer(version, assignment);
    }


    /**
     * 使用指定的版本从 ByteBuffer 中反序列化消费者的分配信息。
     * <p>
     * 实现细节：
     * 1.  检查并规范化指定的协议版本。
     * 2.  使用 {@link ConsumerProtocolAssignment} 的构造函数，配合 {@link ByteBufferAccessor} 和版本号，从 ByteBuffer 中解析数据到 `data` 对象。
     * 3.  创建一个空的 {@link ArrayList} 用于存储反序列化后的 {@link TopicPartition} 对象。
     * 4.  遍历 `data` 对象中的 `assignedPartitions` 列表（每个元素代表一个主题及其分配的分区列表）。
     * 5.  对于每个主题，再遍历其内部分区号列表，为每个主题和分区号组合创建一个新的 {@link TopicPartition} 对象，并添加到 `assignedPartitions` 列表中。
     * 6.  使用反序列化得到的分区列表和用户数据（如果存在，则复制）创建一个新的 {@link Assignment} 对象并返回。
     * 7.  如果在解析过程中发生 {@link BufferUnderflowException}（例如，数据不完整），则捕获该异常并抛出 {@link SchemaException}。
     * </p>
     * <p>
     * 应用场景：消费者客户端收到来自 Coordinator 的分区分配信息（通常是字节流形式）后，调用此方法将其转换为可操作的 {@link Assignment} 对象。
     * </p>
     * <p>
     * 设计考虑：
     * - 错误处理：通过捕获 `BufferUnderflowException` 并转换为更具体的 `SchemaException`，提供了更明确的错误信息。
     * - 数据转换：将协议特定的 `ConsumerProtocolAssignment.TopicPartition` 结构转换为通用的 `TopicPartition` 列表，方便上层逻辑使用。
     * - ByteBuffer 独立性：`data.userData().duplicate()` 确保了用户数据的独立性。
     * </p>
     *
     * @param buffer  包含序列化后的分配信息的 ByteBuffer。
     * @param version 用于反序列化的协议版本号。
     * @return 反序列化得到的 {@link Assignment} 对象。
     * @throws SchemaException 如果在解析过程中发生 ByteBuffer 下溢或数据格式错误。
     */
    public static Assignment deserializeAssignment(final ByteBuffer buffer, short version) {
        // 检查并可能调整传入的 version，确保其在支持的范围内
        version = checkAssignmentVersion(version);

        try {
            // 使用 ConsumerProtocolAssignment 的构造函数从 ByteBuffer 中反序列化数据
            // ByteBufferAccessor 用于以特定版本读取 ByteBuffer 中的数据
            ConsumerProtocolAssignment data =
                new ConsumerProtocolAssignment(new ByteBufferAccessor(buffer), version);

            // 创建一个列表用于存储解析出的 TopicPartition 对象
            List<TopicPartition> assignedPartitions = new ArrayList<>();
            // 遍历从 ByteBuffer 中解析出的 data 对象中的 assignedPartitions 列表
            // 每个 ConsumerProtocolAssignment.TopicPartition (tp) 代表一个主题及其分配的分区
            for (ConsumerProtocolAssignment.TopicPartition tp : data.assignedPartitions()) {
                // 遍历当前主题 tp 下的所有分区号
                for (Integer partition : tp.partitions()) {
                    // 为每个主题和分区号组合创建一个新的 TopicPartition 对象，并添加到 assignedPartitions 列表中
                    assignedPartitions.add(new TopicPartition(tp.topic(), partition));
                }
            }

            // 使用解析出的分区列表和用户数据（如果存在则复制）创建并返回一个新的 Assignment 对象
            return new Assignment(
                assignedPartitions,
                data.userData() != null ? data.userData().duplicate() : null);
        } catch (BufferUnderflowException e) {
            // 如果在解析过程中发生 BufferUnderflowException (例如，buffer中数据不足以完成解析)，
            // 则抛出 SchemaException，指示解析消费者协议分配信息时发生缓冲区下溢。
            throw new SchemaException("Buffer underflow while parsing consumer protocol's assignment", e);
        }
    }


    /**
     * 从 ByteBuffer 中反序列化消费者的分配信息，版本号从 buffer 中自动解析。
     * <p>
     * 应用场景：当消费者收到分配信息时，通常先读取版本号，然后根据版本号进行解析。
     * 此方法封装了这个常用操作，简化了调用方的代码。
     * </p>
     *
     * @param buffer 包含序列化后的分配信息的 ByteBuffer，其起始位置应包含版本号。
     * @return 反序列化得到的 {@link Assignment} 对象。
     */
    public static Assignment deserializeAssignment(final ByteBuffer buffer) {
        // 调用另一个 deserializeAssignment 方法，传入 buffer 和从 buffer 中解析出的版本号
        return deserializeAssignment(buffer, deserializeVersion(buffer));
    }


    /**
     * 使用指定的版本从 ByteBuffer 中反序列化 {@link ConsumerProtocolAssignment} 对象。
     * <p>
     * 应用场景：这是一个更底层的反序列化方法，当需要直接获取协议定义的 {@link ConsumerProtocolAssignment} 对象时使用。
     * 例如，在某些需要直接访问协议内部字段的场景，或者在进行协议转换时。
     * </p>
     * <p>
     * 实现细节：
     * 1. 检查并规范化指定的协议版本。
     * 2. 直接使用 {@link ConsumerProtocolAssignment} 的构造函数和 {@link ByteBufferAccessor} 从 ByteBuffer 中解析数据。
     * 3. 捕获 {@link BufferUnderflowException} 并转换为 {@link SchemaException}。
     * </p>
     *
     * @param buffer  包含序列化后的 ConsumerProtocolAssignment 数据的 ByteBuffer。
     * @param version 用于反序列化的协议版本号。
     * @return 反序列化得到的 {@link ConsumerProtocolAssignment} 对象。
     * @throws SchemaException 如果在解析过程中发生 ByteBuffer 下溢或数据格式错误。
     */
    public static ConsumerProtocolAssignment deserializeConsumerProtocolAssignment(
        final ByteBuffer buffer,
        short version
    ) {
        // 检查并可能调整传入的 version，确保其在支持的范围内
        version = checkAssignmentVersion(version);

        try {
            // 直接使用 ConsumerProtocolAssignment 的构造函数从 ByteBuffer 中反序列化数据
            // ByteBufferAccessor 用于以特定版本读取 ByteBuffer 中的数据
            return new ConsumerProtocolAssignment(new ByteBufferAccessor(buffer), version);
        } catch (BufferUnderflowException e) {
            // 如果在解析过程中发生 BufferUnderflowException，则抛出 SchemaException
            throw new SchemaException("Buffer underflow while parsing consumer protocol's assignment", e);
        }
    }


    /**
     * 从 ByteBuffer 中反序列化 {@link ConsumerProtocolAssignment} 对象，版本号从 buffer 中自动解析。
     * <p>
     * 应用场景：当需要直接反序列化为 {@link ConsumerProtocolAssignment} 对象，并且版本号包含在 ByteBuffer 的起始位置时使用。
     * </p>
     *
     * @param buffer 包含序列化后的 ConsumerProtocolAssignment 数据的 ByteBuffer，其起始位置应包含版本号。
     * @return 反序列化得到的 {@link ConsumerProtocolAssignment} 对象。
     */
    public static ConsumerProtocolAssignment deserializeConsumerProtocolAssignment(final ByteBuffer buffer) {
        // 调用另一个 deserializeConsumerProtocolAssignment 方法，传入 buffer 和从 buffer 中解析出的版本号
        return deserializeConsumerProtocolAssignment(buffer, deserializeVersion(buffer));
    }


    /**
     * 检查并规范化消费者协议订阅信息的版本号。
     * <p>
     * 实现细节：
     * - 如果传入的版本号低于 {@link ConsumerProtocolSubscription#LOWEST_SUPPORTED_VERSION}，则抛出 {@link SchemaException}，表示版本过低不受支持。
     * - 如果传入的版本号高于 {@link ConsumerProtocolSubscription#HIGHEST_SUPPORTED_VERSION}，则返回 {@link ConsumerProtocolSubscription#HIGHEST_SUPPORTED_VERSION}。
     *   这是一种向前兼容策略：当遇到比当前代码支持的更新版本时，尝试使用当前代码能理解的最高版本进行处理。
     * - 如果版本号在支持的范围内，则直接返回该版本号。
     * </p>
     * <p>
     * 应用场景：在序列化或反序列化订阅信息之前，调用此方法确保使用的版本号是有效的，并应用兼容性策略。
     * </p>
     * <p>
     * 设计考虑：
     * - 明确的版本支持范围：通过最低和最高支持版本定义了清晰的兼容性边界。
     * - 向前兼容：对于高于最高支持版本的情况，选择降级到最高支持版本，而不是直接拒绝，这增强了系统的鲁棒性，允许旧的消费者/Broker 处理来自新版本客户端/Broker 的请求（尽管可能丢失新特性）。
     * </p>
     *
     * @param version 要检查的协议版本号。
     * @return 规范化后的协议版本号。
     * @throws SchemaException 如果版本号低于最低支持版本。
     */
    private static short checkSubscriptionVersion(final short version) {
        // 检查版本是否低于最低支持版本
        if (version < ConsumerProtocolSubscription.LOWEST_SUPPORTED_VERSION)
            // 如果是，则抛出 SchemaException，指示不支持的订阅版本
            throw new SchemaException("Unsupported subscription version: " + version);
        // 检查版本是否高于最高支持版本
        else if (version > ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION)
            // 如果是，则返回最高支持版本（向前兼容策略）
            return ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION;
        else
            // 如果版本在支持范围内，则直接返回该版本
            return version;
    }


    /**
     * 检查并规范化消费者协议分配信息的版本号。
     * <p>
     * 实现细节：与 {@link #checkSubscriptionVersion(short)} 类似，但针对的是分配信息 ({@link ConsumerProtocolAssignment}) 的版本。
     * - 如果传入的版本号低于 {@link ConsumerProtocolAssignment#LOWEST_SUPPORTED_VERSION}，则抛出 {@link SchemaException}。
     * - 如果传入的版本号高于 {@link ConsumerProtocolAssignment#HIGHEST_SUPPORTED_VERSION}，则返回 {@link ConsumerProtocolAssignment#HIGHEST_SUPPORTED_VERSION}。
     * - 否则，返回原始版本号。
     * </p>
     * <p>
     * 应用场景：在序列化或反序列化分配信息之前，调用此方法确保使用的版本号是有效的，并应用兼容性策略。
     * </p>
     * <p>
     * 设计考虑：与 `checkSubscriptionVersion` 的设计考虑相同，确保分配协议部分的版本兼容性和鲁棒性。
     * </p>
     *
     * @param version 要检查的协议版本号。
     * @return 规范化后的协议版本号。
     * @throws SchemaException 如果版本号低于最低支持版本。
     */
    private static short checkAssignmentVersion(final short version) {
        // 检查版本是否低于最低支持版本
        if (version < ConsumerProtocolAssignment.LOWEST_SUPPORTED_VERSION)
            // 如果是，则抛出 SchemaException，指示不支持的分配版本
            throw new SchemaException("Unsupported assignment version: " + version);
        // 检查版本是否高于最高支持版本
        else if (version > ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION)
            // 如果是，则返回最高支持版本（向前兼容策略）
            return ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION;
        else
            // 如果版本在支持范围内，则直接返回该版本
            return version;
    }

}
