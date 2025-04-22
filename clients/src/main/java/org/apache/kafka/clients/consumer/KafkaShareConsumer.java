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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ShareConsumerDelegate;
import org.apache.kafka.clients.consumer.internals.ShareConsumerDelegateCreator;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaShareConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.propsToMap;

/**
 * 一个使用共享组（share group）从Kafka集群消费记录的客户端。
 * <p>
 *     <em>这是由KIP-932引入的早期访问功能，目前正在开发中。
 *     在完全实现和发布之前，不适合在生产环境中使用。</em>
 *
 * <h3>跨版本兼容性</h3>
 * 该客户端可以与支持共享组功能的broker版本进行通信。如果调用当前运行的broker版本不支持的API，
 * 将会收到{@link org.apache.kafka.common.errors.UnsupportedVersionException}异常。
 *
 * <h3><a name="sharegroups">共享组和主题订阅</a></h3>
 * Kafka使用<i>共享组</i>的概念来允许一组消费者协同工作，共同消费和处理记录。所有共享相同{@code group.id}
 * 的消费者实例将属于同一个共享组。
 * <p>
 * 组内的每个消费者可以使用{@link #subscribe(Collection)}方法动态设置它想要订阅的主题列表。
 * Kafka会将订阅主题中的每条消息传递给共享组中的一个消费者。与消费者组不同，共享组允许多个消费者
 * 从同一个分区消费数据，这在分区间实现了更灵活的记录共享，但代价是失去了记录的顺序保证。
 * <p>
 * 共享组的成员关系是动态维护的：如果一个消费者失败，分配给它的分区将被重新分配给同组中的其他消费者。
 * 同样，如果有新的消费者加入组，分区分配会被重新评估，分区可能会从现有消费者转移到新消费者。
 * 这个过程被称为<i>重平衡（rebalancing）</i>，详细内容将在<a href="#failures">下文</a>讨论。
 * 当订阅的主题添加了新的分区时，也会触发组重平衡。组会通过定期的元数据刷新自动检测新分区，
 * 并将它们分配给组内成员。
 * <p>
 * 从概念上讲，你可以将共享组视为由多个消费者组成的单个逻辑订阅者。
 * 实际上，在其他消息系统中，共享组大致相当于<em>持久共享订阅（durable shared subscription）</em>。
 * 你可以拥有多个共享组和消费者组独立地从相同的主题消费数据。
 *
 * <h3><a name="failures">检测消费者故障</a></h3>
 * 订阅一组主题后，消费者会在调用{@link #poll(Duration)}时自动加入组。这个方法的设计目的是确保消费者的存活状态。
 * 只要持续调用poll，消费者就会保持在组内并继续从分配给它的分区接收记录。在底层，消费者会定期向broker发送心跳。
 * 如果消费者崩溃或在共享组的会话超时时间内无法发送心跳，则该消费者将被视为已死亡，其分区将被重新分配。
 * <p>
 * 消费者也可能遇到"活锁（livelock）"情况，即它在后台继续发送心跳，但实际上没有处理进度。为了防止消费者在这种
 * 情况下无限期地持有其分区，我们提供了一个使用{@code max.poll.interval.ms}设置的存活检测机制。如果你没有
 * 按照这个频率调用poll，客户端将主动离开共享组。因此，要保持在组内，你必须继续调用poll。
 *
 * <h3>记录传递和确认</h3>
 * 当共享组中的消费者使用{@link #poll(Duration)}获取记录时，它会从匹配其订阅的任何主题分区接收可用记录。
 * 记录在传递给该消费者时会获得一个时间限制的获取锁。当记录被获取时，其他消费者无法访问该记录。默认情况下，
 * 锁定时间为30秒，但也可以通过组配置参数{@code group.share.record.lock.duration.ms}来控制。这个机制的思想是，
 * 一旦锁定时间过期，锁会自动释放，然后记录可以被传递给另一个消费者。持有锁的消费者可以通过以下方式处理记录：
 * <ul>
 *     <li>消费者可以确认记录已成功处理</li>
 *     <li>消费者可以释放记录，使记录可以进行另一次传递尝试</li>
 *     <li>消费者可以拒绝记录，表明该记录无法处理，且不会使该记录可用于另一次传递尝试</li>
 *     <li>消费者可以不做任何操作，在这种情况下，当锁定时间过期时，锁会自动释放</li>
 * </ul>
 * 集群对共享组中每个主题分区的消费者获取的记录数量有限制。一旦达到限制，获取记录将暂时不会返回更多记录，
 * 直到已获取的记录数量减少（这种情况自然发生在锁超时时）。这个限制由broker配置属性
 * {@code group.share.record.lock.partition.limit}控制。通过限制获取锁的持续时间并自动释放锁，
 * broker确保即使在消费者发生故障的情况下，传递也能继续进行。
 * <p>
 * 消费者可以选择使用隐式或显式确认来处理记录。
 * <p>如果应用程序对批次中的任何记录调用{@link #acknowledge(ConsumerRecord, AcknowledgeType)}，
 * 就是使用<em>显式确认</em>。在这种情况下：
 * <ul>
 *     <li>应用程序调用{@link #commitSync()}或{@link #commitAsync()}来将确认提交到Kafka。
 *     如果批次中的任何记录未被确认，它们将保持获取状态，并在未来的poll中再次呈现给应用程序。</li>
 *     <li>应用程序在不先提交的情况下调用{@link #poll(Duration)}，这会异步地将确认提交到Kafka。
 *     在这种情况下，如果提交确认失败，不会抛出异常。如果批次中的任何记录未被确认，它们将保持获取状态，
 *     并在未来的poll中再次呈现给应用程序。</li>
 *     <li>应用程序调用{@link #close()}，这会尝试提交任何待处理的确认并释放所有剩余的已获取记录。</li>
 * </ul>
 * 如果应用程序没有对批次中的任何记录调用{@link #acknowledge(ConsumerRecord, AcknowledgeType)}，
 * 就是使用<em>隐式确认</em>。在这种情况下：
 * <ul>
 *     <li>应用程序调用{@link #commitSync()}或{@link #commitAsync()}，这会隐式地确认所有已传递的记录
 *     已成功处理，并将确认提交到Kafka。</li>
 *     <li>应用程序在不提交的情况下调用{@link #poll(Duration)}，这也会隐式地确认所有已传递的记录，
 *     并异步地将确认提交到Kafka。在这种情况下，如果提交确认失败，不会抛出异常。</li>
 *     <li>应用程序调用{@link #close()}，这会释放所有已获取的记录而不进行确认。</li>
 * </ul>
 * <p>
 * The consumer guarantees that the records returned in the {@code ConsumerRecords} object for a specific topic-partition
 * are in order of increasing offset. For each topic-partition, Kafka guarantees that acknowledgements for the records
 * in a batch are performed atomically. This makes error handling significantly more straightforward because there can be
 * one error code per partition.
 *
 * <h3>Usage Examples</h3>
 * The share consumer APIs offer flexibility to cover a variety of consumption use cases. Here are some examples to
 * demonstrate how to use them.
 *
 * <h4>Acknowledging a batch of records (implicit acknowledgement)</h4>
 * This example demonstrates implicit acknowledgement using {@link #poll(Duration)} to acknowledge the records which
 * were delivered in the previous poll. All the records delivered are implicitly marked as successfully consumed and
 * acknowledged synchronously with Kafka as the consumer fetches more records.
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaShareConsumer&lt;String, String&gt; consumer = new KafkaShareConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;));
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             System.out.printf(&quot;offset = %d, key = %s, value = %s%n&quot;, record.offset(), record.key(), record.value());
 *             doProcessing(record);
 *         }
 *     }
 * </pre>
 *
 * Alternatively, you can use {@link #commitSync()} or {@link #commitAsync()} to commit the acknowledgements, but this is
 * slightly less efficient because there is an additional request sent to Kafka.
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaShareConsumer&lt;String, String&gt; consumer = new KafkaShareConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;));
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             System.out.printf(&quot;offset = %d, key = %s, value = %s%n&quot;, record.offset(), record.key(), record.value());
 *             doProcessing(record);
 *         }
 *         consumer.commitSync();
 *     }
 * </pre>
 *
 * <h4>Per-record acknowledgement (explicit acknowledgement)</h4>
 * This example demonstrates using different acknowledgement types depending on the outcome of processing the records.
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaShareConsumer&lt;String, String&gt; consumer = new KafkaShareConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;));
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             try {
 *                 doProcessing(record);
 *                 consumer.acknowledge(record, AcknowledgeType.ACCEPT);
 *             } catch (Exception e) {
 *                 consumer.acknowledge(record, AcknowledgeType.REJECT);
 *             }
 *         }
 *         consumer.commitSync();
 *     }
 * </pre>
 *
 * Each record processed is separately acknowledged using a call to {@link #acknowledge(ConsumerRecord, AcknowledgeType)}.
 * The {@link AcknowledgeType} argument indicates whether the record was processed successfully or not. In this case,
 * the bad records are rejected meaning that they’re not eligible for further delivery attempts. For a permanent error
 * such as a semantic error, this is appropriate. For a transient error which might not affect a subsequent processing
 * attempt, {@link AcknowledgeType#RELEASE} is more appropriate because the record remains eligible for further delivery attempts.
 * <p>
 * The calls to {@link #acknowledge(ConsumerRecord, AcknowledgeType)} are simply updating local information in the consumer.
 * It is only once {@link #commitSync()} is called that the acknowledgements are committed by sending the new state
 * information to Kafka.
 *
 * <h4>Per-record acknowledgement, ending processing of the batch on an error (explicit acknowledgement)</h4>
 * This example demonstrates ending processing of a batch of records on the first error.
 * <pre>
 *     Properties props = new Properties();
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     KafkaShareConsumer&lt;String, String&gt; consumer = new KafkaShareConsumer&lt;&gt;(props);
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;));
 *     while (true) {
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             try {
 *                 doProcessing(record);
 *                 consumer.acknowledge(record, AcknowledgeType.ACCEPT);
 *             } catch (Exception e) {
 *                 consumer.acknowledge(record, AcknowledgeType.REJECT);
 *                 break;
 *             }
 *         }
 *         consumer.commitSync();
 *     }
 * </pre>
 * There are the following cases in this example:
 * <ol>
 *     <li>The batch contains no records, in which case the application just polls again. The call to {@link #commitSync()}
 *     just does nothing because the batch was empty.</li>
 *     <li>All of the records in the batch are processed successfully. The calls to {@link #acknowledge(ConsumerRecord, AcknowledgeType)}
 *     specifying {@code AcknowledgeType.ACCEPT} mark all records in the batch as successfully processed.</li>
 *     <li>One of the records encounters an exception. The call to {@link #acknowledge(ConsumerRecord, AcknowledgeType)} specifying
 *     {@code AcknowledgeType.REJECT} rejects that record. Earlier records in the batch have already been marked as successfully
 *     processed. The call to {@link #commitSync()} commits the acknowledgements, but the records after the failed record
 *     remain acquired as part of the same delivery attempt and will be presented to the application in response to another poll.</li>
 * </ol>
 *
 * <h3>读取事务性记录</h3>
 * 共享组处理事务性记录的方式由{@code group.share.isolation.level}配置属性控制。在共享组中，
 * 隔离级别应用于整个共享组，而不仅仅是单个消费者。
 * <p>
 * 在<code>read_uncommitted</code>隔离级别下，共享组消费所有非事务性和事务性记录。
 * 消费受高水位标记（high-water mark）的限制。
 * <p>
 * 在<code>read_committed</code>隔离级别下（目前尚不支持），共享组只消费非事务性记录和已提交的事务性记录。
 * 只有非事务性记录和已提交的事务性记录才有资格成为正在处理的记录。消费受最后稳定偏移量的限制，
 * 因此一个开放的事务会阻塞使用read_committed隔离级别的共享组的进度。
 *
 * <h3><a name="multithreaded">多线程处理</a></h3>
 * 消费者不是线程安全的。用户有责任确保多线程访问得到适当的同步。未同步的访问将导致
 * {@link java.util.ConcurrentModificationException}异常。
 * <p>
 * 这个规则的唯一例外是{@link #wakeup()}方法，它可以安全地从外部线程使用来中断活动操作。
 * 在这种情况下，阻塞在操作上的线程将抛出{@link org.apache.kafka.common.errors.WakeupException}异常。
 * 这可以用来从另一个线程关闭消费者。以下代码片段展示了典型的模式：
 *
 * <pre>
 * public class KafkaShareConsumerRunner implements Runnable {
 *     private final AtomicBoolean closed = new AtomicBoolean(false);
 *     private final KafkaShareConsumer consumer;
 *
 *     public KafkaShareConsumerRunner(KafkaShareConsumer consumer) {
 *       this.consumer = consumer;
 *     }
 *
 *     {@literal}@Override
 *     public void run() {
 *         try {
 *             consumer.subscribe(Arrays.asList("topic"));
 *             while (!closed.get()) {
 *                 ConsumerRecords records = consumer.poll(Duration.ofMillis(10000));
 *                 // 处理新记录
 *             }
 *         } catch (WakeupException e) {
 *             // 如果正在关闭则忽略异常
 *             if (!closed.get()) throw e;
 *         } finally {
 *             consumer.close();
 *         }
 *     }
 *
 *     // 可以从单独的线程调用的关闭钩子
 *     public void shutdown() {
 *         closed.set(true);
 *         consumer.wakeup();
 *     }
 * }
 * </pre>
 *
 * 然后在单独的线程中，可以通过设置closed标志并唤醒消费者来关闭消费者。
 * <pre>
 *     closed.set(true);
 *     consumer.wakeup();
 * </pre>
 *
 * <p>
 * 注意，虽然可以使用线程中断而不是{@link #wakeup()}来中止阻塞操作（在这种情况下，将引发{@link InterruptException}），
 * 但我们不建议使用它们，因为它们可能导致消费者的清理关闭被中止。中断主要支持那些无法使用{@link #wakeup()}的情况，
 * 例如当消费者线程由不知道Kafka客户端的代码管理时。
 * <p>
 * 我们有意避免实现特定的线程处理模型。多线程处理有多种可能的选项，其中最直接的方式是为每个消费者
 * 专门分配一个线程。
 */
@InterfaceStability.Evolving
public class KafkaShareConsumer<K, V> implements ShareConsumer<K, V> {

    /**
     * 共享消费者代理创建器，用于创建ShareConsumerDelegate实例
     * 这是一个静态常量，所有KafkaShareConsumer实例共享同一个创建器
     */
    private static final ShareConsumerDelegateCreator CREATOR = new ShareConsumerDelegateCreator();

    /**
     * 共享消费者代理对象，实现了具体的消费者功能
     * 采用代理模式将具体实现委托给此对象，使主类保持简洁
     */
    private final ShareConsumerDelegate<K, V> delegate;

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >here</a>. Values can be
     * either strings or objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaShareConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     */
    /**
     * 使用配置映射创建共享消费者实例
     * 
     * @param configs 消费者配置，键值对形式，支持字符串或对应类型的对象值
     *               例如：数值配置可以接受字符串"42"或整数42
     */
    public KafkaShareConsumer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaShareConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     */
    /**
     * 使用Properties对象创建共享消费者实例
     * 
     * @param properties 消费者配置属性
     */
    public KafkaShareConsumer(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration, and a
     * key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaShareConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    /**
     * 使用Properties对象和指定的键值反序列化器创建共享消费者实例
     * 
     * @param properties 消费者配置属性
     * @param keyDeserializer 键的反序列化器，直接传入时不会调用其configure()方法
     * @param valueDeserializer 值的反序列化器，直接传入时不会调用其configure()方法
     */
    public KafkaShareConsumer(Properties properties,
                              Deserializer<K> keyDeserializer,
                              Deserializer<V> valueDeserializer) {
        this(propsToMap(properties), keyDeserializer, valueDeserializer);
    }

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration, and a key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaShareConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    /**
     * 使用配置映射和指定的键值反序列化器创建共享消费者实例
     * 
     * @param configs 消费者配置映射
     * @param keyDeserializer 键的反序列化器，直接传入时不会调用其configure()方法
     * @param valueDeserializer 值的反序列化器，直接传入时不会调用其configure()方法
     */
    public KafkaShareConsumer(Map<String, Object> configs,
                              Deserializer<K> keyDeserializer,
                              Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
                keyDeserializer, valueDeserializer);
    }

    /**
     * 使用ConsumerConfig对象和指定的键值反序列化器创建共享消费者实例
     * 这是一个包级私有构造函数，主要供内部使用
     * 
     * @param config 消费者配置对象
     * @param keyDeserializer 键的反序列化器
     * @param valueDeserializer 值的反序列化器
     */
    KafkaShareConsumer(ConsumerConfig config,
                              Deserializer<K> keyDeserializer,
                              Deserializer<V> valueDeserializer) {
        delegate = CREATOR.create(config, keyDeserializer, valueDeserializer);
    }

    /**
     * 使用完整参数集创建共享消费者实例
     * 这是一个包级私有构造函数，主要用于测试和内部使用，提供了最大的灵活性
     * 
     * @param logContext 日志上下文
     * @param clientId 客户端ID
     * @param groupId 消费者组ID
     * @param config 消费者配置
     * @param keyDeserializer 键的反序列化器
     * @param valueDeserializer 值的反序列化器
     * @param time 时间实例，用于时间相关操作
     * @param client Kafka客户端实例
     * @param subscriptions 订阅状态管理器
     * @param metadata 消费者元数据
     */
    KafkaShareConsumer(final LogContext logContext,
                       final String clientId,
                       final String groupId,
                       final ConsumerConfig config,
                       final Deserializer<K> keyDeserializer,
                       final Deserializer<V> valueDeserializer,
                       final Time time,
                       final KafkaClient client,
                       final SubscriptionState subscriptions,
                       final ConsumerMetadata metadata) {
        delegate = CREATOR.create(
                logContext, clientId, groupId, config, keyDeserializer, valueDeserializer,
                time, client, subscriptions, metadata);
    }

    /**
     * Get the current subscription. Will return the same topics used in the most recent call to
     * {@link #subscribe(Collection)}, or an empty set if no such call has been made.
     *
     * @return The set of topics currently subscribed to
     */
    /**
     * 获取当前的主题订阅集合
     * 返回最近一次调用subscribe()方法时使用的主题列表
     * 如果从未调用过subscribe()，则返回空集合
     * 
     * @return 当前订阅的主题集合
     */
    @Override
    public Set<String> subscription() {
        return delegate.subscription();
    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment, if there is one.</b> If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * As part of group management, the coordinator will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if any one of the following events are triggered:
     * <ul>
     * <li>A member joins or leaves the share group
     * <li>An existing member of the share group is shut down or fails
     * <li>The number of partitions changes for any of the subscribed topics
     * <li>A subscribed topic is created or deleted
     * </ul>
     *
     * @param topics The list of topics to subscribe to
     *
     * @throws IllegalArgumentException if topics is null or contains null or empty elements
     * @throws KafkaException for any other unrecoverable errors
     */
    /**
     * 订阅指定的主题列表
     * 这会替换当前的订阅（如果有的话），而不是增量添加
     * 如果提供的主题列表为空，效果等同于调用unsubscribe()
     * 
     * @param topics 要订阅的主题列表
     * @throws IllegalArgumentException 如果topics为null或包含null或空元素
     * @throws KafkaException 发生其他不可恢复的错误时
     */
    @Override
    public void subscribe(Collection<String> topics) {
        delegate.subscribe(topics);
    }

    /**
     * Unsubscribe from topics currently subscribed with {@link #subscribe(Collection)}.
     *
     * @throws KafkaException for any other unrecoverable errors
     */
    /**
     * 取消当前通过subscribe()方法订阅的所有主题
     * 
     * @throws KafkaException 发生不可恢复的错误时
     */
    @Override
    public void unsubscribe() {
        delegate.unsubscribe();
    }

    /**
     * Fetch data for the topics specified using {@link #subscribe(Collection)}. It is an error to not have
     * subscribed to any topics before polling for data.
     *
     * <p>
     * This method returns immediately if there are records available. Otherwise, it will await the passed timeout.
     * If the timeout expires, an empty record set will be returned.
     *
     * @param timeout The maximum time to block (must not be greater than {@link Long#MAX_VALUE} milliseconds)
     *
     * @return map of topic to records since the last fetch for the subscribed list of topics
     *
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException if caller lacks Read access to any of the subscribed
     *             topics or to the share group. See the exception for more details
     * @throws IllegalArgumentException if the timeout value is negative
     * @throws IllegalStateException if the consumer is not subscribed to any topics
     * @throws ArithmeticException if the timeout is greater than {@link Long#MAX_VALUE} milliseconds.
     * @throws InvalidTopicException if the current subscription contains any invalid
     *             topic (per {@link org.apache.kafka.common.internals.Topic#validate(String)})
     * @throws WakeupException if {@link #wakeup()} is called before or while this method is called
     * @throws InterruptException if the calling thread is interrupted before or while this method is called
     * @throws KafkaException for any other unrecoverable errors
     */
    /**
     * 获取已订阅主题的数据
     * 如果有可用记录，立即返回；否则，等待指定的超时时间
     * 如果超时时间到期仍无可用记录，则返回空记录集
     * 
     * @param timeout 最大阻塞时间（不能大于Long.MAX_VALUE毫秒）
     * @return 自上次获取以来订阅主题的记录映射
     * @throws AuthenticationException 认证失败时
     * @throws AuthorizationException 缺少对订阅主题或共享组的读取权限时
     * @throws IllegalArgumentException 超时值为负数时
     * @throws IllegalStateException 消费者未订阅任何主题时
     * @throws ArithmeticException 超时值大于Long.MAX_VALUE毫秒时
     * @throws InvalidTopicException 当前订阅包含无效主题时
     * @throws WakeupException 在调用此方法之前或期间调用了wakeup()
     * @throws InterruptException 调用线程在调用此方法之前或期间被中断
     * @throws KafkaException 发生其他不可恢复的错误时
     */
    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        return delegate.poll(timeout);
    }

    /**
     * Acknowledge successful delivery of a record returned on the last {@link #poll(Duration)} call.
     * The acknowledgement is committed on the next {@link #commitSync()}, {@link #commitAsync()} or
     * {@link #poll(Duration)} call.
     *
     * @param record The record to acknowledge
     *
     * @throws IllegalStateException if the record is not waiting to be acknowledged, or the consumer has already
     *                               used implicit acknowledgement
     */
    /**
     * 确认上一次poll()调用返回的记录已成功处理
     * 确认会在下一次commitSync()、commitAsync()或poll()调用时提交
     * 
     * @param record 要确认的记录
     * @throws IllegalStateException 如果记录不在等待确认状态，或消费者已使用了隐式确认
     */
    @Override
    public void acknowledge(ConsumerRecord<K, V> record) {
        delegate.acknowledge(record);
    }

    /**
     * Acknowledge delivery of a record returned on the last {@link #poll(Duration)} call indicating whether
     * it was processed successfully. The acknowledgement is committed on the next {@link #commitSync()},
     * {@link #commitAsync()} or {@link #poll(Duration)} call. By using this method, the consumer is using
     * <b>explicit acknowledgement</b>.
     *
     * @param record The record to acknowledge
     * @param type The acknowledgement type which indicates whether it was processed successfully
     *
     * @throws IllegalStateException if the record is not waiting to be acknowledged, or the consumer has already
     *                               used implicit acknowledgement
     */
    /**
     * 确认上一次poll()调用返回的记录的处理结果
     * 使用显式确认模式，可以指定记录的处理结果类型
     * 确认会在下一次commitSync()、commitAsync()或poll()调用时提交
     * 
     * @param record 要确认的记录
     * @param type 确认类型，表明记录是否处理成功
     * @throws IllegalStateException 如果记录不在等待确认状态，或消费者已使用了隐式确认
     */
    @Override
    public void acknowledge(ConsumerRecord<K, V> record, AcknowledgeType type) {
        delegate.acknowledge(record, type);
    }

    /**
     * Commit the acknowledgements for the records returned. If the consumer is using explicit acknowledgement,
     * the acknowledgements to commit have been indicated using {@link #acknowledge(ConsumerRecord)} or
     * {@link #acknowledge(ConsumerRecord, AcknowledgeType)}. If the consumer is using implicit acknowledgement,
     * all the records returned by the latest call to {@link #poll(Duration)} are acknowledged.
     *
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms}
     * expires.
     *
     * @return A map of the results for each topic-partition for which delivery was acknowledged.
     *         If the acknowledgement failed for a topic-partition, an exception is present.
     *
     * @throws WakeupException if {@link #wakeup()} is called before or while this method is called
     * @throws InterruptException if the thread is interrupted while blocked
     * @throws KafkaException for any other unrecoverable errors
     */
    /**
     * 同步提交已确认的记录
     * 对于显式确认模式，提交通过acknowledge()方法指定的确认
     * 对于隐式确认模式，提交最近一次poll()返回的所有记录
     * 
     * @return 每个主题分区的确认结果映射，如果确认失败则包含异常
     * @throws WakeupException 在调用此方法之前或期间调用了wakeup()
     * @throws InterruptException 线程在阻塞时被中断
     * @throws KafkaException 发生其他不可恢复的错误时
     */
    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync() {
        return delegate.commitSync();
    }

    /**
     * Commit the acknowledgements for the records returned. If the consumer is using explicit acknowledgement,
     * the acknowledgements to commit have been indicated using {@link #acknowledge(ConsumerRecord)} or
     * {@link #acknowledge(ConsumerRecord, AcknowledgeType)}. If the consumer is using implicit acknowledgement,
     * all the records returned by the latest call to {@link #poll(Duration)} are acknowledged.

     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     *
     * @param timeout The maximum amount of time to await completion of the acknowledgement
     *
     * @return A map of the results for each topic-partition for which delivery was acknowledged.
     *         If the acknowledgement failed for a topic-partition, an exception is present.
     *
     * @throws IllegalArgumentException if the {@code timeout} is negative
     * @throws WakeupException if {@link #wakeup()} is called before or while this method is called
     * @throws InterruptException if the thread is interrupted while blocked
     * @throws KafkaException for any other unrecoverable errors
     */
    /**
     * 在指定超时时间内同步提交已确认的记录
     * 
     * @param timeout 等待确认完成的最大时间
     * @return 每个主题分区的确认结果映射，如果确认失败则包含异常
     * @throws IllegalArgumentException 如果timeout为负数
     * @throws WakeupException 在调用此方法之前或期间调用了wakeup()
     * @throws InterruptException 线程在阻塞时被中断
     * @throws KafkaException 发生其他不可恢复的错误时
     */
    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync(Duration timeout) {
        return delegate.commitSync(timeout);
    }

    /**
     * Commit the acknowledgements for the records returned. If the consumer is using explicit acknowledgement,
     * the acknowledgements to commit have been indicated using {@link #acknowledge(ConsumerRecord)} or
     * {@link #acknowledge(ConsumerRecord, AcknowledgeType)}. If the consumer is using implicit acknowledgement,
     * all the records returned by the latest call to {@link #poll(Duration)} are acknowledged.
     *
     * @throws KafkaException for any other unrecoverable errors
     */
    /**
     * 异步提交已确认的记录
     * 对于显式确认模式，提交通过acknowledge()方法指定的确认
     * 对于隐式确认模式，提交最近一次poll()返回的所有记录
     * 
     * @throws KafkaException 发生不可恢复的错误时
     */
    @Override
    public void commitAsync() {
        delegate.commitAsync();
    }

    /**
     * Sets the acknowledgement commit callback which can be used to handle acknowledgement completion.
     *
     * @param callback The acknowledgement commit callback
     */
    /**
     * 设置确认提交回调
     * 可用于处理确认完成事件
     * 
     * @param callback 确认提交回调函数
     */
    @Override
    public void setAcknowledgementCommitCallback(AcknowledgementCommitCallback callback) {
        delegate.setAcknowledgementCommitCallback(callback);
    }

    /**
     * Determines the client's unique client instance ID used for telemetry. This ID is unique to
     * this specific client instance and will not change after it is initially generated.
     * The ID is useful for correlating client operations with telemetry sent to the broker and
     * to its eventual monitoring destinations.
     * <p>
     * If telemetry is enabled, this will first require a connection to the cluster to generate
     * the unique client instance ID. This method waits up to {@code timeout} for the consumer
     * client to complete the request.
     * <p>
     * Client telemetry is controlled by the {@link ConsumerConfig#ENABLE_METRICS_PUSH_CONFIG}
     * configuration option.
     *
     * @param timeout The maximum time to wait for consumer client to determine its client instance ID.
     *                The value must be non-negative. Specifying a timeout of zero means do not
     *                wait for the initial request to complete if it hasn't already.
     *
     * @return The client's assigned instance id used for metrics collection.
     *
     * @throws IllegalArgumentException if the {@code timeout} is negative
     * @throws IllegalStateException if telemetry is not enabled
     * @throws WakeupException if {@link #wakeup()} is called before or while this method is called
     * @throws InterruptException if the thread is interrupted while blocked
     * @throws KafkaException if an unexpected error occurs while trying to determine the client
     *                        instance ID, though this error does not necessarily imply the
     *                        consumer client is otherwise unusable
     */
    /**
     * 获取客户端的唯一实例ID
     * 此ID用于遥测目的，在初始生成后不会改变
     * 
     * @param timeout 等待客户端确定实例ID的最大时间
     * @return 客户端的实例ID
     * @throws IllegalArgumentException 如果timeout为负数
     * @throws IllegalStateException 如果遥测未启用
     * @throws WakeupException 在调用此方法之前或期间调用了wakeup()
     * @throws InterruptException 线程在阻塞时被中断
     * @throws KafkaException 尝试确定客户端实例ID时发生意外错误
     */
    @Override
    public Uuid clientInstanceId(Duration timeout) {
        return delegate.clientInstanceId(timeout);
    }

    /**
     * Get the metrics kept by the consumer
     */
    /**
     * 获取消费者维护的度量指标
     * 
     * @return 度量指标的名称到度量值的映射
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return delegate.metrics();
    }

    /**
     * Add the provided application metric for subscription. This metric will be added to this client's metrics
     * that are available for subscription and sent as telemetry data to the broker.
     * The provided metric must map to an OTLP metric data point type in the OpenTelemetry v1 metrics protobuf message types.
     * Specifically, the metric should be one of the following:
     * <ul>
     *  <li>
     *     Sum: Monotonic total count meter (Counter). Suitable for metrics like total number of X, e.g., total bytes sent.
     *  </li>
     *  <li>
     *     Gauge: Non-monotonic current value meter (UpDownCounter). Suitable for metrics like current value of Y, e.g., current queue count.
     *  </li>
     * </ul>
     * Metrics not matching these types are silently ignored. Executing this method for a previously registered metric
     * is a benign operation and results in updating that metric's entry.
     *
     * @param metric The application metric to register
     */
    /**
     * 注册应用度量指标以进行订阅
     * 该指标将添加到客户端的度量指标中，并作为遥测数据发送到broker
     * 
     * @param metric 要注册的应用度量指标
     */
    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        delegate.registerMetricForSubscription(metric);
    }

    /**
     * Remove the provided application metric for subscription. This metric is removed from this client's metrics
     * and will not be available for subscription any longer. Executing this method with a metric that has not been registered is a
     * benign operation and does not result in any action taken (no-op).
     *
     * @param metric The application metric to remove
     */
    /**
     * 取消注册应用度量指标
     * 该指标将从客户端的度量指标中移除，不再可用于订阅
     * 
     * @param metric 要移除的应用度量指标
     */
    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        delegate.unregisterMetricFromSubscription(metric);
    }

    /**
     * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
     * This will commit acknowledgements if possible within the default timeout.
     * See {@link #close(Duration)} for details. Note that {@link #wakeup()} cannot be used to interrupt close.
     *
     * @throws WakeupException if {@link #wakeup()} is called before or while this method is called
     * @throws InterruptException if the thread is interrupted before or while this method is called
     * @throws KafkaException for any other error during close
     */
    /**
     * 关闭消费者，等待最多30秒进行必要的清理
     * 在默认超时时间内尽可能提交确认
     * 
     * @throws WakeupException 在调用此方法之前或期间调用了wakeup()
     * @throws InterruptException 线程在调用此方法之前或期间被中断
     * @throws KafkaException 关闭期间发生其他错误时
     */
    @Override
    public void close() {
        delegate.close();
    }

    /**
     * Tries to close the consumer cleanly within the specified timeout. This method waits up to
     * {@code timeout} for the consumer to complete acknowledgements and leave the group.
     * If the consumer is unable to complete acknowledgements and gracefully leave the group
     * before the timeout expires, the consumer is force closed. Note that {@link #wakeup()} cannot be
     * used to interrupt close.
     *
     * @param timeout The maximum time to wait for consumer to close gracefully. The value must be
     *                non-negative. Specifying a timeout of zero means do not wait for pending requests to complete.
     *
     * @throws IllegalArgumentException if the {@code timeout} is negative
     * @throws WakeupException if {@link #wakeup()} is called before or while this method is called
     * @throws InterruptException if the thread is interrupted before or while this method is called
     * @throws KafkaException for any other error during close
     */
    /**
     * 在指定超时时间内尝试清理地关闭消费者
     * 
     * @param timeout 等待消费者优雅关闭的最大时间
     * @throws IllegalArgumentException 如果timeout为负数
     * @throws WakeupException 在调用此方法之前或期间调用了wakeup()
     * @throws InterruptException 线程在调用此方法之前或期间被中断
     * @throws KafkaException 关闭期间发生其他错误时
     */
    @Override
    public void close(Duration timeout) {
        delegate.close(timeout);
    }

    /**
     * Wake up the consumer. This method is thread-safe and is useful in particular to abort a long poll.
     * The thread which is blocking in an operation will throw {@link WakeupException}.
     * If no thread is blocking in a method which can throw {@link WakeupException},
     * the next call to such a method will raise it instead.
     */
    /**
     * 唤醒消费者
     * 这是一个线程安全的方法，特别适用于中止长时间的poll操作
     * 被阻塞的线程将抛出WakeupException异常
     */
    @Override
    public void wakeup() {
        delegate.wakeup();
    }

    // 以下方法仅用于测试目的
    
    /**
     * 获取客户端ID
     * 仅用于测试
     */
    String clientId() {
        return delegate.clientId();
    }

    /**
     * 获取度量注册表
     * 仅用于测试
     */
    Metrics metricsRegistry() {
        return delegate.metricsRegistry();
    }

    /**
     * 获取共享消费者度量指标
     * 仅用于测试
     */
    KafkaShareConsumerMetrics kafkaShareConsumerMetrics() {
        return delegate.kafkaShareConsumerMetrics();
    }
}
