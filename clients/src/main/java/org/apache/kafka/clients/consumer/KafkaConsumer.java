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
import org.apache.kafka.clients.consumer.internals.ConsumerDelegate;
import org.apache.kafka.clients.consumer.internals.ConsumerDelegateCreator;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidRegularExpression;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.time.Duration;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.kafka.common.utils.Utils.propsToMap;

/**
 * Kafka消费者客户端，用于从Kafka集群中消费记录。
 * <p>
 * 该客户端具有以下主要特性：
 * 1. 自动处理Kafka broker故障
 * 2. 自动适应主题分区在集群中的迁移
 * 3. 支持消费者组进行负载均衡
 * 4. 维护与broker的TCP连接
 * <p>
 * 重要说明：
 * - 使用后必须关闭消费者，否则会造成连接泄露
 * - 该消费者不是线程安全的
 * - 详细的多线程处理请参考 <a href="#multithreaded">Multi-threaded Processing</a>
 *
 * <h3>跨版本兼容性</h3>
 * 本客户端可以与0.10.0或更新版本的broker通信。不同版本的broker支持的功能可能有所不同：
 * - 示例：0.10.0版本的broker不支持offsetsForTimes功能（该功能在0.10.1版本中添加）
 * - 当调用不受支持的API时，将抛出 {@link org.apache.kafka.common.errors.UnsupportedVersionException} 异常
 * <p>
 *
 * <h3>位移和消费者位置</h3>
 * Kafka为分区中的每条记录维护一个数字位移（offset）。位移具有双重作用：
 * 1. 作为记录在分区内的唯一标识符
 * 2. 表示消费者在分区中的消费位置
 * 
 * 举例说明：
 * - 当消费者位于位置5时，表示它已经消费了位移0到4的记录，下一条将消费位移为5的记录
 * - 注意：位移并不保证连续（例如在压缩主题或使用事务时）
 * - 如果消费者读取了位移4的记录，但位移5没有对应的记录，则位置可能直接前进到6或更高
 * - 同样，如果消费者位置是5但没有位移为5的记录，消费者将返回下一个更高位移的记录
 * 
 * 消费者位置有两个重要概念：
 * 
 * 1. 当前位置 {@link #position(TopicPartition) position}
 * - 表示下一条将要消费的记录的位移
 * - 比该分区中消费者见过的最高位移大1
 * - 在每次调用 {@link #poll(Duration)} 接收消息时自动递增
 * 
 * 2. 提交位置 {@link #commitSync() committed position}
 * - 最后一个安全存储的位移
 * - 如果进程失败并重启，消费者将从此位置恢复
 * - 可以选择自动定期提交位移，或通过提交API手动控制
 * - 提交API包括：{@link #commitSync() commitSync} 和 {@link #commitAsync(OffsetCommitCallback) commitAsync}
 * 
 * 这种区分使消费者可以精确控制记录何时被视为已消费，详细内容将在下文讨论。
 *
 * <h3><a name="consumergroups">消费者组和主题订阅</a></h3>
 *
 * Kafka使用<i>消费者组（consumer groups）</i>的概念来实现多进程间的消费任务分配。这些进程可以：
 * - 运行在同一台机器上
 * - 分布在多台机器上以提供可扩展性和容错性
 * - 具有相同{@code group.id}的所有消费者实例属于同一个消费者组
 * 
 * 消费者组的主要特性：
 * 
 * 1. 动态主题订阅
 * - 每个消费者可以通过{@link #subscribe(Collection, ConsumerRebalanceListener) subscribe} API动态设置要订阅的主题列表
 * - Kafka确保订阅主题中的每条消息只会被消费者组中的一个进程处理
 * - 通过在消费者组成员之间平衡分区来实现，每个分区只分配给组内一个消费者
 * - 示例：如果一个主题有4个分区，消费者组有2个进程，则每个进程会消费2个分区
 * 
 * 2. 动态成员管理
 * - 消费者组成员关系是动态维护的
 * - 进程失败时：其负责的分区会重新分配给同组的其他消费者
 * - 新消费者加入时：部分分区会从现有消费者移动到新消费者
 * - 这个过程称为<i>重平衡（rebalancing）</i>，详见<a href="#failuredetection">下文</a>
 * - 当添加新分区或创建匹配{@link #subscribe(Pattern, ConsumerRebalanceListener) 订阅正则表达式}的新主题时也会触发重平衡
 * - 组会通过定期的元数据刷新自动检测新分区并分配给组内成员
 * 
 * 3. 概念模型
 * - 可以将消费者组视为由多个进程组成的单个逻辑订阅者
 * - Kafka支持对同一个主题有任意数量的消费者组，无需复制数据
 * - 与传统消息系统对比：
 *   - 队列模式：所有进程属于单个消费者组，消息在组内负载均衡
 *   - 发布-订阅模式：每个进程有自己的消费者组，接收主题的所有消息
 * 
 * 4. 重平衡通知
 * - 当发生组重分配时，消费者可以通过{@link ConsumerRebalanceListener}收到通知
 * - 用于完成应用层面的必要操作，如状态清理、手动提交位移等
 * - 详见<a href="#rebalancecallback">在Kafka外部存储位移</a>
 * 
 * 5. 手动分区分配
 * - 消费者也可以使用{@link #assign(Collection)}手动分配特定分区
 * - 这种情况下会禁用动态分区分配和消费者组协调机制
 *
 * <h3><a name="failuredetection">消费者故障检测</a></h3>
 *
 * 订阅主题后，消费者会在调用{@link #poll(Duration)}时自动加入消费者组。poll API的设计目的是确保消费者存活：
 * 
 * 1. 基本工作机制
 * - 只要持续调用poll，消费者就会保持在组内并继续接收分配给它的分区的消息
 * - 消费者会定期向服务器发送心跳
 * - 如果消费者崩溃或在{@code session.timeout.ms}时间内无法发送心跳，则被视为死亡
 * - 死亡消费者的分区会被重新分配给其他消费者
 * 
 * 2. 活锁（Livelock）处理
 * - 可能出现的问题：消费者持续发送心跳但实际上没有处理进度
 * - 解决方案：使用{@code max.poll.interval.ms}设置来进行活性检测
 * - 如果在配置的最大间隔时间内没有调用poll，客户端会主动离开组
 * - 这可能导致位移提交失败（{@link CommitFailedException}异常）
 * - 这是一个安全机制，确保只有活跃成员才能提交位移
 * 
 * 3. Poll循环的配置选项
 * <ol>
 *     <li><code>max.poll.interval.ms</code>（最大轮询间隔）
 *     - 增加间隔可以给消费者更多时间处理返回的记录批次
 *     - 缺点：可能延迟组重平衡，因为消费者只能在poll调用中加入重平衡
 *     - 可用于限制完成重平衡的时间
 *     - 风险：如果消费者无法足够频繁地调用poll，会导致处理进度变慢</li>
 *     
 *     <li><code>max.poll.records</code>（单次轮询最大记录数）
 *     - 限制单次poll返回的记录总数
 *     - 便于预测每个轮询间隔内需要处理的最大记录数
 *     - 通过调整此值可以减少轮询间隔，从而减少组重平衡的影响</li>
 * </ol>
 * 
 * 4. 处理时间不可预测的场景
 * - 上述选项可能不足以处理消息处理时间变化很大的场景
 * - 建议解决方案：将消息处理移至另一个线程
 * - 这样消费者可以继续调用poll，而处理器仍在工作
 * - 注意事项：
 *   - 确保提交的位移不会超过实际处理位置
 *   - 禁用自动提交，仅在线程完成处理后手动提交位移
 *   - 使用{@link #pause(Collection) pause}暂停分区，直到线程处理完之前返回的记录
 *
 * <h3>使用示例</h3>
 * 消费者API提供了灵活的功能来满足各种消费场景的需求。以下是一些示例来演示如何使用它们。
 *
 * <h4>自动提交偏移量</h4>
 * 这个示例演示了Kafka消费者API的一个简单用法，它依赖于自动提交偏移量机制。
 * <p>
 * <pre>
 *     // 创建消费者配置属性
 *     Properties props = new Properties();
 *     // 设置Kafka集群的连接地址
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     // 设置消费者组ID
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     // 启用自动提交偏移量
 *     props.setProperty(&quot;enable.auto.commit&quot;, &quot;true&quot;);
 *     // 设置自动提交的时间间隔（毫秒）
 *     props.setProperty(&quot;auto.commit.interval.ms&quot;, &quot;1000&quot;);
 *     // 设置键的反序列化器
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     // 设置值的反序列化器
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     // 创建消费者实例
 *     KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;&gt;(props);
 *     // 订阅主题foo和bar
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;, &quot;bar&quot;));
 *     // 持续轮询获取消息
 *     while (true) {
 *         // 轮询等待100毫秒获取消息
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         // 遍历处理每条消息
 *         for (ConsumerRecord&lt;String, String&gt; record : records)
 *             System.out.printf(&quot;offset = %d, key = %s, value = %s%n&quot;, record.offset(), record.key(), record.value());
 *     }
 * </pre>
 *
 * 与集群的连接是通过在配置中指定一个或多个代理的地址列表来引导的，使用{@code bootstrap.servers}配置项。
 * 这个列表仅用于发现集群中的其他代理，不需要是集群中所有服务器的完整列表（不过你可能想指定多个地址，
 * 以防在客户端连接时某些服务器处于宕机状态）。
 * <p>
 * 设置{@code enable.auto.commit}为true意味着偏移量会自动提交，提交频率由配置项
 * {@code auto.commit.interval.ms}控制。
 * <p>
 * 在这个示例中，消费者作为消费者组<i>test</i>（通过{@code group.id}配置）的一部分，订阅了主题
 * <i>foo</i>和<i>bar</i>。
 * <p>
 * 反序列化器设置指定了如何将字节转换为对象。例如，通过指定字符串反序列化器，我们表明记录的键和值
 * 都将是简单的字符串。
 *
 * <h4>手动控制偏移量</h4>
 *
 * 除了依赖消费者定期自动提交已消费的偏移量外，用户还可以控制何时将记录标记为已消费并提交其偏移量。
 * 当消息的消费与某些处理逻辑相关联时，这种方式特别有用，因为在这种情况下，只有当消息完成处理后
 * 才应该被视为已消费。
 * <p>
 * <pre>
 *     // 创建消费者配置
 *     Properties props = new Properties();
 *     // 设置Kafka集群地址
 *     props.setProperty(&quot;bootstrap.servers&quot;, &quot;localhost:9092&quot;);
 *     // 设置消费者组ID
 *     props.setProperty(&quot;group.id&quot;, &quot;test&quot;);
 *     // 禁用自动提交偏移量
 *     props.setProperty(&quot;enable.auto.commit&quot;, &quot;false&quot;);
 *     // 设置键和值的反序列化器
 *     props.setProperty(&quot;key.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     props.setProperty(&quot;value.deserializer&quot;, &quot;org.apache.kafka.common.serialization.StringDeserializer&quot;);
 *     // 创建消费者实例
 *     KafkaConsumer&lt;String, String&gt; consumer = new KafkaConsumer&lt;&gt;(props);
 *     // 订阅主题
 *     consumer.subscribe(Arrays.asList(&quot;foo&quot;, &quot;bar&quot;));
 *     // 设置最小批处理大小
 *     final int minBatchSize = 200;
 *     // 创建消息缓冲区
 *     List&lt;ConsumerRecord&lt;String, String&gt;&gt; buffer = new ArrayList&lt;&gt;();
 *     while (true) {
 *         // 轮询获取消息
 *         ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(100));
 *         // 将消息添加到缓冲区
 *         for (ConsumerRecord&lt;String, String&gt; record : records) {
 *             buffer.add(record);
 *         }
 *         // 当缓冲区达到最小批处理大小时
 *         if (buffer.size() &gt;= minBatchSize) {
 *             // 将消息批量写入数据库
 *             insertIntoDb(buffer);
 *             // 同步提交偏移量
 *             consumer.commitSync();
 *             // 清空缓冲区
 *             buffer.clear();
 *         }
 *     }
 * </pre>
 *
 * 在这个示例中，我们将演示如何批量处理消息并将其存储到数据库中。这里采用了以下处理流程：
 * 1. 首先在内存中累积一定数量的消息记录
 * 2. 当累积的消息达到预定批量大小时，将它们批量写入数据库
 * 3. 最后手动提交消费位移
 * <p>
 * 如果像前面的示例那样使用自动提交位移，会带来一个问题：
 * - 消息在被poll()方法返回给用户后就被视为已消费
 * - 如果在消息批量写入数据库之前程序发生故障，这些消息实际上并未被正确处理，但位移却已经提交
 * - 这可能导致消息丢失
 * <p>
 * 为了避免这个问题，我们采用手动提交位移的方式：
 * - 只有在确认消息已经成功写入数据库后，才提交位移
 * - 这样可以精确控制消息何时被视为已消费
 * - 但这也带来了另一种可能性：程序可能在写入数据库后、提交位移前发生故障
 * - 虽然这个时间窗口很小（通常只有几毫秒），但确实存在这种可能
 * - 在这种情况下，接管消费的进程会从上次提交的位移处重新消费，导致这批消息被重复写入数据库
 * <p>
 * 这种处理方式提供了"至少一次"(at-least-once)的传递保证：
 * - 每条消息都会被至少处理一次
 * - 在发生故障的情况下，可能会被重复处理
 * <p>
 * <b>注意：使用自动提交位移也能提供"至少一次"传递保证，但必须满足以下条件：
 * - 必须在调用下一次{@link #poll(Duration)}之前处理完当前poll()返回的所有数据
 * - 必须在调用{@link #close() close()}关闭消费者之前处理完所有数据
 * - 如果违反这些要求，已提交的位移可能超过实际消费的位置，导致消息丢失
 * - 使用手动位移控制的优势在于：你可以完全掌控消息何时被视为已消费</b>
 * <p>
 * 上面的示例使用{@link #commitSync() commitSync}来标记所有接收到的消息为已提交。
 * 在某些情况下，你可能需要更细粒度的控制，比如明确指定要提交的位移。
 * 下面的示例展示了如何在处理完每个分区的消息后提交该分区的位移。
 * <p>
 * <pre>
 *     try {
 *         while(running) {
 *             ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
 *             for (TopicPartition partition : records.partitions()) {
 *                 List&lt;ConsumerRecord&lt;String, String&gt;&gt; partitionRecords = records.records(partition);
 *                 for (ConsumerRecord&lt;String, String&gt; record : partitionRecords) {
 *                     System.out.println(record.offset() + &quot;: &quot; + record.value());
 *                 }
 *                 consumer.commitSync(Collections.singletonMap(partition, records.nextOffsets().get(partition)));
 *             }
 *         }
 *     } finally {
 *       consumer.close();
 *     }
 * </pre>
 *
 * <b>注意：已提交的偏移量应该始终是应用程序将要读取的下一条消息的偏移量。</b>
 * 因此，当调用 {@link #commitSync(Map) commitSync(offsets)} 时，你应该使用 {@code nextRecordToBeProcessed.offset()}
 * 或者如果 {@link ConsumerRecords} 已经消费完毕，则使用 {@link ConsumerRecords#nextOffsets()}。
 * 你还应该添加leader epoch作为提交元数据，这可以从 {@link ConsumerRecord#leaderEpoch()} 或
 * {@link ConsumerRecords#nextOffsets()} 获取。
 *
 * <h4><a name="manualassignment">手动分区分配</a></h4>
 *
 * 在前面的示例中，我们订阅了感兴趣的主题，并让Kafka根据消费者组中的活跃消费者动态分配这些主题的分区。
 * 然而，在某些情况下，你可能需要对分配的具体分区进行更精细的控制。例如：
 * <p>
 * <ul>
 * <li>如果进程维护着与分区相关的某种本地状态（比如本地磁盘上的键值存储），
 * 那么它应该只获取它在磁盘上维护的分区的记录。
 * <li>如果进程本身具有高可用性，并且在失败时会被重启（可能使用YARN、Mesos或AWS等集群管理框架，
 * 或作为流处理框架的一部分）。在这种情况下，不需要Kafka来检测故障并重新分配分区，
 * 因为消费进程会在另一台机器上重新启动。
 * </ul>
 * <p>
 * 要使用这种模式，不需要使用 {@link #subscribe(Collection) subscribe} 来订阅主题，
 * 只需调用 {@link #assign(Collection)} 并传入你想要消费的完整分区列表即可。
 *
 * <pre>
 *     String topic = &quot;foo&quot;;
 *     TopicPartition partition0 = new TopicPartition(topic, 0);
 *     TopicPartition partition1 = new TopicPartition(topic, 1);
 *     consumer.assign(Arrays.asList(partition0, partition1));
 * </pre>
 *
 * 一旦分配完成，你可以像前面的示例一样在循环中调用 {@link #poll(Duration) poll} 来消费记录。
 * 消费者指定的消费者组仍然用于提交偏移量，但此时分区集合只会通过再次调用 {@link #assign(Collection) assign} 来改变。
 * 手动分区分配不使用组协调机制，因此消费者故障不会导致已分配的分区被重新平衡。即使多个消费者共享同一个groupId，
 * 每个消费者也是独立运行的。为了避免偏移量提交冲突，你通常应该确保每个消费者实例都有唯一的groupId。
 * <p>
 * 注意：不能混合使用手动分区分配（即使用 {@link #assign(Collection) assign}）和通过主题订阅进行的动态分区分配
 * （即使用 {@link #subscribe(Collection) subscribe}）。
 *
 * <h4><a name="rebalancecallback">在Kafka外部存储偏移量</h4>
 *
 * 消费者应用程序不必使用Kafka内置的偏移量存储机制，它可以将偏移量存储在自己选择的存储系统中。
 * 这种方式的主要用例是允许应用程序以原子方式将偏移量和消费结果存储在同一个系统中。
 * 虽然这并不总是可行，但当可以实现时，它能使消费操作完全原子化，提供比Kafka默认的"至少一次"语义更强的"精确一次"语义。
 * <p>
 * 以下是两个具体的使用场景示例：
 * <ul>
 * <li>如果消费结果需要存储在关系型数据库中，可以同时将偏移量也存储在数据库中，这样就可以在同一个事务中提交消费结果和偏移量。
 * 这样要么事务成功（消费结果和偏移量都更新），要么事务失败（两者都不更新）。
 * <li>如果消费结果存储在本地存储中，也可以将偏移量一同存储。例如，在构建搜索索引时，可以订阅特定分区并将偏移量和索引数据一起存储。
 * 如果这个过程是原子的，即使发生崩溃导致未同步的数据丢失，剩余的数据也会有对应的偏移量记录。这意味着索引进程在丢失最近更新后重启时，
 * 可以从已有的位置继续索引，确保不会丢失更新。
 * </ul>
 * <p>
 * 要实现自己管理偏移量，只需要遵循以下步骤：
 * <ul>
 * <li>配置 <code>enable.auto.commit=false</code> 禁用自动提交
 * <li>使用每个 {@link ConsumerRecord} 提供的偏移量来保存消费位置
 * <li>重启时使用 {@link #seek(TopicPartition, long)} 恢复消费者位置
 * </ul>
 * <p>
 * 这种用法在手动分区分配的情况下最简单（比如上面描述的搜索索引用例）。如果使用自动分区分配，则需要特别注意处理分区分配变化的情况。
 * 这可以通过在调用 {@link #subscribe(Collection, ConsumerRebalanceListener)} 和 
 * {@link #subscribe(Pattern, ConsumerRebalanceListener)} 时提供 {@link ConsumerRebalanceListener} 实例来实现。
 * 例如，当分区被收回时，消费者需要通过实现 {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)} 来提交这些分区的偏移量。
 * 当分区被分配时，消费者需要通过实现 {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)} 来查找这些新分区的偏移量并正确初始化消费位置。
 * <p>
 * {@link ConsumerRebalanceListener} 的另一个常见用途是在分区被移走时清除应用程序为这些分区维护的任何缓存。
 *
 * <h4>控制消费者位置</h4>
 *
 * 在大多数使用场景中，消费者会简单地从头到尾消费记录，并定期提交其位置（自动或手动）。
 * 然而，Kafka允许消费者手动控制其位置，可以在分区内任意前进或后退。这意味着消费者可以：
 * - 重新消费较早的记录
 * - 跳过中间记录直接消费最新的记录
 * <p>
 * 手动控制消费者位置在以下几种情况下特别有用：
 * <p>
 * 1. 时间敏感的记录处理
 * - 当消费者落后太多时，可能不希望追赶处理所有历史记录
 * - 而是直接跳到最新的记录开始消费
 * <p>
 * 2. 维护本地状态的系统
 * - 如前文所述的系统，消费者在启动时需要将位置初始化到本地存储的状态
 * - 如果本地状态丢失（比如磁盘损坏），可以通过重新消费所有数据在新机器上重建状态
 * - 前提是Kafka保留了足够的历史数据
 * <p>
 * Kafka提供了以下方法来指定消费位置：
 * - {@link #seek(TopicPartition, long)} - 指定新的消费位置
 * - {@link #seekToBeginning(Collection)} - 将位置重置到最早的可用偏移量
 * - {@link #seekToEnd(Collection)} - 将位置设置到最新的偏移量
 *
 * <h4>消费流量控制</h4>
 *
 * 当消费者被分配了多个分区时，默认会同时从所有分区获取数据，这些分区具有相同的消费优先级。
 * 但在某些场景下，消费者可能希望：
 * - 先以全速从部分分区获取数据
 * - 等这些分区数据较少或消费完毕后，再开始获取其他分区的数据
 *
 * <p>
 * 典型的应用场景包括：
 * 1. 流处理中的主题连接
 * - 处理器从两个主题获取数据并执行连接操作
 * - 当其中一个主题严重落后时，处理器希望暂停消费领先的主题
 * - 让落后的流追赶上来
 * 
 * 2. 消费者启动时的引导过程
 * - 存在大量历史数据需要追赶
 * - 应用程序通常希望先获取某些主题的最新数据
 * - 然后再考虑获取其他主题的数据
 *
 * <p>
 * Kafka支持通过以下方法动态控制消费流量：
 * - {@link #pause(Collection)} - 暂停指定分区的消费
 * - {@link #resume(Collection)} - 恢复已暂停分区的消费
 * 这些控制在后续的 {@link #poll(Duration)} 调用中生效。
 *
 * <h3>读取事务消息</h3>
 *
 * <p>
 * Kafka从0.11.0版本开始引入了事务功能，允许应用程序以原子方式写入多个主题和分区。
 * 为了使事务功能正常工作，从这些分区读取数据的消费者需要配置为只读取已提交的数据。
 * 这可以通过在消费者配置中设置{@code isolation.level=read_committed}来实现。
 *
 * <p>
 * 在<code>read_committed</code>模式下，消费者只会读取那些已成功提交的事务消息。
 * 对于非事务消息，它会像以前一样继续读取。在此模式下，客户端不会进行缓冲。
 * 相反，对于<code>read_committed</code>消费者来说，分区的结束位移将是该分区中
 * 属于未完成事务的第一条消息的位移。这个位移被称为"最后稳定位移
 * The following snippet shows the typical pattern:
 *
 * <pre>
 * public class KafkaConsumerRunner implements Runnable {
 *     private final AtomicBoolean closed = new AtomicBoolean(false);
 *     private final KafkaConsumer consumer;
 *
 *     public KafkaConsumerRunner(KafkaConsumer consumer) {
 *       this.consumer = consumer;
 *     }
 *
 *     {@literal}@Override
 *     public void run() {
 *         try {
 *             consumer.subscribe(Arrays.asList("topic"));
 *             while (!closed.get()) {
 *                 ConsumerRecords records = consumer.poll(Duration.ofMillis(10000));
 *                 // Handle new records
 *             }
 *         } catch (WakeupException e) {
 *             // Ignore exception if closing
 *             if (!closed.get()) throw e;
 *         } finally {
 *             consumer.close();
 *         }
 *     }
 *
 *     // Shutdown hook which can be called from a separate thread
 *     public void shutdown() {
 *         closed.set(true);
 *         consumer.wakeup();
 *     }
 * }
 * </pre>
 *
 * 然后在一个单独的线程中，通过设置关闭标志并唤醒消费者来关闭消费者。
 * 这种方式允许在一个线程中安全地关闭消费者，而不会干扰正在进行消费操作的主线程。
 *
 * <p>
 * <pre>
 *     closed.set(true);
 *     consumer.wakeup();
 * </pre>
 *
 * <p>
 * 注意：虽然可以使用线程中断而不是{@link #wakeup()}来中止阻塞操作（这种情况下会抛出{@link InterruptException}），
 * 但我们不建议使用中断，因为它们可能导致消费者的清理关闭被中止。中断主要用于那些无法使用{@link #wakeup()}的场景，
 * 例如当消费者线程由不了解Kafka客户端的代码管理时。
 *
 * <p>
 * 我们有意避免实现特定的线程处理模型。这为实现多线程处理记录提供了几种选择。
 *
 * <h4>1. 每个线程一个消费者</h4>
 *
 * 一个简单的选项是为每个线程分配自己的消费者实例。这种方法有以下优缺点：
 * <ul>
 * <li><b>优点</b>：实现最简单
 * <li><b>优点</b>：由于不需要线程间协调，通常是最快的
 * <li><b>优点</b>：在分区级别上很容易实现顺序处理（每个线程只需按接收顺序处理消息）
 * <li><b>缺点</b>：更多的消费者意味着更多的TCP连接（每个线程一个）。不过通常Kafka处理连接非常高效，
 * 所以这个成本一般较小
 * <li><b>缺点</b>：多个消费者意味着向服务器发送更多请求，并且数据批处理略少，可能导致I/O吞吐量下降
 * <li><b>缺点</b>：所有进程的总线程数将受限于分区总数
 * </ul>
 *
 * <h4>2. 消费和处理解耦</h4>
 *
 * 另一种方案是使用一个或多个消费者线程专门进行数据消费，并将{@link ConsumerRecords}实例
 * 传递给一个阻塞队列，由处理器线程池从队列中获取数据进行实际的记录处理。
 *
 * 这种方案同样有其优缺点：
 * <ul>
 * <li><b>优点</b>：允许独立扩展消费者和处理器的数量。这使得可以用单个消费者为多个处理器线程提供数据，
 * 避免了分区数量的限制
 * <li><b>缺点</b>：在处理器之间保证顺序需要特别注意，因为线程独立执行，较早的数据块可能在较晚的数据块之后
 * 处理，这完全取决于线程执行的时序。对于不要求顺序的处理来说，这不是问题
 * <li><b>缺点</b>：手动提交位移变得更困难，因为需要所有线程协调以确保该分区的处理已完成
 * </ul>
 *
 * 这种方案有许多可能的变体。例如，每个处理器线程可以有自己的队列，消费者线程可以使用TopicPartition进行
 * 哈希来决定将消息放入哪个队列，从而确保顺序消费并简化提交。
 */
public class KafkaConsumer<K, V> implements Consumer<K, V> {

    private static final ConsumerDelegateCreator CREATOR = new ConsumerDelegateCreator();

    private final ConsumerDelegate<K, V> delegate;

    /**
     * 通过提供一组键值对配置来实例化消费者。有效的配置字符串
     * 在<a href="http://kafka.apache.org/documentation.html#consumerconfigs">这里</a>有详细文档。配置值可以是
     * 字符串或适当类型的对象（例如，数字配置可以接受字符串"42"或整数42）。
     * <p>
     * 有效的配置字符串在{@link ConsumerConfig}中有文档说明。
     * <p>
     * 注意：创建{@code KafkaConsumer}后，必须始终调用{@link #close()}以避免资源泄漏。
     *
     * @param configs 消费者配置，以Map形式提供
     */
    public KafkaConsumer(Map<String, Object> configs) {
        // 调用带反序列化器的构造函数，但反序列化器参数设为null
        this(configs, null, null);
    }

    /**
     * 通过提供{@link java.util.Properties}对象作为配置来实例化消费者。
     * <p>
     * 有效的配置字符串在{@link ConsumerConfig}中有文档说明。
     * <p>
     * 注意：创建{@code KafkaConsumer}后，必须始终调用{@link #close()}以避免资源泄漏。
     *
     * @param properties 消费者配置属性
     */
    public KafkaConsumer(Properties properties) {
        // 调用带反序列化器的构造函数，但反序列化器参数设为null
        this(properties, null, null);
    }

    /**
     * 通过提供{@link java.util.Properties}对象作为配置，以及键和值的{@link Deserializer}来实例化消费者。
     * <p>
     * 有效的配置字符串在{@link ConsumerConfig}中有文档说明。
     * <p>
     * 注意：创建{@code KafkaConsumer}后，必须始终调用{@link #close()}以避免资源泄漏。
     *
     * @param properties 消费者配置属性
     * @param keyDeserializer 实现{@link Deserializer}的键反序列化器。当直接传入反序列化器时，
     *                        不会在消费者中调用其configure()方法
     * @param valueDeserializer 实现{@link Deserializer}的值反序列化器。当直接传入反序列化器时，
     *                          不会在消费者中调用其configure()方法
     */
    public KafkaConsumer(Properties properties,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        // 将Properties转换为Map，并调用Map版本的构造函数
        this(propsToMap(properties), keyDeserializer, valueDeserializer);
    }

    /**
     * 通过提供一组键值对配置，以及键和值的{@link Deserializer}来实例化消费者。
     * <p>
     * 有效的配置字符串在{@link ConsumerConfig}中有文档说明。
     * <p>
     * 注意：创建{@code KafkaConsumer}后，必须始终调用{@link #close()}以避免资源泄漏。
     *
     * @param configs 消费者配置
     * @param keyDeserializer 实现{@link Deserializer}的键反序列化器。当直接传入反序列化器时，
     *                        不会在消费者中调用其configure()方法
     * @param valueDeserializer 实现{@link Deserializer}的值反序列化器。当直接传入反序列化器时，
     *                          不会在消费者中调用其configure()方法
     */
    public KafkaConsumer(Map<String, Object> configs,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        // 创建ConsumerConfig对象，并将反序列化器添加到配置中
        this(new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
                keyDeserializer, valueDeserializer);
    }

    /**
     * 内部构造函数，使用ConsumerConfig和反序列化器创建消费者实例
     *
     * @param config 消费者配置对象
     * @param keyDeserializer 键反序列化器
     * @param valueDeserializer 值反序列化器
     */
    KafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        // 使用工厂创建者创建委托对象
        delegate = CREATOR.create(config, keyDeserializer, valueDeserializer);
    }

    /**
     * 内部构造函数，用于测试目的，允许注入所有必要的依赖项
     *
     * @param logContext 日志上下文
     * @param time 时间实例
     * @param config 消费者配置
     * @param keyDeserializer 键反序列化器
     * @param valueDeserializer 值反序列化器
     * @param client Kafka客户端
     * @param subscriptions 订阅状态
     * @param metadata 消费者元数据
     * @param assignors 分区分配器列表
     */
    KafkaConsumer(LogContext logContext,
                  Time time,
                  ConsumerConfig config,
                  Deserializer<K> keyDeserializer,
                  Deserializer<V> valueDeserializer,
                  KafkaClient client,
                  SubscriptionState subscriptions,
                  ConsumerMetadata metadata,
                  List<ConsumerPartitionAssignor> assignors) {
        // 使用工厂创建者创建完整的委托对象
        delegate = CREATOR.create(
            logContext,
            time,
            config,
            keyDeserializer,
            valueDeserializer,
            client,
            subscriptions,
            metadata,
            assignors
        );
    }

    /**
     * 获取当前分配给该消费者的分区集合。
     * 
     * 分区分配的来源有两种情况：
     * 1. 如果是通过{@link #assign(Collection)}方法直接分配分区，则返回完全相同的分区集合
     * 2. 如果是通过主题订阅的方式，则返回当前分配给消费者的主题分区集合
     *    - 如果分配尚未发生，可能返回空集合
     *    - 如果正在进行分区重分配，也可能返回空集合
     * 
     * @return 当前分配给该消费者的分区集合
     */
    public Set<TopicPartition> assignment() {
        // 通过委托对象获取当前分配的分区集合
        return delegate.assignment();
    }

    /**
     * 获取当前订阅的主题集合。
     * 
     * 返回结果有两种情况：
     * 1. 如果之前调用过{@link #subscribe(Collection, ConsumerRebalanceListener)}方法，
     *    则返回最近一次订阅的主题集合
     * 2. 如果从未调用过订阅方法，则返回空集合
     * 
     * @return 当前订阅的主题集合
     */
    public Set<String> subscription() {
        // 通过委托对象获取当前订阅的主题集合
        return delegate.subscription();
    }

    /**
     * 订阅指定的主题列表，以获取动态分配的分区。
     * 
     * 重要说明：
     * <b>主题订阅不是增量式的。新的主题列表会完全替换当前的分配（如果存在的话）。</b>
     * 另外，不能同时使用主题订阅（组管理）和手动分区分配（通过{@link #assign(Collection)}）。
     *
     * 特殊情况：
     * - 如果提供的主题列表为空，效果等同于调用{@link #unsubscribe()}取消订阅
     *
     * 组管理机制：
     * 作为组管理的一部分，消费者会跟踪属于特定组的消费者列表。
     * 当发生以下任一事件时，将触发重平衡操作：
     * <ul>
     * <li>任何已订阅主题的分区数量发生变化
     * <li>订阅的主题被创建或删除
     * <li>消费者组中的现有成员关闭或失败
     * <li>新成员加入消费者组
     * </ul>
     *
     * 重平衡监听器：
     * - 当触发上述事件时，会首先调用提供的监听器，通知消费者的分配已被撤销
     * - 然后在收到新的分配时再次调用监听器
     * - 注意：重平衡只会在主动调用{@link #poll(Duration)}期间发生，因此回调也只会在这期间被调用
     *
     * 监听器行为：
     * - 新提供的监听器会立即覆盖之前通过subscribe设置的任何监听器
     * - 保证通过此接口撤销/分配的分区都来自本次调用订阅的主题
     * - 更多详细信息请参见{@link ConsumerRebalanceListener}
     *
     * @param topics 要订阅的主题列表
     * @param listener 非空的监听器实例，用于接收已订阅主题的分区分配/撤销通知
     * @throws IllegalArgumentException 如果topics为null、包含null元素或空元素，或者listener为null
     * @throws IllegalStateException 如果之前使用模式调用了{@code subscribe()}，或者之前调用了assign
     *                              （且未随后调用{@link #unsubscribe()}），或者未配置至少一个分区分配策略
     */
    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        // 通过委托对象执行主题订阅操作
        delegate.subscribe(topics, listener);
    }

    /**
     * 订阅指定的主题列表以获取动态分配的分区。
     * <b>主题订阅不是增量的。这个列表会替换当前的分配（如果存在的话）。</b>
     * 不能将主题订阅与组管理机制和通过{@link #assign(Collection)}进行的手动分区分配组合使用。
     * 
     * 如果给定的主题列表为空，则等同于调用{@link #unsubscribe()}。
     * 
     * <p>
     * 这是{@link #subscribe(Collection, ConsumerRebalanceListener)}的简化版本，
     * 使用了一个空操作监听器。如果你需要能够寻找特定的偏移量，应该优先使用
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}，因为组重平衡会导致分区偏移量被重置。
     * 如果你正在进行自己的偏移量管理，也应该提供自己的监听器，因为监听器可以让你在重平衡完成前提交偏移量。
     *
     * @param topics 要订阅的主题列表
     * @throws IllegalArgumentException 如果topics为null或包含null或空元素
     * @throws IllegalStateException 如果之前使用模式调用了{@code subscribe()}，或之前调用了assign
     *                              （且未随后调用{@link #unsubscribe()}），或者未配置至少一个分区分配策略
     */
    @Override
    public void subscribe(Collection<String> topics) {
        // 将订阅请求委托给内部的delegate对象处理
        delegate.subscribe(topics);
    }

    /**
     * 订阅所有匹配指定模式的主题以获取动态分配的分区。
     * 模式匹配将定期针对检查时存在的所有主题进行。
     * 这可以通过{@code metadata.max.age.ms}配置来控制：通过降低最大元数据年龄，
     * 消费者将更频繁地刷新元数据并检查匹配的主题。
     * <p>
     * 关于{@link ConsumerRebalanceListener}的使用详情，请参见{@link #subscribe(Collection, ConsumerRebalanceListener)}。
     * 当匹配提供模式的主题发生变化以及消费者组成员关系发生变化时，通常会触发重平衡。
     * 组重平衡仅在主动调用{@link #poll(Duration)}时进行。
     *
     * @param pattern 要订阅的模式
     * @param listener 非空监听器实例，用于获取已订阅主题的分区分配/撤销通知
     * @throws IllegalArgumentException 如果pattern或listener为null
     * @throws IllegalStateException 如果之前使用主题列表调用了{@code subscribe()}，或之前调用了assign
     *                              （且未随后调用{@link #unsubscribe()}），或者未配置至少一个分区分配策略
     */
    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        // 将带有模式和监听器的订阅请求委托给内部的delegate对象处理
        delegate.subscribe(pattern, listener);
    }

    /**
     * 订阅所有匹配指定模式的主题以获取动态分配的分区。
     * 模式匹配将定期针对检查时存在的主题进行。
     * <p>
     * 这是{@link #subscribe(Pattern, ConsumerRebalanceListener)}的简化版本，
     * 使用了一个空操作监听器。如果你需要能够寻找特定的偏移量，应该优先使用
     * {@link #subscribe(Pattern, ConsumerRebalanceListener)}，因为组重平衡会导致分区偏移量被重置。
     * 如果你正在进行自己的偏移量管理，也应该提供自己的监听器，因为监听器可以让你在重平衡完成前提交偏移量。
     *
     * @param pattern 要订阅的模式
     * @throws IllegalArgumentException 如果pattern为null
     * @throws IllegalStateException 如果之前使用主题列表调用了{@code subscribe()}，或之前调用了assign
     *                              （且未随后调用{@link #unsubscribe()}），或者未配置至少一个分区分配策略
     */
    @Override
    public void subscribe(Pattern pattern) {
        // 将仅带有模式的订阅请求委托给内部的delegate对象处理
        delegate.subscribe(pattern);
    }

    /**
     * 订阅所有匹配指定模式的主题，以获取动态分配的分区。
     * 系统会定期对所有主题进行模式匹配。此功能仅在CONSUMER组协议下支持
     * （参见 {@link ConsumerConfig#GROUP_PROTOCOL_CONFIG}）。
     * <p>
     * 如果提供的模式与Google RE2/J不兼容，在调用此subscribe方法后的
     * {@link #poll(Duration)}调用中将抛出{@link InvalidRegularExpression}异常。
     * <p>
     * 关于{@link ConsumerRebalanceListener}的使用详情，请参见
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}。
     * 当匹配提供模式的主题发生变化或消费者组成员变化时，会触发重平衡。
     * 组重平衡仅在主动调用{@link #poll(Duration)}时进行。
     *
     * @param pattern  要订阅的模式，必须与Google RE2/J兼容
     * @param listener 非空的监听器实例，用于接收已订阅主题的分区分配/撤销通知
     * @throws IllegalArgumentException 如果pattern为null或空，或listener为null
     * @throws IllegalStateException    如果之前已调用{@code subscribe()}订阅了主题，
     *                                  或之前已调用assign（且未随后调用{@link #unsubscribe()}）
     */
    @Override
    public void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener listener) {
        // 将订阅请求委托给内部实现类处理
        delegate.subscribe(pattern, listener);
    }

    /**
     * 订阅所有匹配指定模式的主题，以获取动态分配的分区。
     * 系统会定期对主题进行模式匹配。此功能仅在CONSUMER组协议下支持
     * （参见 {@link ConsumerConfig#GROUP_PROTOCOL_CONFIG}）
     * <p>
     * 如果提供的模式与Google RE2/J不兼容，在调用此subscribe方法后的
     * {@link #poll(Duration)}调用中将抛出{@link InvalidRegularExpression}异常。
     * <p>
     * 这是{@link #subscribe(Pattern, ConsumerRebalanceListener)}的简化版本，
     * 使用一个空操作监听器。如果你需要能够寻找特定偏移量，应该优先使用
     * {@link #subscribe(Pattern, ConsumerRebalanceListener)}，因为组重平衡会导致分区偏移量重置。
     * 如果你正在进行自己的偏移量管理，也应该提供自己的监听器，因为监听器可以让你在重平衡完成前提交偏移量。
     *
     * @param pattern 要订阅的模式，必须与Google RE2/J兼容
     * @throws IllegalArgumentException 如果pattern为null或空
     * @throws IllegalStateException    如果之前已调用{@code subscribe()}订阅了主题，
     *                                  或之前已调用assign（且未随后调用{@link #unsubscribe()}）
     */
    @Override
    public void subscribe(SubscriptionPattern pattern) {
        // 使用默认的空操作监听器调用订阅方法
        delegate.subscribe(pattern);
    }

    /**
     * 取消订阅当前通过{@link #subscribe(Collection)}或{@link #subscribe(Pattern)}订阅的主题。
     * 这也会清除通过{@link #assign(Collection)}直接分配的所有分区。
     *
     * @throws org.apache.kafka.common.KafkaException 对于任何其他不可恢复的错误（例如重平衡回调错误）
     */
    public void unsubscribe() {
        // 委托给内部实现类处理取消订阅操作
        delegate.unsubscribe();
    }

    /**
     * 手动为此消费者分配分区列表。此接口不允许增量分配，
     * 会替换之前的分配（如果存在）。
     * <p>
     * 如果给定的主题分区列表为空，其效果与调用{@link #unsubscribe()}相同。
     * <p>
     * 通过此方法进行的手动主题分配不使用消费者的组管理功能。
     * 因此，当组成员身份或集群和主题元数据发生变化时，不会触发重平衡操作。
     * 注意，不能同时使用手动分区分配（{@link #assign(Collection)}）
     * 和组分配（{@link #subscribe(Collection, ConsumerRebalanceListener)}）。
     * <p>
     * 如果启用了自动提交，在新分配替换旧分配之前，
     * 将触发一次异步提交（基于旧分配）。
     *
     * @param partitions 要分配给此消费者的分区列表
     * @throws IllegalArgumentException 如果partitions为null或包含null或空主题
     * @throws IllegalStateException 如果之前已通过topics或pattern调用了{@code subscribe()}
     *                               （且未随后调用{@link #unsubscribe()}）
     */
    @Override
    public void assign(Collection<TopicPartition> partitions) {
        // 将分区分配请求委托给内部实现类处理
        delegate.assign(partitions);
    }

    /**
     * 从已订阅的主题或分区获取数据。在调用此方法之前必须先订阅主题或分配分区，否则会报错。
     * <p>
     * 每次poll操作时，消费者会：
     * 1. 使用上次消费的位移作为起始位置
     * 2. 按顺序获取数据
     * 3. 起始位移可以通过以下方式设置：
     *    - 手动设置：使用{@link #seek(TopicPartition, long)}方法
     *    - 自动设置：使用已订阅分区的最后提交位移
     *
     * <p>
     * 此方法的返回行为：
     * 1. 如果有可用记录，立即返回
     * 2. 如果在read_committed隔离级别下，位置越过了控制记录或已中止的事务，也会立即返回
     * 3. 否则，等待指定的超时时间
     * 4. 如果超时，返回空记录集
     * 注意：执行自定义的{@link ConsumerRebalanceListener}回调时，可能会超出指定的超时时间
     *
     * @param timeout 最大阻塞时间（不能大于{@link Long#MAX_VALUE}毫秒）
     *
     * @return 返回一个映射，包含从上次获取后订阅的主题和分区的记录
     *
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException 
     *             当分区的位移未定义或超出范围，且未配置位移重置策略时抛出
     * @throws org.apache.kafka.common.errors.WakeupException 
     *             当在调用此方法之前或期间调用了{@link #wakeup()}时抛出
     * @throws org.apache.kafka.common.errors.InterruptException 
     *             当调用线程在调用此方法之前或期间被中断时抛出
     * @throws org.apache.kafka.common.errors.AuthenticationException 
     *             当认证失败时抛出。详见异常信息
     * @throws org.apache.kafka.common.errors.AuthorizationException 
     *             当调用者缺少对订阅主题或配置的groupId的读取权限时抛出。详见异常信息
     * @throws org.apache.kafka.common.KafkaException 
     *             当发生其他不可恢复的错误时抛出（如：无效的groupId、会话超时、反序列化错误、重平衡回调异常等）
     * @throws java.lang.IllegalArgumentException 
     *             当超时值为负数时抛出
     * @throws java.lang.IllegalStateException 
     *             当消费者未订阅任何主题或未手动分配任何分区时抛出
     * @throws java.lang.ArithmeticException 
     *             当超时时间大于{@link Long#MAX_VALUE}毫秒时抛出
     * @throws org.apache.kafka.common.errors.InvalidTopicException 
     *             当当前订阅包含任何无效主题时抛出（根据{@link org.apache.kafka.common.internals.Topic#validate(String)}验证）
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException 
     *             当消费者尝试获取稳定位移但broker不支持此功能时抛出；
     *             或当消费者尝试通过{@link #subscribe(SubscriptionPattern)}或
     *             {@link #subscribe(SubscriptionPattern, ConsumerRebalanceListener)}订阅模式但broker不支持时抛出
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException 
     *             当此消费者实例被broker隔离时抛出
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        // 将poll请求委托给实际的消费者实现类处理
        return delegate.poll(timeout);
    }

    /**
     * 为所有已订阅的主题和分区提交上次{@link #poll(Duration) poll()}返回的位移。
     * <p>
     * 位移提交说明：
     * 1. 此方法仅将位移提交到Kafka
     * 2. 提交的位移将用于：
     *    - 每次重平衡后的第一次获取
     *    - 消费者启动时的初始位置
     * 3. 如果需要将位移存储在Kafka之外的系统中，不应使用此API
     * <p>
     * 同步提交特性：
     * 1. 这是一个同步提交操作，会阻塞直到：
     *    - 提交成功
     *    - 遇到不可恢复的错误（此时抛出异常）
     *    - 超过{@code default.api.timeout.ms}指定的超时时间（此时抛出{@link org.apache.kafka.common.errors.TimeoutException}）
     * <p>
     * 注意：之前通过{@link #commitAsync(OffsetCommitCallback)}（或类似方法）发送的异步位移提交的回调
     * 保证会在此方法完成之前被调用。
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException 
     *             当提交失败且无法重试时抛出。
     *             此致命错误仅在以下情况发生：
     *             1. 使用{@link #subscribe(Collection)}进行自动组管理
     *             2. 存在使用相同<code>group.id</code>的活跃组正在使用组管理
     *             当尝试提交不再分配给此消费者的分区时（例如消费者已不再是组的成员），将抛出此异常
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException 
     *             当消费者实例正在进行重平衡，尚未确定分配给消费者的分区时抛出。
     *             处理方法：
     *             1. 先通过调用{@link #poll(Duration)}完成重平衡
     *             2. 之后再考虑提交
     *             注意：重平衡后重新提交时：
     *             - 分配的分区可能已经改变
     *             - 对于仍然分配的分区，如果{@link #poll(Duration)}返回了更多记录，它们的获取位置也可能改变
     * @throws org.apache.kafka.common.errors.WakeupException 
     *             当在调用此方法之前或期间调用了{@link #wakeup()}时抛出
     * @throws org.apache.kafka.common.errors.InterruptException 
     *             当调用线程在调用此方法之前或期间被中断时抛出
     * @throws org.apache.kafka.common.errors.AuthenticationException 
     *             当认证失败时抛出。详见异常信息
     * @throws org.apache.kafka.common.errors.AuthorizationException 
     *             当未被授权访问主题或配置的groupId时抛出。详见异常信息
     * @throws org.apache.kafka.common.KafkaException 
     *             当发生其他不可恢复的错误时抛出（如：位移元数据过大或主题不存在）
     * @throws org.apache.kafka.common.errors.TimeoutException 
     *             当在{@code default.api.timeout.ms}指定的超时时间内未能成功完成位移提交时抛出
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException 
     *             当此消费者使用经典组协议且实例被broker隔离时抛出
     */
    @Override
    public void commitSync() {
        // 将同步提交请求委托给实际的消费者实现类处理
        delegate.commitSync();
    }

    /**
     * 为上一次 {@link #poll(Duration) poll()} 返回的所有已订阅主题和分区提交偏移量。
     * <p>
     * 此方法仅将偏移量提交到Kafka。提交的偏移量将在每次重平衡后的第一次获取时以及启动时使用。
     * 因此，如果你需要将偏移量存储在Kafka以外的其他地方，不应使用此API。
     * <p>
     * 这是一个同步提交操作，会阻塞直到以下情况之一发生：
     * - 提交成功
     * - 遇到不可恢复的错误（此时会将错误抛出给调用者）
     * - 超过指定的超时时间
     * <p>
     * 注意：之前通过 {@link #commitAsync(OffsetCommitCallback)} （或类似方法）发送的异步偏移量提交，
     * 其回调函数保证会在此方法完成之前被调用。
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException 如果提交失败且无法重试。
     *             这种情况只会在以下场景发生：
     *             - 使用 {@link #subscribe(Collection)} 进行自动组管理
     *             - 存在使用相同 <code>group.id</code> 的活跃组正在使用组管理
     *             在这些情况下，如果你尝试提交不再分配给此消费者的分区（例如消费者已不再是组的一部分），
     *             就会抛出此异常。
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException 如果消费者实例正在进行重平衡，
     *            此时尚未确定哪些分区会被分配给消费者。在这种情况下，你可以先通过调用 {@link #poll(Duration)} 
     *            完成重平衡，然后再考虑提交。
     *            注意：重平衡后重新提交时，分配的分区可能已经改变，而且对于仍然分配的分区，
     *            如果从 {@link #poll(Duration)} 调用返回了更多记录，它们的获取位置也可能已经改变。
     * @throws org.apache.kafka.common.errors.WakeupException 如果在调用此函数之前或期间调用了 {@link #wakeup()}
     * @throws org.apache.kafka.common.errors.InterruptException 如果在调用此函数之前或期间调用线程被中断
     * @throws org.apache.kafka.common.errors.AuthenticationException 如果认证失败。详见异常信息
     * @throws org.apache.kafka.common.errors.AuthorizationException 如果未被授权访问主题或配置的groupId。详见异常信息
     * @throws org.apache.kafka.common.KafkaException 对于任何其他不可恢复的错误（例如偏移量元数据太大或主题不存在）
     * @throws org.apache.kafka.common.errors.TimeoutException 如果在偏移量提交成功完成之前超时
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException 如果此消费者使用经典组协议且此实例被broker隔离
     */
    @Override
    public void commitSync(Duration timeout) {
        // 调用委托对象执行同步提交操作，传入超时时间参数
        delegate.commitSync(timeout);
    }

    /**
     * 为指定的主题和分区列表提交指定的偏移量。
     * <p>
     * 此方法将偏移量提交到Kafka。提交的偏移量将在每次重平衡后的第一次获取时以及启动时使用。
     * 因此，如果你需要将偏移量存储在Kafka以外的其他地方，不应使用此API。
     * 提交的偏移量应该是你的应用程序将要消费的下一条消息的偏移量，
     * 即 {@code nextRecordToBeProcessed.offset()} （或 {@link ConsumerRecords#nextOffsets()}）。
     * 你还应该添加leader epoch作为提交元数据，可以从 {@link ConsumerRecord#leaderEpoch()} 
     * 或 {@link ConsumerRecords#nextOffsets()} 获取。
     * 如果使用 {@link #subscribe(Collection)} 进行自动组管理，
     * 则提交的偏移量必须属于当前自动分配的分区。
     * <p>
     * 这是一个同步提交操作，会阻塞直到以下情况之一发生：
     * - 提交成功
     * - 遇到不可恢复的错误（此时会将错误抛出给调用者）
     * - 超过 {@code default.api.timeout.ms} 指定的超时时间
     *   （此时会向调用者抛出 {@link org.apache.kafka.common.errors.TimeoutException}）
     * <p>
     * 注意：之前通过 {@link #commitAsync(OffsetCommitCallback)} （或类似方法）发送的异步偏移量提交，
     * 其回调函数保证会在此方法完成之前被调用。
     *
     * @param offsets 包含分区偏移量及其相关元数据的映射
     * @throws org.apache.kafka.clients.consumer.CommitFailedException 如果提交失败且无法重试。
     *             这种情况只会在以下场景发生：
     *             - 使用 {@link #subscribe(Collection)} 进行自动组管理
     *             - 存在使用相同 <code>group.id</code> 的活跃组正在使用组管理
     *             在这些情况下，如果你尝试提交不再分配给此消费者的分区（例如消费者已不再是组的一部分），
     *             就会抛出此异常。
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException 如果消费者实例正在进行重平衡，
     *            此时尚未确定哪些分区会被分配给消费者。在这种情况下，你可以先通过调用 {@link #poll(Duration)} 
     *            完成重平衡，然后再考虑提交。
     *            注意：重平衡后重新提交时，分配的分区可能已经改变，而且对于仍然分配的分区，
     *            如果从 {@link #poll(Duration)} 调用返回了更多记录，它们的获取位置也可能已经改变，
     *            所以重试提交时应考虑更新传入的 {@code offset} 参数。
     * @throws org.apache.kafka.common.errors.WakeupException 如果在调用此函数之前或期间调用了 {@link #wakeup()}
     * @throws org.apache.kafka.common.errors.InterruptException 如果在调用此函数之前或期间调用线程被中断
     * @throws org.apache.kafka.common.errors.AuthenticationException 如果认证失败。详见异常信息
     * @throws org.apache.kafka.common.errors.AuthorizationException 如果未被授权访问主题或配置的groupId。详见异常信息
     * @throws java.lang.IllegalArgumentException 如果提交的偏移量为负数
     * @throws org.apache.kafka.common.KafkaException 对于任何其他不可恢复的错误（例如偏移量元数据太大或主题不存在）
     * @throws org.apache.kafka.common.errors.TimeoutException 如果在偏移量提交成功完成之前超时
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException 如果此消费者使用经典组协议且此实例被broker隔离
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        // 调用委托对象执行同步提交操作，传入偏移量映射参数
        delegate.commitSync(offsets);
    }

    /**
     * 为指定的主题和分区列表提交指定的偏移量，并指定超时时间。
     * <p>
     * 此方法将偏移量提交到Kafka。提交的偏移量将在每次重平衡后的第一次获取时以及启动时使用。
     * 因此，如果你需要将偏移量存储在Kafka以外的其他地方，不应使用此API。
     * 提交的偏移量应该是你的应用程序将要消费的下一条消息的偏移量，
     * 即 {@code nextRecordToBeProcessed.offset()} （或 {@link ConsumerRecords#nextOffsets()}）。
     * 你还应该添加leader epoch作为提交元数据，可以从 {@link ConsumerRecord#leaderEpoch()} 
     * 或 {@link ConsumerRecords#nextOffsets()} 获取。
     * 如果使用 {@link #subscribe(Collection)} 进行自动组管理，
     * 则提交的偏移量必须属于当前自动分配的分区。
     * <p>
     * 这是一个同步提交操作，会阻塞直到以下情况之一发生：
     * - 提交成功
     * - 遇到不可恢复的错误（此时会将错误抛出给调用者）
     * - 超过指定的超时时间
     * <p>
     * 注意：之前通过 {@link #commitAsync(OffsetCommitCallback)} （或类似方法）发送的异步偏移量提交，
     * 其回调函数保证会在此方法完成之前被调用。
     *
     * @param offsets 包含分区偏移量及其相关元数据的映射
     * @param timeout 等待偏移量提交完成的最长时间
     * @throws org.apache.kafka.clients.consumer.CommitFailedException 如果提交失败且无法重试。
     *             这种情况只会在以下场景发生：
     *             - 使用 {@link #subscribe(Collection)} 进行自动组管理
     *             - 存在使用相同 <code>group.id</code> 的活跃组正在使用组管理
     *             在这些情况下，如果你尝试提交不再分配给此消费者的分区（例如消费者已不再是组的一部分），
     *             就会抛出此异常。
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException 如果消费者实例正在进行重平衡，
     *            此时尚未确定哪些分区会被分配给消费者。在这种情况下，你可以先通过调用 {@link #poll(Duration)} 
     *            完成重平衡，然后再考虑提交。
     *            注意：重平衡后重新提交时，分配的分区可能已经改变，而且对于仍然分配的分区，
     *            如果从 {@link #poll(Duration)} 调用返回了更多记录，它们的获取位置也可能已经改变，
     *            所以重试提交时应考虑更新传入的 {@code offset} 参数。
     * @throws org.apache.kafka.common.errors.WakeupException 如果在调用此函数之前或期间调用了 {@link #wakeup()}
     * @throws org.apache.kafka.common.errors.InterruptException 如果在调用此函数之前或期间调用线程被中断
     * @throws org.apache.kafka.common.errors.AuthenticationException 如果认证失败。详见异常信息
     * @throws org.apache.kafka.common.errors.AuthorizationException 如果未被授权访问主题或配置的groupId。详见异常信息
     * @throws java.lang.IllegalArgumentException 如果提交的偏移量为负数
     * @throws org.apache.kafka.common.KafkaException 对于任何其他不可恢复的错误（例如偏移量元数据太大或主题不存在）
     * @throws org.apache.kafka.common.errors.TimeoutException 如果在偏移量提交成功完成之前超时
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException 如果此消费者使用经典组协议且此实例被broker隔离
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        // 调用委托对象执行同步提交操作，传入偏移量映射和超时时间参数
        delegate.commitSync(offsets, timeout);
    }

    /**
     * 为所有已订阅的主题和分区提交最后一次 {@link #poll(Duration)} 返回的位移。
     * 等同于调用 {@link #commitAsync(OffsetCommitCallback) commitAsync(null)}。
     * 这是一个异步操作，不会阻塞消费者线程。
     * 
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException 如果消费者使用经典组协议且被broker隔离时抛出此异常
     */
    @Override
    public void commitAsync() {
        // 调用委托对象的异步提交方法，不带回调函数
        delegate.commitAsync();
    }

    /**
     * 为所有已订阅的主题和分区提交最后一次 {@link #poll(Duration) poll()} 返回的位移。
     * <p>
     * 此方法仅将位移提交到Kafka。提交的位移将在每次重平衡后的第一次获取和启动时使用。
     * 因此，如果需要将位移存储在Kafka之外的其他系统中，不应使用此API。
     * <p>
     * 这是一个异步调用，不会阻塞。遇到的任何错误要么传递给回调函数（如果提供），要么被丢弃。
     * <p>
     * 通过多次调用此API提交的位移保证按调用顺序发送。相应的提交回调也按相同顺序调用。
     * 另外请注意，通过此API提交的位移保证在后续调用 {@link #commitSync()} （及其变体）返回之前完成。
     *
     * @param callback 提交完成时要调用的回调函数
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException 如果消费者使用经典组协议且被broker隔离时抛出此异常
     */
    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        // 调用委托对象的异步提交方法，带回调函数
        delegate.commitAsync(callback);
    }

    /**
     * 为指定的主题和分区列表提交指定的位移到Kafka。
     * <p>
     * 此方法将位移提交到Kafka。提交的位移将在每次重平衡后的第一次获取和启动时使用。
     * 因此，如果需要将位移存储在Kafka之外的其他系统中，不应使用此API。
     * 提交的位移应该是应用程序将要消费的下一条消息的位移，
     * 即 {@code nextRecordToBeProcessed.offset()} （或 {@link ConsumerRecords#nextOffsets()}）。
     * 你还应该添加leader epoch作为提交元数据，可以从 {@link ConsumerRecord#leaderEpoch()} 或
     * {@link ConsumerRecords#nextOffsets()} 获取。
     * 如果使用 {@link #subscribe(Collection)} 进行自动组管理，
     * 则提交的位移必须属于当前自动分配的分区。
     * <p>
     * 这是一个异步调用，不会阻塞。遇到的任何错误要么传递给回调函数（如果提供），要么被丢弃。
     * <p>
     * 通过多次调用此API提交的位移保证按调用顺序发送。相应的提交回调也按相同顺序调用。
     * 另外请注意，通过此API提交的位移保证在后续调用 {@link #commitSync()} （及其变体）返回之前完成。
     *
     * @param offsets 按分区划分的位移映射及其关联元数据。此映射将在内部复制，因此在返回后修改映射是安全的。
     * @param callback 提交完成时要调用的回调函数
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException 如果消费者使用经典组协议且被broker隔离时抛出此异常
     */
    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        // 调用委托对象的异步提交方法，提交指定的位移映射，带回调函数
        delegate.commitAsync(offsets, callback);
    }

    /**
     * 覆盖消费者在下一次 {@link #poll(Duration) poll(timeout)} 中将使用的获取位移。
     * 如果对同一分区多次调用此API，则在下一次poll()时将使用最新的位移。
     * 注意，如果在消费过程中任意使用此API重置获取位移，可能会丢失数据。
     * <p>
     * 当调用poll()时，将获取指定位移的下一条消费者记录，前提是该位移存在对应的记录（即它是一个有效的位移）。
     * <p>
     * {@link #seekToBeginning(Collection)} 将转到主题中的第一个位移。
     * seek(0)等同于对起始位移为0的TopicPartition调用seekToBeginning，
     * 前提是位移0处的记录仍然可用。
     * {@link #seekToEnd(Collection)} 等同于寻找分区的最后一个位移，但行为取决于
     * {@code isolation.level}，详见 {@link #seekToEnd(Collection)} 文档。
     * <p>
     * 寻找小于日志起始位移或大于日志结束位移的位移意味着达到了无效位移。
     * 无效位移行为由 {@code auto.offset.reset} 属性控制。
     * 如果设置为"earliest"，下一次poll将从起始位移返回记录。
     * 如果设置为"latest"，将寻找最后一个位移（类似于seekToEnd()）。
     * 如果设置为"none"，将抛出 {@code OffsetOutOfRangeException}。
     * <p>
     * 注意，seek位移不会改变正在进行的获取请求，它将在下一个获取请求中生效。
     * 因此，消费者可能需要等待 {@code fetch.max.wait.ms} 才能开始从所需位移获取记录。
     *
     * @param partition 将执行seek操作的TopicPartition
     * @param offset poll()将返回的下一个位移
     * @throws IllegalArgumentException 如果提供的位移为负数
     * @throws IllegalStateException 如果提供的TopicPartition未分配给此消费者
     */
    @Override
    public void seek(TopicPartition partition, long offset) {
        // 调用委托对象的seek方法，设置指定分区的下一个消费位移
        delegate.seek(partition, offset);
    }

    /**
     * 覆盖消费者在下一次 {@link #poll(Duration) poll(timeout)} 中将使用的获取位移。
     * 如果对同一分区多次调用此API，则在下一次poll()时将使用最新的位移。
     * 注意，如果在消费过程中任意使用此API重置获取位移，可能会丢失数据。
     * 此方法允许同时设置leaderEpoch和所需的位移。
     *
     * @throws IllegalArgumentException 如果提供的位移为负数
     * @throws IllegalStateException 如果提供的TopicPartition未分配给此消费者
     */
    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        // 调用委托对象的seek方法，设置指定分区的下一个消费位移和元数据
        delegate.seek(partition, offsetAndMetadata);
    }

    /**
     * 将指定分区的消费位置重置到最早的位移（即第一个可用的位移）。
     * 这个方法是延迟执行的，只有在调用{@link #poll(Duration)}或{@link #position(TopicPartition)}时才会实际执行重置操作。
     * 如果没有提供分区列表，则会重置当前消费者已分配的所有分区的位移。
     * <p>
     * 使用场景：
     * 1. 需要重新消费某些分区的所有历史数据时
     * 2. 在消费者重启后希望从头开始处理数据时
     * 3. 进行数据迁移或备份时需要获取完整数据
     * <p>
     * 注意事项：
     * 1. 此操作会丢弃之前的消费位置，请谨慎使用
     * 2. 如果启用了自动提交，在执行seek操作后应该禁用自动提交或手动提交新位置
     * 3. 此操作不会立即触发数据获取，而是在下次poll时生效
     *
     * @param partitions 要重置位移的分区集合，如果为null则抛出IllegalArgumentException
     * @throws IllegalArgumentException 如果partitions参数为null
     * @throws IllegalStateException 如果指定的分区未被分配给当前消费者
     */
    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        // 调用委托对象执行实际的重置操作
        delegate.seekToBeginning(partitions);
    }

    /**
     * Seek to the last offset for each of the given partitions. This function evaluates lazily, seeking to the
     * final offset in all partitions only when {@link #poll(Duration)} or {@link #position(TopicPartition)} are called.
     * If no partitions are provided, seek to the final offset for all of the currently assigned partitions.
     * <p>
     * If {@code isolation.level=read_committed}, the end offset will be the Last Stable Offset, i.e., the offset
     * of the first message with an open transaction.
     *
     * @throws IllegalArgumentException if {@code partitions} is {@code null}
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        delegate.seekToEnd(partitions);
    }

    /**
     * Get the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
     * This method may issue a remote call to the server if there is no current position for the given partition.
     * <p>
     * This call will block until either the position could be determined or an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms} expires
     * (in which case a {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     *
     * @param partition The partition to get the position for
     * @return The current position of the consumer (that is, the offset of the next record to be fetched)
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if no offset is currently defined for
     *             the partition
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts to fetch stable offsets
     *             when the broker doesn't support this feature
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the position cannot be determined before the
     *             timeout specified by {@code default.api.timeout.ms} expires
     */
    @Override
    public long position(TopicPartition partition) {
        return delegate.position(partition);
    }

    /**
     * Get the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
     * This method may issue a remote call to the server if there is no current position
     * for the given partition.
     * <p>
     * This call will block until the position can be determined, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     *
     * @param partition The partition to get the position for
     * @param timeout The maximum amount of time to await determination of the current position
     * @return The current position of the consumer (that is, the offset of the next record to be fetched)
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if no offset is currently defined for
     *             the partition
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.TimeoutException if the position cannot be determined before the
     *             passed timeout expires
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public long position(TopicPartition partition, final Duration timeout) {
        return delegate.position(partition, timeout);
    }

    /**
     * Get the last committed offsets for the given partitions (whether the commit happened by this process or
     * another). The returned offsets will be used as the position for the consumer in the event of a failure.
     * <p>
     * If any of the partitions requested do not exist, an exception would be thrown.
     * <p>
     * This call will do a remote call to get the latest committed offsets from the server, and will block until the
     * committed offsets are gotten successfully, an unrecoverable error is encountered (in which case it is thrown to
     * the caller), or the timeout specified by {@code default.api.timeout.ms} expires (in which case a
     * {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     *
     * @param partitions The partitions to check
     * @return The latest committed offsets for the given partitions; {@code null} will be returned for the
     *         partition if there is no such message.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts to fetch stable offsets
     *             when the broker doesn't support this feature
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be found before
     *             the timeout specified by {@code default.api.timeout.ms} expires.
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return delegate.committed(partitions);
    }

    /**
     * Get the last committed offsets for the given partitions (whether the commit happened by this process or
     * another). The returned offsets will be used as the position for the consumer in the event of a failure.
     * <p>
     * If any of the partitions requested do not exist, an exception would be thrown.
     * <p>
     * This call will block to do a remote call to get the latest committed offsets from the server.
     *
     * @param partitions The partitions to check
     * @param timeout  The maximum amount of time to await the latest committed offsets
     * @return The latest committed offsets for the given partitions; {@code null} will be returned for the
     *         partition if there is no such message.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the committed offset cannot be found before
     *             expiration of the timeout
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions, final Duration timeout) {
        return delegate.committed(partitions, timeout);
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
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to determine the client
     *                        instance ID, though this error does not necessarily imply the
     *                        consumer client is otherwise unusable.
     * @throws IllegalArgumentException If the {@code timeout} is negative.
     * @throws IllegalStateException If telemetry is not enabled ie, config `{@code enable.metrics.push}`
     *                               is set to `{@code false}`.
     * @return The client's assigned instance id used for metrics collection.
     */
    @Override
    public Uuid clientInstanceId(Duration timeout) {
        return delegate.clientInstanceId(timeout);
    }

  /**
     * Get the metrics kept by the consumer
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return delegate.metrics();
    }

    /**
     * Get metadata about the partitions for a given topic. This method will issue a remote call to the server if it
     * does not already have any metadata about the given topic.
     *
     * @param topic The topic to get partition metadata for
     *
     * @return The list of partitions, which will be empty when the given topic is not found
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the specified topic. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         the amount of time allocated by {@code default.api.timeout.ms} expires.
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return delegate.partitionsFor(topic);
    }

    /**
     * Get metadata about the partitions for a given topic. This method will issue a remote call to the server if it
     * does not already have any metadata about the given topic.
     *
     * @param topic The topic to get partition metadata for
     * @param timeout The maximum of time to await topic metadata
     *
     * @return The list of partitions, which will be empty when the given topic is not found
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the specified topic. See
     *             the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if topic metadata cannot be fetched before expiration
     *             of the passed timeout
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return delegate.partitionsFor(topic, timeout);
    }

    /**
     * Get metadata about partitions for all topics that the user is authorized to view. This method will issue a
     * remote call to the server.

     * @return The map of topics and its partitions
     *
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         the amount of time allocated by {@code default.api.timeout.ms} expires.
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return delegate.listTopics();
    }

    /**
     * Get metadata about partitions for all topics that the user is authorized to view. This method will issue a
     * remote call to the server.
     *
     * @param timeout The maximum time this operation will block to fetch topic metadata
     *
     * @return The map of topics and its partitions
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.TimeoutException if the topic metadata could not be fetched before
     *             expiration of the passed timeout
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return delegate.listTopics(timeout);
    }

    /**
     * Suspend fetching from the requested partitions. Future calls to {@link #poll(Duration)} will not return
     * any records from these partitions until they have been resumed using {@link #resume(Collection)}.
     * Note that this method does not affect partition subscription. In particular, it does not cause a group
     * rebalance when automatic assignment is used.
     *
     * Note: Rebalance will not preserve the pause/resume state.
     * @param partitions The partitions which should be paused
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void pause(Collection<TopicPartition> partitions) {
        delegate.pause(partitions);
    }

    /**
     * Resume specified partitions which have been paused with {@link #pause(Collection)}. New calls to
     * {@link #poll(Duration)} will return records from these partitions if there are any to be fetched.
     * If the partitions were not previously paused, this method is a no-op.
     * @param partitions The partitions which should be resumed
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void resume(Collection<TopicPartition> partitions) {
        delegate.resume(partitions);
    }

    /**
     * Add the provided application metric for subscription.
     * This metric will be added to this client's metrics
     * that are available for subscription and sent as
     * telemetry data to the broker.
     * The provided metric must map to an OTLP metric data point
     * type in the OpenTelemetry v1 metrics protobuf message types.
     * Specifically, the metric should be one of the following:
     * <ul>
     *  <li>
     *     `Sum`: Monotonic total count meter (Counter). Suitable for metrics like total number of X, e.g., total bytes sent.
     *  </li>
     *  <li>
     *     `Gauge`: Non-monotonic current value meter (UpDownCounter). Suitable for metrics like current value of Y, e.g., current queue count.
     *  </li>
     * </ul>
     * Metrics not matching these types are silently ignored.
     * Executing this method for a previously registered metric is a benign operation and results in updating that metrics entry.
     *
     * @param metric The application metric to register
     */
    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        delegate.registerMetricForSubscription(metric);
    }

    /**
     * Remove the provided application metric for subscription.
     * This metric is removed from this client's metrics
     * and will not be available for subscription any longer.
     * Executing this method with a metric that has not been registered is a
     * benign operation and does not result in any action taken (no-op).
     *
     * @param metric The application metric to remove
     */
    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        delegate.unregisterMetricFromSubscription(metric);
    }

    /**
     * Get the set of partitions that were previously paused by a call to {@link #pause(Collection)}.
     *
     * @return The set of paused partitions
     */
    @Override
    public Set<TopicPartition> paused() {
        return delegate.paused();
    }

    /**
     * Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
     *
     * This is a blocking call. The consumer does not have to be assigned the partitions.
     * If the message format version in a partition is before 0.10.0, i.e. the messages do not have timestamps, null
     * will be returned for that partition.
     *
     * @param timestampsToSearch the mapping from partition to the timestamp to look up.
     *
     * @return a mapping from partition to the timestamp and offset of the first message with timestamp greater
     *         than or equal to the target timestamp. {@code null} will be returned for the partition if there is no
     *         such message.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws IllegalArgumentException if the target timestamp is negative
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         the amount of time allocated by {@code default.api.timeout.ms} expires.
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not support looking up
     *         the offsets by timestamp
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return delegate.offsetsForTimes(timestampsToSearch);
    }

    /**
     * Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
     *
     * This is a blocking call. The consumer does not have to be assigned the partitions.
     * If the message format version in a partition is before 0.10.0, i.e. the messages do not have timestamps, null
     * will be returned for that partition.
     *
     * @param timestampsToSearch the mapping from partition to the timestamp to look up.
     * @param timeout The maximum amount of time to await retrieval of the offsets
     *
     * @return a mapping from partition to the timestamp and offset of the first message with timestamp greater
     *         than or equal to the target timestamp. {@code null} will be returned for the partition if there is no
     *         such message.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws IllegalArgumentException if the target timestamp is negative
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         expiration of the passed timeout
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not support looking up
     *         the offsets by timestamp
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return delegate.offsetsForTimes(timestampsToSearch, timeout);
    }

    /**
     * Get the first offset for the given partitions.
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToBeginning(Collection)
     *
     * @param partitions the partitions to get the earliest offsets.
     * @return The earliest available offsets for the given partitions
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         expiration of the configured {@code default.api.timeout.ms}
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return delegate.beginningOffsets(partitions);
    }

    /**
     * Get the first offset for the given partitions.
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToBeginning(Collection)
     *
     * @param partitions the partitions to get the earliest offsets
     * @param timeout The maximum amount of time to await retrieval of the beginning offsets
     *
     * @return The earliest available offsets for the given partitions
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         expiration of the passed timeout
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return delegate.beginningOffsets(partitions, timeout);
    }

    /**
     * Get the end offsets for the given partitions. In the default {@code read_uncommitted} isolation level, the end
     * offset is the high watermark (that is, the offset of the last successfully replicated message plus one). For
     * {@code read_committed} consumers, the end offset is the last stable offset (LSO), which is the minimum of
     * the high watermark and the smallest offset of any open transaction. Finally, if the partition has never been
     * written to, the end offset is 0.
     *
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToEnd(Collection)
     *
     * @param partitions the partitions to get the end offsets.
     * @return The end offsets for the given partitions.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be fetched before
     *         the amount of time allocated by {@code default.api.timeout.ms} expires
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return delegate.endOffsets(partitions);
    }

    /**
     * Get the end offsets for the given partitions. In the default {@code read_uncommitted} isolation level, the end
     * offset is the high watermark (that is, the offset of the last successfully replicated message plus one). For
     * {@code read_committed} consumers, the end offset is the last stable offset (LSO), which is the minimum of
     * the high watermark and the smallest offset of any open transaction. Finally, if the partition has never been
     * written to, the end offset is 0.
     *
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToEnd(Collection)
     *
     * @param partitions the partitions to get the end offsets.
     * @param timeout The maximum amount of time to await retrieval of the end offsets
     *
     * @return The end offsets for the given partitions.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException if the offsets could not be fetched before
     *         expiration of the passed timeout
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return delegate.endOffsets(partitions, timeout);
    }

    /**
     * Get the consumer's current lag on the partition. Returns an "empty" {@link OptionalLong} if the lag is not known,
     * for example if there is no position yet, or if the end offset is not known yet.
     *
     * <p>
     * This method uses locally cached metadata. If the log end offset is not known yet, it triggers a request to fetch
     * the log end offset, but returns immediately.
     *
     * @param topicPartition The partition to get the lag for.
     *
     * @return This {@code Consumer} instance's current lag for the given partition.
     *
     * @throws IllegalStateException if the {@code topicPartition} is not assigned
     */
    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        return delegate.currentLag(topicPartition);
    }

    /**
     * Return the current group metadata associated with this consumer.
     *
     * @return consumer group metadata
     * @throws org.apache.kafka.common.errors.InvalidGroupIdException if consumer does not have a group
     */
    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return delegate.groupMetadata();
    }

    /**
     * Alert the consumer to trigger a new rebalance by rejoining the group. This is a nonblocking call that forces
     * the consumer to trigger a new rebalance on the next {@link #poll(Duration)} call. Note that this API does not
     * itself initiate the rebalance, so you must still call {@link #poll(Duration)}. If a rebalance is already in
     * progress this call will be a no-op. If you wish to force an additional rebalance you must complete the current
     * one by calling poll before retrying this API.
     * <p>
     * You do not need to call this during normal processing, as the consumer group will manage itself
     * automatically and rebalance when necessary. However there may be situations where the application wishes to
     * trigger a rebalance that would otherwise not occur. For example, if some condition external and invisible to
     * the Consumer and its group changes in a way that would affect the userdata encoded in the
     * {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription Subscription}, the Consumer
     * will not be notified and no rebalance will occur. This API can be used to force the group to rebalance so that
     * the assignor can perform a partition reassignment based on the latest userdata. If your assignor does not use
     * this userdata, or you do not use a custom
     * {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor ConsumerPartitionAssignor}, you should not
     * use this API.
     *
     * @param reason The reason why the new rebalance is needed.
     *
     * @throws java.lang.IllegalStateException if the consumer does not use group subscription
     */
    @Override
    public void enforceRebalance(final String reason) {
        delegate.enforceRebalance(reason);
    }

    /**
     * @see #enforceRebalance(String)
     */
    @Override
    public void enforceRebalance() {
        delegate.enforceRebalance();
    }

    /**
     * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
     * If auto-commit is enabled, this will commit the current offsets if possible within the default
     * timeout. See {@link #close(Duration)} for details. Note that {@link #wakeup()}
     * cannot be used to interrupt close.
     *
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted
     *             before or while this function is called
     * @throws org.apache.kafka.common.KafkaException for any other error during close
     */
    @Override
    public void close() {
        delegate.close();
    }

    /**
     * Tries to close the consumer cleanly within the specified timeout. This method waits up to
     * {@code timeout} for the consumer to complete pending commits and leave the group.
     * If auto-commit is enabled, this will commit the current offsets if possible within the
     * timeout. If the consumer is unable to complete offset commits and gracefully leave the group
     * before the timeout expires, the consumer is force closed. Note that {@link #wakeup()} cannot be
     * used to interrupt close.
     * <p>
     * The actual maximum wait time is bounded by the {@link ConsumerConfig#REQUEST_TIMEOUT_MS_CONFIG} setting, which
     * only applies to operations performed with the broker (coordinator-related requests and
     * fetch sessions). Even if a larger timeout is specified, the consumer will not wait longer than
     * {@link ConsumerConfig#REQUEST_TIMEOUT_MS_CONFIG} for these requests to complete during the close operation.
     * Note that the execution time of callbacks (such as {@link OffsetCommitCallback} and
     * {@link ConsumerRebalanceListener}) does not consume time from the close timeout.
     *
     * @param timeout The maximum time to wait for consumer to close gracefully. The value must be
     *                non-negative. Specifying a timeout of zero means do not wait for pending requests to complete.
     *
     * @throws IllegalArgumentException If the {@code timeout} is negative.
     * @throws InterruptException If the thread is interrupted before or while this function is called
     * @throws org.apache.kafka.common.KafkaException for any other error during close
     */
    @Override
    public void close(Duration timeout) {
        delegate.close(timeout);
    }

    /**
     * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long poll.
     * The thread which is blocking in an operation will throw {@link org.apache.kafka.common.errors.WakeupException}.
     * If no thread is blocking in a method which can throw {@link org.apache.kafka.common.errors.WakeupException}, the next call to such a method will raise it instead.
     */
    @Override
    public void wakeup() {
        delegate.wakeup();
    }

    // Functions below are for testing only
    String clientId() {
        return delegate.clientId();
    }

    Metrics metricsRegistry() {
        return delegate.metricsRegistry();
    }

    KafkaConsumerMetrics kafkaConsumerMetrics() {
        return delegate.kafkaConsumerMetrics();
    }

    boolean updateAssignmentMetadataIfNeeded(final Timer timer) {
        return delegate.updateAssignmentMetadataIfNeeded(timer);
    }
}
