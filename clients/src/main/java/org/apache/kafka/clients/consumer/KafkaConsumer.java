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
     * A consumer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >here</a>. Values can be
     * either strings or objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     */
    public KafkaConsumer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     */
    public KafkaConsumer(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration, and a
     * key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaConsumer(Properties properties,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(propsToMap(properties), keyDeserializer, valueDeserializer);
    }

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration, and a key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *            won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaConsumer(Map<String, Object> configs,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
                keyDeserializer, valueDeserializer);
    }

    KafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        delegate = CREATOR.create(config, keyDeserializer, valueDeserializer);
    }

    KafkaConsumer(LogContext logContext,
                  Time time,
                  ConsumerConfig config,
                  Deserializer<K> keyDeserializer,
                  Deserializer<V> valueDeserializer,
                  KafkaClient client,
                  SubscriptionState subscriptions,
                  ConsumerMetadata metadata,
                  List<ConsumerPartitionAssignor> assignors) {
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
     * Get the set of partitions currently assigned to this consumer. If subscription happened by directly assigning
     * partitions using {@link #assign(Collection)} then this will simply return the same partitions that
     * were assigned. If topic subscription was used, then this will give the set of topic partitions currently assigned
     * to the consumer (which may be none if the assignment hasn't happened yet, or the partitions are in the
     * process of getting reassigned).
     * @return The set of partitions currently assigned to this consumer
     */
    public Set<TopicPartition> assignment() {
        return delegate.assignment();
    }

    /**
     * Get the current subscription. Will return the same topics used in the most recent call to
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, or an empty set if no such call has been made.
     * @return The set of topics currently subscribed to
     */
    public Set<String> subscription() {
        return delegate.subscription();
    }

    /**
     * Subscribe to the given list of topics to get dynamically
     * assigned partitions. <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> Note that it is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     *
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if any one of the following events are triggered:
     * <ul>
     * <li>Number of partitions change for any of the subscribed topics
     * <li>A subscribed topic is created or deleted
     * <li>An existing member of the consumer group is shutdown or fails
     * <li>A new member is added to the consumer group
     * </ul>
     * <p>
     * When any of these events are triggered, the provided listener will be invoked first to indicate that
     * the consumer's assignment has been revoked, and then again when the new assignment has been received.
     * Note that rebalances will only occur during an active call to {@link #poll(Duration)}, so callbacks will
     * also only be invoked during that time.
     *
     * The provided listener will immediately override any listener set in a previous call to subscribe.
     * It is guaranteed, however, that the partitions revoked/assigned through this interface are from topics
     * subscribed in this call. See {@link ConsumerRebalanceListener} for more details.
     *
     * @param topics The list of topics to subscribe to
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation for the
     *                 subscribed topics
     * @throws IllegalArgumentException If topics is null or contains null or empty elements, or if listener is null
     * @throws IllegalStateException If {@code subscribe()} is called previously with pattern, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        delegate.subscribe(topics, listener);
    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> It is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     *
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * This is a short-hand for {@link #subscribe(Collection, ConsumerRebalanceListener)}, which
     * uses a no-op listener. If you need the ability to seek to particular offsets, you should prefer
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, since group rebalances will cause partition offsets
     * to be reset. You should also provide your own listener if you are doing your own offset
     * management since the listener gives you an opportunity to commit offsets before a rebalance finishes.
     *
     * @param topics The list of topics to subscribe to
     * @throws IllegalArgumentException If topics is null or contains null or empty elements
     * @throws IllegalStateException If {@code subscribe()} is called previously with pattern, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Collection<String> topics) {
        delegate.subscribe(topics);
    }

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     * The pattern matching will be done periodically against all topics existing at the time of check.
     * This can be controlled through the {@code metadata.max.age.ms} configuration: by lowering
     * the max metadata age, the consumer will refresh metadata more often and check for matching topics.
     * <p>
     * See {@link #subscribe(Collection, ConsumerRebalanceListener)} for details on the
     * use of the {@link ConsumerRebalanceListener}. Generally rebalances are triggered when there
     * is a change to the topics matching the provided pattern and when consumer group membership changes.
     * Group rebalances only take place during an active call to {@link #poll(Duration)}.
     *
     * @param pattern Pattern to subscribe to
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation for the
     *                 subscribed topics
     * @throws IllegalArgumentException If pattern or listener is null
     * @throws IllegalStateException If {@code subscribe()} is called previously with topics, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        delegate.subscribe(pattern, listener);
    }

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     * The pattern matching will be done periodically against topics existing at the time of check.
     * <p>
     * This is a short-hand for {@link #subscribe(Pattern, ConsumerRebalanceListener)}, which
     * uses a no-op listener. If you need the ability to seek to particular offsets, you should prefer
     * {@link #subscribe(Pattern, ConsumerRebalanceListener)}, since group rebalances will cause partition offsets
     * to be reset. You should also provide your own listener if you are doing your own offset
     * management since the listener gives you an opportunity to commit offsets before a rebalance finishes.
     *
     * @param pattern Pattern to subscribe to
     * @throws IllegalArgumentException If pattern is null
     * @throws IllegalStateException If {@code subscribe()} is called previously with topics, or assign is called
     *                               previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                               configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Pattern pattern) {
        delegate.subscribe(pattern);
    }

    /**
     * Subscribe to all topics matching the specified pattern, to get dynamically assigned partitions.
     * The pattern matching will be done periodically against all topics. This is only supported under the
     * CONSUMER group protocol (see {@link ConsumerConfig#GROUP_PROTOCOL_CONFIG}).
     * <p>
     * If the provided pattern is not compatible with Google RE2/J, an {@link InvalidRegularExpression} will be
     * eventually thrown on a call to {@link #poll(Duration)} following this call to subscribe.
     * <p>
     * See {@link #subscribe(Collection, ConsumerRebalanceListener)} for details on the
     * use of the {@link ConsumerRebalanceListener}. Generally, rebalances are triggered when there
     * is a change to the topics matching the provided pattern and when consumer group membership changes.
     * Group rebalances only take place during an active call to {@link #poll(Duration)}.
     *
     * @param pattern  Pattern to subscribe to, that must be compatible with Google RE2/J.
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation for the
     *                 subscribed topics.
     * @throws IllegalArgumentException If pattern is null or empty, or if the listener is null.
     * @throws IllegalStateException    If {@code subscribe()} is called previously with topics, or assign is called
     *                                  previously (without a subsequent call to {@link #unsubscribe()}).
     */
    @Override
    public void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener listener) {
        delegate.subscribe(pattern, listener);
    }

    /**
     * Subscribe to all topics matching the specified pattern, to get dynamically assigned partitions.
     * The pattern matching will be done periodically against topics. This is only supported under the
     * CONSUMER group protocol (see {@link ConsumerConfig#GROUP_PROTOCOL_CONFIG})
     * <p>
     * If the provided pattern is not compatible with Google RE2/J, an {@link InvalidRegularExpression} will be
     * eventually thrown on a call to {@link #poll(Duration)} following this call to subscribe.
     * <p>
     * This is a short-hand for {@link #subscribe(Pattern, ConsumerRebalanceListener)}, which
     * uses a no-op listener. If you need the ability to seek to particular offsets, you should prefer
     * {@link #subscribe(Pattern, ConsumerRebalanceListener)}, since group rebalances will cause partition offsets
     * to be reset. You should also provide your own listener if you are doing your own offset
     * management since the listener gives you an opportunity to commit offsets before a rebalance finishes.
     *
     * @param pattern Pattern to subscribe to, that must be compatible with Google RE2/J.
     * @throws IllegalArgumentException If pattern is null or empty.
     * @throws IllegalStateException    If {@code subscribe()} is called previously with topics, or assign is called
     *                                  previously (without a subsequent call to {@link #unsubscribe()}).
     */
    @Override
    public void subscribe(SubscriptionPattern pattern) {
        delegate.subscribe(pattern);
    }

    /**
     * Unsubscribe from topics currently subscribed with {@link #subscribe(Collection)} or {@link #subscribe(Pattern)}.
     * This also clears any partitions directly assigned through {@link #assign(Collection)}.
     *
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. rebalance callback errors)
     */
    public void unsubscribe() {
        delegate.unsubscribe();
    }

    /**
     * Manually assign a list of partitions to this consumer. This interface does not allow for incremental assignment
     * and will replace the previous assignment (if there is one).
     * <p>
     * If the given list of topic partitions is empty, it is treated the same as {@link #unsubscribe()}.
     * <p>
     * Manual topic assignment through this method does not use the consumer's group management
     * functionality. As such, there will be no rebalance operation triggered when group membership or cluster and topic
     * metadata change. Note that it is not possible to use both manual partition assignment with {@link #assign(Collection)}
     * and group assignment with {@link #subscribe(Collection, ConsumerRebalanceListener)}.
     * <p>
     * If auto-commit is enabled, an async commit (based on the old assignment) will be triggered before the new
     * assignment replaces the old one.
     *
     * @param partitions The list of partitions to assign this consumer
     * @throws IllegalArgumentException If partitions is null or contains null or empty topics
     * @throws IllegalStateException If {@code subscribe()} is called previously with topics or pattern
     *                               (without a subsequent call to {@link #unsubscribe()})
     */
    @Override
    public void assign(Collection<TopicPartition> partitions) {
        delegate.assign(partitions);
    }

    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It is an error to not have
     * subscribed to any topics or partitions before polling for data.
     * <p>
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially. The last
     * consumed offset can be manually set through {@link #seek(TopicPartition, long)} or automatically set as the last committed
     * offset for the subscribed list of partitions
     *
     * <p>
     * This method returns immediately if there are records available or if the position advances past control records
     * or aborted transactions when isolation.level=read_committed.
     * Otherwise, it will await the passed timeout. If the timeout expires, an empty record set will be returned.
     * Note that this method may block beyond the timeout in order to execute custom
     * {@link ConsumerRebalanceListener} callbacks.
     *
     *
     * @param timeout The maximum time to block (must not be greater than {@link Long#MAX_VALUE} milliseconds)
     *
     * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
     *
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if the offset for a partition or set of
     *             partitions is undefined or out of range and no offset reset policy has been configured
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if caller lacks Read access to any of the subscribed
     *             topics or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. invalid groupId or
     *             session timeout, errors deserializing key/value pairs, your rebalance callback thrown exceptions,
     *             or any new error cases in future versions)
     * @throws java.lang.IllegalArgumentException if the timeout value is negative
     * @throws java.lang.IllegalStateException if the consumer is not subscribed to any topics or manually assigned any
     *             partitions to consume from
     * @throws java.lang.ArithmeticException if the timeout is greater than {@link Long#MAX_VALUE} milliseconds.
     * @throws org.apache.kafka.common.errors.InvalidTopicException if the current subscription contains any invalid
     *             topic (per {@link org.apache.kafka.common.internals.Topic#validate(String)})
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts to fetch stable offsets
     *             when the broker doesn't support this feature. Also, if the consumer attempts to subscribe to a
     *             SubscriptionPattern via {@link #subscribe(SubscriptionPattern)} or
     *             {@link #subscribe(SubscriptionPattern, ConsumerRebalanceListener)} and the broker doesn't
     *             support this feature.
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        return delegate.poll(timeout);
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms} expires
     * (in which case a {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This fatal error can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout specified by {@code default.api.timeout.ms} expires
     *            before successful completion of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer is using the classic group protocol
     *            and this instance gets fenced by broker.
     */
    @Override
    public void commitSync() {
        delegate.commitSync();
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the passed timeout expires.
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before successful completion
     *            of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer is using the classic group protocol
     *            and this instance gets fenced by broker.
     */
    @Override
    public void commitSync(Duration timeout) {
        delegate.commitSync(timeout);
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. {@code nextRecordToBeProcessed.offset()} (or {@link ConsumerRecords#nextOffsets()}).
     * You should also add the leader epoch as commit metadata, which can be obtained from
     * {@link ConsumerRecord#leaderEpoch()} or {@link ConsumerRecords#nextOffsets()}.
     * If automatic group management with {@link #subscribe(Collection)} is used,
     * then the committed offsets must belong to the currently auto-assigned partitions.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds or an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms} expires
     * (in which case a {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @param offsets A map of offsets by partition with associated metadata
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call, so when you retry committing
     *            you should consider updating the passed in {@code offset} parameter.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws java.lang.IllegalArgumentException if the committed offset is negative
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before successful completion
     *            of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer is using the classic group protocol
     *            and this instance gets fenced by broker.
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        delegate.commitSync(offsets);
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. {@code nextRecordToBeProcessed.offset()} (or {@link ConsumerRecords#nextOffsets()}).
     * You should also add the leader epoch as commit metadata, which can be obtained from
     * {@link ConsumerRecord#leaderEpoch()} or {@link ConsumerRecords#nextOffsets()}.
     * If automatic group management with {@link #subscribe(Collection)} is used,
     * then the committed offsets must belong to the currently auto-assigned partitions.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @param offsets A map of offsets by partition with associated metadata
     * @param timeout The maximum amount of time to await completion of the offset commit
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *             when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *             consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *            so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *            complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *            NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *            and also for those partitions that are still assigned their fetch positions may have changed too
     *            if more records are returned from the {@link #poll(Duration)} call, so when you retry committing
     *            you should consider updating the passed in {@code offset} parameter.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId. See the exception for more details
     * @throws java.lang.IllegalArgumentException if the committed offset is negative
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before successful completion
     *            of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer is using the classic group protocol
     *            and this instance gets fenced by broker.
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        delegate.commitSync(offsets, timeout);
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration)} for all the subscribed list of topics and partition.
     * Same as {@link #commitAsync(OffsetCommitCallback) commitAsync(null)}
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer is using the classic group protocol
     *            and this instance gets fenced by broker.
     */
    @Override
    public void commitAsync() {
        delegate.commitAsync();
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for the subscribed list of topics and partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     * <p>
     * Offsets committed through multiple calls to this API are guaranteed to be sent in the same order as
     * the invocations. Corresponding commit callbacks are also invoked in the same order. Additionally note that
     * offsets committed through this API are guaranteed to complete before a subsequent call to {@link #commitSync()}
     * (and variants) returns.
     *
     * @param callback Callback to invoke when the commit completes
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer is using the classic group protocol
     *             and this instance gets fenced by broker.
     */
    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        delegate.commitAsync(callback);
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions to Kafka.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. {@code nextRecordToBeProcessed.offset()} (or {@link ConsumerRecords#nextOffsets()}).
     * You should also add the leader epoch as commit metadata, which can be obtained from
     * {@link ConsumerRecord#leaderEpoch()} or {@link ConsumerRecords#nextOffsets()}.
     * If automatic group management with {@link #subscribe(Collection)} is used,
     * then the committed offsets must belong to the currently auto-assigned partitions.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     * <p>
     * Offsets committed through multiple calls to this API are guaranteed to be sent in the same order as
     * the invocations. Corresponding commit callbacks are also invoked in the same order. Additionally note that
     * offsets committed through this API are guaranteed to complete before a subsequent call to {@link #commitSync()}
     * (and variants) returns.
     *
     * @param offsets A map of offsets by partition with associate metadata. This map will be copied internally, so it
     *                is safe to mutate the map after returning.
     * @param callback Callback to invoke when the commit completes
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer is using the classic group protocol
     *             and this instance gets fenced by broker.
     */
    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        delegate.commitAsync(offsets, callback);
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(Duration) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets
     * <p>
     * The next Consumer Record which will be retrieved when poll() is invoked will have the offset specified, given that
     * a record with that offset exists (i.e. it is a valid offset).
     * <p>
     * {@link #seekToBeginning(Collection)} will go to the first offset in the topic.
     * seek(0) is equivalent to seekToBeginning for a TopicPartition with beginning offset 0,
     * assuming that there is a record at offset 0 still available.
     * {@link #seekToEnd(Collection)} is equivalent to seeking to the last offset of the partition, but behavior depends on
     * {@code isolation.level}, so see {@link #seekToEnd(Collection)} documentation for more details.
     * <p>
     * Seeking to the offset smaller than the log start offset or larger than the log end offset
     * means an invalid offset is reached.
     * Invalid offset behaviour is controlled by the {@code auto.offset.reset} property.
     * If this is set to "earliest", the next poll will return records from the starting offset.
     * If it is set to "latest", it will seek to the last offset (similar to seekToEnd()).
     * If it is set to "none", an {@code OffsetOutOfRangeException} will be thrown.
     * <p>
     * Note that, the seek offset won't change to the in-flight fetch request, it will take effect in next fetch request.
     * So, the consumer might wait for {@code fetch.max.wait.ms} before starting to fetch the records from desired offset.
     *
     * @param partition the TopicPartition on which the seek will be performed.
     * @param offset the next offset returned by poll().
     * @throws IllegalArgumentException if the provided offset is negative
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     */
    @Override
    public void seek(TopicPartition partition, long offset) {
        delegate.seek(partition, offset);
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(Duration) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets. This
     * method allows for setting the leaderEpoch along with the desired offset.
     *
     * @throws IllegalArgumentException if the provided offset is negative
     * @throws IllegalStateException if the provided TopicPartition is not assigned to this consumer
     */
    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        delegate.seek(partition, offsetAndMetadata);
    }

    /**
     * Seek to the first offset for each of the given partitions. This function evaluates lazily, seeking to the
     * first offset in all partitions only when {@link #poll(Duration)} or {@link #position(TopicPartition)} are called.
     * If no partitions are provided, seek to the first offset for all of the currently assigned partitions.
     *
     * @throws IllegalArgumentException if {@code partitions} is {@code null}
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
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
