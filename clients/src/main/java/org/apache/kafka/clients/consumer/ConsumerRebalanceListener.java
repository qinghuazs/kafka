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

import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;

/**
 * 一个回调接口，用户可以实现该接口来在消费者分配的分区集合发生变化时触发自定义操作。
 * <p>
 * 此接口仅适用于由Kafka自动管理消费者组成员关系的情况。如果消费者直接分配分区，
 * 这些分区将永远不会被重新分配，此回调接口也就不适用。
 * <p>
 * 当Kafka管理组成员关系时，在以下情况下会触发分区重新分配：组成员发生变化或成员的订阅发生变化。
 * 这可能发生在进程死亡、新进程实例被添加或失败的旧实例重新恢复后。
 * 分区重新分配也可能由影响已订阅主题的变化触发（例如，当分区数量被管理员调整时）。
 * <p>
 * 此功能有许多用途。一个常见用途是在自定义存储中保存偏移量。通过在
 * {@link #onPartitionsRevoked(Collection)}调用中保存偏移量，我们可以确保在分区分配发生变化时
 * 偏移量得到保存。
 * <p>
 * 另一个用途是刷新消费者可能保持的任何中间结果缓存。例如，
 * 考虑这样一个场景：消费者订阅了包含用户页面浏览的主题，目标是统计每个用户在每个五分钟窗口内的
 * 页面浏览次数。假设主题按用户ID进行分区，这样特定用户的所有事件都会发送到单个消费者实例。
 * 消费者可以在内存中保持每个用户操作的运行计数，只在缓存变得太大时才将其刷新到远程数据存储。
 * 但是如果分区被重新分配，它可能希望在新的所有者接管消费之前自动触发此缓存的刷新。
 * <p>
 * 此回调仅在分区分配发生变化时，作为{@link Consumer#poll(java.time.Duration) poll(long)}调用的一部分
 * 在用户线程中执行。
 * <p>
 * 在正常情况下，如果一个分区从一个消费者重新分配给另一个消费者，则旧消费者将
 * 始终在新消费者为同一分区调用{@link #onPartitionsAssigned(Collection) onPartitionsAssigned}之前
 * 调用{@link #onPartitionsRevoked(Collection) onPartitionsRevoked}。因此，如果在
 * {@link #onPartitionsRevoked(Collection) onPartitionsRevoked}调用中由一个消费者成员保存了偏移量或其他状态，
 * 它将在接管该分区的其他消费者成员触发其{@link #onPartitionsAssigned(Collection) onPartitionsAssigned}回调以加载状态时始终可访问。
 * <p>
 * 您可以将撤销视为优雅地放弃分区所有权的方式。在某些情况下，消费者可能没有机会这样做。
 * 例如，如果会话超时，则可能在我们有机会优雅地撤销分区之前重新分配分区。
 * 对于这种情况，我们有第三个回调{@link #onPartitionsLost(Collection)}。这个函数与
 * {@link #onPartitionsRevoked(Collection)}的区别在于，在调用{@link #onPartitionsLost(Collection)}时，
 * 这些分区可能已经被组中的其他成员拥有，因此用户将无法提交其消费的偏移量。
 * 用户可以不同地实现这两个函数（默认情况下，
 * {@link #onPartitionsLost(Collection)}将直接调用{@link #onPartitionsRevoked(Collection)}）；例如，在
 * {@link #onPartitionsLost(Collection)}中，我们不需要存储偏移量，因为我们知道这些分区不再由消费者拥有。
 * <p>
 * 在重平衡事件期间，{@link #onPartitionsAssigned(Collection) onPartitionsAssigned}函数将在
 * 重平衡完成时始终被精确触发一次。也就是说，即使消费者成员没有新分配的分区，其
 * {@link #onPartitionsAssigned(Collection) onPartitionsAssigned}仍将被触发，但带有一个空的分区集合。
 * 因此，此函数也可用于通知重平衡事件已发生。
 * 在急切重平衡中，{@link #onPartitionsRevoked(Collection)}将始终在重平衡开始时被调用。另一方面，
 * {@link #onPartitionsLost(Collection)}仅在有非空分区丢失时才会被调用。
 * 在协作重平衡中，{@link #onPartitionsRevoked(Collection)}和{@link #onPartitionsLost(Collection)}
 * 仅在重平衡事件期间从该消费者成员撤销或丢失非空分区时才会被触发。
 * <p>
 * 在这些嵌套调用中可能会抛出{@link org.apache.kafka.common.errors.WakeupException}或
 * {@link org.apache.kafka.common.errors.InterruptException}。在这种情况下，异常将传播到
 * 正在执行此回调的当前{@link KafkaConsumer#poll(java.time.Duration)}调用。这意味着
 * 不需要捕获这些异常并重新尝试唤醒或中断消费者线程。
 * 同样，如果回调函数实现本身抛出异常，该异常也将传播到当前的
 * {@link KafkaConsumer#poll(java.time.Duration)}调用。
 * <p>
 * 请注意，回调仅作为分配变更的通知。
 * 它们不能用于表示对变更的接受。
 * 因此，从回调抛出异常不会以任何方式影响分配，
 * 因为它将一直传播到{@link KafkaConsumer#poll(java.time.Duration)}调用。
 * 如果用户在调用者中捕获异常，回调仍被认为是成功的，不会尝试进一步重试。
 * <p>
 *
 * 以下是用于保存偏移量的回调实现的示例代码：
 * <pre>
 * {@code
 *   public class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {
 *       private Consumer<?,?> consumer;
 *
 *       public SaveOffsetsOnRebalance(Consumer<?,?> consumer) {
 *           this.consumer = consumer;
 *       }
 *
 *       public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 *           // 使用这里未描述的自定义代码将偏移量保存在外部存储中
 *           for(TopicPartition partition: partitions)
 *              saveOffsetInExternalStore(consumer.position(partition));
 *       }
 *
 *       public void onPartitionsLost(Collection<TopicPartition> partitions) {
 *           // 不需要保存偏移量，因为这些分区可能已经被其他消费者拥有
 *       }
 *
 *       public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
 *           // 使用这里未描述的自定义代码从外部存储读取偏移量
 *           for(TopicPartition partition: partitions)
 *              consumer.seek(partition, readOffsetFromExternalStore(partition));
 *       }
 *   }
 * }
 * </pre>
 */
public interface ConsumerRebalanceListener {

    /**
     * 用户可以实现此回调方法来处理向自定义存储提交偏移量。
     * 此方法将在重平衡操作期间当消费者必须放弃某些分区时被调用。
     * 它也可能在消费者被关闭（{@link KafkaConsumer#close(Duration)}）
     * 或取消订阅（{@link KafkaConsumer#unsubscribe()}）时被调用。
     * 建议在此回调中将偏移量提交到Kafka或自定义偏移量存储中，以防止数据重复。
     * <p>
     * 在急切重平衡中，它将始终在重平衡开始时和消费者停止获取数据后被调用。
     * 在协作重平衡中，它将在重平衡结束时对被撤销的分区集合调用（当且仅当该集合非空时）。
     * 有关此API的使用示例，请参见{@link KafkaConsumer KafkaConsumer}的使用示例部分。
     * <p>
     * 撤销回调通常使用消费者实例来提交偏移量。在这些嵌套调用中可能会抛出
     * {@link org.apache.kafka.common.errors.WakeupException}或{@link org.apache.kafka.common.errors.InterruptException}。
     * 在这种情况下，异常将传播到正在执行此回调的当前{@link KafkaConsumer#poll(java.time.Duration)}调用。
     * 这意味着不需要捕获这些异常并重新尝试唤醒或中断消费者线程。
     *
     * @param partitions 分配给消费者但现在需要被撤销的分区列表（可能不包括所有当前分配的分区，
     *                   即可能仍有一些分区保留）
     * @throws org.apache.kafka.common.errors.WakeupException 如果从对{@link KafkaConsumer}的嵌套调用中抛出
     * @throws org.apache.kafka.common.errors.InterruptException 如果从对{@link KafkaConsumer}的嵌套调用中抛出
     */
    void onPartitionsRevoked(Collection<TopicPartition> partitions);

    /**
     * 用户可以实现此回调方法来在成功完成分区重新分配时处理自定义偏移量。
     * 此方法将在分区重新分配完成后且在消费者开始获取数据之前被调用，
     * 并且仅作为{@link Consumer#poll(java.time.Duration) poll(long)}调用的结果。
     * <p>
     * 在正常情况下，保证消费者组中的所有进程都将在任何实例执行其
     * {@link #onPartitionsAssigned(Collection)}回调之前执行它们的
     * {@link #onPartitionsRevoked(Collection)}回调。在异常情况下，分区可能会在
     * 未通知旧所有者的情况下迁移（即其{@link #onPartitionsRevoked(Collection)}回调未触发），
     * 当旧所有者消费者意识到此事件时，消费者将触发{@link #onPartitionsLost(Collection)}回调。
     * <p>
     * 分配回调通常使用消费者实例来查询偏移量。在这些嵌套调用中可能会抛出
     * {@link org.apache.kafka.common.errors.WakeupException}或{@link org.apache.kafka.common.errors.InterruptException}。
     * 在这种情况下，异常将传播到正在执行此回调的当前{@link KafkaConsumer#poll(java.time.Duration)}调用。
     * 这意味着不需要捕获这些异常并重新尝试唤醒或中断消费者线程。
     *
     * @param partitions 现在分配给消费者的分区列表（不包括之前拥有的分区，
     *                   即此列表将仅包括新添加的分区）
     * @throws org.apache.kafka.common.errors.WakeupException 如果从对{@link KafkaConsumer}的嵌套调用中抛出
     * @throws org.apache.kafka.common.errors.InterruptException 如果从对{@link KafkaConsumer}的嵌套调用中抛出
     */
    void onPartitionsAssigned(Collection<TopicPartition> partitions);

    /**
     * 您可以实现此回调方法来处理已重新分配给其他消费者的分区的资源清理。
     * 在正常执行期间不会调用此方法，因为在重平衡事件期间重新分配给其他消费者之前，
     * 拥有的分区首先会通过调用{@link ConsumerRebalanceListener#onPartitionsRevoked}被撤销。
     * 但是，在异常情况下，当消费者意识到它不再拥有此分区时（即不是通过正常重平衡事件撤销），
     * 则会调用此方法。
     * <p>
     * 例如，如果消费者的会话超时已过期，或者收到表明消费者不再是组成员的致命错误，
     * 则会调用此函数。
     * <p>
     * 默认情况下，它将只触发{@link ConsumerRebalanceListener#onPartitionsRevoked}；
     * 对于想要区分已撤销分区和丢失分区的处理逻辑的用户，他们可以覆盖默认实现。
     * <p>
     * 在这些嵌套调用中可能会抛出{@link org.apache.kafka.common.errors.WakeupException}或
     * {@link org.apache.kafka.common.errors.InterruptException}。在这种情况下，异常将传播到
     * 正在执行此回调的当前{@link KafkaConsumer#poll(java.time.Duration)}调用。这意味着
     * 不需要捕获这些异常并重新尝试唤醒或中断消费者线程。
     *
     * @param partitions 之前分配给消费者但现在已重新分配给其他消费者的分区列表。
     *                   使用当前协议，这将始终包括消费者之前分配的所有分区，
     *                   但这在未来的协议中可能会改变（即可能仍有一些分区保留）
     * @throws org.apache.kafka.common.errors.WakeupException 如果从对{@link KafkaConsumer}的嵌套调用中抛出
     * @throws org.apache.kafka.common.errors.InterruptException 如果从对{@link KafkaConsumer}的嵌套调用中抛出
     */
    default void onPartitionsLost(Collection<TopicPartition> partitions) {
        onPartitionsRevoked(partitions);
    }
}
