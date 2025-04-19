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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 内置默认分区器。这是一个直接被RecordAccumulator使用的工具类，不实现Partitioner接口。
 * 
 * 该类为每个topic维护了自适应粘性分区(KIP-794)所需的各种簿记信息。每个topic对应一个分区器实例。
 * 
 * 主要功能：
 * 1. 实现粘性分区策略：在一定时间/数据量内将消息发送到同一分区，以提高批处理效率
 * 2. 自适应负载均衡：根据分区队列大小动态调整分区选择概率
 * 3. 并发控制：通过原子引用和锁机制确保线程安全
 */
public class BuiltInPartitioner {
    // 日志记录器
    private final Logger log;
    // 当前分区器负责的topic
    private final String topic;
    // 粘性批次大小：决定在切换分区前向同一分区发送多少字节的数据
    private final int stickyBatchSize;

    // 分区负载统计信息，用于自适应分区选择。可能为null表示禁用自适应或统计信息不可用
    private volatile PartitionLoadStats partitionLoadStats = null;
    // 当前使用的粘性分区信息，使用原子引用确保线程安全的更新
    private final AtomicReference<StickyPartitionInfo> stickyPartitionInfo = new AtomicReference<>();


    /**
     * BuiltInPartitioner构造函数
     *
     * @param logContext 日志上下文，用于创建日志记录器
     * @param topic 分区器负责的topic
     * @param stickyBatchSize 粘性批次大小，决定在切换分区前向同一分区发送多少字节的数据
     * @throws IllegalArgumentException 当stickyBatchSize小于1时抛出
     */
    public BuiltInPartitioner(LogContext logContext, String topic, int stickyBatchSize) {
        // 初始化日志记录器
        this.log = logContext.logger(BuiltInPartitioner.class);
        // 设置topic
        this.topic = topic;
        // 验证粘性批次大小的有效性
        if (stickyBatchSize < 1) {
            throw new IllegalArgumentException("stickyBatchSize must be >= 1 but got " + stickyBatchSize);
        }
        this.stickyBatchSize = stickyBatchSize;
    }

    /**
     * 根据分区负载统计信息计算下一个要使用的分区
     * 
     * 分区选择策略：
     * 1. 如果没有负载统计信息，使用均匀分布随机选择分区
     * 2. 如果有负载统计信息，根据分区队列大小的反比作为权重进行加权随机选择
     * 
     * @param cluster 集群信息，用于获取topic的分区信息
     * @return 选择的分区号
     */
    private int nextPartition(Cluster cluster) {
        // 获取一个随机数作为基础值
        int random = randomPartition();

        // 将易变变量缓存到本地变量，提高性能和一致性
        PartitionLoadStats partitionLoadStats = this.partitionLoadStats;
        int partition;

        if (partitionLoadStats == null) {
            // 没有负载统计信息时，使用均匀分布选择分区
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (!availablePartitions.isEmpty()) {
                // 有可用分区时，从可用分区中随机选择
                partition = availablePartitions.get(random % availablePartitions.size()).partition();
            } else {
                // 没有可用分区时，从所有分区中随机选择
                List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
                partition = random % partitions.size();
            }
        } else {
            // 使用负载统计信息进行加权随机选择
            // 注意：没有leader的分区会被排除在统计信息之外
            assert partitionLoadStats.length > 0;

            // 获取累积频率表，用于加权随机选择
            int[] cumulativeFrequencyTable = partitionLoadStats.cumulativeFrequencyTable;
            // 生成一个在累积频率范围内的加权随机数
            int weightedRandom = random % cumulativeFrequencyTable[partitionLoadStats.length - 1];

            // 使用二分查找在累积频率表中找到对应的分区索引
            // 累积频率表是有序的，可以使用二分查找提高效率
            int searchResult = Arrays.binarySearch(cumulativeFrequencyTable, 0, partitionLoadStats.length, weightedRandom);

            // 二分查找返回值说明：
            // 1. 如果找到元素，返回该元素的索引
            // 2. 如果没找到，返回 -(插入点) - 1
            // 插入点是第一个大于目标值的元素的索引
            // 例如，对于累积频率表 [4,5,8]：
            // - 查找3：返回-1(插入点0)，abs(-1+1)=0，选择索引0
            // - 查找4：返回0，abs(0+1)=1，选择索引1
            int partitionIndex = Math.abs(searchResult + 1);
            assert partitionIndex < partitionLoadStats.length;
            // 根据索引获取实际的分区号
            partition = partitionLoadStats.partitionIds[partitionIndex];
        }

        log.trace("Switching to partition {} in topic {}", partition, topic);
        return partition;
    }

    /**
     * 生成一个正整数的随机分区号
     * 
     * @return 正整数的随机值
     */
    int randomPartition() {
        return Utils.toPositive(ThreadLocalRandom.current().nextInt());
    }

    /**
     * 仅用于测试。当分区负载统计信息存在时，返回随机数的范围上限
     * 
     * @return 累积频率表的最大值
     */
    public int loadStatsRangeEnd() {
        assert partitionLoadStats != null;
        assert partitionLoadStats.length > 0;
        return partitionLoadStats.cumulativeFrequencyTable[partitionLoadStats.length - 1];
    }

    /**
     * 获取当前选择的粘性分区信息。该方法与{@link #isPartitionChanged}和{@link #updatePartitionInfo}配合使用。
     * 
     * 工作流程：
     * 1. 调用peekCurrentPartitionInfo获取要锁定的分区
     * 2. 锁定分区的批次队列
     * 3. 在锁定状态下调用isPartitionChanged确保没有并发修改
     * 4. 将数据追加到缓冲区
     * 5. 调用updatePartitionInfo更新已生产字节数并可能切换分区
     * 
     * 重要：步骤3-5必须在分区批次队列的锁定状态下执行
     * 
     * @param cluster 集群信息（当没有当前分区时需要用于选择新分区）
     * @return 粘性分区信息对象
     */
    StickyPartitionInfo peekCurrentPartitionInfo(Cluster cluster) {
        // 尝试获取当前的粘性分区信息
        StickyPartitionInfo partitionInfo = stickyPartitionInfo.get();
        if (partitionInfo != null)
            return partitionInfo;

        // 作为第一个创建粘性分区信息的线程
        partitionInfo = new StickyPartitionInfo(nextPartition(cluster));
        // 使用CAS操作安全地设置粘性分区信息
        if (stickyPartitionInfo.compareAndSet(null, partitionInfo))
            return partitionInfo;

        // 发生了竞争，返回其他线程设置的分区信息
        return stickyPartitionInfo.get();
    }

    /**
     * 检查分区是否被并发线程修改。注意：此函数必须在分区批次队列的锁定状态下调用
     * 
     * @param partitionInfo 由peekCurrentPartitionInfo返回的粘性分区信息对象
     * @return 如果粘性分区对象已被修改（发生竞争条件）则返回true
     */
    boolean isPartitionChanged(StickyPartitionInfo partitionInfo) {
        // 如果调用者没有使用内置分区器，partitionInfo可能为null
        return partitionInfo != null && stickyPartitionInfo.get() != partitionInfo;
    }

    /**
     * 更新分区信息，包括追加的字节数，并可能触发分区切换
     * 注意：此函数必须在分区批次队列的锁定状态下调用
     * 
     * @param partitionInfo 由peekCurrentPartitionInfo返回的粘性分区信息对象
     * @param appendedBytes 追加到此分区的字节数
     * @param cluster 集群信息
     */
    void updatePartitionInfo(StickyPartitionInfo partitionInfo, int appendedBytes, Cluster cluster) {
        updatePartitionInfo(partitionInfo, appendedBytes, cluster, true);
    }

    /**
     * 更新分区信息，包括追加的字节数，并可能触发分区切换
     * 注意：此函数必须在分区批次队列的锁定状态下调用
     * 
     * @param partitionInfo 由peekCurrentPartitionInfo返回的粘性分区信息对象
     * @param appendedBytes 追加到此分区的字节数
     * @param cluster 集群信息
     * @param enableSwitch 如果为true，在生产足够字节数时切换分区
     */
    void updatePartitionInfo(StickyPartitionInfo partitionInfo, int appendedBytes, Cluster cluster, boolean enableSwitch) {
        // 如果调用者没有使用内置分区器，partitionInfo可能为null
        if (partitionInfo == null)
            return;

        // 确保partitionInfo没有被并发修改
        assert partitionInfo == stickyPartitionInfo.get();
        // 原子地增加已生产字节数
        int producedBytes = partitionInfo.producedBytes.addAndGet(appendedBytes);

        if (producedBytes >= stickyBatchSize * 2) {
            log.trace("已生产{}字节，超过批次大小{}字节的两倍，切换开关设置为{}", producedBytes, stickyBatchSize, enableSwitch);
        }

        // 在以下两种情况下切换分区：
        // 1. 已生产字节数达到阈值且允许切换
        // 2. 已生产字节数达到阈值的两倍（强制切换）
        if (producedBytes >= stickyBatchSize && enableSwitch || producedBytes >= stickyBatchSize * 2) {
            // 已向此分区生产足够数据，切换到下一个分区
            StickyPartitionInfo newPartitionInfo = new StickyPartitionInfo(nextPartition(cluster));
            stickyPartitionInfo.set(newPartitionInfo);
        }
    }

    /**
     * 根据每个分区的队列大小更新分区负载统计信息
     * 注意：为避免内存分配，直接在queueSizes数组上进行修改
     *
     * @param queueSizes 队列大小数组，不包含没有leader的分区
     * @param partitionIds 队列对应的分区ID数组，不包含没有leader的分区
     * @param length 数组的逻辑长度（可能小于数组实际长度）：
     *              我们可能会基于延迟排除一些分区，为避免重新分配数组，
     *              只是减少逻辑长度
     * 对测试可见
     */
    /**
     * 更新分区负载统计信息，用于实现自适应分区选择
     * 
     * 该方法通过构建累积频率表(CFT)来实现基于队列大小的加权随机分区选择。
     * 算法的核心思想是：队列越小的分区被选中的概率越大，从而实现负载均衡。
     *
     * @param queueSizes 各分区当前的队列大小数组，不包含没有leader的分区
     * @param partitionIds 与队列大小对应的分区ID数组
     * @param length 数组的有效长度（可能小于数组实际长度，因为某些分区可能因延迟过高被排除）
     */
    public void updatePartitionLoadStats(int[] queueSizes, int[] partitionIds, int length) {
        // 如果没有队列大小信息，禁用自适应分区
        if (queueSizes == null) {
            log.trace("No load stats for topic {}, not using adaptive", topic);
            partitionLoadStats = null;
            return;
        }
        // 验证参数有效性
        assert queueSizes.length == partitionIds.length;
        assert length <= queueSizes.length;

        // 如果可用分区数量太少，不启用自适应分区逻辑
        // 注意：即使只有一个可用分区，如果是因为其他分区被延迟排除，我们仍然保留自适应逻辑
        // 参见RecordAccumulator#partitionReady方法中队列大小的构建过程
        if (length < 1 || queueSizes.length < 2) {
            log.trace("The number of partitions is too small: available={}, all={}, not using adaptive for topic {}",
                    length, queueSizes.length, topic);
            partitionLoadStats = null;
            return;
        }

        // 构建累积频率表(CFT)，实现就地转换以避免额外内存分配
        // 算法步骤：
        // 1. 初始状态：每个元素是分区的队列大小
        // 2. 反转权重：用(最大队列大小+1)减去队列大小，使得队列小的分区获得更大的权重
        // 3. 转换为累积和：每个元素加上前一个元素的值
        // 
        // 示例：假设有3个分区，队列大小分别为[0,3,1]
        // 1) 找到最大队列大小3，加1得到4
        // 2) 反转权重：[4,1,3] (4-0, 4-3, 4-1)
        // 3) 累积求和：[4,5,8]
        // 
        // 使用方式：生成[0,8)范围内的随机数，找到第一个大于该随机数的累积频率值
        // - 随机数0-3会选中分区0（概率4/8）
        // - 随机数4会选中分区1（概率1/8）
        // - 随机数5-7会选中分区2（概率3/8）

        // 计算最大队列大小并检查是否所有队列大小相同
        int maxSizePlus1 = queueSizes[0];
        boolean allEqual = true;
        for (int i = 1; i < length; i++) {
            if (queueSizes[i] != maxSizePlus1)
                allEqual = false;
            if (queueSizes[i] > maxSizePlus1)
                maxSizePlus1 = queueSizes[i];
        }
        ++maxSizePlus1;

        // 如果所有分区队列大小相同且没有分区被排除，不需要复杂的概率计算
        if (allEqual && length == queueSizes.length) {
            log.trace("All queue lengths are the same, not using adaptive for topic {}", topic);
            partitionLoadStats = null;
            return;
        }

        // 构建累积频率表：反转权重并累积求和
        queueSizes[0] = maxSizePlus1 - queueSizes[0];  // 第一个元素只需反转
        for (int i = 1; i < length; i++) {
            // 后续元素：先反转权重，再加上前一个元素的累积和
            queueSizes[i] = maxSizePlus1 - queueSizes[i] + queueSizes[i - 1];
        }
        
        log.trace("Partition load stats for topic {}: CFT={}, IDs={}, length={}",
                topic, queueSizes, partitionIds, length);
        // 更新分区负载统计信息
        partitionLoadStats = new PartitionLoadStats(queueSizes, partitionIds, length);
    }

    /**
     * 当前粘性分区的信息
     * 用于实现粘性分区功能：在一定数据量范围内将消息持续发送到同一个分区
     */
    public static class StickyPartitionInfo {
        // 当前使用的分区索引
        private final int index;
        // 已向该分区发送的字节数，使用原子整数确保线程安全
        private final AtomicInteger producedBytes = new AtomicInteger();

        StickyPartitionInfo(int index) {
            this.index = index;
        }

        /**
         * 获取当前粘性分区的分区号
         * @return 分区号
         */
        public int partition() {
            return index;
        }
    }

    /**
     * 默认的键值分区哈希函数
     * 使用MurmurHash2算法计算消息键的哈希值，并映射到指定范围的分区号
     *
     * @param serializedKey 序列化后的消息键字节数组
     * @param numPartitions 主题的分区数量
     * @return 介于[0, numPartitions-1]之间的分区号
     */
    public static int partitionForKey(final byte[] serializedKey, final int numPartitions) {
        return Utils.toPositive(Utils.murmur2(serializedKey)) % numPartitions;
    }

    /**
     * 主题的分区负载统计信息，用于实现自适应分区分配
     * 通过累积频率表实现基于队列大小的加权随机分区选择
     */
    private static final class PartitionLoadStats {
        // 累积频率表：用于实现加权随机选择的概率分布
        public final int[] cumulativeFrequencyTable;
        // 分区ID数组：与累积频率表中的索引位置一一对应
        public final int[] partitionIds;
        // 有效数据长度：可能小于数组实际长度，因为某些分区可能被排除
        public final int length;

        /**
         * 构造分区负载统计信息对象
         *
         * @param cumulativeFrequencyTable 累积频率表，用于加权随机选择
         * @param partitionIds 分区ID数组，与频率表位置对应
         * @param length 有效数据长度
         */
        public PartitionLoadStats(int[] cumulativeFrequencyTable, int[] partitionIds, int length) {
            assert cumulativeFrequencyTable.length == partitionIds.length;
            assert length <= cumulativeFrequencyTable.length;
            this.cumulativeFrequencyTable = cumulativeFrequencyTable;
            this.partitionIds = partitionIds;
            this.length = length;
        }
    }
}
