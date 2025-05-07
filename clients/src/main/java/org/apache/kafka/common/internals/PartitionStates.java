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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * 这个类是用于执行获取请求(fetch requests)的重要构建块，其中主题分区需要通过轮询(round-robin)方式进行轮转，
 * 以确保在存在获取响应大小限制的情况下实现公平性和一定程度的确定性。由于同一主题的所有分区分组在一起时，
 * 获取请求的序列化效率更高，因此我们在`set`方法中执行这种分组操作。
 *
 * 当分区被移动到末尾时，同一主题可能会重复出现多次。在最优情况下，单个主题会"环绕"并出现两次。然而，由于分区
 * 以不同的顺序获取且分区领导权发生变化，我们可能会偏离最优情况。如果这在实践中成为问题，我们可以通过跟踪每个
 * 节点的分区或经常调用`set`来改进。
 *
 * 注意，除了{@link #size()}方法（返回当前跟踪的分区数量）外，此类不是线程安全的。
 */
public class PartitionStates<S> {

    /**
     * 使用LinkedHashMap存储TopicPartition到状态S的映射
     * 保持插入顺序，用于实现分区的轮转和分组
     */
    private final LinkedHashMap<TopicPartition, S> map = new LinkedHashMap<>();

    /**
     * 分区集合的不可修改视图
     * 对PartitionStates实例的更改会反映在此视图中
     */
    private final Set<TopicPartition> partitionSetView = Collections.unmodifiableSet(map.keySet());

    /**
     * 当前已分配分区的数量
     * 使用volatile确保线程安全的读取
     */
    private volatile int size = 0;

    /**
     * 创建一个空的PartitionStates实例
     */
    public PartitionStates() {}

    /**
     * 将指定的主题分区移动到末尾
     * 如果分区存在，则先移除再添加，保持其状态不变
     * 
     * @param topicPartition 要移动的主题分区
     */
    public void moveToEnd(TopicPartition topicPartition) {
        // 先移除分区及其状态
        S state = map.remove(topicPartition);
        // 如果分区存在，则将其添加到末尾
        if (state != null)
            map.put(topicPartition, state);
    }

    /**
     * 更新指定主题分区的状态并将其移动到末尾
     * 
     * @param topicPartition 要更新的主题分区
     * @param state 新的状态值
     */
    public void updateAndMoveToEnd(TopicPartition topicPartition, S state) {
        // 移除原有的分区
        map.remove(topicPartition);
        // 将分区及其新状态添加到末尾
        map.put(topicPartition, state);
        // 更新分区数量
        updateSize();
    }

    /**
     * 更新指定主题分区的状态
     * 
     * @param topicPartition 要更新的主题分区
     * @param state 新的状态值
     */
    public void update(TopicPartition topicPartition, S state) {
        // 更新或添加分区的状态
        map.put(topicPartition, state);
        // 更新分区数量
        updateSize();
    }

    /**
     * 移除指定的主题分区
     * 
     * @param topicPartition 要移除的主题分区
     */
    public void remove(TopicPartition topicPartition) {
        // 移除分区
        map.remove(topicPartition);
        // 更新分区数量
        updateSize();
    }

    /**
     * 返回分区集合的不可修改视图
     * 对PartitionStates实例的更改会反映在此视图中
     * 
     * @return 分区集合的不可修改视图
     */
    public Set<TopicPartition> partitionSet() {
        return partitionSetView;
    }

    /**
     * 清空所有分区状态
     */
    public void clear() {
        // 清空映射
        map.clear();
        // 更新分区数量
        updateSize();
    }

    /**
     * 检查是否包含指定的主题分区
     * 
     * @param topicPartition 要检查的主题分区
     * @return 如果包含该分区则返回true，否则返回false
     */
    public boolean contains(TopicPartition topicPartition) {
        return map.containsKey(topicPartition);
    }

    /**
     * 返回所有分区状态值的迭代器
     * 
     * @return 状态值迭代器
     */
    public Iterator<S> stateIterator() {
        return map.values().iterator();
    }

    /**
     * 对每个主题分区及其状态执行指定的操作
     * 
     * @param biConsumer 要执行的操作
     */
    public void forEach(BiConsumer<TopicPartition, S> biConsumer) {
        map.forEach(biConsumer);
    }

    /**
     * 返回分区状态映射的不可修改视图
     * 
     * @return 分区到状态的映射视图
     */
    public Map<TopicPartition, S> partitionStateMap() {
        return Collections.unmodifiableMap(map);
    }

    /**
     * 按顺序返回所有分区的状态值列表
     * 
     * @return 状态值列表
     */
    public List<S> partitionStateValues() {
        return new ArrayList<>(map.values());
    }

    /**
     * 获取指定主题分区的状态值
     * 
     * @param topicPartition 主题分区
     * @return 分区的状态值，如果分区不存在则返回null
     */
    public S stateValue(TopicPartition topicPartition) {
        return map.get(topicPartition);
    }

    /**
     * 获取当前正在跟踪的分区数量
     * 此方法是线程安全的
     * 
     * @return 分区数量
     */
    public int size() {
        return size;
    }

    /**
     * 使用提供的映射更新构建器的状态（即清除先前的状态）
     * 构建器会"按主题分批"，例如如果我们有a、b和c三个主题，每个主题有两个分区，
     * 最终可能得到如下顺序（主题内分区的顺序取决于接收到的映射的迭代顺序）：
     * a0, a1, b1, b0, c0, c1
     * 
     * @param partitionToState 新的分区状态映射
     */
    public void set(Map<TopicPartition, S> partitionToState) {
        // 清空当前映射
        map.clear();
        // 使用新的映射更新状态
        update(partitionToState);
        // 更新分区数量
        updateSize();
    }

    /**
     * 更新当前跟踪的分区数量
     * 使用volatile变量确保线程安全
     */
    private void updateSize() {
        size = map.size();
    }

    /**
     * 更新分区状态映射
     * 首先按主题对分区进行分组，然后按主题顺序添加分区
     * 
     * @param partitionToState 要更新的分区状态映射
     */
    private void update(Map<TopicPartition, S> partitionToState) {
        // 创建主题到分区列表的映射，用于分组
        LinkedHashMap<String, List<TopicPartition>> topicToPartitions = new LinkedHashMap<>();
        // 遍历所有分区，按主题进行分组
        for (TopicPartition tp : partitionToState.keySet()) {
            List<TopicPartition> partitions = topicToPartitions.computeIfAbsent(tp.topic(), k -> new ArrayList<>());
            partitions.add(tp);
        }
        // 按主题顺序处理分区
        for (Map.Entry<String, List<TopicPartition>> entry : topicToPartitions.entrySet()) {
            // 将同一主题的所有分区添加到映射中
            for (TopicPartition tp : entry.getValue()) {
                S state = partitionToState.get(tp);
                map.put(tp, state);
            }
        }
    }

    /**
     * PartitionState类表示一个分区的状态信息
     * 这是一个不可变的类，通过泛型参数S支持任意类型的状态值
     * 主要用于在PartitionStates类中存储和管理分区状态
     *
     * @param <S> 状态值的类型参数
     */
    public static class PartitionState<S> {
        /**
         * 分区的标识符，包含主题名称和分区号
         * 使用final修饰确保不可变性
         */
        private final TopicPartition topicPartition;

        /**
         * 分区的状态值，类型由泛型参数S决定
         * 使用final修饰确保不可变性
         */
        private final S value;

        /**
         * 创建一个新的PartitionState实例
         * 使用Objects.requireNonNull确保参数非空
         *
         * @param topicPartition 分区标识符，不能为null
         * @param state 分区状态值，不能为null
         * @throws NullPointerException 如果任一参数为null
         */
        public PartitionState(TopicPartition topicPartition, S state) {
            this.topicPartition = Objects.requireNonNull(topicPartition);
            this.value = Objects.requireNonNull(state);
        }

        /**
         * 获取分区的状态值
         *
         * @return 分区的状态值
         */
        public S value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            PartitionState<?> that = (PartitionState<?>) o;

            return topicPartition.equals(that.topicPartition) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            int result = topicPartition.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }

        /**
         * 获取分区标识符
         *
         * @return 分区的TopicPartition对象
         */
        public TopicPartition topicPartition() {
            return topicPartition;
        }

        @Override
        public String toString() {
            return "PartitionState(" + topicPartition + "=" + value + ')';
        }
    }

}
