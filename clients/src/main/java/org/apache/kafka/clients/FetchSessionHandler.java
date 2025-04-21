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

package org.apache.kafka.clients;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest.PartitionData;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.requests.FetchMetadata.INVALID_SESSION_ID;

/**
 * FetchSessionHandler维护与broker连接的fetch会话状态。
 *
 * 基于KIP-227协议，客户端可以创建增量fetch会话。
 * 这些会话允许客户端反复获取一组分区的信息，而无需在每个请求和响应中
 * 显式枚举所有分区。
 *
 * FetchSessionHandler跟踪会话中的分区。它还决定每个fetch请求中需要包含
 * 哪些分区，以及每个请求应附加什么fetch会话元数据。在broker接收端对应的
 * 类是FetchManager。
 */
public class FetchSessionHandler {
    // 用于日志记录的Logger实例
    private final Logger log;

    // broker节点ID
    private final int node;

    /**
     * 下一个fetch请求的元数据
     * 包含会话ID和epoch等信息
     */
    private FetchMetadata nextMetadata = FetchMetadata.INITIAL;

    /**
     * 创建一个新的FetchSessionHandler实例
     * @param logContext 日志上下文
     * @param node broker节点ID
     */
    public FetchSessionHandler(LogContext logContext, int node) {
        this.log = logContext.logger(FetchSessionHandler.class);
        this.node = node;
    }

    // 用于测试,返回当前会话ID
    public int sessionId() {
        return nextMetadata.sessionId();
    }

    /**
     * 存储fetch请求会话中的所有分区
     * 使用LinkedHashMap保持分区的插入顺序
     */
    private LinkedHashMap<TopicPartition, PartitionData> sessionPartitions =
        new LinkedHashMap<>(0);

    /**
     * 存储fetch请求会话中所有主题的ID到名称的映射
     * 用于支持基于主题ID的请求
     */
    private Map<Uuid, String> sessionTopicNames = new HashMap<>(0);

    /**
     * 获取会话中的主题ID到名称的映射
     */
    public Map<Uuid, String> sessionTopicNames() {
        return sessionTopicNames;
    }

    /**
     * FetchRequestData封装了一个fetch请求所需的所有数据
     * 包括要发送的分区、要忘记的分区、会话元数据等
     */
    public static class FetchRequestData {
        /**
         * 本次fetch请求需要发送的分区列表
         * 对于增量请求,这只包含有变化的分区
         */
        private final Map<TopicPartition, PartitionData> toSend;

        /**
         * 需要从会话中移除的分区列表
         * 这些分区将不再被跟踪
         */
        private final List<TopicIdPartition> toForget;

        /**
         * 版本>=13时需要替换的分区列表
         * 这些分区的主题ID发生了变化
         */
        private final List<TopicIdPartition> toReplace;

        /**
         * fetch请求会话中的所有分区
         * 包括新增的、保持不变的和将被移除的分区
         */
        private final Map<TopicPartition, PartitionData> sessionPartitions;

        /**
         * 本次fetch请求使用的元数据
         * 包含会话ID、是否增量请求等信息
         */
        private final FetchMetadata metadata;

        /**
         * 标识是否所有主题都有主题ID
         * 如果为true,则可以使用基于主题ID的请求
         */
        private final boolean canUseTopicIds;

        /**
         * 创建一个新的FetchRequestData实例
         * 
         * @param toSend 本次fetch请求需要发送的分区列表,对于增量请求只包含变化的分区
         * @param toForget 需要从会话中移除的分区列表,这些分区将不再被跟踪
         * @param toReplace 需要替换的分区列表(版本>=13),这些分区的主题ID发生了变化
         * @param sessionPartitions fetch会话中的所有分区,包括新增、不变和将被移除的分区
         * @param metadata 本次fetch请求的元数据,包含会话ID和是否增量请求等信息
         * @param canUseTopicIds 是否可以使用基于主题ID的请求,为true时表示所有主题都有ID
         */
        FetchRequestData(Map<TopicPartition, PartitionData> toSend,
                         List<TopicIdPartition> toForget,
                         List<TopicIdPartition> toReplace,
                         Map<TopicPartition, PartitionData> sessionPartitions,
                         FetchMetadata metadata,
                         boolean canUseTopicIds) {
            this.toSend = toSend;
            this.toForget = toForget;
            this.toReplace = toReplace;
            this.sessionPartitions = sessionPartitions;
            this.metadata = metadata;
            this.canUseTopicIds = canUseTopicIds;
        }

        /**
         * 获取本次fetch请求需要发送的分区列表
         * 对于增量请求,这个列表只包含相比上次请求有变化的分区
         * 对于完整请求,这个列表包含所有需要获取数据的分区
         * 
         * @return 需要在本次请求中发送的分区到数据的映射
         */
        public Map<TopicPartition, PartitionData> toSend() {
            return toSend;
        }

        /**
         * 获取需要从fetch会话中移除的分区列表
         * 这些分区在后续的fetch请求中将不再被跟踪
         * 
         * @return 需要从会话中移除的分区列表
         */
        public List<TopicIdPartition> toForget() {
            return toForget;
        }

        /**
         * 获取需要替换的分区列表
         * 这些分区的主题ID发生了变化,需要在版本>=13的请求中进行替换
         * 
         * @return 需要替换的分区列表
         */
        public List<TopicIdPartition> toReplace() {
            return toReplace;
        }

        /**
         * 获取fetch会话中的所有分区
         * 包括本次新增的、保持不变的和将被移除的所有分区
         * 
         * @return 会话中所有分区到数据的映射
         */
        public Map<TopicPartition, PartitionData> sessionPartitions() {
            return sessionPartitions;
        }

        /**
         * 获取本次fetch请求的元数据
         * 包含会话ID、epoch和是否为增量请求等信息
         * 
         * @return fetch请求的元数据
         */
        public FetchMetadata metadata() {
            return metadata;
        }

        /**
         * 检查是否可以使用基于主题ID的请求
         * 只有当所有主题都有主题ID时才返回true
         * 
         * @return 如果可以使用主题ID则返回true,否则返回false
         */
        public boolean canUseTopicIds() {
            return canUseTopicIds;
        }

        @Override
        public String toString() {
            StringBuilder bld;
            if (metadata.isFull()) {
                bld = new StringBuilder("FullFetchRequest(toSend=(");
                String prefix = "";
                for (TopicPartition partition : toSend.keySet()) {
                    bld.append(prefix);
                    bld.append(partition);
                    prefix = ", ";
                }
            } else {
                bld = new StringBuilder("IncrementalFetchRequest(toSend=(");
                String prefix = "";
                for (TopicPartition partition : toSend.keySet()) {
                    bld.append(prefix);
                    bld.append(partition);
                    prefix = ", ";
                }
                bld.append("), toForget=(");
                prefix = "";
                for (TopicIdPartition partition : toForget) {
                    bld.append(prefix);
                    bld.append(partition);
                    prefix = ", ";
                }
                bld.append("), toReplace=(");
                prefix = "";
                for (TopicIdPartition partition : toReplace) {
                    bld.append(prefix);
                    bld.append(partition);
                    prefix = ", ";
                }
                bld.append("), implied=(");
                prefix = "";
                for (TopicPartition partition : sessionPartitions.keySet()) {
                    if (!toSend.containsKey(partition)) {
                        bld.append(prefix);
                        bld.append(partition);
                        prefix = ", ";
                    }
                }
            }
            if (canUseTopicIds) {
                bld.append("), canUseTopicIds=True");
            } else {
                bld.append("), canUseTopicIds=False");
            }
            bld.append(")");
            return bld.toString();
        }
    }

    public class Builder {
        /**
         * 存储主题ID到主题名称的映射关系
         * 用于支持基于主题ID的请求功能
         */
        private final Map<Uuid, String> topicNames;

        /**
         * 标识是否需要复制会话分区数据
         * true表示需要创建会话分区的深拷贝
         * false表示直接使用原始会话分区数据
         */
        private final boolean copySessionPartitions;

        /**
         * 下一个fetch请求中需要获取数据的分区列表
         * 
         * 使用LinkedHashMap而不是普通Map来维护插入顺序非常重要:
         * 1. 对于全量fetch请求,如果响应空间不足以返回所有分区数据,
         *    服务器将优先返回列表前面的分区数据
         * 2. 可以利用列表顺序来优化增量fetch请求的准备工作
         */
        private LinkedHashMap<TopicPartition, PartitionData> next;

        /**
         * 没有主题ID的分区数量
         * 用于判断是否可以使用基于主题ID的请求
         */
        private int partitionsWithoutTopicIds = 0;

        /**
         * 创建一个默认的Builder实例
         * 初始化空的分区列表和主题名称映射
         * 默认需要复制会话分区数据
         */
        Builder() {
            this.next = new LinkedHashMap<>();
            this.topicNames = new HashMap<>();
            this.copySessionPartitions = true;
        }

        /**
         * 创建一个指定初始容量的Builder实例
         * 
         * @param initialSize 分区列表的初始容量
         * @param copySessionPartitions 是否需要复制会话分区数据
         */
        Builder(int initialSize, boolean copySessionPartitions) {
            this.next = new LinkedHashMap<>(initialSize);
            this.topicNames = new HashMap<>();
            this.copySessionPartitions = copySessionPartitions;
        }

        /**
         * 添加需要在下一个fetch请求中获取数据的分区
         * 
         * @param topicPartition 主题分区
         * @param data 分区数据
         */
        public void add(TopicPartition topicPartition, PartitionData data) {
            next.put(topicPartition, data);
            // 主题ID在添加分区和构建请求之间不应该改变,所以可以使用putIfAbsent
            if (data.topicId.equals(Uuid.ZERO_UUID)) {
                partitionsWithoutTopicIds++;
            } else {
                topicNames.putIfAbsent(data.topicId, topicPartition.topic());
            }
        }

        /**
         * 构建FetchRequestData实例
         * 根据nextMetadata.isFull()的值决定构建全量请求还是增量请求
         * 
         * @return 封装了fetch请求数据的FetchRequestData实例
         */
        public FetchRequestData build() {
            // 检查是否所有分区都有主题ID
            boolean canUseTopicIds = partitionsWithoutTopicIds == 0;

            // 处理全量fetch请求
            if (nextMetadata.isFull()) {
                if (log.isDebugEnabled()) {
                    log.debug("Built full fetch {} for node {} with {}.",
                            nextMetadata, node, topicPartitionsToLogString(next.keySet()));
                }
                // 更新会话分区
                sessionPartitions = next;
                next = null;
                // 只有在使用主题ID时才添加主题ID到会话
                if (canUseTopicIds) {
                    sessionTopicNames = topicNames;
                } else {
                    sessionTopicNames = Collections.emptyMap();
                }
                Map<TopicPartition, PartitionData> toSend =
                        Collections.unmodifiableMap(new LinkedHashMap<>(sessionPartitions));
                return new FetchRequestData(toSend, Collections.emptyList(), Collections.emptyList(), toSend, nextMetadata, canUseTopicIds);
            }

            // 处理增量fetch请求
            // 创建用于跟踪分区变化的列表
            List<TopicIdPartition> added = new ArrayList<>();     // 新增的分区
            List<TopicIdPartition> removed = new ArrayList<>();   // 移除的分区
            List<TopicIdPartition> altered = new ArrayList<>();   // 修改的分区
            List<TopicIdPartition> replaced = new ArrayList<>();  // 替换的分区(主题ID变化)

            // 遍历当前会话中的所有分区,检查它们在下一个请求中的状态
            for (Iterator<Entry<TopicPartition, PartitionData>> iter =
                 sessionPartitions.entrySet().iterator(); iter.hasNext(); ) {
                Entry<TopicPartition, PartitionData> entry = iter.next();
                TopicPartition topicPartition = entry.getKey();
                PartitionData prevData = entry.getValue();
                PartitionData nextData = next.remove(topicPartition);

                if (nextData != null) {
                    // 检查分区的主题ID是否发生变化
                    if (!prevData.topicId.equals(nextData.topicId)
                            && !prevData.topicId.equals(Uuid.ZERO_UUID)
                            && !nextData.topicId.equals(Uuid.ZERO_UUID)) {
                        // 主题ID变化,将分区添加到replaced列表
                        next.put(topicPartition, nextData);
                        entry.setValue(nextData);
                        replaced.add(new TopicIdPartition(prevData.topicId, topicPartition));
                    } else if (!prevData.equals(nextData)) {
                        // 分区数据变化,将分区添加到altered列表
                        next.put(topicPartition, nextData);
                        entry.setValue(nextData);
                        altered.add(new TopicIdPartition(nextData.topicId, topicPartition));
                    }
                } else {
                    // 分区在下一个请求中不存在,从会话中移除
                    iter.remove();
                    removed.add(new TopicIdPartition(prevData.topicId, topicPartition));
                    // 如果移除的分区没有主题ID,则不能使用主题ID特性
                    if (canUseTopicIds && prevData.topicId.equals(Uuid.ZERO_UUID))
                        canUseTopicIds = false;
                }
            }

            // 添加新的分区到会话
            for (Entry<TopicPartition, PartitionData> entry : next.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                PartitionData nextData = entry.getValue();
                if (sessionPartitions.containsKey(topicPartition)) {
                    // 在前面的循环中,所有同时存在于sessionPartitions和next中的分区
                    // 都被移到了next的末尾或从next中移除
                    // 因此,一旦遇到这样的分区,就说明没有更多未处理的分区了
                    break;
                }
                sessionPartitions.put(topicPartition, nextData);
                added.add(new TopicIdPartition(nextData.topicId, topicPartition));
            }

            // 根据是否可以使用主题ID来更新会话的主题名称映射
            // 如果主题ID不一致或者切换了主题ID的使用状态,这些错误将在接收broker中处理
            if (canUseTopicIds) {
                sessionTopicNames = topicNames;
            } else {
                sessionTopicNames = Collections.emptyMap();
            }

            // 记录调试日志
            if (log.isDebugEnabled()) {
                log.debug("Built incremental fetch {} for node {}. Added {}, altered {}, removed {}, " +
                          "replaced {} out of {}", nextMetadata, node, topicIdPartitionsToLogString(added),
                          topicIdPartitionsToLogString(altered), topicIdPartitionsToLogString(removed),
                          topicIdPartitionsToLogString(replaced), topicPartitionsToLogString(sessionPartitions.keySet()));
            }

            // 准备返回数据
            Map<TopicPartition, PartitionData> toSend = Collections.unmodifiableMap(next);
            // 根据copySessionPartitions决定是否创建会话分区的深拷贝
            Map<TopicPartition, PartitionData> curSessionPartitions = copySessionPartitions
                    ? Collections.unmodifiableMap(new LinkedHashMap<>(sessionPartitions))
                    : Collections.unmodifiableMap(sessionPartitions);
            next = null;

            // 创建并返回FetchRequestData实例
            return new FetchRequestData(toSend,
                    Collections.unmodifiableList(removed),
                    Collections.unmodifiableList(replaced),
                    curSessionPartitions,
                    nextMetadata,
                    canUseTopicIds);
        }
    }

    /**
     * 创建一个默认的Builder实例
     * 用于构建fetch请求数据
     * 
     * @return 新的Builder实例,使用默认配置
     */
    public Builder newBuilder() {
        return new Builder();
    }

    /**
     * 创建一个可以预设大小的Builder实例
     * 这个Builder主要用于副本获取器(Replica Fetcher)
     * 它允许预先设置PartitionData hashmap的大小,并且可以选择是否复制会话分区数据
     * 
     * @param size 分区数据hashmap的初始大小
     * @param copySessionPartitions 是否需要对会话分区进行深拷贝
     * @return 新的Builder实例,使用指定的配置
     */
    public Builder newBuilder(int size, boolean copySessionPartitions) {
        return new Builder(size, copySessionPartitions);
    }

    /**
     * 将主题分区集合转换为日志字符串
     * 如果启用了跟踪日志级别,则返回详细的分区列表
     * 否则只返回分区数量的统计信息
     * 
     * @param partitions 需要转换为字符串的主题分区集合
     * @return 格式化后的日志字符串
     */
    private String topicPartitionsToLogString(Collection<TopicPartition> partitions) {
        if (!log.isTraceEnabled()) {
            return String.format("%d partition(s)", partitions.size());
        }
        return "(" + partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", ")) + ")";
    }

    /**
     * 将主题ID分区集合转换为日志字符串
     * 如果启用了跟踪日志级别,则返回详细的分区列表
     * 否则只返回分区数量的统计信息
     * 
     * @param partitions 需要转换为字符串的主题ID分区集合
     * @return 格式化后的日志字符串
     */
    private String topicIdPartitionsToLogString(Collection<TopicIdPartition> partitions) {
        if (!log.isTraceEnabled()) {
            return String.format("%d partition(s)", partitions.size());
        }
        return "(" + partitions.stream().map(TopicIdPartition::toString).collect(Collectors.joining(", ")) + ")";
    }

    /**
     * 查找在目标集合中缺失的元素
     * 遍历源集合中的每个元素,检查它是否存在于目标集合中
     * 如果不存在,则将其添加到结果集合中
     *
     * @param toFind 源集合,包含需要查找的元素
     * @param toSearch 目标集合,在这个集合中查找元素
     * @return 包含所有在目标集合中缺失的元素的集合
     */
    static <T> Set<T> findMissing(Set<T> toFind, Set<T> toSearch) {
        Set<T> ret = new LinkedHashSet<>();
        for (T toFindItem: toFind) {
            if (!toSearch.contains(toFindItem)) {
                ret.add(toFindItem);
            }
        }
        return ret;
    }

    /**
     * 验证完整fetch响应中是否包含了会话中的所有分区
     * 检查响应中的分区列表是否与会话中的分区列表匹配
     * 同时也验证主题ID的一致性(如果版本>=13)
     *
     * @param topicPartitions 从FetchResponse中获取的主题分区集合
     * @param ids 从FetchResponse中获取的主题ID集合
     * @param version FetchResponse的版本号
     * @return 如果验证通过返回null,否则返回描述问题的字符串
     */
    String verifyFullFetchResponsePartitions(Set<TopicPartition> topicPartitions, Set<Uuid> ids, short version) {
        StringBuilder bld = new StringBuilder();
        // 查找响应中多出的分区
        Set<TopicPartition> extra =
            findMissing(topicPartitions, sessionPartitions.keySet());
        // 查找响应中缺失的分区
        Set<TopicPartition> omitted =
            findMissing(sessionPartitions.keySet(), topicPartitions);
        // 查找响应中多出的主题ID
        Set<Uuid> extraIds = new HashSet<>();
        if (version >= 13) {
            extraIds = findMissing(ids, sessionTopicNames.keySet());
        }
        // 构建错误信息
        if (!omitted.isEmpty()) {
            bld.append("omittedPartitions=(").append(omitted.stream().map(TopicPartition::toString).collect(Collectors.joining(", "))).append("), ");
        }
        if (!extra.isEmpty()) {
            bld.append("extraPartitions=(").append(extra.stream().map(TopicPartition::toString).collect(Collectors.joining(", "))).append("), ");
        }
        if (!extraIds.isEmpty()) {
            bld.append("extraIds=(").append(extraIds.stream().map(Uuid::toString).collect(Collectors.joining(", "))).append("), ");
        }
        if ((!omitted.isEmpty()) || (!extra.isEmpty()) || (!extraIds.isEmpty())) {
            bld.append("response=(").append(topicPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", "))).append(")");
            return bld.toString();
        }
        return null;
    }

    /**
     * 验证增量fetch响应中的分区是否都包含在会话中
     * 检查响应中是否有会话中不存在的分区
     * 同时也验证主题ID的一致性(如果版本>=13)
     *
     * @param topicPartitions 从FetchResponse中获取的主题分区集合
     * @param ids 从FetchResponse中获取的主题ID集合
     * @param version FetchResponse的版本号
     * @return 如果验证通过返回null,否则返回描述问题的字符串
     */
    String verifyIncrementalFetchResponsePartitions(Set<TopicPartition> topicPartitions, Set<Uuid> ids, short version) {
        // 查找响应中多出的主题ID
        Set<Uuid> extraIds = new HashSet<>();
        if (version >= 13) {
            extraIds = findMissing(ids, sessionTopicNames.keySet());
        }
        // 查找响应中多出的分区
        Set<TopicPartition> extra =
            findMissing(topicPartitions, sessionPartitions.keySet());
        // 构建错误信息
        StringBuilder bld = new StringBuilder();
        if (!extra.isEmpty())
            bld.append("extraPartitions=(").append(extra.stream().map(TopicPartition::toString).collect(Collectors.joining(", "))).append("), ");
        if (!extraIds.isEmpty())
            bld.append("extraIds=(").append(extraIds.stream().map(Uuid::toString).collect(Collectors.joining(", "))).append("), ");
        if ((!extra.isEmpty()) || (!extraIds.isEmpty())) {
            bld.append("response=(").append(topicPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", "))).append(")");
            return bld.toString();
        }
        return null;
    }

    /**
     * 创建一个描述FetchResponse中分区信息的日志字符串
     * 该方法根据日志级别生成不同详细程度的日志信息
     *
     * @param topicPartitions FetchResponse中包含的主题分区集合
     * @return 格式化的日志字符串
     */
    private String responseDataToLogString(Set<TopicPartition> topicPartitions) {
        // 如果不是TRACE级别日志,只返回简单的统计信息
        if (!log.isTraceEnabled()) {
            // 计算隐含分区数(会话中存在但响应中未包含的分区)
            int implied = sessionPartitions.size() - topicPartitions.size();
            if (implied > 0) {
                // 如果有隐含分区,返回响应分区数和隐含分区数
                return String.format(" with %d response partition(s), %d implied partition(s)",
                    topicPartitions.size(), implied);
            } else {
                // 如果没有隐含分区,只返回响应分区数
                return String.format(" with %d response partition(s)",
                    topicPartitions.size());
            }
        }

        // TRACE级别日志,生成详细的分区信息
        StringBuilder bld = new StringBuilder();
        // 添加响应中包含的分区列表
        bld.append(" with response=(").
            append(topicPartitions.stream().map(TopicPartition::toString).collect(Collectors.joining(", "))).
            append(")");

        // 添加隐含分区列表(会话中存在但响应中未包含的分区)
        String prefix = ", implied=(";
        String suffix = "";
        for (TopicPartition partition : sessionPartitions.keySet()) {
            if (!topicPartitions.contains(partition)) {
                bld.append(prefix);
                bld.append(partition);
                prefix = ", ";
                suffix = ")";
            }
        }
        bld.append(suffix);
        return bld.toString();
    }

    /**
     * 处理从服务器返回的fetch响应
     * 该方法负责验证响应的有效性,维护fetch会话状态,并处理各种错误情况
     *
     * @param response fetch响应对象
     * @param version 请求的版本号
     * @return 如果响应格式正确且可以处理则返回true,否则返回false
     */
    public boolean handleResponse(FetchResponse response, short version) {
        // 检查响应是否包含错误
        if (response.error() != Errors.NONE) {
            // 记录错误信息
            log.info("Node {} was unable to process the fetch request with {}: {}.",
                node, nextMetadata, response.error());
            // 根据错误类型更新会话状态
            if (response.error() == Errors.FETCH_SESSION_ID_NOT_FOUND) {
                // 如果会话不存在,重置为初始状态
                nextMetadata = FetchMetadata.INITIAL;
            } else {
                // 其他错误,关闭现有会话并尝试创建新会话
                nextMetadata = nextMetadata.nextCloseExistingAttemptNew();
            }
            return false;
        }

        // 获取响应中包含的主题分区集合
        Set<TopicPartition> topicPartitions = response.responseData(sessionTopicNames, version).keySet();

        // 处理全量fetch响应
        if (nextMetadata.isFull()) {
            // 处理空响应的特殊情况(KIP-219限流机制)
            if (topicPartitions.isEmpty() && response.throttleTimeMs() > 0) {
                // 通常情况下,空的全量响应是无效的
                // 但是根据KIP-219规范,如果broker想要限流客户端,
                // 它会返回一个空响应并设置throttleTimeMs值
                // 这种情况下不需要记录警告,因为这不是错误
                // 但是空的全量响应仍然无法处理,所以返回false
                if (log.isDebugEnabled()) {
                    log.debug("Node {} sent a empty full fetch response to indicate that this " +
                        "client should be throttled for {} ms.", node, response.throttleTimeMs());
                }
                nextMetadata = FetchMetadata.INITIAL;
                return false;
            }

            // 验证全量响应中的分区信息
            String problem = verifyFullFetchResponsePartitions(topicPartitions, response.topicIds(), version);
            if (problem != null) {
                // 响应格式无效
                log.info("Node {} sent an invalid full fetch response with {}", node, problem);
                nextMetadata = FetchMetadata.INITIAL;
                return false;
            } else if (response.sessionId() == INVALID_SESSION_ID) {
                // 服务器不支持或不想创建增量fetch会话
                if (log.isDebugEnabled())
                    log.debug("Node {} sent a full fetch response{}", node, responseDataToLogString(topicPartitions));
                nextMetadata = FetchMetadata.INITIAL;
                return true;
            } else {
                // 服务器创建了新的增量fetch会话
                if (log.isDebugEnabled())
                    log.debug("Node {} sent a full fetch response that created a new incremental " +
                            "fetch session {}{}", node, response.sessionId(), responseDataToLogString(topicPartitions));
                nextMetadata = FetchMetadata.newIncremental(response.sessionId());
                return true;
            }
        } else {
            // 处理增量fetch响应
            // 验证增量响应中的分区信息
            String problem = verifyIncrementalFetchResponsePartitions(topicPartitions, response.topicIds(), version);
            if (problem != null) {
                // 响应格式无效
                log.info("Node {} sent an invalid incremental fetch response with {}", node, problem);
                nextMetadata = nextMetadata.nextCloseExistingAttemptNew();
                return false;
            } else if (response.sessionId() == INVALID_SESSION_ID) {
                // 服务器关闭了增量fetch会话
                if (log.isDebugEnabled())
                    log.debug("Node {} sent an incremental fetch response closing session {}{}",
                            node, nextMetadata.sessionId(), responseDataToLogString(topicPartitions));
                nextMetadata = FetchMetadata.INITIAL;
                return true;
            } else {
                // 服务器继续使用现有的增量fetch会话
                // 这里不需要为KIP-219做特殊处理,因为空的增量fetch请求是完全有效的
                if (log.isDebugEnabled())
                    log.debug("Node {} sent an incremental fetch response with throttleTimeMs = {} " +
                        "for session {}{}", node, response.throttleTimeMs(), response.sessionId(),
                        responseDataToLogString(topicPartitions));
                nextMetadata = nextMetadata.nextIncremental();
                return true;
            }
        }
    }

    /**
     * 通知客户端在下一个fetch请求中关闭现有会话
     * 这个方法通常在客户端主动需要关闭会话时调用
     */
    public void notifyClose() {
        // 记录调试日志
        if (log.isDebugEnabled()) {
            log.debug("Set the metadata for next fetch request to close the existing session ID={}", nextMetadata.sessionId());
        }
        // 更新元数据状态,准备在下一个请求中关闭会话
        nextMetadata = nextMetadata.nextCloseExisting();
    }

    /**
     * 处理发送fetch请求时遇到的错误
     * 当发生网络错误时,会关闭现有会话并在下一次请求中尝试创建新会话
     *
     * @param t 发生的异常
     */
    public void handleError(Throwable t) {
        // 记录错误信息
        log.info("Error sending fetch request {} to node {}:", nextMetadata, node, t);
        // 更新元数据状态,准备关闭现有会话并尝试创建新会话
        nextMetadata = nextMetadata.nextCloseExistingAttemptNew();
    }

    /**
     * 获取当前fetch会话中的所有主题分区
     *
     * @return 会话中跟踪的所有主题分区的集合
     */
    public Set<TopicPartition> sessionTopicPartitions() {
        // 返回会话中所有分区的集合视图
        return sessionPartitions.keySet();
    }
}
