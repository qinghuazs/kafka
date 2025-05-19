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

package kafka.server.share;

import kafka.server.MetadataCache;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.persister.ShareCoordinatorMetadataCacheHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.OptionConverters;

/**
 * Kafka共享协调器的元数据缓存辅助实现类
 * 该类主要负责处理共享消费者组相关的元数据操作，包括：
 * 1. 检查特定主题是否存在
 * 2. 获取共享协调器节点信息
 * 3. 获取集群中的活跃节点列表
 * 
 * 在Kafka的共享消费者组管理中，该类扮演着重要角色，它通过访问元数据缓存来提供必要的集群信息，
 * 确保共享消费者组功能的正常运行。
 */
public class ShareCoordinatorMetadataCacheHelperImpl implements ShareCoordinatorMetadataCacheHelper {
    /** 元数据缓存，用于存储和访问Kafka集群的元数据信息 */
    private final MetadataCache metadataCache;
    
    /** 将共享分区键映射到分区号的函数，用于确定特定共享组应该被分配到哪个分区 */
    private final Function<SharePartitionKey, Integer> keyToPartitionMapper;
    
    /** Broker间通信使用的监听器名称，用于在获取节点信息时指定正确的监听器 */
    private final ListenerName interBrokerListenerName;
    
    /** 日志记录器，用于记录运行时的重要信息和异常 */
    private final Logger log = LoggerFactory.getLogger(ShareCoordinatorMetadataCacheHelperImpl.class);

    /**
     * 构造函数
     * @param metadataCache 元数据缓存实例，用于访问集群元数据
     * @param keyToPartitionMapper 将共享分区键映射到分区号的函数
     * @param interBrokerListenerName Broker间通信使用的监听器名称
     * @throws NullPointerException 如果任何参数为null
     */
    public ShareCoordinatorMetadataCacheHelperImpl(
        MetadataCache metadataCache,
        Function<SharePartitionKey, Integer> keyToPartitionMapper,
        ListenerName interBrokerListenerName
    ) {
        // 确保所有必需的参数都不为null
        this.metadataCache = Objects.requireNonNull(metadataCache, "metadataCache must not be null");
        this.keyToPartitionMapper = Objects.requireNonNull(keyToPartitionMapper, "keyToPartitionMapper must not be null");
        this.interBrokerListenerName = Objects.requireNonNull(interBrokerListenerName, "interBrokerListenerName must not be null");
    }

    /**
     * 检查指定的主题是否存在于元数据缓存中
     * 
     * @param topic 要检查的主题名称
     * @return 如果主题存在返回true，否则返回false
     */
    @Override
    public boolean containsTopic(String topic) {
        try {
            // 直接查询元数据缓存，检查主题是否存在
            return metadataCache.contains(topic);
        } catch (Exception e) {
            // 如果查询过程中发生异常，记录警告日志并返回false
            log.warn("Exception checking {} in metadata cache", topic, e);
        }
        return false;
    }

    /**
     * 获取指定共享分区键对应的共享协调器节点
     * 
     * @param key 共享分区键，包含组ID和其他标识信息
     * @param internalTopicName 内部主题名称
     * @return 如果找到对应的协调器节点则返回该节点，否则返回Node.noNode()
     */
    @Override
    public Node getShareCoordinator(SharePartitionKey key, String internalTopicName) {
        try {
            // 首先检查内部主题是否存在
            if (metadataCache.contains(internalTopicName)) {
                // 创建只包含目标主题的集合
                Set<String> topicSet = new HashSet<>();
                topicSet.add(internalTopicName);

                // 获取主题的元数据信息
                // 将Java集合转换为Scala集合，然后获取主题元数据，最后再转回Java集合
                List<MetadataResponseData.MetadataResponseTopic> topicMetadata = CollectionConverters.asJava(
                    metadataCache.getTopicMetadata(
                        CollectionConverters.asScala(topicSet),
                        interBrokerListenerName,
                        false,  // 不包含已授权的操作
                        false   // 不包含群集授权的操作
                    )
                );

                // 检查元数据响应的有效性
                if (topicMetadata == null || topicMetadata.isEmpty() || topicMetadata.get(0).errorCode() != Errors.NONE.code()) {
                    return Node.noNode();
                } else {
                    // 使用映射函数计算目标分区号
                    int partition = keyToPartitionMapper.apply(key);
                    // 在主题的所有分区中查找目标分区，并确保该分区有可用的leader
                    Optional<MetadataResponseData.MetadataResponsePartition> response = topicMetadata.get(0).partitions().stream()
                        .filter(responsePart -> responsePart.partitionIndex() == partition
                            && responsePart.leaderId() != MetadataResponse.NO_LEADER_ID)
                        .findFirst();

                    if (response.isPresent()) {
                        // 如果找到目标分区，获取其leader节点信息
                        // 将Scala的Option转换为Java的Optional，如果节点不存在则返回Node.noNode()
                        return OptionConverters.toJava(metadataCache.getAliveBrokerNode(response.get().leaderId(), interBrokerListenerName))
                            .orElse(Node.noNode());
                    } else {
                        // 如果没有找到目标分区，返回空节点
                        return Node.noNode();
                    }
                }
            }
        } catch (Exception e) {
            // 如果在获取过程中发生任何异常，记录警告日志
            log.warn("Exception while getting share coordinator", e);
        }
        return Node.noNode();
    }

    /**
     * 获取集群中所有活跃的broker节点列表
     * 
     * @return 活跃broker节点列表，如果发生异常则返回空列表
     */
    @Override
    public List<Node> getClusterNodes() {
        try {
            // 获取所有活跃的broker节点
            // 将Scala的序列转换为Java的List
            return CollectionConverters.asJava(metadataCache.getAliveBrokerNodes(interBrokerListenerName).toSeq());
        } catch (Exception e) {
            // 如果获取过程中发生异常，记录警告日志
            log.warn("Exception while getting cluster nodes", e);
        }
        // 发生异常时返回空列表
        return List.of();
    }
}
