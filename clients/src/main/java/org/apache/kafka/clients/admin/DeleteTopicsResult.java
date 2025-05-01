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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Map;

/**
 * {@link Admin#deleteTopics(Collection)} 调用的结果类。
 * 该类用于处理异步删除主题操作的结果，支持通过主题ID和主题名称两种方式进行删除。
 * 
 * 该类的API仍在演进中，详见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class DeleteTopicsResult {
    /**
     * 存储基于主题ID的删除操作的Future结果映射。
     * Key为主题ID（Uuid），Value为代表删除操作完成状态的Future。
     * Future的Void返回类型表示操作成功时不返回具体值，仅表示完成状态。
     */
    private final Map<Uuid, KafkaFuture<Void>> topicIdFutures;

    /**
     * 存储基于主题名称的删除操作的Future结果映射。
     * Key为主题名称（String），Value为代表删除操作完成状态的Future。
     * Future的Void返回类型表示操作成功时不返回具体值，仅表示完成状态。
     */
    private final Map<String, KafkaFuture<Void>> nameFutures;

    /**
     * 构造函数，用于创建DeleteTopicsResult实例。
     * 注意：topicIdFutures和nameFutures不能同时为null或同时有值，必须且只能指定其中一个。
     *
     * @param topicIdFutures 基于主题ID的删除操作Future映射
     * @param nameFutures 基于主题名称的删除操作Future映射
     * @throws IllegalArgumentException 当两个参数同时为null或同时不为null时抛出
     */
    protected DeleteTopicsResult(Map<Uuid, KafkaFuture<Void>> topicIdFutures, Map<String, KafkaFuture<Void>> nameFutures) {
        // 确保两个参数不能同时存在值
        if (topicIdFutures != null && nameFutures != null)
            throw new IllegalArgumentException("topicIdFutures and nameFutures cannot both be specified.");
        // 确保两个参数不能同时为null
        if (topicIdFutures == null && nameFutures == null)
            throw new IllegalArgumentException("topicIdFutures and nameFutures cannot both be null.");
        this.topicIdFutures = topicIdFutures;
        this.nameFutures = nameFutures;
    }

    /**
     * 创建基于主题ID的DeleteTopicsResult实例的工厂方法。
     *
     * @param topicIdFutures 主题ID到删除操作Future的映射
     * @return 新的DeleteTopicsResult实例
     */
    static DeleteTopicsResult ofTopicIds(Map<Uuid, KafkaFuture<Void>> topicIdFutures) {
        return new DeleteTopicsResult(topicIdFutures, null);
    }

    /**
     * 创建基于主题名称的DeleteTopicsResult实例的工厂方法。
     *
     * @param nameFutures 主题名称到删除操作Future的映射
     * @return 新的DeleteTopicsResult实例
     */
    static DeleteTopicsResult ofTopicNames(Map<String, KafkaFuture<Void>> nameFutures) {
        return new DeleteTopicsResult(null, nameFutures);
    }

    /**
     * 当使用 {@link Admin#deleteTopics(TopicCollection, DeleteTopicsOptions)} 时传入TopicIdCollection时使用此方法。
     * 
     * @return 返回主题ID到对应删除操作Future的映射，用于检查各个主题删除操作的状态。
     *         如果删除请求使用的是主题名称而不是ID，则返回null。
     */
    public Map<Uuid, KafkaFuture<Void>> topicIdValues() {
        return topicIdFutures;
    }

    /**
     * 当使用 {@link Admin#deleteTopics(TopicCollection, DeleteTopicsOptions)} 时传入TopicNameCollection时使用此方法。
     * 
     * @return 返回主题名称到对应删除操作Future的映射，用于检查各个主题删除操作的状态。
     *         如果删除请求使用的是主题ID而不是名称，则返回null。
     */
    public Map<String, KafkaFuture<Void>> topicNameValues() {
        return nameFutures;
    }

    /**
     * 获取一个代表所有主题删除操作的组合Future。
     * 
     * @return 返回一个Future，只有当所有主题删除操作都成功时，该Future才会成功完成。
     *         如果任何一个主题删除失败，该Future将会失败。
     *         实现上根据使用的是主题ID还是名称来选择对应的Future集合进行组合。
     */
    public KafkaFuture<Void> all() {
        // 如果topicIdFutures为null，说明使用的是主题名称方式，否则使用主题ID方式
        // 使用KafkaFuture.allOf组合所有Future，只有全部成功才返回成功
        return (topicIdFutures == null) ? KafkaFuture.allOf(nameFutures.values().toArray(new KafkaFuture[0])) :
            KafkaFuture.allOf(topicIdFutures.values().toArray(new KafkaFuture[0]));
    }
}
