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
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * 该类表示调用{@link Admin#listTopics()}方法的结果。
 * 用于异步获取Kafka集群中的主题列表信息。
 * <p>
 * 该类的API仍在演进中，详细信息请参见{@link Admin}。
 * <p>
 * 应用场景：
 * 1. 获取集群中所有主题的详细信息
 * 2. 监控主题的创建和删除
 * 3. 在管理工具中展示主题列表
 * 4. 在需要主题元数据的场景中使用
 */
@InterfaceStability.Evolving
public class ListTopicsResult {
    /**
     * 存储主题列表查询结果的Future对象
     * 键为主题名称，值为主题的详细信息（TopicListing对象）
     * 该Future完成时表示查询操作已结束
     */
    final KafkaFuture<Map<String, TopicListing>> future;

    /**
     * 构造函数，初始化主题列表查询结果对象
     *
     * @param future 包含主题名称到TopicListing映射的Future对象
     */
    ListTopicsResult(KafkaFuture<Map<String, TopicListing>> future) {
        this.future = future;
    }

    /**
     * 返回一个Future对象，该对象包含主题名称到TopicListing对象的映射
     * TopicListing对象包含了主题的详细信息，如是否为内部主题等
     * 
     * @return 返回包含主题详细信息映射的Future对象
     */
    public KafkaFuture<Map<String, TopicListing>> namesToListings() {
        return future;
    }

    /**
     * 返回一个Future对象，该对象包含所有TopicListing对象的集合
     * 使用thenApply方法转换原始Map的values为Collection
     * 
     * @return 返回包含所有主题详细信息的Future对象
     */
    public KafkaFuture<Collection<TopicListing>> listings() {
        return future.thenApply(Map::values);
    }

    /**
     * 返回一个Future对象，该对象包含所有主题名称的集合
     * 使用thenApply方法转换原始Map的keySet为Set
     * 
     * @return 返回包含所有主题名称的Future对象
     */
    public KafkaFuture<Set<String>> names() {
        return future.thenApply(Map::keySet);
    }
}
