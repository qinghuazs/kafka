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
package org.apache.kafka.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * 一个用于表示主题集合的类。这个集合可以通过主题名称或主题ID来定义主题。
 * 该类是一个抽象基类，提供了两种具体实现：TopicIdCollection和TopicNameCollection。
 */
public abstract class TopicCollection {

    // 私有构造函数，防止直接实例化
    private TopicCollection() {}

    /**
     * 创建一个通过主题ID定义的主题集合
     * @param topics 主题ID集合
     * @return 返回一个TopicIdCollection实例，包含指定的主题ID集合
     */
    public static TopicIdCollection ofTopicIds(Collection<Uuid> topics) {
        return new TopicIdCollection(topics);
    }

    /**
     * 创建一个通过主题名称定义的主题集合
     * @param topics 主题名称集合
     * @return 返回一个TopicNameCollection实例，包含指定的主题名称集合
     */
    public static TopicNameCollection ofTopicNames(Collection<String> topics) {
        return new TopicNameCollection(topics);
    }

    /**
     * 通过主题ID定义主题集合的具体实现类。
     * 该类不支持进一步继承，只能使用这里提供的实现。
     */
    public static class TopicIdCollection extends TopicCollection {
        // 存储主题ID的不可变集合
        private final Collection<Uuid> topicIds;

        // 私有构造函数，确保只能通过工厂方法创建实例
        private TopicIdCollection(Collection<Uuid> topicIds) {
            // 创建一个新的ArrayList来存储主题ID，实现防御性复制
            this.topicIds = new ArrayList<>(topicIds);
        }

        /**
         * 获取主题ID集合
         * @return 返回一个不可修改的主题ID集合视图
         */
        public Collection<Uuid> topicIds() {
            return Collections.unmodifiableCollection(topicIds);
        }
    }

    /**
     * 通过主题名称定义主题集合的具体实现类。
     * 该类不支持进一步继承，只能使用这里提供的实现。
     */
    public static class TopicNameCollection extends TopicCollection {
        // 存储主题名称的不可变集合
        private final Collection<String> topicNames;

        // 私有构造函数，确保只能通过工厂方法创建实例
        private TopicNameCollection(Collection<String> topicNames) {
            // 创建一个新的ArrayList来存储主题名称，实现防御性复制
            this.topicNames = new ArrayList<>(topicNames);
        }

        /**
         * 获取主题名称集合
         * @return 返回一个不可修改的主题名称集合视图
         */
        public Collection<String> topicNames() {
            return Collections.unmodifiableCollection(topicNames);
        }
    }
}
