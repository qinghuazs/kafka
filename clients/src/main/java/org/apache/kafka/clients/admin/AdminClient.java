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

import java.util.Map;
import java.util.Properties;

/**
 * Kafka内置管理客户端的基类
 *
 * 客户端代码应优先使用新的{@link Admin}接口，而不是这个类。
 * 这个类在未来的版本中可能会被移除，但为了避免不必要的警告，暂未标记为过时。
 *
 * 该类提供了创建管理客户端实例的工厂方法，支持通过Properties或Map形式的配置来创建实例。
 * 实现了Admin接口，提供了管理Kafka集群的各种操作能力。
 */
public abstract class AdminClient implements Admin {

    /**
     * 使用给定的配置创建一个新的Admin实例
     *
     * @param props 配置属性，包含连接Kafka集群所需的各种参数
     * @return 返回一个新的KafkaAdminClient实例
     */
    public static AdminClient create(Properties props) {
        // 调用Admin接口的工厂方法创建实例，并转换为AdminClient类型
        return (AdminClient) Admin.create(props);
    }

    /**
     * 使用给定的配置创建一个新的Admin实例
     *
     * @param conf 配置映射，包含连接Kafka集群所需的各种参数
     * @return 返回一个新的KafkaAdminClient实例
     */
    public static AdminClient create(Map<String, Object> conf) {
        // 调用Admin接口的工厂方法创建实例，并转换为AdminClient类型
        return (AdminClient) Admin.create(conf);
    }
}
