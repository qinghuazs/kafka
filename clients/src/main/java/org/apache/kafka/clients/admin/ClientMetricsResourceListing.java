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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;

/**
 * 客户端度量资源列表项类，用于表示和管理Kafka客户端的度量资源。
 * 该类是Kafka管理客户端API的一部分，主要用于客户端监控和度量收集系统中。
 * 
 * 应用场景：
 * 1. 在客户端度量监控中标识特定的度量资源
 * 2. 作为Admin.listClientMetricsResources()方法的返回结果的组成部分
 * 3. 用于客户端监控系统中资源的唯一标识和管理
 */
@InterfaceStability.Evolving
public class ClientMetricsResourceListing {
    /**
     * 度量资源的名称
     * 该字段用于唯一标识一个客户端度量资源，是只读的且不可变的
     */
    private final String name;

    /**
     * 创建一个新的客户端度量资源列表项
     * 
     * @param name 度量资源的名称，用于标识该资源
     */
    public ClientMetricsResourceListing(String name) {
        this.name = name;
    }

    /**
     * 获取度量资源的名称
     * 
     * @return 返回该度量资源的名称
     */
    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientMetricsResourceListing that = (ClientMetricsResourceListing) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "ClientMetricsResourceListing(" +
            "name='" + name +
            ')';
    }
}
