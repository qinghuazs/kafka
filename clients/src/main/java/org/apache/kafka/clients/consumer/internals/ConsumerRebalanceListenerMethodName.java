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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

/**
 * 消费者重平衡监听器方法名称枚举
 * 该类为{@link ConsumerRebalanceListener}接口中的方法提供静态名称，
 * 用于在编译时提供更好的类型安全保证。
 */
public enum ConsumerRebalanceListenerMethodName {

    /**
     * 分区撤销方法名称
     * 当分区从消费者撤销时调用的方法
     */
    ON_PARTITIONS_REVOKED("onPartitionsRevoked"),

    /**
     * 分区分配方法名称
     * 当新的分区分配给消费者时调用的方法
     */
    ON_PARTITIONS_ASSIGNED("onPartitionsAssigned"),

    /**
     * 分区丢失方法名称
     * 当分区因某些原因（如会话超时）丢失时调用的方法
     */
    ON_PARTITIONS_LOST("onPartitionsLost");

    /**
     * 完全限定的方法名称
     * 存储格式为"ConsumerRebalanceListener.方法名"
     */
    private final String fullyQualifiedMethodName;

    /**
     * 构造函数
     * 使用方法名创建枚举实例，并生成完全限定的方法名称
     *
     * @param methodName 方法名称
     */
    ConsumerRebalanceListenerMethodName(String methodName) {
        // 使用String.format生成完全限定的方法名
        // 格式为：ConsumerRebalanceListener.methodName
        this.fullyQualifiedMethodName = String.format("%s.%s", ConsumerRebalanceListener.class.getSimpleName(), methodName);
    }

    /**
     * 获取完全限定的方法名称
     * 返回格式如：{@code ConsumerRebalanceListener.onPartitionsRevoked}
     * 主要用于日志消息记录
     *
     * @return 完全限定的方法名称字符串
     */
    public String fullyQualifiedMethodName() {
        // 返回存储的完全限定方法名
        return fullyQualifiedMethodName;
    }
}
