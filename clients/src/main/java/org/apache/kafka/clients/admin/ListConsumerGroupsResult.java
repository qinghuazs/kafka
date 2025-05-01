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
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Admin#listConsumerGroups()调用的结果类。
 * <p>
 * 该类的API仍在演进中，详细信息请参见Admin接口的说明。
 */
@InterfaceStability.Evolving
public class ListConsumerGroupsResult {
    /**
     * 包含所有消费者组列表的Future
     * 如果有任何错误发生，这个Future将返回异常
     */
    private final KafkaFutureImpl<Collection<ConsumerGroupListing>> all;

    /**
     * 只包含有效的消费者组列表的Future
     * 即使发生错误，这个Future也会返回所有可以获取到的有效结果
     */
    private final KafkaFutureImpl<Collection<ConsumerGroupListing>> valid;

    /**
     * 包含所有发生的错误的Future
     * 用于收集查询过程中发生的所有异常
     */
    private final KafkaFutureImpl<Collection<Throwable>> errors;

    /**
     * 构造函数，初始化结果对象并处理异步结果
     *
     * @param future 包含原始查询结果的Future对象
     */
    ListConsumerGroupsResult(KafkaFuture<Collection<Object>> future) {
        // 初始化三个Future对象
        this.all = new KafkaFutureImpl<>();
        this.valid = new KafkaFutureImpl<>();
        this.errors = new KafkaFutureImpl<>();

        // 为原始future添加结果处理回调
        future.thenApply(results -> {
            // 创建临时列表存储错误和有效结果
            ArrayList<Throwable> curErrors = new ArrayList<>();
            ArrayList<ConsumerGroupListing> curValid = new ArrayList<>();

            // 遍历所有结果对象，分类处理
            for (Object resultObject : results) {
                if (resultObject instanceof Throwable) {
                    // 如果是异常对象，添加到错误列表
                    curErrors.add((Throwable) resultObject);
                } else {
                    // 如果是消费者组列表对象，添加到有效结果列表
                    curValid.add((ConsumerGroupListing) resultObject);
                }
            }

            // 处理all Future的完成状态
            if (!curErrors.isEmpty()) {
                // 如果有错误，使用第一个错误完成all Future
                all.completeExceptionally(curErrors.get(0));
            } else {
                // 如果没有错误，使用有效结果完成all Future
                all.complete(curValid);
            }

            // 完成valid和errors Future
            valid.complete(curValid);  // 使用有效结果完成valid Future
            errors.complete(curErrors);  // 使用错误列表完成errors Future
            return null;
        });
    }

    /**
     * 返回一个Future，该Future要么产生一个异常，要么产生完整的消费者组列表。
     * 
     * 如果发生失败，该Future将只返回第一个发生的异常。
     *
     * @return 返回包含所有消费者组列表的Future，如果有错误则返回异常
     */
    public KafkaFuture<Collection<ConsumerGroupListing>> all() {
        return all;
    }

    /**
     * 返回一个只包含有效列表的Future。
     * 
     * 该Future永远不会因错误而失败。错误会被完全忽略。
     * 如果无法获取任何结果，将返回空集合。
     * 如果发生错误但仍能返回部分结果，该Future将返回这些部分结果。
     * 使用此Future时，建议同时检查errors Future以便显示和处理错误。
     *
     * @return 返回包含有效消费者组列表的Future
     */
    public KafkaFuture<Collection<ConsumerGroupListing>> valid() {
        return valid;
    }

    /**
     * 返回一个只包含发生的错误的Future。
     * 
     * 如果该Future返回的集合非空，则很可能在valid()集合中缺少一些元素。
     * 
     * 该Future本身永远不会因错误而失败。如果发生错误，
     * 该Future将成功返回一个至少包含一个异常的集合。
     *
     * @return 返回包含所有错误的Future
     */
    public KafkaFuture<Collection<Throwable>> errors() {
        return errors;
    }
}
