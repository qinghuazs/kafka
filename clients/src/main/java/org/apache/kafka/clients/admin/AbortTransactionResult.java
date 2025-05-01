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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

/**
 * {@link Admin#abortTransaction(AbortTransactionSpec, AbortTransactionOptions)}方法的结果类
 *
 * 该类用于表示事务中止操作的执行结果。对于每个主题分区，都会返回一个Future对象，
 * 用于异步地获取该分区上的事务中止操作的完成状态。
 *
 * 注意：这个类的API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class AbortTransactionResult {
    /**
     * 存储每个主题分区对应的事务中止操作Future结果
     * Key为主题分区，Value为对应的操作Future
     */
    private final Map<TopicPartition, KafkaFuture<Void>> futures;

    /**
     * 构造函数
     *
     * @param futures 主题分区到其事务中止操作Future的映射
     */
    AbortTransactionResult(Map<TopicPartition, KafkaFuture<Void>> futures) {
        // 初始化futures映射
        this.futures = futures;
    }

    /**
     * 获取一个Future，该Future在指定的事务中止操作完成时（无论成功还是失败）完成
     * 
     * 当调用{@link Admin#abortTransaction(AbortTransactionSpec, AbortTransactionOptions)}后，
     * 可以通过这个方法获取的Future来监控操作的完成状态。如果操作成功完成，Future将正常完成；
     * 如果发生错误或超时，Future将抛出异常。
     *
     * @return 返回一个组合了所有分区操作结果的Future
     */
    public KafkaFuture<Void> all() {
        // 使用KafkaFuture.allOf组合所有分区的Future，只有全部成功才返回成功
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }

}
