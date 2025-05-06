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

/**
 * The result of the {@link Admin#unregisterBroker(int, UnregisterBrokerOptions)} call.
 * 该类表示调用{@link Admin#unregisterBroker(int, UnregisterBrokerOptions)}方法的结果。
 *
 * The API of this class is evolving, see {@link Admin} for details.
 * 此类的API仍在演进中，详细信息请参见{@link Admin}。
 *
 * 该类用于处理Kafka集群中注销broker的操作结果。
 * 在以下场景中使用：
 * 1. 当需要从Kafka集群中移除一个broker时
 * 2. 在broker维护或升级期间临时移除broker
 * 3. 在集群扩缩容时管理broker的注销操作
 */
public class UnregisterBrokerResult {
    /**
     * 用于追踪注销broker操作完成状态的Future对象
     * 当操作成功完成时，Future将成功完成
     * 当操作失败时，Future将携带异常信息失败
     */
    private final KafkaFuture<Void> future;

    /**
     * 构造函数，初始化注销broker结果对象
     * 
     * @param future 包含注销操作状态的Future对象
     */
    UnregisterBrokerResult(final KafkaFuture<Void> future) {
        this.future = future;
    }

    /**
     * Return a future which succeeds if the operation is successful.
     * 返回一个Future对象，如果注销操作成功，该Future将成功完成。
     * 
     * 实现细节：
     * 1. 直接返回构造函数中传入的future对象
     * 2. 该future对象在注销操作完成时被完成
     * 3. 如果注销操作成功，future将正常完成
     * 4. 如果注销操作失败，future将携带异常信息失败
     *
     * @return 返回一个KafkaFuture对象，用于追踪注销操作的完成状态
     */
    public KafkaFuture<Void> all() {
        return future;
    }
}
