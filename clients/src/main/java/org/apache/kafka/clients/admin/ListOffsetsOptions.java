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

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

/**
 * Options for {@link AdminClient#listOffsets(Map)}.
 * 用于{@link AdminClient#listOffsets(Map)}方法的配置选项类。
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 * 该类的API仍在演进中，详情请参见{@link AdminClient}。
 */
@InterfaceStability.Evolving
public class ListOffsetsOptions extends AbstractOptions<ListOffsetsOptions> {

    /**
     * 隔离级别配置，用于控制消费者在读取消息时是否可以看到未提交的事务消息
     * - READ_UNCOMMITTED：允许读取未提交的事务消息，适用于对数据一致性要求不高的场景
     * - READ_COMMITTED：只能读取已提交的事务消息，适用于需要强一致性的场景
     */
    private final IsolationLevel isolationLevel;

    /**
     * 默认构造函数，使用READ_UNCOMMITTED作为默认的隔离级别
     * 这意味着默认情况下可以读取未提交的事务消息
     */
    public ListOffsetsOptions() {
        this(IsolationLevel.READ_UNCOMMITTED);
    }

    /**
     * 带隔离级别参数的构造函数
     * @param isolationLevel 指定的隔离级别，可以是READ_UNCOMMITTED或READ_COMMITTED
     */
    public ListOffsetsOptions(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    /**
     * 获取当前配置的隔离级别
     * @return 返回当前配置的隔离级别，用于确定是否可以读取未提交的事务消息
     */
    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }
}
