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

import java.util.Collection;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * 用于 {@link Admin#describeProducers(Collection)} 的配置选项类。
 * 该类用于描述Kafka生产者的信息，包括活跃的生产者会话、事务状态等。
 *
 * 该类的API仍在演进中，详情请参见 {@link Admin}。
 */
@InterfaceStability.Evolving
public class DescribeProducersOptions extends AbstractOptions<DescribeProducersOptions> {
    // 存储要查询的broker ID，使用OptionalInt允许该值为空
    private OptionalInt brokerId = OptionalInt.empty();

    /**
     * 设置要查询的broker ID
     * 
     * @param brokerId 要查询的broker的ID
     * @return 当前DescribeProducersOptions实例，支持链式调用
     */
    public DescribeProducersOptions brokerId(int brokerId) {
        // 将传入的broker ID包装为OptionalInt对象并存储
        this.brokerId = OptionalInt.of(brokerId);
        // 返回当前实例以支持方法链式调用
        return this;
    }

    /**
     * 获取配置的broker ID
     * 
     * @return 返回配置的broker ID，如果未配置则返回空的OptionalInt
     */
    public OptionalInt brokerId() {
        // 返回存储的broker ID
        return brokerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DescribeProducersOptions that = (DescribeProducersOptions) o;
        return Objects.equals(brokerId, that.brokerId) &&
            Objects.equals(timeoutMs, that.timeoutMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId, timeoutMs);
    }

    @Override
    public String toString() {
        return "DescribeProducersOptions(" +
            "brokerId=" + brokerId +
            ", timeoutMs=" + timeoutMs +
            ')';
    }
}
