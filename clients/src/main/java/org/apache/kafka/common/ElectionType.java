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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Arrays;
import java.util.Set;

/**
 * Kafka分区leader选举的类型选项，用于{@link org.apache.kafka.clients.admin.Admin#electLeaders(ElectionType, Set, org.apache.kafka.clients.admin.ElectLeadersOptions)}方法。
 * 
 * 该类的API仍在演进中，详见{@link org.apache.kafka.clients.admin.Admin}。
 *
 * 选举类型包括：
 * - PREFERRED: 首选副本选举，只从ISR(同步副本集合)中选择leader
 * - UNCLEAN: 不干净的选举，允许从非ISR副本中选择leader，可能会丢失数据
 */
@InterfaceStability.Evolving
public enum ElectionType {
    // 首选副本选举类型，字节值为0
    PREFERRED((byte) 0), 
    // 不干净的选举类型，字节值为1
    UNCLEAN((byte) 1);

    // 选举类型对应的字节值
    public final byte value;

    // 构造函数，初始化选举类型的字节值
    ElectionType(byte value) {
        this.value = value;
    }

    /**
     * 根据字节值获取对应的选举类型
     * 
     * @param value 选举类型的字节值
     * @return 对应的ElectionType枚举值
     * @throws IllegalArgumentException 当传入的字节值不是有效的选举类型值时抛出
     */
    public static ElectionType valueOf(byte value) {
        if (value == PREFERRED.value) {
            return PREFERRED;  // 返回首选副本选举类型
        } else if (value == UNCLEAN.value) {
            return UNCLEAN;   // 返回不干净的选举类型
        } else {
            throw new IllegalArgumentException(
                    String.format("Value %s must be one of %s", value, Arrays.asList(ElectionType.values())));
        }
    }
}
