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

import java.util.Locale;

/**
 * Kafka的事务隔离级别定义。
 * 支持两种隔离级别：
 * - READ_UNCOMMITTED：读未提交，可以读取到未提交的事务消息
 * - READ_COMMITTED：读已提交，只能读取已提交的事务消息
 */
public enum IsolationLevel {
    // 读未提交级别，id为0
    READ_UNCOMMITTED((byte) 0), 
    // 读已提交级别，id为1
    READ_COMMITTED((byte) 1);

    // 隔离级别的数字标识
    private final byte id;

    /**
     * 构造函数
     * @param id 隔离级别的数字标识
     */
    IsolationLevel(byte id) {
        this.id = id;
    }

    /**
     * 获取隔离级别的数字标识
     * @return 隔离级别对应的字节值
     */
    public byte id() {
        return id;
    }

    /**
     * 根据数字标识获取对应的隔离级别枚举值
     * @param id 隔离级别的数字标识
     * @return 对应的IsolationLevel枚举值
     * @throws IllegalArgumentException 如果id不是有效的隔离级别标识
     */
    public static IsolationLevel forId(byte id) {
        switch (id) {
            case 0:
                return READ_UNCOMMITTED;
            case 1:
                return READ_COMMITTED;
            default:
                throw new IllegalArgumentException("未知的隔离级别 " + id);
        }
    }

    /**
     * 返回隔离级别的小写字符串表示
     */
    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ROOT);
    }
}
