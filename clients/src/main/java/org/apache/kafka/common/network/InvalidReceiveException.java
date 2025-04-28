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
package org.apache.kafka.common.network;

import org.apache.kafka.common.KafkaException;

/**
 * 无效接收异常类，用于处理Kafka网络层中接收数据时遇到的异常情况。
 * 
 * 该异常通常在以下场景中抛出：
 * 1. 接收到的数据包格式不符合预期格式
 * 2. 数据包长度与声明的长度不匹配
 * 3. 接收过程中出现数据损坏或不完整
 * 4. 网络传输过程中的数据校验失败
 * 
 * 继承自KafkaException，提供了对网络层特定异常的分类处理能力
 */
public class InvalidReceiveException extends KafkaException {

    /**
     * 构造一个新的InvalidReceiveException实例
     * 
     * @param message 异常描述信息，用于说明具体的接收错误原因
     */
    public InvalidReceiveException(String message) {
        super(message);
    }

}
