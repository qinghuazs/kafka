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

/**
 * Kafka异常体系的基类
 * 
 * 该类继承自RuntimeException，是所有Kafka相关异常的父类。
 * 作为未检查异常，它不需要显式地在方法签名中声明或捕获。
 * 主要用于封装Kafka系统中的各类运行时错误，如配置错误、网络问题等。
 */
public class KafkaException extends RuntimeException {

    // 序列化版本ID，用于类版本控制
    private static final long serialVersionUID = 1L;

    /**
     * 创建一个带有错误消息和原因的Kafka异常
     * 
     * @param message 描述异常的详细信息
     * @param cause 导致当前异常的原始异常
     */
    public KafkaException(String message, Throwable cause) {
        // 调用父类RuntimeException的构造函数，传入消息和原因
        super(message, cause);
    }

    /**
     * 创建一个只带有错误消息的Kafka异常
     * 
     * @param message 描述异常的详细信息
     */
    public KafkaException(String message) {
        // 调用父类RuntimeException的构造函数，只传入消息
        super(message);
    }

    /**
     * 创建一个只带有原因的Kafka异常
     * 
     * @param cause 导致当前异常的原始异常
     */
    public KafkaException(Throwable cause) {
        // 调用父类RuntimeException的构造函数，只传入原因
        super(cause);
    }

    /**
     * 创建一个没有详细信息的Kafka异常
     * 通常用于不需要特定错误信息的场景
     */
    public KafkaException() {
        // 调用父类RuntimeException的默认构造函数
        super();
    }
}
