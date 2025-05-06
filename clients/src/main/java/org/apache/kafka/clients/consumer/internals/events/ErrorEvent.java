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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.common.KafkaException;

/**
 * Kafka消费者客户端中的错误事件类，用于处理和传递后台任务执行过程中发生的异常。
 * 该类继承自BackgroundEvent，专门用于封装运行时异常，确保所有异常都被统一处理。
 */
public class ErrorEvent extends BackgroundEvent {

    /**
     * 存储实际的运行时异常对象。
     * 如果原始异常不是RuntimeException类型，会被封装成KafkaException。
     */
    private final RuntimeException error;

    /**
     * 创建一个新的错误事件实例。
     * 
     * @param t 原始的异常对象，可以是任何Throwable类型
     */
    public ErrorEvent(Throwable t) {
        // 调用父类构造器，设置事件类型为ERROR
        super(Type.ERROR);
        // 如果传入的异常是RuntimeException类型，直接使用；否则将其封装为KafkaException
        this.error = t instanceof RuntimeException ? (RuntimeException) t : new KafkaException(t);
    }

    /**
     * 获取错误事件中包含的运行时异常。
     * 
     * @return 运行时异常对象，可能是原始的RuntimeException或封装后的KafkaException
     */
    public RuntimeException error() {
        return error;
    }

    /**
     * 重写父类的toStringBase方法，添加错误信息到字符串表示中。
     * 
     * @return 包含错误信息的字符串表示
     */
    @Override
    public String toStringBase() {
        return super.toStringBase() + ", error=" + error;
    }
}