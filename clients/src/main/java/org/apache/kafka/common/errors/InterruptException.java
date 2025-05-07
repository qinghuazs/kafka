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
package org.apache.kafka.common.errors;

import org.apache.kafka.common.KafkaException;

/**
 * InterruptedException的非检查型包装异常
 * 
 * 该异常在以下场景中抛出：
 * 1. 当Kafka操作（如生产、消费）被外部中断时
 * 2. 当线程在等待I/O操作完成时被中断
 * 3. 当需要优雅关闭Kafka客户端时
 * 
 * 异常处理机制：
 * - 将检查型的InterruptedException转换为非检查型异常
 * - 保持线程的中断状态（通过Thread.currentThread().interrupt()）
 * - 允许上层应用更灵活地处理中断
 */
public class InterruptException extends KafkaException {

    private static final long serialVersionUID = 1L;
    
    /**
     * 使用InterruptedException构造异常
     * 
     * @param cause 原始的中断异常
     */
    public InterruptException(InterruptedException cause) {
        super(cause);
        // 保持线程的中断状态
        Thread.currentThread().interrupt();
    }
    
    /**
     * 使用错误消息和InterruptedException构造异常
     * 
     * @param message 描述中断原因的详细信息
     * @param cause 原始的中断异常
     */
    public InterruptException(String message, InterruptedException cause) {
        super(message, cause);
        // 保持线程的中断状态
        Thread.currentThread().interrupt();
    }

    /**
     * 仅使用错误消息构造异常
     * 
     * @param message 描述中断原因的详细信息
     */
    public InterruptException(String message) {
        super(message, new InterruptedException());
        // 保持线程的中断状态
        Thread.currentThread().interrupt();
    }

}
