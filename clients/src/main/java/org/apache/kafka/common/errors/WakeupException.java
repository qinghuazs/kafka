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
 * 用于指示阻塞操作被外部线程中断的异常。
 * 
 * 在Kafka中，某些操作（如消费者的poll操作）可能会长时间阻塞。
 * 为了能够优雅地中断这些操作，Kafka提供了wakeup机制。
 * 
 * 典型应用场景：
 * 1. 消费者轮询中断：
 *    - 使用KafkaConsumer.wakeup()方法可以中断正在进行的poll(Duration)调用
 *    - 常用于应用程序关闭时安全地退出消费者线程
 * 
 * 2. 多线程环境：
 *    - 主线程需要通知消费者线程停止工作
 *    - 确保消费者能够及时响应关闭请求
 * 
 * 3. 超时控制：
 *    - 在外部线程中设置超时检查
 *    - 当操作超时时通过wakeup强制中断
 * 
 * 注意事项：
 * 1. 这是一个受控异常，用于流程控制，而不是错误情况
 * 2. 捕获此异常后，应该正确清理资源并关闭消费者
 * 3. 在重新使用消费者之前，需要重置wakeup状态
 */
public class WakeupException extends KafkaException {
    private static final long serialVersionUID = 1L;

}
