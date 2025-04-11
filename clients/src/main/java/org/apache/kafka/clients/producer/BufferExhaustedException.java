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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.errors.TimeoutException;

/**
 * 当Kafka生产者无法在max.block.ms配置的时间内为新记录分配内存时，会抛出此异常。
 * 这种情况通常发生在生产者的缓冲区已满的时候。
 *
 * 生产者的缓冲区大小由buffer.memory参数控制，用于存储尚未发送到服务器的记录。
 * 当生产者的发送速度超过了向Kafka服务器发送数据的速度时，记录会积累在缓冲区中。
 * 如果缓冲区被填满，新的send()调用将被阻塞max.block.ms毫秒。
 * 如果在此时间内无法获得足够的缓冲区空间，则会抛出此异常。
 *
 * 在早期版本中，这种情况下会抛出TimeoutException。
 * 为了保持向后兼容性并确保现有的异常处理代码能继续工作，
 * 此类继承自TimeoutException。
 *
 * 相关配置参数：
 * - buffer.memory：设置生产者可用于缓冲等待发送到服务器的记录的内存大小
 * - max.block.ms：send()方法和partitionsFor()方法阻塞的最大时间
 */
public class BufferExhaustedException extends TimeoutException {

    private static final long serialVersionUID = 1L;

    public BufferExhaustedException(String message) {
        super(message);
    }

}
