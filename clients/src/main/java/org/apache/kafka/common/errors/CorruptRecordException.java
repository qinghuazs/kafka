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

/**
 * 当消息记录损坏时抛出此异常。
 * 
 * 应用场景：
 * 1. 消息记录的CRC校验失败，通常表示网络传输或磁盘存储过程中发生了数据损坏
 * 2. 消息大小超过了有效限制
 * 3. 压缩主题中的消息具有空键值
 * 
 * 设计考虑：
 * 1. 继承自RetriableException，表明这是一个可重试的异常
 * 2. 提供默认的错误消息，详细说明可能的损坏原因
 * 3. 支持自定义错误消息和异常链，便于进行故障诊断
 * 4. 作为数据完整性验证的重要组成部分
 */
public class CorruptRecordException extends RetriableException {

    private static final long serialVersionUID = 1L;

    public CorruptRecordException() {
        super("This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.");
    }

    public CorruptRecordException(String message) {
        super(message);
    }

    public CorruptRecordException(Throwable cause) {
        super(cause);
    }

    public CorruptRecordException(String message, Throwable cause) {
        super(message, cause);
    }

}
