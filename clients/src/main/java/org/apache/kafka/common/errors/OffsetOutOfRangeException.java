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
 * 当未定义重置策略，且请求的偏移量超出服务器为指定分区保存的偏移量范围时抛出此异常。
 * 
 * 应用场景：
 * 1. 消费者请求的偏移量大于分区的最大偏移量
 * 2. 消费者请求的偏移量小于分区的最小偏移量
 * 3. 消息已被清理或过期，导致请求的偏移量不可用
 * 4. 未配置auto.offset.reset策略时的偏移量越界
 * 
 * 设计考虑：
 * - 继承自InvalidOffsetException，表明这是一种特定的偏移量错误
 * - 帮助识别消费者组的偏移量配置问题
 * - 提示用户需要设置合适的偏移量重置策略
 */
public class OffsetOutOfRangeException extends InvalidOffsetException {

    private static final long serialVersionUID = 1L;

    public OffsetOutOfRangeException(String message) {
        super(message);
    }

    public OffsetOutOfRangeException(String message, Throwable cause) {
        super(message, cause);
    }

}
