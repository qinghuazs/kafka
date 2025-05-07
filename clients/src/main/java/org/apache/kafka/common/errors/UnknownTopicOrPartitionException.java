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
 * 当请求的主题或分区不存在时抛出此异常。
 * 
 * 应用场景：
 * 1. 消费者尝试订阅不存在的主题
 * 2. 生产者尝试向不存在的主题发送消息
 * 3. 管理操作针对不存在的主题或分区
 * 4. 元数据可能过期导致的临时性错误
 * 
 * 设计考虑：
 * - 作为可重试异常，因为主题或分区可能在后续被创建
 * - 区别于InvalidTopicException，后者表示主题名称格式无效
 * - 通常与过期的元数据缓存相关，需要刷新元数据后重试
 * 
 * @see InvalidTopicException 参考无效主题异常类
 */
public class UnknownTopicOrPartitionException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    public UnknownTopicOrPartitionException() {
    }

    public UnknownTopicOrPartitionException(String message) {
        super(message);
    }

    public UnknownTopicOrPartitionException(Throwable throwable) {
        super(throwable);
    }

    public UnknownTopicOrPartitionException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
