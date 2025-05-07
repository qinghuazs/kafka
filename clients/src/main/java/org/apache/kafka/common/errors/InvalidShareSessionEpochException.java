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
 * 当共享会话的epoch值无效时抛出的异常。
 * 
 * 应用场景：
 * 1. 在分布式会话管理中，当检测到会话的epoch值过期或不匹配
 * 2. 当多个消费者实例尝试使用不同的epoch值访问同一个共享会话
 * 3. 在会话迁移或恢复过程中，如果新的消费者实例使用了无效的epoch值
 * 
 * 设计考虑：
 * 1. 继承自RetriableException，表示这是一个可重试的异常
 * 2. 当发生此异常时，客户端可以通过重新获取最新的epoch值来恢复
 * 3. 用于确保分布式环境下会话状态的一致性和有序性
 */
public class InvalidShareSessionEpochException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public InvalidShareSessionEpochException(String message) {
        super(message);
    }
}
