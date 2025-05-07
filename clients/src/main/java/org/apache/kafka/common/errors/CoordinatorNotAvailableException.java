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
 * 当协调器不可用时抛出此异常。
 * 
 * 应用场景：
 * 1. 组协调器（Group Coordinator）上下文：
 *    - 当尝试提交元数据或偏移量请求时，如果组元数据主题尚未创建
 *    - 当消费者组首次启动，需要创建元数据主题时
 * 
 * 2. 事务协调器（Transaction Coordinator）上下文：
 *    - 当底层事务日志副本数不足时
 *    - 当向事务日志追加数据超时时
 * 
 * 设计考虑：
 * 1. 继承自RetriableException，表明这是一个可重试的异常
 * 2. 提供单例实例INSTANCE，用于频繁使用的场景
 * 3. 支持自定义错误消息，便于提供更详细的错误信息
 * 4. 允许包含原始异常作为cause，便于异常链追踪
 */
public class CoordinatorNotAvailableException extends RetriableException {
    public static final CoordinatorNotAvailableException INSTANCE = new CoordinatorNotAvailableException();

    private static final long serialVersionUID = 1L;

    private CoordinatorNotAvailableException() {
        super();
    }

    public CoordinatorNotAvailableException(String message) {
        super(message);
    }

    public CoordinatorNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

}
