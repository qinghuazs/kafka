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
 * 可重试异常基类。表示一个临时性的异常，通过重试操作可能会成功。
 * 
 * 应用场景：
 * 1. 网络通信暂时性故障
 * 2. 服务器临时负载过高
 * 3. 分布式系统中的临时状态不一致
 * 4. 资源暂时性不可用
 * 
 * 异常特点：
 * 1. 非永久性失败，重试可能成功
 * 2. 通常用于处理分布式系统中的瞬态故障
 * 3. 作为Kafka中可重试错误类型的标记接口
 * 
 * 处理机制：
 * 1. 客户端可以实现重试逻辑
 * 2. 支持配置重试策略（次数、间隔等）
 * 3. 可以结合退避算法优化重试
 * 
 * 设计考虑：
 * 1. 抽象类设计，统一可重试异常的处理方式
 * 2. 继承自ApiException，便于异常分类和处理
 * 3. 有助于实现更可靠的错误恢复机制
 */
public abstract class RetriableException extends ApiException {

    private static final long serialVersionUID = 1L;

    public RetriableException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetriableException(String message) {
        super(message);
    }

    public RetriableException(Throwable cause) {
        super(cause);
    }

    public RetriableException() {
    }

}
