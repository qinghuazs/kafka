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
 * 表示代理服务器从生产者收到了意外的序列号，这意味着可能发生了数据丢失。
 * 
 * 处理策略：
 * 1. 仅启用幂等性的生产者（设置了enable.idempotence但未配置transactional.id）：
 *    - 可以继续使用同一个生产者实例发送消息
 *    - 但存在已发送记录重排序的风险
 * 2. 事务型生产者：
 *    - 这是一个致命错误
 *    - 必须关闭生产者实例
 * 
 * 应用场景：
 * 1. 网络分区导致消息乱序到达
 * 2. 生产者重试导致序列号不连续
 * 3. 多个生产者实例并发写入
 * 
 * 设计考虑：
 * - 保证消息的精确一次语义（exactly-once semantics）
 * - 维护生产者会话的消息顺序
 * - 区分幂等性和事务性场景的错误处理
 */
public class OutOfOrderSequenceException extends ApiException {

    public OutOfOrderSequenceException(String msg) {
        super(msg);
    }
}
