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
 * 限流配额超限异常
 * 
 * 当对资源的操作超过了限流配额时抛出此异常。这是一个可重试的异常，表示当前操作被临时限流。
 * 
 * 触发场景：
 * 1. 客户端请求速率超过了配置的QPS限制
 * 2. 生产者消息发送速率超过了带宽配额
 * 3. 消费者拉取速率超过了配置的限制
 * 4. 单个客户端占用过多的broker资源
 * 
 * 处理建议：
 * - 获取throttleTimeMs值，在指定时间后重试
 * - 检查并调整客户端配置的限流参数
 * - 考虑增加客户端实例来分散负载
 * - 评估是否需要申请更高的资源配额
 */
public class ThrottlingQuotaExceededException extends RetriableException {
    private int throttleTimeMs = 0;

    public ThrottlingQuotaExceededException(String message) {
        super(message);
    }

    public ThrottlingQuotaExceededException(int throttleTimeMs, String message) {
        super(message);
        this.throttleTimeMs = throttleTimeMs;
    }

    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }
}
