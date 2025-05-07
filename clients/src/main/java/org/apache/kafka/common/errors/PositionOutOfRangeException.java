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
 * 位置越界异常
 * 
 * 当消费者尝试从一个不存在的位置读取消息时抛出此异常。这种情况通常发生在以下场景：
 * 1. 消费者请求的偏移量已经被删除（由于日志保留策略）
 * 2. 消费者请求的偏移量超出了当前分区的最新位置
 * 3. 消费者尝试从一个负数偏移量读取消息
 * 
 * 处理建议：
 * - 检查消费者组的偏移量配置
 * - 根据实际情况选择合适的重置策略（earliest或latest）
 * - 考虑调整日志保留策略以满足业务需求
 */
public class PositionOutOfRangeException extends ApiException {

    private static final long serialVersionUID = 1;

    public PositionOutOfRangeException(String s) {
        super(s);
    }

    public PositionOutOfRangeException(String message, Throwable cause) {
        super(message, cause);
    }

}
