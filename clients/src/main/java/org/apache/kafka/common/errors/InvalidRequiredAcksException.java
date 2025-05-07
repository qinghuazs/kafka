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
 * 当生产者设置的消息确认机制（acks）参数无效时抛出此异常。
 * 
 * 应用场景：
 * 1. 生产者配置了无效的acks值，有效值包括：
 *    - acks=0：不等待任何确认
 *    - acks=1：等待leader副本确认
 *    - acks=-1或all：等待所有同步副本确认
 * 2. 在生产者API中使用了不支持的确认级别
 * 
 * 设计考虑：
 * - 确保消息可靠性和持久性的正确配置
 * - 在配置错误时及时提醒用户
 * - 防止因确认机制配置错误导致数据丢失
 */
public class InvalidRequiredAcksException extends ApiException {
    private static final long serialVersionUID = 1L;

    public InvalidRequiredAcksException(String message) {
        super(message);
    }
}
