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
 * 当由于记录状态无效而无法完成消息投递确认时抛出此异常。
 * 
 * 应用场景：
 * 1. 生产者尝试确认已过期或已被清理的消息
 * 2. 事务提交时发现消息状态异常
 * 3. 副本同步过程中发现消息状态不一致
 * 
 * 设计考虑：
 * - 用于保证消息投递的可靠性和一致性
 * - 帮助识别消息生命周期管理中的异常情况
 * - 通常表明系统中存在消息状态追踪的问题
 */
public class InvalidRecordStateException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidRecordStateException(String message) {
        super(message);
    }
}
