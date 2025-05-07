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
 * 表示由于最近的领导者选举后，高水位标记落后于epoch起始偏移量，导致领导者无法保证单调递增的偏移量。
 * 
 * 应用场景：
 * 1. 发生领导者选举后，新领导者还未完全同步消息数据
 * 2. 消费者请求的偏移量在高水位标记和epoch起始偏移量之间
 * 3. 分区副本同步过程中的临时状态
 * 
 * 设计考虑：
 * - 作为可重试异常，允许客户端在短暂等待后重新尝试操作
 * - 保证消息的有序性和一致性
 * - 在领导者切换期间维护消息的完整性
 */
public class OffsetNotAvailableException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public OffsetNotAvailableException(String message) {
        super(message);
    }
}
