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

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * 当尝试使用未正确释放的实例ID时抛出此异常。
 * 
 * 应用场景：
 * 1. 消费者组成员重新加入组时，发现之前的实例ID未释放
 * 2. 消费者实例异常退出，未能正常释放实例ID
 * 3. 消费者组再平衡过程中的实例ID冲突
 * 
 * 设计考虑：
 * - 确保消费者组成员的唯一性和正确性
 * - 防止同一个实例ID被多个消费者同时使用
 * - 帮助识别消费者实例的异常退出情况
 */
@InterfaceStability.Evolving
public class UnreleasedInstanceIdException extends ApiException {
    public UnreleasedInstanceIdException(String message) {
        super(message);
    }
}
