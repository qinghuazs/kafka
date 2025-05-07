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
 * 当尝试删除一个仍然包含活跃成员的消费者组时抛出此异常。
 * 
 * 应用场景：
 * 1. 管理员尝试删除一个仍有消费者在线的消费者组
 * 2. 防止误操作导致的数据消费中断
 * 3. 确保消费者组的安全清理
 * 
 * 设计考虑：
 * 1. 作为消费者组删除操作的安全检查机制
 * 2. 强制要求先优雅关闭所有消费者后才能删除组
 * 3. 避免因删除活跃组而导致的数据消费异常
 */
public class GroupNotEmptyException extends ApiException {
    /**
     * 构造函数
     * @param message 异常描述信息，通常包含消费者组ID和当前活跃成员数量
     */
    public GroupNotEmptyException(String message) {
        super(message);
    }
}
