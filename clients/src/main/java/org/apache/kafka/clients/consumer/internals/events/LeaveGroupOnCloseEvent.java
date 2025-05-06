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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;

import java.time.Duration;

/**
 * 当用户调用 {@link Consumer#close()} 方法时，系统会发送此事件来通知 {@link ConsumerMembershipManager}
 * 执行必要的步骤以尝试清理地退出消费者组。事件的超时时间基于以下两种情况：
 * 1. 用户调用 {@link Consumer#close(Duration)} 时提供的超时值
 * 2. 用户调用 {@link Consumer#close()} 时使用 {@link ConsumerUtils#DEFAULT_CLOSE_TIMEOUT_MS} 默认超时值
 * 
 * 应用场景：
 * - 消费者优雅关闭：确保消费者在关闭前能够正常退出消费者组，避免造成消费者组的重平衡
 * - 资源清理：通过有序的关闭流程，确保所有资源得到适当释放
 * 
 * 设计考虑：
 * - 继承自 CompletableApplicationEvent<Void>，支持异步完成通知
 * - 使用超时机制确保关闭操作不会无限期等待
 * - 通过心跳响应确认退出状态，提供可靠的退出机制
 * 
 * 当成员管理器收到确认已离开消费者组的心跳响应时，该事件被视为完成。
 */
public class LeaveGroupOnCloseEvent extends CompletableApplicationEvent<Void> {

    /**
     * 构造函数，创建一个新的LeaveGroupOnCloseEvent实例
     * 
     * 实现细节：
     * 1. 调用父类构造函数，传入事件类型LEAVE_GROUP_ON_CLOSE
     * 2. 设置事件的截止时间deadlineMs
     * 
     * @param deadlineMs 事件的绝对截止时间（毫秒），超过此时间事件将被视为超时
     */
    public LeaveGroupOnCloseEvent(final long deadlineMs) {
        super(Type.LEAVE_GROUP_ON_CLOSE, deadlineMs);
    }
}
