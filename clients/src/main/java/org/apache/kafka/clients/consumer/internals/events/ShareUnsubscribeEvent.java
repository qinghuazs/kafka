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

/**
 * Application event triggered when a user calls the unsubscribe API. This will make the consumer
 * release all its assignments and send a heartbeat request to leave the share group.
 * This event holds a future that will complete when the invocation of callbacks to release
 * complete and the heartbeat to leave the group is sent out (minimal effort to send the
 * leave group heartbeat, without waiting for any response or considering timeouts).
 */
/**
 * 取消订阅事件类
 * 当用户调用unsubscribe API时触发此应用程序事件
 * 这将使消费者释放其所有分配的分区，并发送心跳请求以离开共享组
 * 
 * 该事件持有一个Future，当以下条件满足时完成：
 * 1. 释放资源的回调函数调用完成
 * 2. 离开组的心跳请求已发送（仅确保发送请求，不等待响应也不考虑超时）
 */
public class ShareUnsubscribeEvent extends CompletableApplicationEvent<Void> {
    
    /**
     * 构造函数，初始化取消订阅事件
     *
     * @param deadlineMs 事件处理的截止时间（毫秒）
     */
    public ShareUnsubscribeEvent(final long deadlineMs) {
        // 调用父类构造函数，指定事件类型为SHARE_UNSUBSCRIBE和截止时间
        super(Type.SHARE_UNSUBSCRIBE, deadlineMs);
    }
}
