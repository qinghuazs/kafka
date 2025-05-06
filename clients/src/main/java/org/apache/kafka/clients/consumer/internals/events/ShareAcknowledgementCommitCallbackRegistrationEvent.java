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
 * 共享确认提交回调注册事件类
 * 该类用于在Kafka消费者内部处理确认提交回调的注册状态
 * 继承自ApplicationEvent基类，表示这是一个应用层级的事件
 */
public class ShareAcknowledgementCommitCallbackRegistrationEvent extends ApplicationEvent {

    /**
     * 标识回调是否已经注册的状态标志
     * true表示回调已经注册
     * false表示回调尚未注册
     */
    boolean isCallbackRegistered;

    /**
     * 构造函数，初始化共享确认提交回调注册事件
     *
     * @param isCallbackRegistered 回调是否已注册的状态标志
     */
    public ShareAcknowledgementCommitCallbackRegistrationEvent(boolean isCallbackRegistered) {
        // 调用父类构造函数，指定事件类型为SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK_REGISTRATION
        super(Type.SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK_REGISTRATION);
        // 初始化回调注册状态
        this.isCallbackRegistered = isCallbackRegistered;
    }

    /**
     * 获取回调是否已注册的状态
     *
     * @return 如果回调已注册则返回true，否则返回false
     */
    public boolean isCallbackRegistered() {
        // 返回回调注册状态
        return isCallbackRegistered;
    }
}
