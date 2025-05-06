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
 * 消费者关闭时的提交事件类
 * 
 * 该事件在Kafka消费者即将关闭时被触发，用于执行最终的偏移量提交操作。
 * 这是确保消费者优雅关闭的重要组成部分，可以防止消息重复消费：
 * 1. 当消费者关闭时，如果还有未提交的偏移量，会触发该事件
 * 2. 事件处理器会确保这些偏移量被同步提交到Kafka
 * 3. 只有在提交完成后，消费者才会继续执行关闭流程
 * 
 * 该事件继承自ApplicationEvent基类，使用COMMIT_ON_CLOSE类型，
 * 表明这是一个在消费者关闭阶段的特殊提交事件。
 */
public class CommitOnCloseEvent extends ApplicationEvent {

    /**
     * 构造函数
     * 初始化一个CommitOnCloseEvent事件实例
     * 通过调用父类构造器，将事件类型设置为COMMIT_ON_CLOSE
     */
    public CommitOnCloseEvent() {
        super(Type.COMMIT_ON_CLOSE);
    }
}
