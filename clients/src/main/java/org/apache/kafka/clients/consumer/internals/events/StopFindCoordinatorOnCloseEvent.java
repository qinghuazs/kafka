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
 * This event is raised when the consumer is closing to prevent the CoordinatorRequestManager from
 * generating FindCoordinator requests. This event ensures that no new coordinator requests
 * are initiated once the consumer has completed all coordinator-dependent operations and
 * is in the process of shutting down.
 */
/**
 * 停止查找协调器关闭事件类
 * 当消费者正在关闭时触发此事件，用于阻止CoordinatorRequestManager生成FindCoordinator请求
 * 该事件确保在消费者完成所有依赖协调器的操作并且正在关闭过程中时，
 * 不会再发起新的协调器请求
 */
public class StopFindCoordinatorOnCloseEvent extends ApplicationEvent {
    
    /**
     * 构造函数，初始化停止查找协调器关闭事件
     * 设置事件类型为STOP_FIND_COORDINATOR_ON_CLOSE
     */
    public StopFindCoordinatorOnCloseEvent() {
        // 调用父类构造函数，指定事件类型为STOP_FIND_COORDINATOR_ON_CLOSE
        super(Type.STOP_FIND_COORDINATOR_ON_CLOSE);
    }
}
