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
 * 当对一个已被消费者组订阅的主题执行某些受限操作时抛出此异常。
 * 
 * 应用场景：
 * 1. 尝试删除一个仍有消费者组订阅的主题
 * 2. 尝试修改已订阅主题的关键配置（如分区数）
 * 3. 保护正在消费的主题不被意外修改
 * 
 * 设计考虑：
 * 1. 确保主题变更操作不会影响现有的消费者组
 * 2. 强制要求先处理订阅关系后再进行主题管理
 * 3. 作为主题管理的安全检查机制
 */
public class GroupSubscribedToTopicException extends ApiException {
    /**
     * 构造函数
     * @param message 异常描述信息，通常包含主题名称和订阅该主题的消费者组信息
     */
    public GroupSubscribedToTopicException(String message) {
        super(message);
    }
}
