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
 * 当尝试创建一个已经存在的Topic时抛出此异常。
 * 在Kafka中，Topic名称必须是唯一的。当客户端或管理员尝试创建一个与现有Topic同名的新Topic时，
 * 会触发此异常。这种机制确保了Topic命名的唯一性，防止出现命名冲突。
 * 
 * 应用场景：
 * 1. 通过AdminClient创建新Topic时，如果指定的Topic名称已存在
 * 2. 当启用了auto.create.topics.enable配置，并且尝试自动创建一个已存在的Topic时
 * 3. 在Topic创建API调用中指定了重复的Topic名称
 */
public class TopicExistsException extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 构造一个TopicExistsException异常实例
     * 
     * @param message 异常描述信息，通常包含具体的Topic名称和创建失败的原因
     */
    public TopicExistsException(String message) {
        super(message);
    }

    /**
     * 构造一个TopicExistsException异常实例
     * 
     * @param message 异常描述信息，通常包含具体的Topic名称和创建失败的原因
     * @param cause 导致此异常的原始异常，用于异常链的构建和问题追踪
     */
    public TopicExistsException(String message, Throwable cause) {
        super(message, cause);
    }

}
