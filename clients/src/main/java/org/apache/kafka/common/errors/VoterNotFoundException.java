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
 * 当在Kafka集群中找不到指定的投票者节点时抛出此异常。
 * 
 * 在Kafka的控制器选举和分区leader选举过程中，
 * 每个broker都可以作为投票者参与投票。
 * 当系统无法找到预期的投票者时，会抛出此异常。
 * 
 * 应用场景：
 * 1. 控制器选举：当参与投票的broker不可用
 * 2. 分区leader选举：当ISR中的某个副本不可用
 * 3. 集群配置变更：当涉及的投票者节点已离线或被移除
 */
public class VoterNotFoundException extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息构造异常
     * 
     * @param message 描述投票者未找到的错误消息
     */
    public VoterNotFoundException(String message) {
        super(message);
    }

    /**
     * 使用指定的错误消息和原因构造异常
     * 
     * @param message 描述投票者未找到的错误消息
     * @param cause 导致此异常的原始异常
     */
    public VoterNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
