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
 * 投票集不一致异常
 * 
 * 该异常在以下场景中抛出：
 * 1. 当Kafka集群中的Controller发现投票节点集合与预期不符时
 * 2. 当进行Leader选举时，参与投票的节点集合发生变化
 * 3. 当KRaft模式下，投票者配置与实际运行的投票者不匹配时
 * 
 * 投票集的作用：
 * - 在KRaft（Kafka Raft）模式下维护集群一致性
 * - 确保Leader选举的正确性和可靠性
 * - 防止脑裂情况的发生
 */
public class InconsistentVoterSetException extends ApiException {

    private static final long serialVersionUID = 1;

    /**
     * 使用指定的错误消息构造异常
     * 
     * @param s 描述投票集不一致问题的详细信息
     */
    public InconsistentVoterSetException(String s) {
        super(s);
    }

    /**
     * 使用指定的错误消息和原因构造异常
     * 
     * @param message 描述投票集不一致问题的详细信息
     * @param cause 导致此异常的原始异常
     */
    public InconsistentVoterSetException(String message, Throwable cause) {
        super(message, cause);
    }

}
