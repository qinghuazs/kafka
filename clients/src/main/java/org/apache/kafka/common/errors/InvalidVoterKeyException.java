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
 * 无效投票者密钥异常
 * 
 * 当Kafka集群进行控制器选举或其他投票操作时，如果提供的投票者密钥无效或不匹配时抛出此异常。
 * 
 * 应用场景：
 * 1. 控制器选举：验证参与投票的节点的合法性
 * 2. 集群成员管理：确保只有授权的节点可以参与投票
 * 3. 安全性控制：防止未经授权的节点参与集群决策
 * 
 * 设计考虑：
 * 1. 继承自ApiException，用于处理API层面的认证和授权异常
 * 2. 提供序列化支持，确保在分布式环境中的异常传递
 * 3. 包含详细的错误信息和原因，便于问题诊断
 */
public class InvalidVoterKeyException extends ApiException {

    private static final long serialVersionUID = 1;

    public InvalidVoterKeyException(String s) {
        super(s);
    }

    public InvalidVoterKeyException(String message, Throwable cause) {
        super(message, cause);
    }

}
