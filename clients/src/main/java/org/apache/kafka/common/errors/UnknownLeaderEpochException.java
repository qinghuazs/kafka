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
 * 未知领导者纪元异常
 * 
 * 当请求中包含的领导者纪元（leader epoch）大于接收该请求的broker上的领导者纪元时，会抛出此异常。
 * 这种情况通常发生在客户端观察到元数据更新，但该更新尚未传播到所有broker时。
 * 
 * 应用场景：
 * 1. 在分区领导者选举和切换过程中，用于处理不同broker之间的领导者纪元不一致的情况
 * 2. 在副本同步和数据复制过程中，确保数据一致性和顺序性
 * 
 * 错误处理：
 * 1. 这是一个可重试异常（RetriableException的子类），客户端可以直接重试请求
 * 2. 客户端无需在重试前刷新元数据
 * 3. 通常这种不一致是暂时的，会在元数据传播完成后自动解决
 */
public class UnknownLeaderEpochException extends RetriableException {
    private static final long serialVersionUID = 1L;

    /**
     * 创建一个未知领导者纪元异常
     * 
     * @param message 异常描述信息
     */
    public UnknownLeaderEpochException(String message) {
        super(message);
    }

    /**
     * 创建一个未知领导者纪元异常
     * 
     * @param message 异常描述信息
     * @param cause 导致此异常的原始异常
     */
    public UnknownLeaderEpochException(String message, Throwable cause) {
        super(message, cause);
    }

}
