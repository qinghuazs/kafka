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
 * 当分区的Leader发生变更，新的Leader被选举出来时抛出此异常
 *
 * 应用场景：
 * 1. 原Leader节点失效，触发了新Leader的选举过程
 * 2. 在处理请求过程中发生了计划内的Leader切换
 * 3. 分区重分配导致Leader变更
 *
 * 设计考虑：
 * 1. 继承自ApiException，用于处理Leader选举相关的状态变化
 * 2. 作为一个提示性异常，告知客户端需要刷新元数据
 * 3. 通常需要客户端重新获取最新的Leader信息后重试操作
 */
public class NewLeaderElectedException extends ApiException {
    public NewLeaderElectedException(String message) {
        super(message);
    }
}
