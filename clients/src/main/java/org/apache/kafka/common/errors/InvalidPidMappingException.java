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
 * 当生产者ID（PID）映射关系无效时抛出此异常。
 * 
 * 应用场景：
 * 1. 事务性生产者初始化时无法获取有效的PID
 * 2. 生产者尝试使用已过期或无效的PID
 * 3. 集群中PID映射信息不一致或损坏
 * 
 * 设计考虑：
 * - PID用于保证消息幂等性和事务完整性
 * - 该异常帮助及时发现生产者标识问题，确保消息可靠性
 * - 通常需要重新初始化生产者来解决此问题
 */
public class InvalidPidMappingException extends ApiException {
    public InvalidPidMappingException(String message) {
        super(message);
    }
}
