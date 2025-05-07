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
 * 当客户端尝试使用未经授权的事务ID执行事务操作时抛出此异常。
 * 在Kafka的事务处理中，每个生产者都需要一个唯一的事务ID，并且必须具有相应的权限才能使用该ID。
 * 这个异常通常表示客户端没有足够的权限来执行特定的事务操作。
 * 
 * 应用场景：
 * 1. 当生产者尝试初始化事务，但没有使用指定事务ID的权限
 * 2. 当生产者尝试提交或中止事务，但事务ID的权限已被撤销
 * 3. 在集群进行安全审计时，可能会收回某些事务ID的使用权限
 */
public class TransactionalIdAuthorizationException extends AuthorizationException {
    /**
     * 构造一个TransactionalIdAuthorizationException异常实例
     * 
     * @param message 异常描述信息，通常包含未授权的事务ID和具体的权限错误信息
     */
    public TransactionalIdAuthorizationException(final String message) {
        super(message);
    }
}
