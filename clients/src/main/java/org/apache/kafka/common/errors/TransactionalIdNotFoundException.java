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
 * 当尝试访问或操作一个不存在的事务ID时抛出的异常。
 * 这个异常通常在以下场景抛出：
 * 1. 客户端尝试使用一个未注册的事务ID
 * 2. 事务ID已过期或被删除
 * 3. 事务协调器无法找到指定的事务ID对应的元数据
 */
public class TransactionalIdNotFoundException extends ApiException {

    /**
     * 使用指定的错误消息构造事务ID未找到异常
     * @param message 描述事务ID未找到原因的错误消息
     */
    public TransactionalIdNotFoundException(String message) {
        super(message);
    }
}
