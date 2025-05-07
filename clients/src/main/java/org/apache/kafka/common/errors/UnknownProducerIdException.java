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
 * 未知生产者ID异常
 * 
 * 当broker无法找到与指定producerId关联的生产者元数据时，会抛出此异常。
 * 
 * 异常触发场景：
 * 1. 生产者的消息因超过保留时间而被删除
 * 2. 最后一条带有该producerId的消息被删除后，生产者的元数据也从broker中移除
 * 3. 生产者使用已失效的producerId尝试发送新消息
 * 
 * 应用场景：
 * 1. 在启用幂等性发送时，用于维护生产者会话的有效性
 * 2. 在事务性生产者中，确保生产者状态的一致性
 * 3. 防止已失效的生产者继续发送消息
 * 
 * 错误处理：
 * 1. 生产者需要重新初始化，获取新的producerId
 * 2. 对于事务性生产者，可能需要中止当前事务并重新开始
 * 3. 应用程序需要处理消息重发逻辑
 */
public class UnknownProducerIdException extends OutOfOrderSequenceException {

    /**
     * 创建一个未知生产者ID异常
     * 
     * @param message 异常描述信息
     */
    public UnknownProducerIdException(String message) {
        super(message);
    }

}
