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
 * 当尝试访问或操作一个不存在的消费者组ID时抛出此异常。
 * 
 * 应用场景：
 * 1. 当消费者尝试加入一个不存在的消费者组时
 * 2. 当管理员尝试查看或修改一个不存在的消费者组的信息时
 * 3. 当组ID在系统中找不到对应的元数据时
 * 
 * 设计考虑：
 * 1. 继承自ApiException以便于统一异常处理
 * 2. 提供明确的错误信息，帮助快速定位问题
 * 3. 作为消费者组管理的安全检查机制之一
 */
public class GroupIdNotFoundException extends ApiException {
    /**
     * 构造函数
     * @param message 异常描述信息，用于说明具体的错误原因
     */
    public GroupIdNotFoundException(String message) {
        super(message);
    }
}
