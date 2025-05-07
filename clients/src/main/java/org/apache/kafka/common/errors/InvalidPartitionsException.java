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
 * 当Kafka操作涉及的分区配置无效时抛出此异常。
 * 
 * 应用场景：
 * 1. 创建主题时指定了无效的分区数量（如负数或超过系统限制）
 * 2. 修改主题分区时的分配方案不合法
 * 3. 请求访问不存在的分区
 * 4. 分区重分配操作参数错误
 * 
 * 设计考虑：
 * - 作为分区管理的基础异常类，帮助定位分区配置和操作中的错误
 * - 通过详细的错误信息指导用户进行正确的分区操作
 */
public class InvalidPartitionsException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidPartitionsException(String message) {
        super(message);
    }

    public InvalidPartitionsException(String message, Throwable cause) {
        super(message, cause);
    }

}
