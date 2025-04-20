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
package org.apache.kafka.common;

import org.apache.kafka.common.errors.ApiException;

/**
 * 表示记录无效的异常。
 * 当Kafka遇到格式错误、校验失败或其他导致记录不可用的情况时，
 * 会抛出此异常。这是一个API级别的异常，继承自ApiException。
 */
public class InvalidRecordException extends ApiException {

    private static final long serialVersionUID = 1;

    /**
     * 使用指定的错误消息创建异常
     * @param s 描述记录无效原因的错误消息
     */
    public InvalidRecordException(String s) {
        super(s);
    }

    /**
     * 使用指定的错误消息和原因创建异常
     * @param message 描述记录无效原因的错误消息
     * @param cause 导致此异常的原始异常
     */
    public InvalidRecordException(String message, Throwable cause) {
        super(message, cause);
    }

}
