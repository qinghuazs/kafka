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
 * 当请求的客户端不支持指定分区的压缩类型时抛出此异常。
 * 
 * 应用场景：
 * 1. 消费者尝试读取使用了不支持压缩算法的消息
 * 2. 生产者配置了客户端库不支持的压缩方式
 * 3. 不同版本客户端之间的压缩算法兼容性问题
 * 
 * 设计考虑：
 * - 及早发现压缩配置问题，避免运行时数据处理失败
 * - 确保生产者和消费者使用兼容的压缩方式
 * - 帮助用户正确配置消息压缩策略
 */
public class UnsupportedCompressionTypeException extends ApiException {

    private static final long serialVersionUID = 1L;

    public UnsupportedCompressionTypeException(String message) {
        super(message);
    }

    public UnsupportedCompressionTypeException(String message, Throwable cause) {
        super(message, cause);
    }

}
