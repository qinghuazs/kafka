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
 * 当客户端尝试保存消费者偏移量（consumer offset）时，如果关联的元数据大小超过了服务器允许的最大值时，将抛出此异常。
 * 
 * 应用场景：
 * 1. 消费者组提交偏移量：当消费者组成员提交他们的偏移量时，可以携带自定义的元数据信息
 * 2. 自定义元数据：开发者可能会在偏移量中存储额外的跟踪信息或处理状态
 * 
 * 设计考虑：
 * 1. 服务器对元数据大小的限制是为了防止过大的元数据占用过多的存储空间
 * 2. 这个限制有助于维护集群的稳定性和性能
 * 3. 继承自ApiException表明这是一个客户端API使用相关的异常
 */
public class OffsetMetadataTooLarge extends ApiException {

    private static final long serialVersionUID = 1L;

    /**
     * 创建一个无参数的OffsetMetadataTooLarge异常实例
     */
    public OffsetMetadataTooLarge() {
    }

    /**
     * 使用指定的错误消息创建异常实例
     * @param message 描述异常的详细信息
     */
    public OffsetMetadataTooLarge(String message) {
        super(message);
    }

    /**
     * 使用导致此异常的原始异常创建异常实例
     * @param cause 导致此异常的原始异常
     */
    public OffsetMetadataTooLarge(Throwable cause) {
        super(cause);
    }

    /**
     * 使用错误消息和原始异常创建异常实例
     * @param message 描述异常的详细信息
     * @param cause 导致此异常的原始异常
     */
    public OffsetMetadataTooLarge(String message, Throwable cause) {
        super(message, cause);
    }

}
