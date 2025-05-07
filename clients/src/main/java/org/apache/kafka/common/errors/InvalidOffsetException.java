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
 * 当分区集合的偏移量无效（未定义或超出范围）且未配置重置策略时抛出此异常。
 * 
 * 应用场景：
 * 1. 消费者组件在拉取消息时，如果请求的偏移量不存在或已过期被删除
 * 2. 手动设置消费者偏移量时指定了无效值
 * 3. 消费者组重平衡后获取到无效的偏移量
 * 
 * 设计考虑：
 * - 区别于OffsetOutOfRangeException，本异常表示更严重的错误状态，因为没有可用的重置策略
 * - 帮助开发者及时发现消费者配置问题或数据一致性问题
 * 
 * @see OffsetOutOfRangeException 对比此异常，它支持通过配置auto.offset.reset来恢复
 */
public class InvalidOffsetException extends ApiException {

    private static final long serialVersionUID = 1L;

    public InvalidOffsetException(String message) {
        super(message);
    }

    public InvalidOffsetException(String message, Throwable cause) {
        super(message, cause);
    }

}
