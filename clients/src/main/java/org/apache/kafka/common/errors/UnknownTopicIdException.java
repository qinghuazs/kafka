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
 * 未知主题ID异常
 * 
 * 当客户端使用了一个broker无法识别的主题ID时抛出此异常。
 * 
 * 应用场景：
 * 1. 主题已被删除，但客户端仍在使用旧的主题ID
 * 2. 主题元数据在不同broker之间不同步
 * 3. 客户端使用了错误的主题ID进行操作
 * 
 * 错误处理：
 * 1. 客户端需要刷新主题元数据
 * 2. 重新获取有效的主题ID
 * 3. 验证主题是否仍然存在
 * 
 * 继承自InvalidMetadataException，表示这是一个元数据相关的错误
 */
public class UnknownTopicIdException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    /**
     * 创建一个未知主题ID异常
     * 
     * @param message 异常描述信息
     */
    public UnknownTopicIdException(String message) {
        super(message);
    }

}
