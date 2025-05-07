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
 * 主题ID不一致异常
 * 
 * 该异常在以下场景中抛出：
 * 1. 当客户端缓存的主题ID与服务器端的不匹配时
 * 2. 当主题被删除后重新创建，导致主题ID发生变化时
 * 3. 当不同的broker报告了同一主题的不同ID时
 * 
 * 主题ID的作用：
 * - 唯一标识Kafka集群中的主题
 * - 在主题重命名场景中保持一致性
 * - 帮助检测主题的重建和更改
 */
public class InconsistentTopicIdException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    /**
     * 使用指定的错误消息构造异常
     * 
     * @param message 描述主题ID不一致问题的详细信息
     */
    public InconsistentTopicIdException(String message) {
        super(message);
    }

}
