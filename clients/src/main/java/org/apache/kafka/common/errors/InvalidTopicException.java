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

import java.util.HashSet;
import java.util.Set;


/**
 * 客户端尝试对无效主题执行操作时抛出的异常。
 * 
 * 应用场景：
 * 1. 当主题名称过长或包含无效字符时
 * 2. 当主题名称不符合Kafka的命名规范时
 * 3. 当尝试在不合法的主题上执行生产或消费操作时
 * 
 * 设计考虑：
 * 1. 继承自ApiException，表示这是一个不可重试的异常
 * 2. 维护一个无效主题集合（invalidTopics），用于记录所有检测到的无效主题
 * 3. 提供多个构造方法，支持不同的异常创建场景
 * 4. 与UnknownTopicOrPartitionException区分，后者表示主题不存在而非无效
 * 
 * 注意：这个异常是不可重试的，因为无效的主题名称不会自动变为有效
 * 
 * @see UnknownTopicOrPartitionException
 */
public class InvalidTopicException extends ApiException {
    private static final long serialVersionUID = 1L;

    private final Set<String> invalidTopics;

    public InvalidTopicException() {
        super();
        invalidTopics = new HashSet<>();
    }

    public InvalidTopicException(String message, Throwable cause) {
        super(message, cause);
        invalidTopics = new HashSet<>();
    }

    public InvalidTopicException(String message) {
        super(message);
        invalidTopics = new HashSet<>();
    }

    public InvalidTopicException(Throwable cause) {
        super(cause);
        invalidTopics = new HashSet<>();
    }

    public InvalidTopicException(Set<String> invalidTopics) {
        super("Invalid topics: " + invalidTopics);
        this.invalidTopics = invalidTopics;
    }

    public InvalidTopicException(String message, Set<String> invalidTopics) {
        super(message);
        this.invalidTopics = invalidTopics;
    }

    public Set<String> invalidTopics() {
        return invalidTopics;
    }
}
