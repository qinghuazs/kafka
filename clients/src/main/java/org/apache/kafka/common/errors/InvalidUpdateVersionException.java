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
 * 无效更新版本异常
 * 
 * 当客户端尝试使用不兼容或不支持的版本号进行更新操作时抛出此异常。
 * 
 * 应用场景：
 * 1. 版本兼容性检查：确保客户端和服务器之间的版本兼容
 * 2. API版本管理：防止使用过时或不支持的API版本进行更新
 * 3. 集群升级：在集群滚动升级过程中处理版本不匹配的情况
 * 
 * 设计考虑：
 * 1. 继承自ApiException，用于处理API版本相关的异常情况
 * 2. 提供多个构造方法，支持不同的异常信息传递方式
 * 3. 作为版本控制机制的一部分，确保系统的稳定性和兼容性
 */
public class InvalidUpdateVersionException extends ApiException {

    public InvalidUpdateVersionException(String message) {
        super(message);
    }

    public InvalidUpdateVersionException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
