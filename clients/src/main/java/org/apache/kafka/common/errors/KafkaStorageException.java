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
 * Kafka存储异常
 * 
 * 当处理请求时发生与磁盘相关的IOException时抛出此异常。
 * 客户端在收到KafkaStorageException时应该请求更新元数据并进行重试。
 * 
 * 异常处理指南：
 * 1. 服务器日志加载阶段：
 *    - 如果服务器尚未完成日志加载，IOException无需转换为KafkaStorageException
 *    - 这样可以区分启动阶段和运行时的存储问题
 * 
 * 2. 服务器正常运行阶段：
 *    - 捕获IOException后，触发LogDirFailureChannel.maybeAddOfflineLogDir()
 *    - 可以选择记录并忽略IOException，或转换为KafkaStorageException重新抛出
 *    - 这有助于统一存储错误的处理方式
 * 
 * 3. 异常捕获位置：
 *    - 推荐在Log类中而不是在ReplicaManager或LogSegment中捕获IOException
 *    - 这样可以在更底层处理存储异常，提供更准确的错误信息
 * 
 * 应用场景：
 * 1. 磁盘故障检测：及时发现并处理磁盘读写错误
 * 2. 存储空间管理：处理磁盘空间不足等存储资源问题
 * 3. 日志管理：处理日志文件的创建、读写和删除等操作异常
 * 
 * 设计考虑：
 * 1. 继承自InvalidMetadataException，表明这可能需要更新元数据来解决
 * 2. 提供多个构造方法，支持不同的异常信息传递方式
 * 3. 包含序列化支持，确保在分布式环境中的异常传递
 */
public class KafkaStorageException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    public KafkaStorageException() {
        super();
    }

    public KafkaStorageException(String message) {
        super(message);
    }

    public KafkaStorageException(Throwable cause) {
        super(cause);
    }

    public KafkaStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
