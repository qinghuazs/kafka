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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;

/**
 * 查找协调器请求类，用于查找消费者组协调器或事务协调器。
 * 该请求支持两种主要场景：
 * 1. 消费者组场景：查找管理消费者组的协调器节点
 * 2. 事务场景：查找管理事务的协调器节点
 * 
 * 版本兼容说明：
 * - v0-v3：仅支持单个协调器查找
 * - v4及以上：支持批量查找多个协调器
 */
public class FindCoordinatorRequest extends AbstractRequest {

    /**
     * 支持批量查找协调器的最小版本号
     * 从该版本开始支持在一个请求中查找多个协调器
     */
    public static final short MIN_BATCHED_VERSION = 4;

    /**
     * FindCoordinatorRequest的构建器类
     * 用于创建FindCoordinatorRequest实例，处理版本兼容性逻辑
     */
    public static class Builder extends AbstractRequest.Builder<FindCoordinatorRequest> {
        private final FindCoordinatorRequestData data;

        public Builder(FindCoordinatorRequestData data) {
            super(ApiKeys.FIND_COORDINATOR);
            this.data = data;
        }

        /**
         * 构建FindCoordinatorRequest实例
         * @param version 协议版本号
         * @return FindCoordinatorRequest实例
         * 
         * 实现说明：
         * 1. 检查事务协调器类型的版本兼容性
         * 2. 处理批量查找的版本兼容性：
         *   - 对于旧版本：将批量请求转换为单个请求
         *   - 对于新版本：支持批量查找
         */
        @Override
        public FindCoordinatorRequest build(short version) {
            // 检查事务协调器的版本兼容性
            if (version < 1 && data.keyType() == CoordinatorType.TRANSACTION.id()) {
                throw new UnsupportedVersionException("Cannot create a v" + version + " FindCoordinator request " +
                        "because we require features supported only in 2 or later.");
            }
            // 获取批量查找的key数量
            int batchedKeys = data.coordinatorKeys().size();
            if (version < MIN_BATCHED_VERSION) {
                // 旧版本不支持批量查找
                if (batchedKeys > 1)
                    throw new NoBatchedFindCoordinatorsException("Cannot create a v" + version + " FindCoordinator request " +
                        "because we require features supported only in " + MIN_BATCHED_VERSION + " or later.");
                // 如果只有一个key，转换为单个查找请求
                if (batchedKeys == 1) {
                    data.setKey(data.coordinatorKeys().get(0));
                    data.setCoordinatorKeys(Collections.emptyList());
                }
            } else if (batchedKeys == 0 && data.key() != null) {
                // 新版本：将单个key转换为批量查找格式
                data.setCoordinatorKeys(Collections.singletonList(data.key()));
                data.setKey(""); // default value
            }
            return new FindCoordinatorRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }

        public FindCoordinatorRequestData data() {
            return data;
        }
    }

    /**
     * 表示不支持批量查找协调器的异常
     * 当使用旧版本协议尝试批量查找协调器时抛出此异常
     * 在这种情况下，需要逐个查找协调器
     */
    public static class NoBatchedFindCoordinatorsException extends UnsupportedVersionException {
        private static final long serialVersionUID = 1L;

        public NoBatchedFindCoordinatorsException(String message) {
            super(message);
        }
    }

    /**
     * 请求的数据内容
     * 包含查找协调器所需的所有信息，如key类型、协调器keys等
     */
    private final FindCoordinatorRequestData data;

    /**
     * 构造函数
     * @param data 请求数据
     * @param version 协议版本
     */
    private FindCoordinatorRequest(FindCoordinatorRequestData data, short version) {
        super(ApiKeys.FIND_COORDINATOR, version);
        this.data = data;
    }

    /**
     * 生成错误响应
     * @param throttleTimeMs 限流时间（毫秒）
     * @param e 异常
     * @return 错误响应
     * 
     * 实现说明：
     * 1. 根据版本处理限流时间
     * 2. 根据版本生成不同格式的错误响应：
     *   - 旧版本：生成单个错误响应
     *   - 新版本：生成批量错误响应
     */
    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        FindCoordinatorResponseData response = new FindCoordinatorResponseData();
        // 版本2及以上支持限流
        if (version() >= 2) {
            response.setThrottleTimeMs(throttleTimeMs);
        }
        Errors error = Errors.forException(e);
        // 根据版本生成不同格式的错误响应
        if (version() < MIN_BATCHED_VERSION) {
            return FindCoordinatorResponse.prepareOldResponse(error, Node.noNode());
        } else {
            return FindCoordinatorResponse.prepareErrorResponse(error, data.coordinatorKeys());
        }
    }

    /**
     * 从ByteBuffer解析请求
     * @param buffer 包含序列化请求数据的buffer
     * @param version 协议版本
     * @return FindCoordinatorRequest实例
     */
    public static FindCoordinatorRequest parse(ByteBuffer buffer, short version) {
        return new FindCoordinatorRequest(new FindCoordinatorRequestData(new ByteBufferAccessor(buffer), version),
            version);
    }

    @Override
    public FindCoordinatorRequestData data() {
        return data;
    }

    /**
     * 协调器类型枚举
     * 定义了Kafka支持的不同类型的协调器：
     * - GROUP: 消费者组协调器，负责管理消费者组的成员和分区分配
     * - TRANSACTION: 事务协调器，负责管理事务的状态和提交/回滚操作
     * - SHARE: 共享资源协调器
     */
    public enum CoordinatorType {
        GROUP((byte) 0),      // 消费者组协调器
        TRANSACTION((byte) 1), // 事务协调器
        SHARE((byte) 2);      // 共享资源协调器

        final byte id;

        CoordinatorType(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        /**
         * 根据ID获取协调器类型
         * @param id 协调器类型ID
         * @return 对应的协调器类型
         * @throws InvalidRequestException 当ID未知时抛出此异常
         */
        public static CoordinatorType forId(byte id) {
            switch (id) {
                case 0:
                    return GROUP;
                case 1:
                    return TRANSACTION;
                case 2:
                    return SHARE;
                default:
                    throw new InvalidRequestException("Unknown coordinator type received: " + id);
            }
        }
    }

}
