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

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Kafka协议中请求的头部信息
 * 该类负责管理和处理Kafka请求的元数据，包括API版本、客户端标识、请求版本等关键信息
 */
public class RequestHeader implements AbstractRequestResponse {
    // 表示请求头大小未初始化的常量值
    private static final int SIZE_NOT_INITIALIZED = -1;
    // 存储请求头的具体数据
    private final RequestHeaderData data;
    // 请求头的版本号
    private final short headerVersion;
    // 请求头的字节大小，初始为未初始化状态
    private int size = SIZE_NOT_INITIALIZED;

    /**
     * 创建一个新的请求头
     * @param requestApiKey 请求的API类型
     * @param requestVersion 请求的API版本
     * @param clientId 客户端标识符
     * @param correlationId 请求的关联ID，用于匹配请求和响应
     */
    public RequestHeader(ApiKeys requestApiKey, short requestVersion, String clientId, int correlationId) {
        this(new RequestHeaderData(). // 创建新的请求头数据对象
                setRequestApiKey(requestApiKey.id). // 设置API类型ID
                setRequestApiVersion(requestVersion). // 设置API版本
                setClientId(clientId). // 设置客户端ID
                setCorrelationId(correlationId), // 设置关联ID
            requestApiKey.requestHeaderVersion(requestVersion)); // 根据请求版本获取对应的头部版本
    }

    /**
     * 使用已有的请求头数据创建请求头
     * @param data 请求头数据
     * @param headerVersion 头部版本号
     */
    public RequestHeader(RequestHeaderData data, short headerVersion) {
        this.data = data; // 存储请求头数据
        this.headerVersion = headerVersion; // 设置头部版本
    }

    /**
     * 获取请求的API类型
     * @return 返回对应的ApiKeys枚举值
     */
    public ApiKeys apiKey() {
        return ApiKeys.forId(data.requestApiKey()); // 根据API ID获取对应的ApiKeys枚举
    }

    /**
     * 获取API版本号
     * @return 返回API的版本号
     */
    public short apiVersion() {
        return data.requestApiVersion(); // 返回请求的API版本
    }

    /**
     * 获取请求头的版本号
     * @return 返回请求头的版本号
     */
    public short headerVersion() {
        return headerVersion; // 返回头部版本号
    }

    /**
     * 获取客户端标识符
     * @return 返回客户端的ID
     */
    public String clientId() {
        return data.clientId(); // 返回客户端ID
    }

    /**
     * 获取请求的关联ID
     * @return 返回用于关联请求和响应的ID
     */
    public int correlationId() {
        return data.correlationId(); // 返回关联ID
    }

    /**
     * 获取原始的请求头数据
     * @return 返回RequestHeaderData对象
     */
    public RequestHeaderData data() {
        return data; // 返回请求头数据对象
    }

    // Visible for testing.
    /**
     * 将请求头写入ByteBuffer
     * @param buffer 目标缓冲区
     * @param serializationCache 序列化缓存对象
     */
    void write(ByteBuffer buffer, ObjectSerializationCache serializationCache) {
        data.write(new ByteBufferAccessor(buffer), serializationCache, headerVersion); // 将数据写入缓冲区
    }

    /**
     * Calculates the size of {@link RequestHeader} in bytes.
     *
     * This method to calculate size should be only when it is immediately followed by
     * {@link #write(ByteBuffer, ObjectSerializationCache)} method call. In such cases, ObjectSerializationCache
     * helps to avoid the serialization twice. In all other cases, {@link #size()} should be preferred instead.
     *
     * Calls to this method leads to calculation of size every time it is invoked. {@link #size()} should be preferred
     * instead.
     *
     * Visible for testing.
     */
    /**
     * 计算请求头的字节大小
     * @param serializationCache 序列化缓存对象
     * @return 返回请求头的字节大小
     */
    int size(ObjectSerializationCache serializationCache) {
        this.size = data.size(serializationCache, headerVersion); // 计算并缓存大小
        return size; // 返回计算的大小
    }

    /**
     * Returns the size of {@link RequestHeader} in bytes.
     *
     * Calls to this method are idempotent and inexpensive since it returns the cached value of size after the first
     * invocation.
     */
    /**
     * 获取请求头的字节大小，如果未计算则进行计算
     * @return 返回请求头的字节大小
     */
    public int size() {
        if (this.size == SIZE_NOT_INITIALIZED) { // 如果大小未初始化
            this.size = size(new ObjectSerializationCache()); // 计算大小
        }
        return size; // 返回大小
    }

    /**
     * 检查当前API版本是否受支持
     * @return 如果API版本受支持返回true，否则返回false
     */
    public boolean isApiVersionSupported() {
        return apiKey().isVersionSupported(apiVersion()); // 检查API版本是否支持
    }

    /**
     * 检查当前API版本是否已废弃
     * @return 如果API版本已废弃返回true，否则返回false
     */
    public boolean isApiVersionDeprecated() {
        return apiKey().isVersionDeprecated(apiVersion()); // 检查API版本是否已废弃
    }

    /**
     * 创建对应的响应头
     * @return 返回与当前请求对应的ResponseHeader对象
     */
    public ResponseHeader toResponseHeader() {
        return new ResponseHeader(data.correlationId(), // 使用相同的关联ID
                apiKey().responseHeaderVersion(apiVersion())); // 获取对应的响应头版本
    }

    public static RequestHeader parse(ByteBuffer buffer) {
        short apiKeyId = -1;
        try {
            // We derive the header version from the request api version, so we read that first.
            // The request api version is part of `RequestHeaderData`, so we reset the buffer position after the read.
            int bufferStartPositionForHeader = buffer.position();
            apiKeyId = buffer.getShort();
            short apiVersion = buffer.getShort();
            ApiKeys apiKey = ApiKeys.forId(apiKeyId);

            // `apiKey.requestHeaderVersion` will fail if there are no valid versions - we do this check first in order to
            // provide a more helpful message
            if (!apiKey.hasValidVersion())
                throw new InvalidRequestException("Unsupported api with key " + apiKeyId + " (" + apiKey.name + ") and version " + apiVersion);

            short headerVersion = apiKey.requestHeaderVersion(apiVersion);
            buffer.position(bufferStartPositionForHeader);
            final RequestHeaderData headerData = new RequestHeaderData(new ByteBufferAccessor(buffer), headerVersion);
            // Due to a quirk in the protocol, client ID is marked as nullable.
            // However, we treat a null client ID as equivalent to an empty client ID.
            if (headerData.clientId() == null) {
                headerData.setClientId("");
            }
            final RequestHeader header = new RequestHeader(headerData, headerVersion);
            // Size of header is calculated by the shift in the position of buffer's start position during parsing.
            // Prior to parsing, the buffer's start position points to header data and after the parsing operation
            // the buffer's start position points to api message. For more information on how the buffer is
            // constructed, see RequestUtils#serialize()
            header.size = Math.max(buffer.position() - bufferStartPositionForHeader, 0);
            return header;
        } catch (UnsupportedVersionException e) {
            throw new InvalidRequestException("Unknown API key " + apiKeyId, e);
        } catch (InvalidRequestException e) {
            throw e;
        } catch (Throwable ex) {
            throw new InvalidRequestException("Error parsing request header. Our best guess of the apiKeyId is: " +
                    apiKeyId, ex);
        }
    }

    @Override
    public String toString() {
        return "RequestHeader(apiKey=" + apiKey() +
                ", apiVersion=" + apiVersion() +
                ", clientId=" + clientId() +
                ", correlationId=" + correlationId() +
                ", headerVersion=" + headerVersion +
                ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestHeader that = (RequestHeader) o;
        return headerVersion == that.headerVersion &&
            Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, headerVersion);
    }
}
