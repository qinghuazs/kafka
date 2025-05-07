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
package org.apache.kafka.common.compress;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * 无压缩编解码器实现类
 * 实现了一个不进行任何压缩的编解码器，直接传递原始数据。
 * 
 * 应用场景：
 * 1. 数据已经是压缩格式（如图片、视频等）
 * 2. 小数据量传输，压缩开销大于收益
 * 3. CPU资源受限，需要避免压缩开销
 * 4. 调试和测试场景
 *
 * 设计考虑：
 * 1. 单例模式：所有实例共享相同行为
 * 2. 零开销实现：不引入任何额外处理
 * 3. 透明传递：保持数据原样不变
 * 4. 最小化内存使用：避免不必要的缓冲
 */
public class NoCompression implements Compression {

    /**
     * 私有构造函数
     * 防止外部直接创建实例，支持单例模式
     */
    private NoCompression() {}

    /**
     * 获取压缩类型
     * 
     * 实现说明：
     * - 返回NONE表示不进行压缩
     * - 用于标识数据的压缩状态
     *
     * @return 无压缩类型枚举值
     */
    @Override
    public CompressionType type() {
        // 返回NONE类型，表示不进行压缩
        return CompressionType.NONE;
    }

    /**
     * 包装输出流
     * 直接返回原始输出流，不进行任何包装
     * 
     * 实现说明：
     * - 零开销实现：直接返回输入的流
     * - 保持原始数据不变
     *
     * @param bufferStream 原始输出流
     * @param messageVersion 消息版本（此处未使用）
     * @return 原始输出流
     */
    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion) {
        // 直接返回原始输出流，不做任何包装
        return bufferStream;
    }

    /**
     * 包装输入缓冲区
     * 将ByteBuffer包装为InputStream，不进行解压缩
     * 
     * 实现说明：
     * - 最小化包装：仅转换接口类型
     * - 直接读取原始数据
     *
     * @param buffer 输入缓冲区
     * @param messageVersion 消息版本（此处未使用）
     * @param decompressionBufferSupplier 解压缩缓冲区提供者（此处未使用）
     * @return 包装后的输入流
     */
    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        // 创建ByteBufferInputStream直接读取原始数据
        return new ByteBufferInputStream(buffer);
    }

    /**
     * NoCompression构建器类
     * 用于创建NoCompression实例
     * 
     * 设计说明：
     * - 简单工厂模式：统一的实例创建接口
     * - 无配置选项：所有实例行为相同
     */
    public static class Builder implements Compression.Builder<NoCompression> {

        /**
         * 构建NoCompression实例
         * 
         * 实现说明：
         * - 创建新的NoCompression实例
         * - 支持工厂方法模式
         *
         * @return 新的NoCompression实例
         */
        @Override
        public NoCompression build() {
            // 创建并返回新的NoCompression实例
            return new NoCompression();
        }
    }
}
