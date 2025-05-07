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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ChunkedBytesStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.CompressionType.LZ4;

/**
 * LZ4压缩编解码器实现类
 * 实现了基于LZ4算法的压缩和解压缩功能。
 * 
 * 应用场景：
 * 1. 高性能消息压缩：LZ4以其快速的压缩和解压缩速度著称
 * 2. 实时数据处理：适用于对延迟敏感的场景
 * 3. 大批量消息处理：在保持较好压缩率的同时提供极快的解压速度
 * 4. 内存敏感场景：通过压缩减少内存和带宽使用
 *
 * 设计考虑：
 * 1. 可配置压缩级别：平衡压缩率和性能
 * 2. 版本兼容性：支持不同的消息格式版本
 * 3. 分块处理：优化内存使用和处理效率
 * 4. 异常处理：统一的异常转换机制
 */
public class Lz4Compression implements Compression {

    /**
     * LZ4压缩级别
     * 控制压缩率和性能的平衡
     * - 较低的级别：更快的压缩速度，较低的压缩率
     * - 较高的级别：更高的压缩率，较慢的压缩速度
     */
    private final int level;

    /**
     * 私有构造函数
     * 创建指定压缩级别的LZ4压缩器
     *
     * @param level 压缩级别
     */
    private Lz4Compression(int level) {
        // 初始化压缩级别
        this.level = level;
    }

    /**
     * 获取压缩类型
     *
     * @return LZ4压缩类型枚举值
     */
    @Override
    public CompressionType type() {
        // 返回LZ4压缩类型
        return LZ4;
    }

    /**
     * 创建用于压缩数据的输出流
     * 
     * 实现细节：
     * 1. 使用Lz4BlockOutputStream进行分块压缩
     * 2. 支持旧版消息格式（MAGIC_VALUE_V0）
     * 3. 应用指定的压缩级别
     *
     * @param buffer 用于写入压缩数据的缓冲输出流
     * @param messageVersion 消息格式版本
     * @return 包装后的输出流
     * @throws KafkaException 如果创建输出流时发生错误
     */
    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
        try {
            // 创建新的LZ4分块输出流，根据消息版本决定是否使用旧格式
            return new Lz4BlockOutputStream(buffer, level, messageVersion == RecordBatch.MAGIC_VALUE_V0);
        } catch (Throwable e) {
            // 将所有异常包装为KafkaException
            throw new KafkaException(e);
        }
    }

    /**
     * 创建用于解压缩数据的输入流
     * 
     * 实现细节：
     * 1. 使用Lz4BlockInputStream进行分块解压缩
     * 2. 通过ChunkedBytesStream支持分块读取
     * 3. 使用提供的缓冲区供应器优化内存使用
     *
     * @param inputBuffer 包含压缩数据的ByteBuffer
     * @param messageVersion 消息格式版本
     * @param decompressionBufferSupplier 解压缩缓冲区提供者
     * @return 包装后的输入流
     * @throws KafkaException 如果创建输入流时发生错误
     */
    @Override
    public InputStream wrapForInput(ByteBuffer inputBuffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        try {
            // 创建LZ4分块输入流并用ChunkedBytesStream包装
            // 支持分块读取和缓冲区复用
            return new ChunkedBytesStream(
                    new Lz4BlockInputStream(inputBuffer, decompressionBufferSupplier, messageVersion == RecordBatch.MAGIC_VALUE_V0),
                    decompressionBufferSupplier, decompressionOutputSize(), true);
        } catch (Throwable e) {
            // 将所有异常包装为KafkaException
            throw new KafkaException(e);
        }
    }

    /**
     * 获取解压缩输出缓冲区的推荐大小
     * 
     * 实现说明：
     * - 返回2KB作为解压缩缓冲区大小
     * - 基于历史实现和skipArray机制选择的大小
     * - 参考PR #6785的实现
     *
     * @return 推荐的缓冲区大小（2KB）
     */
    @Override
    public int decompressionOutputSize() {
        // 返回2KB作为内部中间缓冲区大小
        // 这个大小基于历史实现中的skipArray机制
        return 2 * 1024; // 2KB
    }

    /**
     * LZ4压缩构建器类
     * 用于创建LZ4Compression实例
     * 
     * 应用场景：
     * 1. 自定义压缩级别
     * 2. 灵活配置压缩器
     * 3. 支持链式调用
     */
    public static class Builder implements Compression.Builder<Lz4Compression> {
        /**
         * 压缩级别，默认使用LZ4的默认级别
         */
        private int level = LZ4.defaultLevel();

        /**
         * 设置压缩级别
         *
         * @param level 要设置的压缩级别
         * @return 构建器实例，支持链式调用
         * @throws IllegalArgumentException 如果压缩级别无效
         */
        public Builder level(int level) {
            // 验证压缩级别是否在有效范围内
            if (level < LZ4.minLevel() || LZ4.maxLevel() < level) {
                throw new IllegalArgumentException("lz4 doesn't support given compression level: " + level);
            }

            // 设置压缩级别并返回this以支持链式调用
            this.level = level;
            return this;
        }

        /**
         * 构建LZ4Compression实例
         *
         * @return 新的LZ4Compression实例
         */
        @Override
        public Lz4Compression build() {
            // 创建并返回新的LZ4Compression实例
            return new Lz4Compression(level);
        }
    }
}
