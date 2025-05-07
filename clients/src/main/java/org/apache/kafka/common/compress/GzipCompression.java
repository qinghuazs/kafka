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
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ChunkedBytesStream;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;

import static org.apache.kafka.common.record.CompressionType.GZIP;

/**
 * GZIP压缩编解码器实现类
 * 实现了基于GZIP算法的压缩和解压缩功能。
 * 
 * 应用场景：
 * 1. 消息批次压缩：减少网络传输和存储开销
 * 2. 大数据压缩：处理大量文本或二进制数据
 * 3. 网络传输优化：减少带宽使用
 * 4. 存储空间优化：减少磁盘使用
 */
public class GzipCompression implements Compression {

    /**
     * GZIP压缩级别
     * 控制压缩率和性能的平衡
     * - 较低的级别：更快的压缩速度，较低的压缩率
     * - 较高的级别：更高的压缩率，较慢的压缩速度
     */
    private final int level;

    /**
     * 私有构造函数
     * 创建指定压缩级别的GZIP压缩器
     *
     * @param level 压缩级别
     */
    private GzipCompression(int level) {
        // 初始化压缩级别
        this.level = level;
    }

    /**
     * 获取压缩类型
     *
     * @return GZIP压缩类型枚举值
     */
    @Override
    public CompressionType type() {
        // 返回GZIP压缩类型
        return GZIP;
    }

    /**
     * 创建用于压缩数据的输出流
     * 
     * 实现细节：
     * 1. 使用16KB的输入缓冲区（未压缩数据）
     * 2. 使用8KB的输出缓冲区（压缩数据）
     * 3. 针对小数据量写入进行优化
     *
     * @param buffer 用于写入压缩数据的缓冲输出流
     * @param messageVersion 消息格式版本
     * @return 包装后的输出流
     * @throws KafkaException 如果创建输出流时发生错误
     */
    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
        try {
            // 创建GzipOutputStream并用BufferedOutputStream包装
            // - 压缩输出缓冲区设置为8KB（默认0.5KB）
            // - 未压缩输入缓冲区设置为16KB（默认无缓冲）
            // 这些设置确保在写入小量数据时也能保持合理的性能
            return new BufferedOutputStream(new GzipOutputStream(buffer, 8 * 1024, level), 16 * 1024);
        } catch (Exception e) {
            // 将所有异常包装为KafkaException
            throw new KafkaException(e);
        }
    }

    /**
     * 创建用于解压缩数据的输入流
     * 
     * 实现细节：
     * 1. 使用8KB的输入缓冲区（压缩数据）
     * 2. 使用可配置大小的输出缓冲区（未压缩数据）
     * 3. 支持分块解压缩
     *
     * @param buffer 包含压缩数据的ByteBuffer
     * @param messageVersion 消息格式版本
     * @param decompressionBufferSupplier 解压缩缓冲区提供者
     * @return 包装后的输入流
     * @throws KafkaException 如果创建输入流时发生错误
     */
    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        try {
            // 创建GZIPInputStream并用ChunkedBytesStream包装
            // - 压缩输入缓冲区设置为8KB（默认0.5KB）
            // - 使用ChunkedBytesStream支持分块解压缩
            // - 未压缩输出缓冲区大小由decompressionOutputSize()提供
            return new ChunkedBytesStream(new GZIPInputStream(new ByteBufferInputStream(buffer), 8 * 1024),
                                          decompressionBufferSupplier,
                                          decompressionOutputSize(),
                                          false);
        } catch (Exception e) {
            // 将所有异常包装为KafkaException
            throw new KafkaException(e);
        }
    }

    /**
     * 获取解压缩输出缓冲区的推荐大小
     * 
     * @return 推荐的缓冲区大小（16KB）
     */
    @Override
    public int decompressionOutputSize() {
        // 返回16KB作为解压缩缓冲区大小
        // 这个值基于历史实现选择（参考PR #6785）
        return 16 * 1024;
    }

    /**
     * GzipCompression构建器类
     * 用于创建GzipCompression实例
     * 
     * 应用场景：
     * 1. 自定义压缩级别
     * 2. 灵活配置压缩器
     * 3. 支持链式调用
     */
    public static class Builder implements Compression.Builder<GzipCompression> {
        /**
         * 压缩级别，默认使用GZIP的默认级别
         */
        private int level = GZIP.defaultLevel();

        /**
         * 设置压缩级别
         *
         * @param level 要设置的压缩级别
         * @return 构建器实例，支持链式调用
         * @throws IllegalArgumentException 如果压缩级别无效
         */
        public Builder level(int level) {
            // 验证压缩级别是否在有效范围内
            // 允许使用默认级别或在最小和最大级别之间的值
            if ((level < GZIP.minLevel() || GZIP.maxLevel() < level) && level != GZIP.defaultLevel()) {
                throw new IllegalArgumentException("gzip doesn't support given compression level: " + level);
            }

            // 设置压缩级别并返回this以支持链式调用
            this.level = level;
            return this;
        }

        /**
         * 构建GzipCompression实例
         *
         * @return 新的GzipCompression实例
         */
        @Override
        public GzipCompression build() {
            // 创建并返回新的GzipCompression实例
            return new GzipCompression(level);
        }
    }
}
