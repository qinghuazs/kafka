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

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.CompressionType.ZSTD;

/**
 * Zstd压缩编解码器实现类
 * 实现了基于Facebook的Zstandard(zstd)算法的压缩和解压缩功能。
 * 
 * 应用场景：
 * 1. 高压缩率需求：Zstd提供比gzip更好的压缩率
 * 2. 快速压缩场景：在保持高压缩率的同时提供较快的压缩速度
 * 3. 大规模数据处理：适用于需要平衡压缩率和性能的场景
 * 4. 实时数据压缩：支持流式处理和分块压缩
 *
 * 设计考虑：
 * 1. 可配置压缩级别：平衡压缩率和性能
 * 2. 缓冲区优化：针对小数据量写入进行优化
 * 3. JNI调用优化：减少跨JNI边界的调用次数
 * 4. 内存复用：通过BufferSupplier复用缓冲区
 */
public class ZstdCompression implements Compression {

    /**
     * Zstd压缩级别
     * 控制压缩率和性能的平衡
     * - 较低的级别：更快的压缩速度，较低的压缩率
     * - 较高的级别：更高的压缩率，较慢的压缩速度
     */
    private final int level;

    /**
     * 私有构造函数
     * 创建指定压缩级别的Zstd压缩器
     *
     * @param level 压缩级别
     */
    private ZstdCompression(int level) {
        // 初始化压缩级别
        this.level = level;
    }

    /**
     * 获取压缩类型
     *
     * @return Zstd压缩类型枚举值
     */
    @Override
    public CompressionType type() {
        // 返回ZSTD压缩类型
        return ZSTD;
    }

    /**
     * 创建用于压缩数据的输出流
     * 
     * 实现细节：
     * 1. 使用16KB的输入缓冲区优化小数据量写入
     * 2. 使用RecyclingBufferPool复用缓冲区
     * 3. 支持可配置的压缩级别
     *
     * @param bufferStream 用于写入压缩数据的缓冲输出流
     * @param messageVersion 消息格式版本（此处未使用）
     * @return 包装后的输出流
     * @throws KafkaException 如果创建输出流时发生错误
     */
    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion) {
        try {
            // 创建ZstdOutputStream并用BufferedOutputStream包装
            // - 使用RecyclingBufferPool.INSTANCE作为缓冲区池
            // - 设置指定的压缩级别
            // - 使用16KB的输入缓冲区优化小数据量写入性能
            return new BufferedOutputStream(new ZstdOutputStreamNoFinalizer(bufferStream, RecyclingBufferPool.INSTANCE, level), 16 * 1024);
        } catch (Throwable e) {
            // 将所有异常包装为KafkaException
            throw new KafkaException(e);
        }
    }

    /**
     * 创建用于解压缩数据的输入流
     * 
     * 实现细节：
     * 1. 使用ChunkedBytesStream支持分块读取
     * 2. 通过wrapForZstdInput创建基础解压缩流
     * 3. 使用提供的缓冲区供应器优化内存使用
     *
     * @param buffer 包含压缩数据的ByteBuffer
     * @param messageVersion 消息格式版本（此处未使用）
     * @param decompressionBufferSupplier 解压缩缓冲区提供者
     * @return 包装后的输入流
     * @throws KafkaException 如果创建输入流时发生错误
     */
    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        try {
            // 创建ChunkedBytesStream包装Zstd输入流
            // - 支持分块读取
            // - 使用提供的缓冲区供应器
            // - 设置推荐的解压缩缓冲区大小
            return new ChunkedBytesStream(wrapForZstdInput(buffer, decompressionBufferSupplier),
                    decompressionBufferSupplier,
                    decompressionOutputSize(),
                    false);
        } catch (Throwable e) {
            // 将所有异常包装为KafkaException
            throw new KafkaException(e);
        }
    }

    /**
     * 创建Zstd解压缩输入流
     * 用于测试的可见方法
     * 
     * 实现细节：
     * 1. 创建自定义BufferPool避免锁和软引用
     * 2. 优化JNI调用，建议批量读取数据
     * 3. 支持缓冲区的获取和释放
     *
     * @param buffer 压缩数据缓冲区
     * @param decompressionBufferSupplier 解压缩缓冲区提供者
     * @return Zstd输入流
     * @throws IOException 如果创建输入流时发生IO错误
     */
    public static ZstdInputStreamNoFinalizer wrapForZstdInput(ByteBuffer buffer, BufferSupplier decompressionBufferSupplier) throws IOException {
        // 创建自定义BufferPool，使用提供的BufferSupplier
        // 避免使用zstd-jni的RecyclingBufferPool，因为它需要锁和软引用
        final BufferPool bufferPool = new BufferPool() {
            @Override
            public ByteBuffer get(int capacity) {
                // 从供应器获取指定容量的缓冲区
                return decompressionBufferSupplier.get(capacity);
            }

            @Override
            public void release(ByteBuffer buffer) {
                // 释放缓冲区回供应器
                decompressionBufferSupplier.release(buffer);
            }
        };
        
        // 创建ZstdInputStreamNoFinalizer
        // 建议批量读取数据以减少JNI调用次数
        return new ZstdInputStreamNoFinalizer(new ByteBufferInputStream(buffer), bufferPool);
    }

    /**
     * 获取解压缩输出缓冲区的推荐大小
     * 此大小必须小于等于ZSTD_BLOCKSIZE_MAX
     * 
     * 实现说明：
     * - 返回16KB作为解压缩缓冲区大小
     * - 基于历史实现选择的大小
     * - 参考PR #6785的实现
     *
     * @return 推荐的缓冲区大小（16KB）
     */
    @Override
    public int decompressionOutputSize() {
        // 返回16KB作为解压缩缓冲区大小
        return 16 * 1024;
    }

    /**
     * Zstd压缩构建器类
     * 用于创建ZstdCompression实例
     * 
     * 应用场景：
     * 1. 自定义压缩级别
     * 2. 灵活配置压缩器
     * 3. 支持链式调用
     */
    public static class Builder implements Compression.Builder<ZstdCompression> {
        /**
         * 压缩级别，默认使用ZSTD的默认级别
         */
        private int level = ZSTD.defaultLevel();

        /**
         * 设置压缩级别
         *
         * @param level 要设置的压缩级别
         * @return 构建器实例，支持链式调用
         * @throws IllegalArgumentException 如果压缩级别无效
         */
        public Builder level(int level) {
            // 验证压缩级别是否在有效范围内
            if (level < ZSTD.minLevel() || ZSTD.maxLevel() < level) {
                throw new IllegalArgumentException("zstd doesn't support given compression level: " + level);
            }

            // 设置压缩级别并返回this以支持链式调用
            this.level = level;
            return this;
        }

        /**
         * 构建ZstdCompression实例
         *
         * @return 新的ZstdCompression实例
         */
        @Override
        public ZstdCompression build() {
            // 创建并返回新的ZstdCompression实例
            return new ZstdCompression(level);
        }
    }
}
