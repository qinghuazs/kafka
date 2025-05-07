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
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * 压缩编解码器接口
 * 定义了Kafka中数据压缩和解压缩的标准接口。
 * 
 * 应用场景：
 * 1. 消息批次压缩：减少网络传输和存储开销
 * 2. 消息解压缩：读取压缩的消息数据
 * 3. 支持多种压缩算法：GZIP、Snappy、LZ4、ZSTD等
 * 4. 压缩算法动态选择：根据配置选择合适的压缩方式
 */
public interface Compression {

    /**
     * 获取此压缩编解码器的压缩类型
     *
     * @return 压缩类型枚举值
     */
    CompressionType type();

    /**
     * 使用当前压缩算法包装输出流，用于压缩数据
     * 
     * 实现要求：
     * 1. 支持动态扩容：在写入过程中可能需要扩展底层缓冲区
     * 2. 保持缓冲区访问：压缩后仍需访问底层缓冲区
     * 3. 版本兼容：支持不同的消息格式版本
     *
     * @param bufferStream 用于写入压缩数据的缓冲输出流
     * @param messageVersion 使用的记录格式版本
     * @return 包装后的输出流，用于写入要压缩的数据
     */
    OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion);

    /**
     * 使用当前压缩算法包装输入缓冲区，用于解压缩数据
     * 
     * 实现要求：
     * 1. 缓冲区复用：通过supplier重用解压缩缓冲区
     * 2. 性能优化：避免小批量记录分配大缓冲区
     * 3. 版本兼容：支持不同的消息格式版本
     *
     * @param buffer 包含待解压缩数据的ByteBuffer实例
     * @param messageVersion 使用的记录格式版本
     * @param decompressionBufferSupplier 解压缩缓冲区的提供者
     * @return 包装后的输入流，用于读取解压缩的数据
     */
    InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier);

    /**
     * 获取存储解压缩输出的推荐缓冲区大小
     * 
     * 默认实现：
     * - 抛出UnsupportedOperationException，表示未定义推荐大小
     * - 子类可以覆盖此方法提供具体的推荐值
     *
     * @return 推荐的缓冲区大小（字节）
     * @throws UnsupportedOperationException 如果压缩类型未定义推荐大小
     */
    default int decompressionOutputSize() {
        // 对于未指定推荐解压缩缓冲区大小的压缩类型，抛出异常
        throw new UnsupportedOperationException("Size of decompression buffer is not defined for this compression type=" + type().name);
    }

    /**
     * 压缩编解码器构建器接口
     * 用于创建具体的压缩编解码器实例
     *
     * @param <T> 具体的压缩编解码器类型
     */
    interface Builder<T extends Compression> {
        /**
         * 构建压缩编解码器实例
         *
         * @return 新的压缩编解码器实例
         */
        T build();
    }

    /**
     * 根据压缩算法名称创建对应的构建器
     *
     * @param compressionName 压缩算法名称
     * @return 对应的压缩编解码器构建器
     */
    static Builder<? extends Compression> of(final String compressionName) {
        // 将压缩算法名称转换为对应的压缩类型枚举
        CompressionType compressionType = CompressionType.forName(compressionName);
        // 调用重载方法创建构建器
        return of(compressionType);
    }

    /**
     * 根据压缩类型创建对应的构建器
     *
     * @param compressionType 压缩类型枚举值
     * @return 对应的压缩编解码器构建器
     * @throws IllegalArgumentException 如果压缩类型未知
     */
    static Builder<? extends Compression> of(final CompressionType compressionType) {
        // 根据压缩类型选择对应的构建器
        switch (compressionType) {
            case NONE:
                return none();
            case GZIP:
                return gzip();
            case SNAPPY:
                return snappy();
            case LZ4:
                return lz4();
            case ZSTD:
                return zstd();
            default:
                // 对于未知的压缩类型抛出异常
                throw new IllegalArgumentException("Unknown compression type: " + compressionType.name);
        }
    }

    /**
     * 无压缩编解码器的单例实例
     */
    NoCompression NONE = none().build();

    /**
     * 创建无压缩编解码器的构建器
     *
     * @return 无压缩编解码器构建器
     */
    static NoCompression.Builder none() {
        return new NoCompression.Builder();
    }

    /**
     * 创建GZIP压缩编解码器的构建器
     *
     * @return GZIP压缩编解码器构建器
     */
    static GzipCompression.Builder gzip() {
        return new GzipCompression.Builder();
    }

    /**
     * 创建Snappy压缩编解码器的构建器
     *
     * @return Snappy压缩编解码器构建器
     */
    static SnappyCompression.Builder snappy() {
        return new SnappyCompression.Builder();
    }

    /**
     * 创建LZ4压缩编解码器的构建器
     *
     * @return LZ4压缩编解码器构建器
     */
    static Lz4Compression.Builder lz4() {
        return new Lz4Compression.Builder();
    }

    /**
     * 创建ZSTD压缩编解码器的构建器
     *
     * @return ZSTD压缩编解码器构建器
     */
    static ZstdCompression.Builder zstd() {
        return new ZstdCompression.Builder();
    }
}
