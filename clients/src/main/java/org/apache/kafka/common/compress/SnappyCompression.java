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

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Snappy压缩编解码器实现类
 * 实现了基于Google的Snappy算法的压缩和解压缩功能。
 * 
 * 应用场景：
 * 1. 高性能消息压缩：Snappy以其快速的压缩速度和合理的压缩率著称
 * 2. 实时数据处理：适用于对延迟敏感的场景
 * 3. CPU资源受限场景：Snappy的CPU开销较小
 * 4. 大批量消息处理：在保持较好压缩率的同时提供快速的压缩解压速度
 *
 * 设计考虑：
 * 1. 单例模式：所有实例共享相同行为
 * 2. 无状态设计：每个操作独立执行
 * 3. 异常处理：统一转换为KafkaException
 * 4. 缓冲区优化：自定义跳过缓冲区实现
 */
public class SnappyCompression implements Compression {

    /**
     * 私有构造函数
     * 防止外部直接创建实例，支持单例模式
     */
    private SnappyCompression() {}

    /**
     * 获取压缩类型
     * 
     * 实现说明：
     * - 返回SNAPPY表示使用Snappy压缩算法
     * - 用于标识数据的压缩状态
     *
     * @return Snappy压缩类型枚举值
     */
    @Override
    public CompressionType type() {
        // 返回SNAPPY类型，表示使用Snappy压缩
        return CompressionType.SNAPPY;
    }

    /**
     * 创建用于压缩数据的输出流
     * 
     * 实现说明：
     * - 使用Snappy库的SnappyOutputStream进行压缩
     * - 异常统一转换为KafkaException
     *
     * @param bufferStream 用于写入压缩数据的缓冲输出流
     * @param messageVersion 消息格式版本（此处未使用）
     * @return 包装后的Snappy压缩输出流
     * @throws KafkaException 如果创建输出流时发生错误
     */
    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion) {
        try {
            // 创建新的SnappyOutputStream包装原始输出流
            return new SnappyOutputStream(bufferStream);
        } catch (Throwable e) {
            // 将所有异常包装为KafkaException
            throw new KafkaException(e);
        }
    }

    /**
     * 创建用于解压缩数据的输入流
     * 
     * 实现说明：
     * - 使用自定义的ChunkedBytesStream优化跳过操作
     * - 避免SnappyInputStream默认实现每次分配新的跳过缓冲区
     * - 支持分块读取优化内存使用
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
            // 创建输入流链：
            // 1. ByteBufferInputStream：将ByteBuffer包装为InputStream
            // 2. SnappyInputStream：处理Snappy解压缩
            // 3. ChunkedBytesStream：优化跳过操作和内存使用
            return new ChunkedBytesStream(new SnappyInputStream(new ByteBufferInputStream(buffer)),
                                          decompressionBufferSupplier,
                                          decompressionOutputSize(),
                                          false);
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
        // SnappyInputStream内部已经使用了中间缓冲区
        return 2 * 1024; // 2KB
    }

    /**
     * SnappyCompression构建器类
     * 用于创建SnappyCompression实例
     * 
     * 设计说明：
     * - 简单工厂模式：统一的实例创建接口
     * - 无配置选项：所有实例行为相同
     */
    public static class Builder implements Compression.Builder<SnappyCompression> {

        /**
         * 构建SnappyCompression实例
         * 
         * 实现说明：
         * - 创建新的SnappyCompression实例
         * - 支持工厂方法模式
         *
         * @return 新的SnappyCompression实例
         */
        @Override
        public SnappyCompression build() {
            // 创建并返回新的SnappyCompression实例
            return new SnappyCompression();
        }
    }
}
