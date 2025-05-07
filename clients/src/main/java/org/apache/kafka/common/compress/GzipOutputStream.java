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

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * GZIPOutputStream的扩展实现
 * 在标准GZIP输出流的基础上添加了压缩级别控制功能
 * 
 * 应用场景：
 * 1. 需要控制GZIP压缩级别的场景
 * 2. 在Kafka消息压缩中用于优化压缩比和性能的平衡
 * 3. 大数据批量压缩时的性能调优
 * 4. 自定义压缩策略的实现
 * 
 * 设计考虑：
 * 1. 继承标准GZIPOutputStream以复用现有功能
 * 2. 添加压缩级别控制以提供更灵活的压缩选项
 * 3. 保持简单的接口设计
 * 4. 支持缓冲区大小的自定义
 */
public class GzipOutputStream extends GZIPOutputStream {
    /**
     * 创建新的输出流实例
     * 使用指定的缓冲区大小和压缩级别
     * 
     * 实现细节：
     * 1. 首先通过父类构造器初始化基本的GZIP流
     * 2. 然后设置自定义的压缩级别
     * 3. 支持缓冲区大小的配置以优化性能
     *
     * @param out   底层输出流，用于写入压缩后的数据
     * @param size  输出缓冲区大小（字节）
     * @param level 压缩级别（1-9，1最快但压缩率最低，9最慢但压缩率最高）
     * @throws IOException 如果发生I/O错误
     */
    public GzipOutputStream(OutputStream out, int size, int level) throws IOException {
        // 调用父类构造器，初始化基本的GZIP输出流
        // 设置指定大小的输出缓冲区
        super(out, size);
        // 设置压缩级别
        setLevel(level);
    }

    /**
     * 设置压缩级别
     * 通过修改底层压缩器的压缩级别来控制压缩比和性能
     * 
     * 实现细节：
     * - 使用def（Deflater实例）设置压缩级别
     * - def是GZIPOutputStream的protected字段
     *
     * @param level 压缩级别（1-9）
     */
    private void setLevel(int level) {
        // 设置底层Deflater的压缩级别
        def.setLevel(level);
    }
}
