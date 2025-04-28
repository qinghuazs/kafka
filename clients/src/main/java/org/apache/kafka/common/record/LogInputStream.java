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
package org.apache.kafka.common.record;

import java.io.IOException;

/**
 * An abstraction between an underlying input stream and record iterators, a {@link LogInputStream} only returns
 * the batches at one level. For magic values 0 and 1, this means that it can either handle  iteration
 * at the top level of the log or deep iteration within the payload of a single message, but it does not attempt
 * to handle both. For magic value 2, this is only used for iterating over the top-level record batches (inner
 * records do not follow the {@link RecordBatch} interface).
 *
 * The generic typing allows for implementations which present only a view of the log entries, which enables more
 * efficient iteration when the record data is not actually needed. See for example
 * {@link FileLogInputStream.FileChannelRecordBatch} in which the record is not brought into memory until needed.
 *
 * @param <T> Type parameter of the log entry
 *
 * 这是一个底层输入流和记录迭代器之间的抽象接口，用于在单一层级上返回记录批次。
 * 
 * 版本兼容性：
 * - 对于魔数值0和1：可以处理日志的顶层迭代或单个消息负载内的深层迭代，但不能同时处理两者
 * - 对于魔数值2：仅用于迭代顶层记录批次（内部记录不遵循RecordBatch接口）
 * 
 * 泛型设计优势：
 * - 通过泛型T（RecordBatch的子类）实现类型安全
 * - 允许实现类仅提供日志条目的视图，无需加载实际数据
 * - 提高迭代效率，例如FileLogInputStream.FileChannelRecordBatch在需要时才将记录加载到内存
 * 
 * 应用场景：
 * 1. 日志段文件的顺序读取
 * 2. 消息批次的流式处理
 * 3. 在数据复制和恢复过程中的批次访问
 */
interface LogInputStream<T extends RecordBatch> {

    /**
     * Get the next record batch from the underlying input stream.
     *
     * @return The next record batch or null if there is none
     * @throws IOException for any IO errors
     * 
     * 从底层输入流中获取下一个记录批次
     * 
     * 实现要求：
     * 1. 顺序读取：必须按照日志中的顺序返回记录批次
     * 2. 批次完整性：每次调用返回一个完整的记录批次
     * 3. 资源管理：实现类需要妥善处理流的打开和关闭
     * 
     * @return 下一个记录批次，如果没有更多批次则返回null
     * @throws IOException 当发生IO错误时抛出异常
     */
    T nextBatch() throws IOException;
}
