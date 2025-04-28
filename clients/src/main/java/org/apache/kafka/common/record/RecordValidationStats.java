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

/**
 * This class tracks resource usage during broker record validation for eventual reporting in metrics.
 * Record validation covers integrity checks on inbound data (e.g. checksum verification), structural
 * validation to make sure that records are well-formed, and conversion between record formats if needed.
 *
 * 此类用于跟踪代理记录验证过程中的资源使用情况，以便最终在指标中进行报告。
 * 记录验证包括对入站数据的完整性检查（例如校验和验证）、确保记录格式正确的结构验证，
 * 以及在需要时进行记录格式之间的转换。
 */
public class RecordValidationStats {

    /**
     * 空的验证统计实例，用于初始化或重置统计信息
     */
    public static final RecordValidationStats EMPTY = new RecordValidationStats();

    /**
     * 处理记录时分配的临时内存字节数
     * 这个值会根据记录是否需要解压缩和转换而变化
     */
    private long temporaryMemoryBytes;

    /**
     * 已转换的记录数量
     * 记录在不同格式之间转换时的计数器
     */
    private int numRecordsConverted;

    /**
     * 记录转换所花费的时间（纳秒）
     * 用于性能监控和优化
     */
    private long conversionTimeNanos;

    /**
     * 创建一个新的记录验证统计实例
     * @param temporaryMemoryBytes 临时内存使用量（字节）
     * @param numRecordsConverted 已转换的记录数量
     * @param conversionTimeNanos 转换耗时（纳秒）
     */
    public RecordValidationStats(long temporaryMemoryBytes, int numRecordsConverted, long conversionTimeNanos) {
        // 初始化所有统计字段
        this.temporaryMemoryBytes = temporaryMemoryBytes;
        this.numRecordsConverted = numRecordsConverted;
        this.conversionTimeNanos = conversionTimeNanos;
    }

    /**
     * 创建一个空的记录验证统计实例
     * 所有计数器初始化为0
     */
    public RecordValidationStats() {
        this(0, 0, 0);
    }

    /**
     * 将另一个统计实例的数据累加到当前实例中
     * 用于合并多个验证过程的统计信息
     * @param stats 要累加的统计实例
     */
    public void add(RecordValidationStats stats) {
        // 累加各项统计数据
        temporaryMemoryBytes += stats.temporaryMemoryBytes;
        numRecordsConverted += stats.numRecordsConverted;
        conversionTimeNanos += stats.conversionTimeNanos;
    }

    /**
     * Returns the number of temporary memory bytes allocated to process the records.
     * This size depends on whether the records need decompression and/or conversion:
     * <ul>
     *   <li>Non compressed, no conversion: zero</li>
     *   <li>Non compressed, with conversion: size of the converted buffer</li>
     *   <li>Compressed, no conversion: size of the original buffer after decompression</li>
     *   <li>Compressed, with conversion: size of the original buffer after decompression + size of the converted buffer uncompressed</li>
     * </ul>
     *
     * 返回处理记录时分配的临时内存字节数。
     * 此大小取决于记录是否需要解压缩和/或转换：
     * <ul>
     *   <li>未压缩且无需转换：0字节</li>
     *   <li>未压缩但需要转换：转换后缓冲区的大小</li>
     *   <li>已压缩但无需转换：解压缩后原始缓冲区的大小</li>
     *   <li>已压缩且需要转换：解压缩后的原始缓冲区大小 + 转换后未压缩缓冲区的大小</li>
     * </ul>
     */
    public long temporaryMemoryBytes() {
        return temporaryMemoryBytes;
    }

    /**
     * 获取已转换的记录数量
     * @return 转换的记录总数
     */
    public int numRecordsConverted() {
        return numRecordsConverted;
    }

    /**
     * 获取记录转换过程消耗的总时间（纳秒）
     * @return 转换时间（纳秒）
     */
    public long conversionTimeNanos() {
        return conversionTimeNanos;
    }

    @Override
    public String toString() {
        return String.format("RecordValidationStats(temporaryMemoryBytes=%d, numRecordsConverted=%d, conversionTimeNanos=%d)",
                temporaryMemoryBytes, numRecordsConverted, conversionTimeNanos);
    }
}
