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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.utils.AbstractIterator;

import java.io.EOFException;
import java.io.IOException;

/**
 * 记录批次迭代器，用于从日志输入流中按顺序读取记录批次。
 * 该类继承自AbstractIterator，提供了对记录批次的迭代访问能力。
 * 
 * 泛型设计：
 * - T: 必须是RecordBatch的子类，用于表示具体的记录批次类型
 * - 通过泛型约束确保类型安全，支持不同格式的记录批次处理
 * 
 * 应用场景：
 * 1. 消费者按顺序读取消息时的批次迭代
 * 2. 日志段文件的顺序扫描和处理
 * 3. 在数据复制和恢复过程中的批次读取
 */
class RecordBatchIterator<T extends RecordBatch> extends AbstractIterator<T> {

    /**
     * 日志输入流，用于实际的记录批次读取操作
     * 该流提供了对底层存储的访问能力
     */
    private final LogInputStream<T> logInputStream;

    /**
     * 构造函数，初始化记录批次迭代器
     * @param logInputStream 用于读取记录批次的日志输入流
     */
    RecordBatchIterator(LogInputStream<T> logInputStream) {
        this.logInputStream = logInputStream;
    }

    /**
     * 获取下一个记录批次
     * 该方法实现了AbstractIterator中的抽象方法，提供实际的迭代逻辑
     * 
     * 实现细节：
     * 1. 尝试从日志输入流中读取下一个批次
     * 2. 如果返回null，表示已到达流末尾，调用allDone()标记迭代结束
     * 3. 如果读取成功，返回获取到的批次
     * 
     * 异常处理：
     * - EOFException: 转换为CorruptRecordException，表示记录可能损坏
     * - IOException: 转换为KafkaException，表示一般性的IO错误
     * 
     * @return 下一个记录批次，如果没有更多批次则返回null
     * @throws CorruptRecordException 当遇到意外的EOF时抛出，表示记录可能损坏
     * @throws KafkaException 当发生IO错误时抛出
     */
    @Override
    protected T makeNext() {
        try {
            // 从日志输入流中读取下一个批次
            T batch = logInputStream.nextBatch();
            // 如果没有更多批次，标记迭代结束
            if (batch == null)
                return allDone();
            // 返回读取到的批次
            return batch;
        } catch (EOFException e) {
            // 遇到意外的EOF，说明记录可能损坏
            throw new CorruptRecordException("Unexpected EOF while attempting to read the next batch", e);
        } catch (IOException e) {
            // 发生一般性IO错误
            throw new KafkaException(e);
        }
    }
}
