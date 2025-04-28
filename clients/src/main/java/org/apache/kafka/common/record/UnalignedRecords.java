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
 * 表示一个不需要按偏移量对齐的记录集接口
 * 
 * 这个接口主要用于Kafka的Raft快照获取场景。在传统的消息系统中，记录通常需要按照
 * 偏移量对齐，以保证消息的顺序性和完整性。但在某些特殊场景下，比如获取Raft快照时，
 * 我们需要能够从任意位置读取数据块，而不必考虑消息的边界对齐。
 * 
 * 实现类：
 * - UnalignedMemoryRecords：基于内存的实现，用于处理内存中的数据
 * - UnalignedFileRecords：基于文件的实现，用于处理文件系统中的数据
 * 
 * 通过继承TransferableRecords接口，该接口具备了数据传输的能力，支持将数据写入到
 * 网络通道或文件通道中。
 */
public interface UnalignedRecords extends TransferableRecords {

    /**
     * 创建一个用于发送记录的RecordsSend对象
     * 
     * 实现细节：
     * 1. 使用DefaultRecordsSend封装当前记录集实例
     * 2. 通过sizeInBytes()方法获取记录集的总大小
     * 3. 支持异步传输，可以分片发送大型数据集
     * 
     * @return 返回一个RecordsSend实例，用于处理记录的发送操作
     */
    @Override
    default RecordsSend<? extends BaseRecords> toSend() {
        return new DefaultRecordsSend<>(this, sizeInBytes());
    }
}
