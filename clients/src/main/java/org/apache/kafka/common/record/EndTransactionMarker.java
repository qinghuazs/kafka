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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * 该类表示写入日志以指示事务完成的控制记录。
 * 记录的key指定了{@link ControlRecordType 控制类型}（COMMIT或ABORT），
 * 记录的value包含了用于写入验证的信息（目前仅包含coordinator epoch）。
 * 
 * 应用场景：
 * 1. 在分布式事务提交/回滚时，由事务协调者写入该标记
 * 2. 用于保证事务操作的原子性和持久性
 * 3. 在故障恢复时，用于确定事务的最终状态
 */
public class EndTransactionMarker {
    // 日志记录器
    private static final Logger log = LoggerFactory.getLogger(EndTransactionMarker.class);

    // 当前事务结束标记的版本号，用于序列化和反序列化的版本兼容
    private static final short CURRENT_END_TXN_MARKER_VERSION = 0;
    
    // 事务结束标记的Schema定义，包含版本号和协调器epoch
    private static final Schema END_TXN_MARKER_SCHEMA_VERSION_V0 = new Schema(
            new Field("version", Type.INT16),  // 版本号字段，占用2字节
            new Field("coordinator_epoch", Type.INT32));  // 协调器epoch字段，占用4字节
    
    // 事务结束标记值的大小：2字节(version) + 4字节(coordinator_epoch) = 6字节
    static final int CURRENT_END_TXN_MARKER_VALUE_SIZE = 6;
    
    // 事务结束标记记录的总大小，包含key、value和空headers
    static final int CURRENT_END_TXN_SCHEMA_RECORD_SIZE = DefaultRecord.sizeInBytes(0, 0L,
            ControlRecordType.CURRENT_CONTROL_RECORD_KEY_SIZE,
            EndTransactionMarker.CURRENT_END_TXN_MARKER_VALUE_SIZE,
            Record.EMPTY_HEADERS);

    // 事务控制类型：COMMIT（提交）或ABORT（中止）
    private final ControlRecordType type;
    // 事务协调器的epoch，用于防止脑裂和确保操作顺序
    private final int coordinatorEpoch;

    /**
     * 创建事务结束标记
     * @param type 控制类型（COMMIT或ABORT）
     * @param coordinatorEpoch 协调器epoch，用于确保操作的顺序性和有效性
     */
    public EndTransactionMarker(ControlRecordType type, int coordinatorEpoch) {
        // 验证控制类型是否合法（必须是COMMIT或ABORT）
        ensureTransactionMarkerControlType(type);
        this.type = type;
        this.coordinatorEpoch = coordinatorEpoch;
    }

    /**
     * 获取协调器epoch
     * @return 当前事务协调器的epoch值
     */
    public int coordinatorEpoch() {
        return coordinatorEpoch;
    }

    /**
     * 获取事务控制类型
     * @return 事务的控制类型（COMMIT或ABORT）
     */
    public ControlRecordType controlType() {
        return type;
    }

    /**
     * 构建记录值的结构体
     * @return 包含版本号和协调器epoch的结构体
     */
    private Struct buildRecordValue() {
        // 创建结构体并设置版本号和协调器epoch
        Struct struct = new Struct(END_TXN_MARKER_SCHEMA_VERSION_V0);
        struct.set("version", CURRENT_END_TXN_MARKER_VERSION);
        struct.set("coordinator_epoch", coordinatorEpoch);
        return struct;
    }

    /**
     * 序列化事务结束标记的值部分
     * @return 包含序列化数据的ByteBuffer
     */
    public ByteBuffer serializeValue() {
        // 构建结构体并序列化为字节缓冲区
        Struct valueStruct = buildRecordValue();
        ByteBuffer value = ByteBuffer.allocate(valueStruct.sizeOf());
        valueStruct.writeTo(value);
        value.flip();  // 准备读取：将position设为0，limit设为写入的字节数
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EndTransactionMarker that = (EndTransactionMarker) o;
        return coordinatorEpoch == that.coordinatorEpoch && type == that.type;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + coordinatorEpoch;
        return result;
    }

    /**
     * 验证事务标记的控制类型是否合法
     * @param type 要验证的控制类型
     * @throws IllegalArgumentException 当控制类型既不是COMMIT也不是ABORT时抛出
     */
    private static void ensureTransactionMarkerControlType(ControlRecordType type) {
        // 控制类型必须是COMMIT或ABORT之一
        if (type != ControlRecordType.COMMIT && type != ControlRecordType.ABORT)
            throw new IllegalArgumentException("Invalid control record type for end transaction marker" + type);
    }

    /**
     * 从记录中反序列化事务结束标记
     * @param record 包含事务结束标记的记录
     * @return 反序列化后的EndTransactionMarker对象
     */
    public static EndTransactionMarker deserialize(Record record) {
        // 从记录的key中解析控制类型
        ControlRecordType type = ControlRecordType.parse(record.key());
        // 使用控制类型和记录的value进行反序列化
        return deserializeValue(type, record.value());
    }

    /**
     * 从字节缓冲区中反序列化事务结束标记的值部分
     * @param type 事务的控制类型（COMMIT或ABORT）
     * @param value 包含序列化数据的字节缓冲区
     * @return 反序列化后的EndTransactionMarker对象
     * @throws InvalidRecordException 当数据格式不正确或版本号无效时抛出
     */
    static EndTransactionMarker deserializeValue(ControlRecordType type, ByteBuffer value) {
        // 验证控制类型的合法性
        ensureTransactionMarkerControlType(type);

        // 检查字节缓冲区中的剩余字节数是否足够
        if (value.remaining() < CURRENT_END_TXN_MARKER_VALUE_SIZE)
            throw new InvalidRecordException("Invalid value size found for end transaction marker. Must have " +
                    "at least " + CURRENT_END_TXN_MARKER_VALUE_SIZE + " bytes, but found only " + value.remaining());

        // 读取版本号（前2个字节）
        short version = value.getShort(0);
        // 版本号不能为负数
        if (version < 0)
            throw new InvalidRecordException("Invalid version found for end transaction marker: " + version +
                    ". May indicate data corruption");

        // 如果版本号大于当前版本，记录警告日志但仍尝试解析
        if (version > CURRENT_END_TXN_MARKER_VERSION)
            log.debug("Received end transaction marker value version {}. Parsing as version {}", version,
                    CURRENT_END_TXN_MARKER_VERSION);

        // 读取协调器epoch（从第3个字节开始，占4个字节）
        int coordinatorEpoch = value.getInt(2);
        // 创建并返回新的事务结束标记对象
        return new EndTransactionMarker(type, coordinatorEpoch);
    }

}
