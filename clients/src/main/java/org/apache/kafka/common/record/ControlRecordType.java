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
 * 控制记录指定了一个包含版本和类型的记录键模式：
 *
 * Key => Version Type
 *   Version => Int16（版本号）
 *   Type => Int16（类型标识）
 *
 * 将来可以通过提升版本号来表示新的模式，但必须向后兼容当前模式。
 * 通常，这意味着我们可以添加新字段，但不能删除旧字段。
 *
 * 注意：日志清理器在进行日志压缩时不会考虑控制记录。
 *
 * 值字段的模式由控制记录类型来指定。
 */
public enum ControlRecordType {
    /**
     * 事务中止控制记录
     * 用于标记事务已被中止，所有在该事务中的消息都将被回滚
     */
    ABORT((short) 0),

    /**
     * 事务提交控制记录
     * 用于标记事务已成功提交，事务中的所有消息都将对消费者可见
     */
    COMMIT((short) 1),

    // KRaft集群相关的控制消息
    /**
     * 领导者变更控制记录
     * 用于在KRaft集群中记录领导者节点的变更信息
     */
    LEADER_CHANGE((short) 2),

    /**
     * 快照头部控制记录
     * 用于标记KRaft集群状态快照的开始
     */
    SNAPSHOT_HEADER((short) 3),

    /**
     * 快照尾部控制记录
     * 用于标记KRaft集群状态快照的结束
     */
    SNAPSHOT_FOOTER((short) 4),

    // KRaft成员变更消息
    /**
     * KRaft版本控制记录
     * 用于记录KRaft集群的版本信息
     */
    KRAFT_VERSION((short) 5),

    /**
     * KRaft投票者控制记录
     * 用于记录KRaft集群中投票者成员的变更信息
     */
    KRAFT_VOTERS((short) 6),

    /**
     * 未知类型
     * 用于表示客户端无法识别的控制记录类型，这些记录应被忽略
     */
    UNKNOWN((short) -1);

    // 日志记录器
    private static final Logger log = LoggerFactory.getLogger(ControlRecordType.class);

    // 当前控制记录键的版本号，初始版本为0
    static final short CURRENT_CONTROL_RECORD_KEY_VERSION = 0;
    // 控制记录键的大小（字节数）：2字节版本号 + 2字节类型 = 4字节
    static final int CURRENT_CONTROL_RECORD_KEY_SIZE = 4;
    // 控制记录键的模式定义（版本0）
    private static final Schema CONTROL_RECORD_KEY_SCHEMA_VERSION_V0 = new Schema(
            new Field("version", Type.INT16), // 版本号字段
            new Field("type", Type.INT16));  // 类型字段

    // 控制记录类型的数值标识
    private final short type;

    /**
     * 构造函数
     * @param type 控制记录类型的数值标识
     */
    ControlRecordType(short type) {
        this.type = type;
    }

    /**
     * 获取控制记录类型的数值标识
     * @return 类型标识值
     */
    public short type() {
        return type;
    }

    /**
     * 创建控制记录的键结构
     * 将控制记录类型序列化为包含版本和类型的结构体
     *
     * @return 包含版本号和类型的结构体
     * @throws IllegalArgumentException 当尝试序列化UNKNOWN类型时抛出异常
     */
    public Struct recordKey() {
        // UNKNOWN类型不能被序列化
        if (this == UNKNOWN)
            throw new IllegalArgumentException("Cannot serialize UNKNOWN control record type");

        // 创建一个新的结构体并设置版本号和类型
        Struct struct = new Struct(CONTROL_RECORD_KEY_SCHEMA_VERSION_V0);
        struct.set("version", CURRENT_CONTROL_RECORD_KEY_VERSION);
        struct.set("type", type);
        return struct;
    }

    /**
     * 从字节缓冲区中解析控制记录的类型标识
     *
     * @param key 包含控制记录键的字节缓冲区
     * @return 控制记录的类型标识
     * @throws InvalidRecordException 当键的大小不足或版本号无效时抛出异常
     */
    public static short parseTypeId(ByteBuffer key) {
        // 检查字节缓冲区中剩余的字节数是否足够
        if (key.remaining() < CURRENT_CONTROL_RECORD_KEY_SIZE)
            throw new InvalidRecordException("Invalid value size found for end control record key. Must have " +
                    "at least " + CURRENT_CONTROL_RECORD_KEY_SIZE + " bytes, but found only " + key.remaining());

        // 读取版本号（前2个字节）
        short version = key.getShort(0);
        // 检查版本号的有效性
        if (version < 0)
            throw new InvalidRecordException("Invalid version found for control record: " + version +
                    ". May indicate data corruption");

        // 如果版本号与当前版本不匹配，记录警告日志
        if (version != CURRENT_CONTROL_RECORD_KEY_VERSION)
            log.debug("Received unknown control record key version {}. Parsing as version {}", version,
                    CURRENT_CONTROL_RECORD_KEY_VERSION);
        // 返回类型标识（后2个字节）
        return key.getShort(2);
    }

    /**
     * 根据类型标识获取对应的控制记录类型枚举值
     *
     * @param typeId 控制记录的类型标识
     * @return 对应的控制记录类型枚举值，如果类型标识未知则返回UNKNOWN
     */
    public static ControlRecordType fromTypeId(short typeId) {
        switch (typeId) {
            case 0:  // 事务中止
                return ABORT;
            case 1:  // 事务提交
                return COMMIT;
            case 2:  // 领导者变更
                return LEADER_CHANGE;
            case 3:  // 快照头部
                return SNAPSHOT_HEADER;
            case 4:  // 快照尾部
                return SNAPSHOT_FOOTER;
            case 5:  // KRaft版本
                return KRAFT_VERSION;
            case 6:  // KRaft投票者
                return KRAFT_VOTERS;

            default: // 未知类型
                return UNKNOWN;
        }
    }

    /**
     * 从字节缓冲区中解析控制记录类型
     * 这是一个便捷方法，组合了parseTypeId和fromTypeId的功能
     *
     * @param key 包含控制记录键的字节缓冲区
     * @return 解析出的控制记录类型枚举值
     * @throws InvalidRecordException 当键的格式无效时抛出异常
     */
    public static ControlRecordType parse(ByteBuffer key) {
        // 先解析类型标识，然后转换为对应的枚举值
        return fromTypeId(parseTypeId(key));
    }
}
