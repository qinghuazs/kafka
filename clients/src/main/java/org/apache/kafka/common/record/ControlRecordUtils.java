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

import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;

/**
 * 用于简化控制记录(Control Records)交互的工具类。
 * 控制记录是Kafka中用于管理集群元数据和状态的特殊记录类型，主要用于KRaft(Kafka Raft)模式下的集群管理。
 * 该工具类提供了各种控制记录类型的反序列化功能，包括：
 * - 领导者变更消息(Leader Change)
 * - 快照头部记录(Snapshot Header)
 * - 快照尾部记录(Snapshot Footer)
 * - KRaft版本记录(KRaft Version)
 * - 投票者记录(Voters)
 */
public class ControlRecordUtils {
    /** KRaft版本记录的当前版本号 */
    public static final short KRAFT_VERSION_CURRENT_VERSION = 0;
    /** 领导者变更消息的当前版本号 */
    public static final short LEADER_CHANGE_CURRENT_VERSION = 0;
    /** 快照尾部记录的当前版本号 */
    public static final short SNAPSHOT_FOOTER_CURRENT_VERSION = 0;
    /** 快照头部记录的当前版本号 */
    public static final short SNAPSHOT_HEADER_CURRENT_VERSION = 0;
    /** 投票者记录的当前版本号 */
    public static final short KRAFT_VOTERS_CURRENT_VERSION = 0;

    /**
     * 从Record对象反序列化领导者变更消息
     * 
     * @param record 包含领导者变更信息的Record对象
     * @return 反序列化后的LeaderChangeMessage对象
     * @throws IllegalArgumentException 如果记录类型不是LEADER_CHANGE
     */
    public static LeaderChangeMessage deserializeLeaderChangeMessage(Record record) {
        // 从记录的key中解析控制记录类型
        ControlRecordType recordType = ControlRecordType.parse(record.key());
        // 验证记录类型是否为领导者变更类型
        validateControlRecordType(ControlRecordType.LEADER_CHANGE, recordType);
        
        // 从记录的value中反序列化消息内容
        return deserializeLeaderChangeMessage(record.value());
    }

    /**
     * 从ByteBuffer中反序列化领导者变更消息
     * 
     * @param data 包含序列化的领导者变更消息的ByteBuffer
     * @return 反序列化后的LeaderChangeMessage对象
     */
    public static LeaderChangeMessage deserializeLeaderChangeMessage(ByteBuffer data) {
        // 使用ByteBufferAccessor访问数据，并使用当前版本号创建LeaderChangeMessage对象
        return new LeaderChangeMessage(new ByteBufferAccessor(data.slice()), LEADER_CHANGE_CURRENT_VERSION);
    }

    /**
     * 从Record对象反序列化快照头部记录
     * 
     * @param record 包含快照头部信息的Record对象
     * @return 反序列化后的SnapshotHeaderRecord对象
     * @throws IllegalArgumentException 如果记录类型不是SNAPSHOT_HEADER
     */
    public static SnapshotHeaderRecord deserializeSnapshotHeaderRecord(Record record) {
        // 从记录的key中解析控制记录类型
        ControlRecordType recordType = ControlRecordType.parse(record.key());
        // 验证记录类型是否为快照头部类型
        validateControlRecordType(ControlRecordType.SNAPSHOT_HEADER, recordType);

        // 从记录的value中反序列化消息内容
        return deserializeSnapshotHeaderRecord(record.value());
    }

    /**
     * 从ByteBuffer中反序列化快照头部记录
     * 
     * @param data 包含序列化的快照头部记录的ByteBuffer
     * @return 反序列化后的SnapshotHeaderRecord对象
     */
    public static SnapshotHeaderRecord deserializeSnapshotHeaderRecord(ByteBuffer data) {
        // 使用ByteBufferAccessor访问数据，并使用当前版本号创建SnapshotHeaderRecord对象
        return new SnapshotHeaderRecord(new ByteBufferAccessor(data.slice()), SNAPSHOT_HEADER_CURRENT_VERSION);
    }

    /**
     * 从Record对象反序列化快照尾部记录
     * 
     * @param record 包含快照尾部信息的Record对象
     * @return 反序列化后的SnapshotFooterRecord对象
     * @throws IllegalArgumentException 如果记录类型不是SNAPSHOT_FOOTER
     */
    public static SnapshotFooterRecord deserializeSnapshotFooterRecord(Record record) {
        // 从记录的key中解析控制记录类型
        ControlRecordType recordType = ControlRecordType.parse(record.key());
        // 验证记录类型是否为快照尾部类型
        validateControlRecordType(ControlRecordType.SNAPSHOT_FOOTER, recordType);

        // 从记录的value中反序列化消息内容
        return deserializeSnapshotFooterRecord(record.value());
    }

    /**
     * 从ByteBuffer中反序列化快照尾部记录
     * 
     * @param data 包含序列化的快照尾部记录的ByteBuffer
     * @return 反序列化后的SnapshotFooterRecord对象
     */
    public static SnapshotFooterRecord deserializeSnapshotFooterRecord(ByteBuffer data) {
        // 使用ByteBufferAccessor访问数据，并使用当前版本号创建SnapshotFooterRecord对象
        return new SnapshotFooterRecord(new ByteBufferAccessor(data.slice()), SNAPSHOT_FOOTER_CURRENT_VERSION);
    }

    /**
     * 从Record对象反序列化KRaft版本记录
     * 
     * @param record 包含KRaft版本信息的Record对象
     * @return 反序列化后的KRaftVersionRecord对象
     * @throws IllegalArgumentException 如果记录类型不是KRAFT_VERSION
     */
    public static KRaftVersionRecord deserializeKRaftVersionRecord(Record record) {
        // 从记录的key中解析控制记录类型
        ControlRecordType recordType = ControlRecordType.parse(record.key());
        // 验证记录类型是否为KRaft版本类型
        validateControlRecordType(ControlRecordType.KRAFT_VERSION, recordType);

        // 从记录的value中反序列化消息内容
        return deserializeKRaftVersionRecord(record.value());
    }

    /**
     * 从ByteBuffer中反序列化KRaft版本记录
     * 
     * @param data 包含序列化的KRaft版本记录的ByteBuffer
     * @return 反序列化后的KRaftVersionRecord对象
     */
    public static KRaftVersionRecord deserializeKRaftVersionRecord(ByteBuffer data) {
        // 使用ByteBufferAccessor访问数据，并使用当前版本号创建KRaftVersionRecord对象
        return new KRaftVersionRecord(new ByteBufferAccessor(data.slice()), KRAFT_VERSION_CURRENT_VERSION);
    }

    /**
     * 从Record对象反序列化投票者记录
     * 
     * @param record 包含投票者信息的Record对象
     * @return 反序列化后的VotersRecord对象
     * @throws IllegalArgumentException 如果记录类型不是KRAFT_VOTERS
     */
    public static VotersRecord deserializeVotersRecord(Record record) {
        // 从记录的key中解析控制记录类型
        ControlRecordType recordType = ControlRecordType.parse(record.key());
        // 验证记录类型是否为投票者类型
        validateControlRecordType(ControlRecordType.KRAFT_VOTERS, recordType);

        // 从记录的value中反序列化消息内容
        return deserializeVotersRecord(record.value());
    }

    /**
     * 从ByteBuffer中反序列化投票者记录
     * 
     * @param data 包含序列化的投票者记录的ByteBuffer
     * @return 反序列化后的VotersRecord对象
     */
    public static VotersRecord deserializeVotersRecord(ByteBuffer data) {
        // 使用ByteBufferAccessor访问数据，并使用当前版本号创建VotersRecord对象
        return new VotersRecord(new ByteBufferAccessor(data.slice()), KRAFT_VOTERS_CURRENT_VERSION);
    }

    /**
     * 验证控制记录类型是否匹配预期类型
     * 
     * @param expected 预期的控制记录类型
     * @param actual 实际的控制记录类型
     * @throws IllegalArgumentException 如果实际类型与预期类型不匹配
     */
    private static void validateControlRecordType(ControlRecordType expected, ControlRecordType actual) {
        // 比较实际类型与预期类型是否一致
        if (actual != expected) {
            // 如果不一致，抛出IllegalArgumentException异常，并提供详细的错误信息
            throw new IllegalArgumentException(
                String.format(
                    "Expected %s control record type(%d), but found %s",
                    expected,
                    expected.type(),
                    actual
                )
            );
        }
    }
}
