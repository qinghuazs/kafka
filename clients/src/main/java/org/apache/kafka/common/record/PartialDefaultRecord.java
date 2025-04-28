/*
 * 授权给Apache软件基金会(ASF)的许可证，基于一个或多个贡献者许可协议。
 * 有关版权所有权的其他信息，请参见随本作品分发的NOTICE文件。
 * ASF根据Apache许可证2.0版（"许可证"）将本文件授权给您；
 * 除非符合许可证，否则您不能使用此文件。
 * 您可以在以下位置获取许可证副本：
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * 除非适用法律要求或书面同意，否则根据许可证分发的软件是基于
 * "按原样"提供的，没有任何明示或暗示的担保或条件。
 * 有关许可证下的特定语言管理权限和限制，请参见许可证。
 */
package org.apache.kafka.common.record;

import org.apache.kafka.common.header.Header;

import java.nio.ByteBuffer;

/**
 * PartialDefaultRecord是DefaultRecord的一个优化实现，专门用于只需要记录元数据而不需要实际访问key和value数据的场景。
 * 这种实现方式可以显著提高性能，因为它避免了加载和存储实际的消息内容，只保留了大小信息。
 * 
 * 应用场景：
 * 1. 日志压缩时快速扫描记录而不需要加载完整内容
 * 2. 统计分析时只需要记录大小信息
 * 3. 在需要快速访问消息元数据但不需要实际内容的其他场景
 */
public class PartialDefaultRecord extends DefaultRecord {

    /**
     * 记录key的大小（字节数）
     * 当keySize >= 0 时表示存在key，当keySize < 0 时表示不存在key
     */
    private final int keySize;

    /**
     * 记录value的大小（字节数）
     * 当valueSize >= 0 时表示存在value，当valueSize < 0 时表示不存在value
     */
    private final int valueSize;

    /**
     * 创建PartialDefaultRecord实例
     * @param sizeInBytes 记录的总大小（字节）
     * @param attributes 记录的属性标志
     * @param offset 记录在分区中的偏移量
     * @param timestamp 记录的时间戳
     * @param sequence 记录的序列号
     * @param keySize key的大小
     * @param valueSize value的大小
     */
    PartialDefaultRecord(int sizeInBytes,
                         byte attributes,
                         long offset,
                         long timestamp,
                         int sequence,
                         int keySize,
                         int valueSize) {
        // 调用父类构造器，传入null作为key、value和headers参数，因为这些数据在此实现中不需要保存
        super(sizeInBytes, attributes, offset, timestamp, sequence, null, null, null);

        // 只保存大小信息
        this.keySize = keySize;
        this.valueSize = valueSize;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) &&
            this.keySize == ((PartialDefaultRecord) o).keySize &&
            this.valueSize == ((PartialDefaultRecord) o).valueSize;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + keySize;
        result = 31 * result + valueSize;
        return result;
    }

    @Override
    public String toString() {
        return String.format("PartialDefaultRecord(offset=%d, timestamp=%d, key=%d bytes, value=%d bytes)",
            offset(),
            timestamp(),
            keySize,
            valueSize);
    }

    /**
     * 获取key的大小
     * @return key的字节大小
     */
    @Override
    public int keySize() {
        return keySize;
    }

    /**
     * 检查记录是否包含key
     * @return 当keySize >= 0时返回true，表示存在key；否则返回false
     */
    @Override
    public boolean hasKey() {
        return keySize >= 0;
    }

    /**
     * 获取key的内容
     * 由于PartialDefaultRecord设计上不保存实际的key数据，此方法不支持调用
     * @throws UnsupportedOperationException 当尝试访问key内容时总是抛出此异常
     */
    @Override
    public ByteBuffer key() {
        throw new UnsupportedOperationException("key is skipped in PartialDefaultRecord");
    }

    /**
     * 获取value的大小
     * @return value的字节大小
     */
    @Override
    public int valueSize() {
        return valueSize;
    }

    /**
     * 检查记录是否包含value
     * @return 当valueSize >= 0时返回true，表示存在value；否则返回false
     */
    @Override
    public boolean hasValue() {
        return valueSize >= 0;
    }

    /**
     * 获取value的内容
     * 由于PartialDefaultRecord设计上不保存实际的value数据，此方法不支持调用
     * @throws UnsupportedOperationException 当尝试访问value内容时总是抛出此异常
     */
    @Override
    public ByteBuffer value() {
        throw new UnsupportedOperationException("value is skipped in PartialDefaultRecord");
    }

    /**
     * 获取记录的头部信息
     * 由于PartialDefaultRecord设计上不保存头部信息，此方法不支持调用
     * @throws UnsupportedOperationException 当尝试访问headers时总是抛出此异常
     */
    @Override
    public Header[] headers() {
        throw new UnsupportedOperationException("headers is skipped in PartialDefaultRecord");
    }
}
