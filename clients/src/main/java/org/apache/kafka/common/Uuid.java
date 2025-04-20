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
package org.apache.kafka.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Set;

/**
 * 该类定义了一个不可变的通用唯一标识符(UUID)。它表示一个128位的值。
 * 具体来说，该类生成的随机UUID是变体2(Leach-Salz)版本4 UUID。
 * 这与java.util.UUID生成的UUID类型相同。toString()方法使用base64字符串编码输出，
 * 同样，fromString方法也期望输入base64字符串编码的UUID。
 */
public class Uuid implements Comparable<Uuid> {

    /**
     * 保留的UUID。永远不会被randomUuid方法返回。
     * 该UUID的高64位为0，低64位为1。
     */
    public static final Uuid ONE_UUID = new Uuid(0L, 1L);

    /**
     * KRaft模式下元数据主题的UUID。永远不会被randomUuid方法返回。
     * 它与ONE_UUID相同，用于标识特殊的元数据主题。
     */
    public static final Uuid METADATA_TOPIC_ID = ONE_UUID;

    /**
     * 表示空UUID的特殊值。永远不会被randomUuid方法返回。
     * 该UUID的高64位和低64位都为0。
     */
    public static final Uuid ZERO_UUID = new Uuid(0L, 0L);

    /**
     * 保留的UUID集合，包含了所有不会被randomUuid方法返回的特殊UUID。
     * 目前包含ZERO_UUID和ONE_UUID两个特殊值。
     */
    public static final Set<Uuid> RESERVED = Set.of(ZERO_UUID, ONE_UUID);

    // UUID的高64位，用于存储UUID的前半部分
    private final long mostSignificantBits;
    // UUID的低64位，用于存储UUID的后半部分
    private final long leastSignificantBits;

    /**
     * 构造一个128位的类型4 UUID，其中第一个long表示最高有效64位，
     * 第二个long表示最低有效64位。
     * @param mostSigBits UUID的高64位
     * @param leastSigBits UUID的低64位
     */
    public Uuid(long mostSigBits, long leastSigBits) {
        this.mostSignificantBits = mostSigBits;
        this.leastSignificantBits = leastSigBits;
    }

    /**
     * 生成一个不安全的随机UUID。
     * 该方法直接使用java.util.UUID.randomUUID()生成随机UUID，
     * 但生成的UUID可能是保留值或以'-'开头。
     * @return 随机生成的UUID
     */
    private static Uuid unsafeRandomUuid() {
        java.util.UUID jUuid = java.util.UUID.randomUUID();
        return new Uuid(jUuid.getMostSignificantBits(), jUuid.getLeastSignificantBits());
    }

    /**
     * 静态工厂方法，用于获取一个类型4(伪随机生成)的UUID。
     * 该方法确保生成的UUID不等于0、1，且其字符串表示不以破折号("-")开头。
     * 通过循环生成直到得到满足条件的UUID。
     * 
     * @return 一个有效的随机UUID，不会是保留值且不以'-'开头
     */
    public static Uuid randomUuid() {
        Uuid uuid = unsafeRandomUuid();
        while (RESERVED.contains(uuid) || uuid.toString().startsWith("-")) {
            uuid = unsafeRandomUuid();
        }
        return uuid;
    }

    /**
     * 返回UUID 128位值中的最高有效位(前64位)。
     * @return UUID的高64位值
     */
    public long getMostSignificantBits() {
        return this.mostSignificantBits;
    }

    /**
     * 返回UUID 128位值中的最低有效位(后64位)。
     * @return UUID的低64位值
     */
    public long getLeastSignificantBits() {
        return this.leastSignificantBits;
    }

    /**
     * 判断当前UUID是否与另一个对象相等。
     * 当且仅当obj也是Uuid类型，且具有相同的高64位和低64位值时返回true。
     * 
     * @param obj 要比较的对象
     * @return 如果obj是具有相同值的Uuid则返回true，否则返回false
     */
    @Override
    public boolean equals(Object obj) {
        if ((null == obj) || (obj.getClass() != this.getClass()))
            return false;
        Uuid id = (Uuid) obj;
        return this.mostSignificantBits == id.mostSignificantBits &&
                this.leastSignificantBits == id.leastSignificantBits;
    }

    /**
     * 计算该UUID的哈希码。
     * 通过将高64位和低64位进行异或运算，然后将结果的高32位和低32位再次异或得到。
     * 
     * @return 该UUID的哈希码
     */
    @Override
    public int hashCode() {
        long xor = mostSignificantBits ^ leastSignificantBits;
        return (int) (xor >> 32) ^ (int) xor;
    }

    /**
     * 返回UUID的base64字符串编码。
     * 将UUID的128位值编码为URL安全的base64字符串，不带填充字符。
     * 
     * @return UUID的base64编码字符串表示
     */
    @Override
    public String toString() {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(getBytesFromUuid());
    }

    /**
     * 根据base64字符串编码创建UUID对象。
     * 这个方法用于解析toString()方法生成的字符串表示。
     * 会验证输入字符串的长度和解码后的字节数是否合法。
     * 
     * @param str base64编码的UUID字符串
     * @return 解析得到的UUID对象
     * @throws IllegalArgumentException 如果输入字符串格式不正确
     */
    public static Uuid fromString(String str) {
        if (str.length() > 24) {
            throw new IllegalArgumentException("Input string with prefix `"
                + str.substring(0, 24) + "` is too long to be decoded as a base64 UUID");
        }

        ByteBuffer uuidBytes = ByteBuffer.wrap(Base64.getUrlDecoder().decode(str));
        if (uuidBytes.remaining() != 16) {
            throw new IllegalArgumentException("Input string `" + str + "` decoded as "
                + uuidBytes.remaining() + " bytes, which is not equal to the expected 16 bytes "
                + "of a base64-encoded UUID");
        }

        return new Uuid(uuidBytes.getLong(), uuidBytes.getLong());
    }

    /**
     * 将UUID转换为字节数组。
     * 创建一个16字节的缓冲区，依次写入高64位和低64位。
     * 
     * @return 包含UUID数据的16字节数组
     */
    private byte[] getBytesFromUuid() {
        // Extract bytes for uuid which is 128 bits (or 16 bytes) long.
        ByteBuffer uuidBytes = ByteBuffer.wrap(new byte[16]);
        uuidBytes.putLong(this.mostSignificantBits);
        uuidBytes.putLong(this.leastSignificantBits);
        return uuidBytes.array();
    }

    /**
     * 比较两个UUID的大小。
     * 首先比较高64位，如果相等则比较低64位。
     * 
     * @param other 要比较的另一个UUID
     * @return 如果当前UUID大于other返回1，小于返回-1，相等返回0
     */
    @Override
    public int compareTo(Uuid other) {
        if (mostSignificantBits > other.mostSignificantBits) {
            return 1;
        } else if (mostSignificantBits < other.mostSignificantBits) {
            return -1;
        } else if (leastSignificantBits > other.leastSignificantBits) {
            return 1;
        } else if (leastSignificantBits < other.leastSignificantBits) {
            return -1;
        } else {
            return 0;
        }
    }

    /**
     * 将UUID列表转换为UUID数组。
     * 该方法提供了一种便捷的方式来将List<Uuid>类型转换为Uuid[]类型。
     *
     * @param list          输入的UUID列表
     * @return              转换后的UUID数组，如果输入为null则返回null
     */
    public static Uuid[] toArray(List<Uuid> list) {
        // 如果输入列表为null，则直接返回null
        if (list == null) return null;
        // 创建一个与输入列表大小相同的UUID数组
        Uuid[] array = new Uuid[list.size()];
        // 遍历列表，将每个元素复制到数组中
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        // 返回转换后的数组
        return array;
    }

    /**
     * 将UUID数组转换为UUID列表。
     * 该方法提供了一种便捷的方式来将Uuid[]类型转换为List<Uuid>类型。
     *
     * @param array         输入的UUID数组
     * @return              转换后的UUID列表，如果输入为null则返回null
     */
    public static List<Uuid> toList(Uuid[] array) {
        // 如果输入数组为null，则直接返回null
        if (array == null) return null;
        // 创建一个与输入数组大小相同的ArrayList
        List<Uuid> list = new ArrayList<>(array.length);
        // 使用Arrays.asList将数组转换为List，并添加到ArrayList中
        list.addAll(Arrays.asList(array));
        // 返回转换后的列表
        return list;
    }
}
