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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.zip.Deflater;

import static org.apache.kafka.common.config.ConfigDef.Range.between;

/**
 * 用于指定消息压缩类型的枚举类
 * 该枚举定义了Kafka支持的所有压缩算法类型，包括：
 * - NONE: 不压缩
 * - GZIP: JDK自带的压缩算法，压缩比高但CPU消耗较大
 * - SNAPPY: Google开发的压缩算法，压缩速度快，压缩比适中
 * - LZ4: 压缩和解压缩速度都很快，压缩比适中
 * - ZSTD: Facebook开发的压缩算法，提供了很好的压缩比和性能平衡
 *
 * 每种压缩类型都可以配置不同的压缩级别，级别越高压缩比越大但CPU消耗也越大。
 * 压缩类型信息使用2个比特存储在record batch header的attributes字段中。
 */
public enum CompressionType {
    /**
     * 不进行压缩
     * id=0: 表示不压缩的标识符
     * name="none": 不压缩的名称
     * rate=1.0f: 压缩率为1表示数据大小不变
     */
    NONE((byte) 0, "none", 1.0f),

    /**
     * GZIP压缩算法，随JDK一起提供
     * id=1: GZIP压缩的标识符
     * name="gzip": GZIP压缩的名称
     * rate=1.0f: 压缩率，实际压缩比取决于数据特征和压缩级别
     * 
     * GZIP提供了多个压缩级别选项：
     * - 最小级别(MIN_LEVEL=1)：最快的压缩速度
     * - 最大级别(MAX_LEVEL=9)：最高的压缩比
     * - 默认级别(DEFAULT_LEVEL=-1)：压缩比和速度的平衡
     */
    GZIP((byte) 1, "gzip", 1.0f) {
        public static final int MIN_LEVEL = Deflater.BEST_SPEED;
        public static final int MAX_LEVEL = Deflater.BEST_COMPRESSION;
        public static final int DEFAULT_LEVEL = Deflater.DEFAULT_COMPRESSION;

        @Override
        /**
     * 获取压缩类型的默认压缩级别
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 
     * @return 默认的压缩级别
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public int defaultLevel() {
            return DEFAULT_LEVEL;
        }

        @Override
        /**
     * 获取压缩类型支持的最大压缩级别
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 
     * @return 最大压缩级别
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public int maxLevel() {
            return MAX_LEVEL;
        }

        @Override
        /**
     * 获取压缩类型支持的最小压缩级别
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 
     * @return 最小压缩级别
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public int minLevel() {
            return MIN_LEVEL;
        }

        @Override
        /**
     * 获取用于验证压缩级别配置的验证器
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 验证器确保配置的压缩级别在有效范围内
     * 
     * @return 压缩级别的配置验证器
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public ConfigDef.Validator levelValidator() {
            return ConfigDef.LambdaValidator.with((name, value) -> {
                if (value == null)
                    throw new ConfigException(name, null, "Value must be non-null");
                int level = ((Number) value).intValue();
                if (level > MAX_LEVEL || (level < MIN_LEVEL && level != DEFAULT_LEVEL)) {
                    throw new ConfigException(name, value, "Value must be between " + MIN_LEVEL + " and " + MAX_LEVEL + " or equal to " + DEFAULT_LEVEL);
                }
            }, () -> "[" + MIN_LEVEL + ",...," + MAX_LEVEL + "] or " + DEFAULT_LEVEL);
        }
    },

    // We should only load classes from a given compression library when we actually use said compression library. This
    // is because compression libraries include native code for a set of platforms and we want to avoid errors
    // in case the platform is not supported and the compression library is not actually used.
    // To ensure this, we only reference compression library code from classes that are only invoked when actual usage
    // happens.
    /**
     * Snappy压缩算法
     * id=2: Snappy压缩的标识符
     * name="snappy": Snappy压缩的名称
     * rate=1.0f: 压缩率，实际压缩比取决于数据特征
     * 
     * Snappy优化了压缩速度，适合对延迟敏感的场景
     */
    SNAPPY((byte) 2, "snappy", 1.0f),
    /**
     * LZ4压缩算法
     * id=3: LZ4压缩的标识符
     * name="lz4": LZ4压缩的名称
     * rate=1.0f: 压缩率，实际压缩比取决于数据特征和压缩级别
     * 
     * LZ4提供了多个压缩级别选项：
     * - 最小级别(MIN_LEVEL=1)：较快的压缩速度
     * - 最大级别(MAX_LEVEL=17)：较高的压缩比
     * - 默认级别(DEFAULT_LEVEL=9)：压缩比和速度的平衡
     */
    LZ4((byte) 3, "lz4", 1.0f) {
        // These values come from net.jpountz.lz4.LZ4Constants
        // We may need to update them if the lz4 library changes these values.
        private static final int MIN_LEVEL = 1;
        private static final int MAX_LEVEL = 17;
        private static final int DEFAULT_LEVEL = 9;

        @Override
        /**
     * 获取压缩类型的默认压缩级别
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 
     * @return 默认的压缩级别
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public int defaultLevel() {
            return DEFAULT_LEVEL;
        }

        @Override
        /**
     * 获取压缩类型支持的最大压缩级别
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 
     * @return 最大压缩级别
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public int maxLevel() {
            return MAX_LEVEL;
        }

        @Override
        /**
     * 获取压缩类型支持的最小压缩级别
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 
     * @return 最小压缩级别
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public int minLevel() {
            return MIN_LEVEL;
        }

        @Override
        /**
     * 获取用于验证压缩级别配置的验证器
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 验证器确保配置的压缩级别在有效范围内
     * 
     * @return 压缩级别的配置验证器
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public ConfigDef.Validator levelValidator() {
            return between(MIN_LEVEL, MAX_LEVEL);
        }
    },
    /**
     * Zstandard(ZSTD)压缩算法
     * id=4: ZSTD压缩的标识符
     * name="zstd": ZSTD压缩的名称
     * rate=1.0f: 压缩率，实际压缩比取决于数据特征和压缩级别
     * 
     * ZSTD提供了最灵活的压缩级别选项：
     * - 最小级别(MIN_LEVEL=-131072)：超快速压缩模式
     * - 最大级别(MAX_LEVEL=22)：超高压缩比模式
     * - 默认级别(DEFAULT_LEVEL=3)：良好的压缩比和速度平衡
     * 
     * 注意：为避免在配置解析时加载ZSTD库，这些常量值直接硬编码而不是从库中获取
     */
    ZSTD((byte) 4, "zstd", 1.0f) {
        // These values come from the zstd library. We don't use the Zstd.minCompressionLevel(),
        // Zstd.maxCompressionLevel() and Zstd.defaultCompressionLevel() methods to not load the Zstd library
        // while parsing configuration.
        // See ZSTD_minCLevel in https://github.com/facebook/zstd/blob/dev/lib/compress/zstd_compress.c#L6987
        // and ZSTD_TARGETLENGTH_MAX https://github.com/facebook/zstd/blob/dev/lib/zstd.h#L1249
        private static final int MIN_LEVEL = -131072;
        // See ZSTD_MAX_CLEVEL in https://github.com/facebook/zstd/blob/dev/lib/compress/clevels.h#L19
        private static final int MAX_LEVEL = 22;
        // See ZSTD_CLEVEL_DEFAULT in https://github.com/facebook/zstd/blob/dev/lib/zstd.h#L129
        private static final int DEFAULT_LEVEL = 3;

        @Override
        /**
     * 获取压缩类型的默认压缩级别
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 
     * @return 默认的压缩级别
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public int defaultLevel() {
            return DEFAULT_LEVEL;
        }

        @Override
        /**
     * 获取压缩类型支持的最大压缩级别
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 
     * @return 最大压缩级别
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public int maxLevel() {
            return MAX_LEVEL;
        }

        @Override
        /**
     * 获取压缩类型支持的最小压缩级别
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 
     * @return 最小压缩级别
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public int minLevel() {
            return MIN_LEVEL;
        }

        @Override
        /**
     * 获取用于验证压缩级别配置的验证器
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 验证器确保配置的压缩级别在有效范围内
     * 
     * @return 压缩级别的配置验证器
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public ConfigDef.Validator levelValidator() {
            return between(MIN_LEVEL, MAX_LEVEL);
        }
    };

    /**
     * 压缩类型的属性定义：
     * id: 压缩类型的唯一标识符，使用byte类型足够，因为在record batch header的attributes字段中仅用2位表示
     * name: 压缩类型的名称，用于配置和显示
     * rate: 预期的压缩率，1.0表示不压缩，小于1.0表示有压缩效果
     */
    public final byte id;
    public final String name;
    public final float rate;

    CompressionType(byte id, String name, float rate) {
        this.id = id;
        this.name = name;
        this.rate = rate;
    }

    /**
     * 根据压缩类型ID获取对应的CompressionType枚举值
     * 
     * @param id 压缩类型的标识符
     * @return 对应的CompressionType枚举值
     * @throws IllegalArgumentException 当提供的ID不存在对应的压缩类型时抛出
     */
    public static CompressionType forId(int id) {
        switch (id) {
            case 0:
                return NONE;
            case 1:
                return GZIP;
            case 2:
                return SNAPPY;
            case 3:
                return LZ4;
            case 4:
                return ZSTD;
            default:
                throw new IllegalArgumentException("Unknown compression type id: " + id);
        }
    }

    /**
     * 根据压缩类型名称获取对应的CompressionType枚举值
     * 
     * @param name 压缩类型的名称（大小写敏感）
     * @return 对应的CompressionType枚举值
     * @throws IllegalArgumentException 当提供的名称不存在对应的压缩类型时抛出
     */
    public static CompressionType forName(String name) {
        if (NONE.name.equals(name))
            return NONE;
        else if (GZIP.name.equals(name))
            return GZIP;
        else if (SNAPPY.name.equals(name))
            return SNAPPY;
        else if (LZ4.name.equals(name))
            return LZ4;
        else if (ZSTD.name.equals(name))
            return ZSTD;
        else
            throw new IllegalArgumentException("Unknown compression name: " + name);
    }

    /**
     * 获取压缩类型的默认压缩级别
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 
     * @return 默认的压缩级别
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public int defaultLevel() {
        throw new UnsupportedOperationException("Compression levels are not defined for this compression type: " + name);
    }

    /**
     * 获取压缩类型支持的最大压缩级别
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 
     * @return 最大压缩级别
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public int maxLevel() {
        throw new UnsupportedOperationException("Compression levels are not defined for this compression type: " + name);
    }

    /**
     * 获取压缩类型支持的最小压缩级别
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 
     * @return 最小压缩级别
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public int minLevel() {
        throw new UnsupportedOperationException("Compression levels are not defined for this compression type: " + name);
    }

    /**
     * 获取用于验证压缩级别配置的验证器
     * 仅GZIP、LZ4和ZSTD支持压缩级别配置
     * 验证器确保配置的压缩级别在有效范围内
     * 
     * @return 压缩级别的配置验证器
     * @throws UnsupportedOperationException 当压缩类型不支持压缩级别配置时抛出
     */
    public ConfigDef.Validator levelValidator() {
        throw new UnsupportedOperationException("Compression levels are not defined for this compression type: " + name);
    }

    @Override
    public String toString() {
        return name;
    }

}
