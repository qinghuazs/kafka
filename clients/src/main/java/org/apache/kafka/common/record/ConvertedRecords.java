/*
 * 授权给Apache软件基金会(ASF)的许可证，基于一个或多个贡献者许可协议。
 * 有关版权所有权的其他信息，请参阅随本作品分发的NOTICE文件。
 * ASF根据Apache许可证2.0版（"许可证"）将本文件授权给您；
 * 除非符合许可证，否则您不得使用此文件。
 * 您可以在以下位置获取许可证副本：
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * 除非适用法律要求或书面同意，否则根据许可证分发的软件是基于
 * "按原样"提供的，没有任何明示或暗示的担保或条件。
 * 有关许可证下的特定语言管理权限和限制，请参阅许可证。
 */
package org.apache.kafka.common.record;

/**
 * 记录转换结果的封装类，用于存储转换后的记录集合及其验证统计信息。
 * 
 * @param <T> 继承自Records的泛型参数，表示具体的记录集合类型
 */
public class ConvertedRecords<T extends Records> {

    /**
     * 转换后的记录集合
     * 这个字段存储了经过格式转换后的Kafka记录，可能是MemoryRecords或FileRecords等具体类型
     */
    private final T records;

    /**
     * 记录验证统计信息
     * 包含了记录转换过程中的各项统计数据，如临时内存使用量、转换的记录数量和转换耗时等
     */
    private final RecordValidationStats recordValidationStats;

    /**
     * 构造函数，用于创建记录转换结果实例
     *
     * @param records 转换后的记录集合，可以是MemoryRecords或FileRecords等类型
     * @param recordValidationStats 记录验证统计信息，包含转换过程中的性能指标
     */
    public ConvertedRecords(T records, RecordValidationStats recordValidationStats) {
        // 初始化转换后的记录集合
        this.records = records;
        // 初始化记录验证统计信息
        this.recordValidationStats = recordValidationStats;
    }

    /**
     * 获取转换后的记录集合
     *
     * @return 返回泛型类型的记录集合，包含所有转换后的记录
     */
    public T records() {
        return records;
    }

    /**
     * 获取记录转换的统计信息
     *
     * @return 返回记录验证统计信息，包含转换过程中的性能指标数据
     */
    public RecordValidationStats recordConversionStats() {
        return recordValidationStats;
    }
}
