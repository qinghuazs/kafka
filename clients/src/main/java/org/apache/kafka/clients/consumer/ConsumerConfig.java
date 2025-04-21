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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.clients.consumer.CooperativeStickyAssignor.COOPERATIVE_STICKY_ASSIGNOR_NAME;
import static org.apache.kafka.clients.consumer.RangeAssignor.RANGE_ASSIGNOR_NAME;
import static org.apache.kafka.clients.consumer.RoundRobinAssignor.ROUNDROBIN_ASSIGNOR_NAME;
import static org.apache.kafka.clients.consumer.StickyAssignor.STICKY_ASSIGNOR_NAME;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Kafka消费者的配置类
 * 该类定义了所有Kafka消费者可用的配置项，包括：
 * 1. 基础连接配置：bootstrap.servers, client.id等
 * 2. 消费者组配置：group.id, session.timeout.ms等
 * 3. 消费性能相关配置：fetch.min.bytes, fetch.max.wait.ms等
 * 4. 偏移量管理配置：auto.offset.reset, enable.auto.commit等
 * 5. 安全相关配置：security.protocol, ssl配置等
 */
public class ConsumerConfig extends AbstractConfig {
    // 存储所有配置项定义的ConfigDef对象
    private static final ConfigDef CONFIG;

    /**
     * 只从已订阅主题分配分区的分配器列表
     * 这些分配器只会将消费者订阅的主题的分区分配给消费者
     * 用于优化ConsumerCoordinator#performAssignment方法的性能
     * 当新增分配器时需要更新此列表
     */
    public static final List<String> ASSIGN_FROM_SUBSCRIBED_ASSIGNORS = List.of(
            RANGE_ASSIGNOR_NAME,  // 范围分配器：按主题划分分区范围
            ROUNDROBIN_ASSIGNOR_NAME,  // 轮询分配器：轮流分配所有分区
            STICKY_ASSIGNOR_NAME,  // 粘性分配器：尽量保持现有分配关系
            COOPERATIVE_STICKY_ASSIGNOR_NAME  // 协作式粘性分配器：支持渐进式rebalance
    );

    /*
     * 重要提示：不要修改配置项的字符串常量或其Java变量名
     * 因为这些都是公共API的一部分，修改会破坏用户代码
     */

    /**
     * 消费者组ID配置
     * 标识消费者所属的消费者组。同一组的消费者共同消费主题的分区
     * 如果设置为null，则该消费者不属于任何消费者组
     * 
     * 设计原理：
     * 1. 用于实现消费者的水平扩展，多个消费者可以并行消费同一主题
     * 2. 通过消费者组实现故障转移，当组内某个消费者失败时，其他消费者可以接管分区
     * 3. 每个消费者组都维护自己的偏移量，互不影响
     */
    public static final String GROUP_ID_CONFIG = CommonClientConfigs.GROUP_ID_CONFIG;
    private static final String GROUP_ID_DOC = CommonClientConfigs.GROUP_ID_DOC;

    /**
     * 消费者实例ID配置
     * 用于标识消费者组内的特定消费者实例
     * 
     * 设计原理：
     * 1. 提供静态成员机制，使消费者可以保持固定的分区分配
     * 2. 即使消费者重启，只要实例ID相同，就会被分配相同的分区
     * 3. 避免不必要的rebalance，提高稳定性
     * 
     * 最佳实践：
     * 1. 在需要固定分区分配的场景下使用
     * 2. 确保实例ID在组内唯一
     * 3. 重启时使用相同的实例ID
     */
    public static final String GROUP_INSTANCE_ID_CONFIG = CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG;
    private static final String GROUP_INSTANCE_ID_DOC = CommonClientConfigs.GROUP_INSTANCE_ID_DOC;

    /**
     * 单次poll调用返回的最大记录数
     * 
     * 设计原理：
     * 1. 控制单次拉取的数据量，避免消费者处理过载
     * 2. 不影响底层的fetch操作，消费者会缓存fetch的数据
     * 3. 通过增量返回来平滑消费流程
     * 
     * 最佳实践：
     * 1. 根据消息处理速度和内存限制来设置
     * 2. 设置过大可能导致消费者处理延迟
     * 3. 设置过小可能影响吞吐量
     */
    public static final String MAX_POLL_RECORDS_CONFIG = "max.poll.records";
    private static final String MAX_POLL_RECORDS_DOC = "单次poll()调用返回的最大记录数。" +
        "注意，" + MAX_POLL_RECORDS_CONFIG + "不影响底层的拉取行为。" +
        "消费者会缓存每次拉取请求的记录，并在每次poll时增量返回。";
    public static final int DEFAULT_MAX_POLL_RECORDS = 500;

    /**
     * 两次poll调用的最大时间间隔
     * 如果超过此时间没有调用poll，消费者会被认为死亡，触发rebalance
     * 
     * 设计原理：
     * 1. 检测消费者是否存活
     * 2. 防止消费者长时间占用分区但不消费
     * 3. 确保组内资源得到及时释放
     * 
     * 最佳实践：
     * 1. 设置值应大于消息处理的最大耗时
     * 2. 考虑GC暂停等因素留出余量
     * 3. 避免设置过小导致频繁rebalance
     */
    public static final String MAX_POLL_INTERVAL_MS_CONFIG = CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG;
    private static final String MAX_POLL_INTERVAL_MS_DOC = CommonClientConfigs.MAX_POLL_INTERVAL_MS_DOC;

    /**
     * 会话超时时间配置
     * 消费者与coordinator之间的心跳超时时间
     * 
     * 设计原理：
     * 1. 用于检测消费者故障
     * 2. 控制rebalance的敏感度
     * 3. 平衡可用性和故障检测速度
     * 
     * 最佳实践：
     * 1. 设置为heartbeat.interval.ms的3倍以上
     * 2. 考虑网络延迟设置合适的值
     * 3. 通常设置在10秒到5分钟之间
     */
    public static final String SESSION_TIMEOUT_MS_CONFIG = CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG;
    private static final String SESSION_TIMEOUT_MS_DOC = CommonClientConfigs.SESSION_TIMEOUT_MS_DOC;

    /**
     * 心跳间隔时间配置
     * 消费者向coordinator发送心跳的频率
     * 
     * 设计原理：
     * 1. 维持消费者组成员的存活状态
     * 2. 及时检测消费者故障
     * 3. 控制心跳开销
     * 
     * 最佳实践：
     * 1. 设置为session.timeout.ms的1/3
     * 2. 避免设置过小增加不必要的网络开销
     * 3. 避免设置过大延迟故障检测
     */
    public static final String HEARTBEAT_INTERVAL_MS_CONFIG = CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG;
    private static final String HEARTBEAT_INTERVAL_MS_DOC = CommonClientConfigs.HEARTBEAT_INTERVAL_MS_DOC;

    /**
     * 消费者组协议配置
     * 指定消费者组使用的协议类型
     * 
     * 设计原理：
     * 1. classic：传统的消费者组协议，所有成员同时参与rebalance
     * 2. consumer：新版协议，支持增量式rebalance，减少服务中断
     * 
     * 最佳实践：
     * 1. 新应用推荐使用consumer协议
     * 2. 升级时需要注意协议兼容性
     */
    public static final String GROUP_PROTOCOL_CONFIG = "group.protocol";
    public static final String DEFAULT_GROUP_PROTOCOL = GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT);
    public static final String GROUP_PROTOCOL_DOC = "消费者组使用的协议类型。当前支持 \"classic\" 或 \"consumer\"。" +
        "如果指定为 \"consumer\"，将使用新版消费者组协议，否则使用传统协议。";

    /**
     * 远程分配器配置
     * 用于指定服务端使用的分区分配器
     * 
     * 设计原理：
     * 1. 将分区分配逻辑移至服务端执行
     * 2. 支持自定义的分配策略
     * 3. 便于集中管理和控制
     * 
     * 最佳实践：
     * 1. 仅在使用consumer协议时可用
     * 2. 如果不指定，coordinator会自动选择
     */
    public static final String GROUP_REMOTE_ASSIGNOR_CONFIG = "group.remote.assignor";
    public static final String DEFAULT_GROUP_REMOTE_ASSIGNOR = null;
    public static final String GROUP_REMOTE_ASSIGNOR_DOC = "服务端使用的分配器。如果未指定，" +
        "组协调器将自动选择一个。此配置仅在 group.protocol 设置为 \"consumer\" 时有效。";

    /**
     * <code>bootstrap.servers</code>
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /** <code>client.dns.lookup</code> */
    public static final String CLIENT_DNS_LOOKUP_CONFIG = CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG;

    /**
     * 自动提交配置
     * 控制是否自动提交消费位移
     * 
     * 设计原理：
     * 1. 简化位移管理，自动保存消费进度
     * 2. 定期异步提交，减少性能影响
     * 3. 在出现故障时可能丢失部分提交
     * 
     * 最佳实践：
     * 1. 对数据一致性要求高的场景建议手动提交
     * 2. 使用自动提交时注意提交间隔设置
     * 3. 考虑是否需要精确的位移控制
     */
    public static final String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";
    private static final String ENABLE_AUTO_COMMIT_DOC = "是否在后台自动提交消费者的偏移量。";

    /**
     * 自动提交间隔配置
     * 控制自动提交的频率
     * 
     * 设计原理：
     * 1. 平衡提交频率和性能开销
     * 2. 控制潜在的数据丢失量
     * 3. 避免过于频繁的网络请求
     * 
     * 最佳实践：
     * 1. 根据业务容忍的数据丢失量设置
     * 2. 考虑网络开销和延迟
     * 3. 通常设置在1-10秒之间
     */
    public static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = "auto.commit.interval.ms";
    private static final String AUTO_COMMIT_INTERVAL_MS_DOC = "当enable.auto.commit设置为true时，消费者偏移量自动提交到Kafka的频率（毫秒）。";

    /**
     * 分区分配策略配置
     * 指定消费者组内分区分配的策略列表
     * 
     * 设计原理：
     * 1. 支持多种分配策略，按优先级排序
     * 2. 可以实现自定义的分配策略
     * 3. 不同策略适用于不同场景
     * 
     * 内置策略说明：
     * 1. RangeAssignor: 
     *    - 按主题划分分区范围
     *    - 适合分区数量均匀的场景
     *    - 可能导致分配不均
     * 
     * 2. RoundRobinAssignor:
     *    - 轮询分配所有分区
     *    - 更均匀的分配
     *    - 可能打散相关分区
     * 
     * 3. StickyAssignor:
     *    - 尽量保持现有分配关系
     *    - 减少分区迁移
     *    - 在rebalance时更高效
     * 
     * 4. CooperativeStickyAssignor:
     *    - 支持渐进式rebalance
     *    - 最小化服务中断
     *    - 推荐在生产环境使用
     * 
     * 最佳实践：
     * 1. 新应用推荐使用CooperativeStickyAssignor
     * 2. 考虑业务对分区分配均匀性的要求
     * 3. 评估rebalance对服务的影响
     */
    public static final String PARTITION_ASSIGNMENT_STRATEGY_CONFIG = "partition.assignment.strategy";
    private static final String PARTITION_ASSIGNMENT_STRATEGY_DOC = "消费者用于分配分区所有权的分区分配策略类名列表，按优先级排序。可用选项包括：" +
        "<ul>" +
        "<li>org.apache.kafka.clients.consumer.RangeAssignor：按主题分配分区</li>" +
        "<li>org.apache.kafka.clients.consumer.RoundRobinAssignor：轮询方式分配分区</li>" +
        "<li>org.apache.kafka.clients.consumer.StickyAssignor：保证最大均衡性的同时尽量保持现有分配</li>" +
        "<li>org.apache.kafka.clients.consumer.CooperativeStickyAssignor：遵循StickyAssignor的逻辑，但支持协作式rebalance</li>" +
        "</ul>" +
        "<p>默认分配器是[RangeAssignor, CooperativeStickyAssignor]，默认使用RangeAssignor，" +
        "但通过一次滚动重启移除RangeAssignor即可升级到CooperativeStickyAssignor。</p>" +
        "<p>实现org.apache.kafka.clients.consumer.ConsumerPartitionAssignor接口可以插入自定义分配策略。</p>";

    /**
     * 偏移量重置策略配置
     * 当找不到消费者组的偏移量或偏移量无效时的处理策略
     * 
     * 设计原理：
     * 1. 处理首次消费或偏移量过期的场景
     * 2. 提供多种重置选项满足不同需求
     * 3. 确保消费者可以从合适的位置开始消费
     * 
     * 可选策略：
     * 1. earliest: 从最早的偏移量开始
     * 2. latest: 从最新的偏移量开始
     * 3. by_duration: 从指定时间点开始
     * 4. none: 抛出异常
     * 
     * 最佳实践：
     * 1. 首次消费时建议使用earliest
     * 2. 实时处理场景可以使用latest
     * 3. 注意分区数变化可能导致数据丢失
     */
    public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";
    public static final String AUTO_OFFSET_RESET_DOC = "当Kafka中没有初始偏移量或当前偏移量在服务器上不存在时（如数据被删除）的处理策略：" +
            "<ul><li>earliest：自动重置到最早的偏移量</li>" +
            "<li>latest：自动重置到最新的偏移量</li>" +
            "<li>by_duration:&lt;duration&gt;：自动重置到距当前时间指定时长的偏移量。&lt;duration&gt;必须使用ISO8601格式（PnDTnHnMn.nS）。" +
            "不允许使用负时长。</li>" +
            "<li>none：如果消费者组没有找到先前的偏移量，则向消费者抛出异常</li>" +
            "<li>其他值：向消费者抛出异常</li></ul>" +
            "<p>注意：在设置此配置为latest的情况下更改分区数量可能导致消息传递丢失，" +
            "因为生产者可能在消费者重置偏移量之前就开始向新添加的分区发送消息（即还没有初始偏移量）。</p>";

    /**
     * 最小获取字节数配置
     * 服务器返回获取请求的最小数据量
     * 
     * 设计原理：
     * 1. 控制网络请求的效率
     * 2. 平衡延迟和吞吐量
     * 3. 减少小数据量的频繁请求
     * 
     * 工作流程：
     * 1. 消费者发送fetch请求
     * 2. 如果可用数据小于此值，服务器会等待
     * 3. 直到数据量达到要求或超时
     * 
     * 最佳实践：
     * 1. 批量处理场景可以设置较大值
     * 2. 实时处理场景可以设置较小值
     * 3. 需要权衡延迟和吞吐量
     */
    public static final String FETCH_MIN_BYTES_CONFIG = "fetch.min.bytes";
    public static final int DEFAULT_FETCH_MIN_BYTES = 1;
    private static final String FETCH_MIN_BYTES_DOC = "服务器应该为获取请求返回的最小数据量。如果可用数据量不足，" +
        "请求会等待积累足够的数据量后再返回。默认设置为" + DEFAULT_FETCH_MIN_BYTES + "字节意味着只要有这么多字节的数据可用，" +
        "或者获取请求超时等待数据到达，就会返回响应。将此值设置得更大可以提高服务器的吞吐量，代价是增加一些延迟。";

    /**
     * 最大获取字节数配置
     * 服务器返回获取请求的最大数据量
     * 
     * 设计原理：
     * 1. 控制单次请求的数据量
     * 2. 防止内存溢出
     * 3. 保证消费者能够处理数据
     * 
     * 注意事项：
     * 1. 不是绝对的限制
     * 2. 第一个非空分区的记录批次可能超过此限制
     * 3. 消费者会并行执行多个获取
     * 
     * 最佳实践：
     * 1. 根据消费者内存配置设置
     * 2. 考虑网络带宽限制
     * 3. 需要小于broker的message.max.bytes
     */
    public static final String FETCH_MAX_BYTES_CONFIG = "fetch.max.bytes";
    private static final String FETCH_MAX_BYTES_DOC = "服务器应该为获取请求返回的最大数据量。" +
            "消费者以批次方式获取记录，如果第一个非空分区中的第一个记录批次大于此值，" +
            "该记录批次仍会返回以确保消费者能够继续工作。因此，这不是绝对的最大值。" +
            "broker接受的最大记录批次大小由broker配置message.max.bytes或主题配置max.message.bytes定义。" +
            "注意消费者会并行执行多个获取操作。";
    public static final int DEFAULT_FETCH_MAX_BYTES = 50 * 1024 * 1024;

    /**
     * 获取等待时间配置
     * 服务器等待数据累积的最大时间
     * 
     * 设计原理：
     * 1. 在数据量不足时提供等待机制
     * 2. 避免空轮询消耗资源
     * 3. 平衡实时性和效率
     * 
     * 处理流程：
     * 1. 消费者发送fetch请求
     * 2. 如果数据量小于fetch.min.bytes
     * 3. 服务器最多等待此配置的时间
     * 4. 超时后无论数据量是否满足都返回
     * 
     * 最佳实践：
     * 1. 根据业务对实时性的要求设置
     * 2. 考虑生产者的生产速率
     * 3. 通常设置在100ms-1s之间
     */
    public static final String FETCH_MAX_WAIT_MS_CONFIG = "fetch.max.wait.ms";
    private static final String FETCH_MAX_WAIT_MS_DOC = "当没有足够的数据立即满足fetch.min.bytes要求时，" +
            "服务器在响应获取请求之前将阻塞的最大时间。此配置仅用于本地日志获取。要调整远程获取的最大等待时间，" +
            "请参考broker配置'remote.fetch.max.wait.ms'";
    public static final int DEFAULT_FETCH_MAX_WAIT_MS = 500;

    /**
     * 元数据最大年龄配置
     * 控制元数据的刷新频率
     * 
     * 设计原理：
     * 1. 确保消费者及时感知集群变化
     * 2. 控制元数据刷新开销
     * 3. 平衡一致性和性能
     */
    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;

    /**
     * 分区最大获取字节数配置
     * 限制每个分区返回的数据量
     * 
     * 设计原理：
     * 1. 控制单个分区的数据量
     * 2. 防止单个分区占用过多资源
     * 3. 确保公平的资源分配
     * 
     * 注意事项：
     * 1. 不是绝对限制
     * 2. 第一个记录批次可能超过此限制
     * 3. 需要考虑消息大小
     * 
     * 最佳实践：
     * 1. 根据消息大小设置合适的值
     * 2. 考虑内存限制
     * 3. 需要小于fetch.max.bytes
     */
    public static final String MAX_PARTITION_FETCH_BYTES_CONFIG = "max.partition.fetch.bytes";
    private static final String MAX_PARTITION_FETCH_BYTES_DOC = "服务器为每个分区返回的最大数据量。" +
            "消费者以批次方式获取记录。如果第一个非空分区中的第一个记录批次大于此限制，" +
            "该批次仍会返回以确保消费者能够继续工作。broker接受的最大记录批次大小由broker配置message.max.bytes或" +
            "主题配置max.message.bytes定义。参见" + FETCH_MAX_BYTES_CONFIG + "以限制消费者请求大小。";
    public static final int DEFAULT_MAX_PARTITION_FETCH_BYTES = 1 * 1024 * 1024;

    /**
     * 发送缓冲区大小配置
     * 设置TCP发送缓冲区的大小
     * 
     * 设计原理：
     * 1. 控制网络层面的数据发送效率
     * 2. 影响网络吞吐量和延迟
     * 3. 平衡内存使用和性能
     * 
     * 系统影响：
     * 1. 较大的缓冲区可以提高吞吐量
     * 2. 较小的缓冲区可以减少延迟
     * 3. 影响内存使用量
     * 
     * 最佳实践：
     * 1. 高吞吐量场景可以适当增大
     * 2. 根据网络带宽和延迟调整
     * 3. 考虑系统可用内存
     */
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

    /**
     * 接收缓冲区大小配置
     * 设置TCP接收缓冲区的大小
     * 
     * 设计原理：
     * 1. 控制网络数据接收能力
     * 2. 影响消费者的数据处理能力
     * 3. 防止网络拥塞和数据丢失
     * 
     * 调优建议：
     * 1. 高并发场景下适当增大
     * 2. 考虑消息大小和频率
     * 3. 需要与系统资源匹配
     */
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

    /**
     * 客户端ID配置
     * 用于标识客户端的字符串
     * 
     * 设计原理：
     * 1. 用于监控和调试
     * 2. 在日志中区分不同客户端
     * 3. 用于客户端配额管理
     * 
     * 生成规则：
     * 1. 如果未指定，自动生成
     * 2. 格式：consumer-{groupId}-{序号}
     * 3. 使用groupInstanceId替代序号（如果有）
     * 
     * 最佳实践：
     * 1. 使用有意义的标识符
     * 2. 在监控系统中便于识别
     * 3. 确保在需要时可追踪
     */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

    /**
     * 客户端机架配置
     * 标识客户端所在的机架位置
     * 
     * 设计原理：
     * 1. 支持机架感知功能
     * 2. 优化数据本地性
     * 3. 提高可用性和性能
     * 
     * 使用场景：
     * 1. 跨数据中心部署
     * 2. 机架感知的分区分配
     * 3. 故障域隔离
     */
    public static final String CLIENT_RACK_CONFIG = CommonClientConfigs.CLIENT_RACK_CONFIG;
    public static final String DEFAULT_CLIENT_RACK = CommonClientConfigs.DEFAULT_CLIENT_RACK;

    /**
     * 重连退避时间配置
     * 重试连接失败的broker之前等待的时间
     * 
     * 设计原理：
     * 1. 避免立即重试造成资源浪费
     * 2. 实现退避策略减轻服务器负载
     * 3. 防止连接风暴
     * 
     * 退避策略：
     * 1. 初始等待此配置的时间
     * 2. 之后呈指数增长
     * 3. 最大不超过reconnect.backoff.max.ms
     * 
     * 最佳实践：
     * 1. 根据网络环境调整
     * 2. 考虑故障恢复时间
     * 3. 避免设置过短导致频繁重试
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /**
     * 最大重试退避时间配置
     * 重试退避时间的上限值
     * 
     * 设计原理：
     * 1. 限制重试间隔的最大值
     * 2. 确保重试不会无限延长
     * 3. 在持续故障时保持合理的重试频率
     * 
     * 使用场景：
     * 1. 长时间的网络分区
     * 2. 服务器维护期间
     * 3. 灾难恢复过程
     */
    public static final String RETRY_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MAX_MS_CONFIG;

    /**
     * 指标采样窗口配置
     * 性能指标收集的时间窗口大小
     * 
     * 设计原理：
     * 1. 控制指标统计的精度
     * 2. 平衡精确度和资源消耗
     * 3. 提供合适的监控粒度
     * 
     * 影响因素：
     * 1. 监控系统的要求
     * 2. 系统资源限制
     * 3. 数据分析需求
     * 
     * 最佳实践：
     * 1. 根据监控需求调整
     * 2. 考虑存储开销
     * 3. 平衡实时性和资源消耗
     */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /**
     * 指标样本数量配置
     * 每个指标维护的样本数量
     * 
     * 设计原理：
     * 1. 控制内存中保存的样本数
     * 2. 影响统计结果的准确性
     * 3. 支持移动平均计算
     * 
     * 资源影响：
     * 1. 内存使用量
     * 2. CPU计算开销
     * 3. 监控数据精确度
     */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

    /**
     * 指标记录级别配置
     * 控制要记录的指标详细程度
     * 
     * 设计原理：
     * 1. 灵活控制监控粒度
     * 2. 平衡监控和性能
     * 3. 支持不同监控需求
     * 
     * 可选级别：
     * 1. INFO: 基本指标
     * 2. DEBUG: 详细指标
     * 3. TRACE: 最详细的指标
     */
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

    /**
     * 指标报告器配置
     * 指定用于报告指标的类
     * 
     * 设计原理：
     * 1. 支持自定义指标输出
     * 2. 集成不同监控系统
     * 3. 灵活的指标处理
     * 
     * 默认行为：
     * 1. 使用JMX报告器
     * 2. 支持多个报告器
     * 3. 可扩展的设计
     */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /**
     * CRC校验配置
     * 是否自动检查消息的CRC32校验和
     * 
     * 设计原理：
     * 1. 确保消息完整性
     * 2. 检测传输过程中的损坏
     * 3. 提供数据可靠性保证
     * 
     * 性能影响：
     * 1. 增加一定的CPU开销
     * 2. 影响消息处理速度
     * 3. 可以禁用以提高性能
     * 
     * 最佳实践：
     * 1. 生产环境建议启用
     * 2. 性能测试时可以禁用
     * 3. 根据可靠性需求决定
     */
    public static final String CHECK_CRCS_CONFIG = "check.crcs";
    private static final String CHECK_CRCS_DOC = "是否自动检查消费的记录的CRC32校验和。这确保消息在传输或磁盘存储过程中没有损坏。" +
        "此检查会增加一些开销，因此在追求极致性能的场景下可以禁用。";

    /**
     * 键反序列化器配置
     * 用于反序列化消息键的类
     * 
     * 设计原理：
     * 1. 支持自定义数据格式
     * 2. 提供类型安全
     * 3. 灵活的数据转换
     * 
     * 常用实现：
     * 1. StringDeserializer
     * 2. IntegerDeserializer
     * 3. ByteArrayDeserializer
     * 
     * 注意事项：
     * 1. 必须与生产者序列化器匹配
     * 2. 线程安全的实现
     * 3. 处理异常情况
     */
    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";
    public static final String KEY_DESERIALIZER_CLASS_DOC = "消息键的反序列化器类，必须实现org.apache.kafka.common.serialization.Deserializer接口。";

    /**
     * 值反序列化器配置
     * 用于反序列化消息值的类
     * 
     * 设计原理：
     * 1. 处理不同类型的消息内容
     * 2. 支持自定义数据格式
     * 3. 提供类型转换功能
     * 
     * 使用建议：
     * 1. 选择合适的内置实现
     * 2. 考虑性能影响
     * 3. 处理兼容性问题
     */
    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";
    public static final String VALUE_DESERIALIZER_CLASS_DOC = "消息值的反序列化器类，必须实现org.apache.kafka.common.serialization.Deserializer接口。";

    /**
     * 套接字连接超时配置
     * 建立TCP连接的超时时间
     * 
     * 设计原理：
     * 1. 控制连接建立的等待时间
     * 2. 及时发现连接问题
     * 3. 避免连接挂起
     * 
     * 使用场景：
     * 1. 网络不稳定环境
     * 2. 跨数据中心连接
     * 3. 高延迟网络
     * 
     * 最佳实践：
     * 1. 根据网络状况调整
     * 2. 考虑重试机制
     * 3. 设置合理的超时值
     */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;

    /**
     * 最大套接字连接超时配置
     * 建立TCP连接的最大超时时间
     * 
     * 设计原理：
     * 1. 限制最大连接时间
     * 2. 防止无限等待
     * 3. 在重试情况下提供上限
     * 
     * 应用场景：
     * 1. 需要重试的环境
     * 2. 不稳定的网络
     * 3. 负载均衡场景
     */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;

    /**
     * 连接最大空闲时间配置
     * 连接保持空闲的最长时间
     * 
     * 设计原理：
     * 1. 管理空闲连接资源
     * 2. 避免资源浪费
     * 3. 及时释放无用连接
     * 
     * 资源影响：
     * 1. 连接池大小
     * 2. 系统文件描述符
     * 3. 网络资源占用
     * 
     * 最佳实践：
     * 1. 根据业务访问模式设置
     * 2. 考虑系统资源限制
     * 3. 平衡重用和释放
     */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /**
     * 请求超时配置
     * 等待请求响应的最长时间
     * 
     * 设计原理：
     * 1. 控制单个请求的最长等待时间
     * 2. 避免请求无限等待
     * 3. 及时发现请求问题
     * 
     * 影响因素：
     * 1. 网络延迟
     * 2. 服务器处理能力
     * 3. 请求复杂度
     * 
     * 最佳实践：
     * 1. 设置合理的超时时间
     * 2. 考虑重试机制
     * 3. 监控超时情况
     */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;

    /**
     * 默认API超时配置
     * 消费者API调用的默认超时时间
     * 
     * 设计原理：
     * 1. 为所有API操作提供默认超时
     * 2. 确保操作能够及时完成
     * 3. 防止操作无限阻塞
     * 
     * 应用范围：
     * 1. 同步API调用
     * 2. 阻塞操作
     * 3. 需要等待的操作
     * 
     * 配置建议：
     * 1. 根据操作类型调整
     * 2. 考虑业务容忍度
     * 3. 预留足够余量
     */
    public static final String DEFAULT_API_TIMEOUT_MS_CONFIG = CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG;

    /**
     * 拦截器类配置
     * 用于注册消费者拦截器
     * 
     * 设计原理：
     * 1. 提供消息处理的扩展点
     * 2. 支持自定义处理逻辑
     * 3. 实现横切关注点
     * 
     * 常见用途：
     * 1. 消息过滤
     * 2. 消息转换
     * 3. 监控和统计
     * 4. 数据审计
     * 
     * 使用建议：
     * 1. 避免重量级操作
     * 2. 注意异常处理
     * 3. 保持无状态设计
     */
    public static final String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
    public static final String INTERCEPTOR_CLASSES_DOC = "拦截器类列表。" +
        "通过实现org.apache.kafka.clients.consumer.ConsumerInterceptor接口，" +
        "可以拦截（并可能修改）消费者接收到的记录。默认没有拦截器。";

    /**
     * 排除内部主题配置
     * 是否在订阅时排除内部主题
     * 
     * 设计原理：
     * 1. 保护内部主题
     * 2. 避免误操作
     * 3. 区分业务主题
     * 
     * 内部主题：
     * 1. __consumer_offsets
     * 2. __transaction_state
     * 3. 其他系统主题
     * 
     * 使用建议：
     * 1. 通常保持默认值
     * 2. 特殊场景可以关闭
     * 3. 谨慎操作内部主题
     */
    public static final String EXCLUDE_INTERNAL_TOPICS_CONFIG = "exclude.internal.topics";
    private static final String EXCLUDE_INTERNAL_TOPICS_DOC = "是否在订阅模式下排除匹配的内部主题。" +
            "始终可以通过显式订阅来访问内部主题。";
    public static final boolean DEFAULT_EXCLUDE_INTERNAL_TOPICS = true;

    /**
     * 关闭时离组配置
     * 控制消费者关闭时是否主动离开消费者组
     * 
     * 设计原理：
     * 1. 控制优雅关闭行为
     * 2. 影响重平衡触发时机
     * 3. 管理组成员生命周期
     * 
     * 影响：
     * 1. 重平衡延迟
     * 2. 资源释放时机
     * 3. 其他消费者分配
     * 
     * 注意：这是一个内部配置，未来可能发生变化
     */
    static final String LEAVE_GROUP_ON_CLOSE_CONFIG = "internal.leave.group.on.close";

    /**
     * 事务隔离级别配置
     * 控制如何读取事务性消息
     * 
     * 设计原理：
     * 1. 提供事务消息的读取语义
     * 2. 控制可见性级别
     * 3. 保证数据一致性
     * 
     * 可选级别：
     * 1. read_committed：只读取已提交事务的消息
     * 2. read_uncommitted：读取所有消息
     * 
     * 使用建议：
     * 1. 根据一致性要求选择
     * 2. 考虑性能影响
     * 3. 注意与生产者配合
     */
    public static final String ISOLATION_LEVEL_CONFIG = "isolation.level";
    public static final String ISOLATION_LEVEL_DOC = "控制如何读取事务性写入的消息。" +
            "如果设置为read_committed，consumer.poll()将只返回已提交的事务性消息。" +
            "如果设置为read_uncommitted（默认值），consumer.poll()将返回所有消息，" +
            "包括已中止的事务性消息。非事务性消息在任何模式下都会无条件返回。" +
            "消息始终按偏移量顺序返回。因此，在read_committed模式下，" +
            "consumer.poll()将只返回到最后稳定偏移量（LSO）的消息，" +
            "即第一个打开事务的偏移量减一。特别是，在有正在进行的事务时，" +
            "任何出现在这些事务之后的消息都将被保留，直到相关事务完成。" +
            "因此，read_committed消费者在有进行中的事务时将无法读取到高水位线。" +
            "此外，在read_committed模式下，seekToEnd方法将返回LSO。";

    public static final String DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_UNCOMMITTED.toString();

    /**
     * 允许自动创建主题配置
     * 控制消费者是否允许自动创建主题
     * 
     * 设计原理：
     * 1. 简化主题管理
     * 2. 支持动态创建主题
     * 3. 提供便利性功能
     * 
     * 使用场景：
     * 1. 开发测试环境
     * 2. 自动化部署
     * 3. 动态主题创建
     * 
     * 安全考虑：
     * 1. 生产环境谨慎使用
     * 2. 权限控制
     * 3. 避免误操作
     */
    public static final String ALLOW_AUTO_CREATE_TOPICS_CONFIG = "allow.auto.create.topics";
    private static final String ALLOW_AUTO_CREATE_TOPICS_DOC = "当订阅或分配主题时，是否允许在broker上自动创建主题。" +
            "只有当broker配置auto.create.topics.enable允许时，订阅的主题才会被自动创建。" +
            "使用0.11.0之前版本的broker时，此配置必须设置为true。";
    public static final boolean DEFAULT_ALLOW_AUTO_CREATE_TOPICS = true;

    /**
     * 安全提供者配置
     * 配置自定义的安全提供者
     * 
     * 设计原理：
     * 1. 支持自定义安全实现
     * 2. 扩展安全机制
     * 3. 集成第三方安全提供者
     * 
     * 应用场景：
     * 1. 自定义加密算法
     * 2. 特殊安全需求
     * 3. 企业安全标准
     * 
     * 注意事项：
     * 1. 性能影响
     * 2. 兼容性验证
     * 3. 安全性评估
     */
    public static final String SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG;
    private static final String SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

    /**
     * 消费者客户端ID序列生成器
     * 用于生成唯一的客户端标识符
     * 
     * 设计原理：
     * 1. 确保客户端ID唯一性
     * 2. 支持自动生成模式
     * 3. 便于跟踪和监控
     * 
     * 生成规则：
     * 1. 基于原子计数器
     * 2. 线程安全设计
     * 3. 单调递增
     */
    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * 经典协议不支持的配置列表
     * 列出在使用CLASSIC协议时不支持的配置项
     * 
     * 设计目的：
     * 1. 明确协议限制
     * 2. 防止错误配置
     * 3. 提供清晰的兼容性信息
     * 
     * 使用说明：
     * 1. 用于配置验证
     * 2. 在协议切换时参考
     * 3. 避免不兼容配置
     */
    private static final List<String> CLASSIC_PROTOCOL_UNSUPPORTED_CONFIGS = Collections.singletonList(
            GROUP_REMOTE_ASSIGNOR_CONFIG
    );

    /**
     * 消费者协议不支持的配置列表
     * 列出在使用CONSUMER协议时不支持的配置项
     * 
     * 设计目的：
     * 1. 区分协议特性
     * 2. 避免无效配置
     * 3. 协议兼容性管理
     * 
     * 包含配置：
     * 1. 分区分配策略
     * 2. 心跳间隔
     * 3. 会话超时
     */
    private static final List<String> CONSUMER_PROTOCOL_UNSUPPORTED_CONFIGS = List.of(
            PARTITION_ASSIGNMENT_STRATEGY_CONFIG, 
            HEARTBEAT_INTERVAL_MS_CONFIG, 
            SESSION_TIMEOUT_MS_CONFIG
    );

    /**
     * 配置定义初始化
     * 定义所有消费者配置项的元数据和验证规则
     * 
     * 设计原理：
     * 1. 集中管理配置定义
     * 2. 提供配置验证
     * 3. 支持配置文档生成
     * 
     * 配置分类：
     * 1. 基础连接配置
     * 2. 消费者组配置
     * 3. 性能相关配置
     * 4. 安全相关配置
     * 
     * 验证规则：
     * 1. 类型检查
     * 2. 取值范围验证
     * 3. 依赖关系检查
     */
    static {
        CONFIG = new ConfigDef().define(BOOTSTRAP_SERVERS_CONFIG,
                                        Type.LIST,
                                        Collections.emptyList(),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.HIGH,
                                        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                                .define(CLIENT_DNS_LOOKUP_CONFIG,
                                        Type.STRING,
                                        ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                        in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                           ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
                                .define(GROUP_ID_CONFIG, Type.STRING, null, Importance.HIGH, GROUP_ID_DOC)
                                .define(GROUP_INSTANCE_ID_CONFIG,
                                        Type.STRING,
                                        null,
                                        new ConfigDef.NonEmptyString(),
                                        Importance.MEDIUM,
                                        GROUP_INSTANCE_ID_DOC)
                                .define(SESSION_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        45000,
                                        Importance.HIGH,
                                        SESSION_TIMEOUT_MS_DOC)
                                .define(HEARTBEAT_INTERVAL_MS_CONFIG,
                                        Type.INT,
                                        3000,
                                        Importance.HIGH,
                                        HEARTBEAT_INTERVAL_MS_DOC)
                                .define(PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                                        Type.LIST,
                                        List.of(RangeAssignor.class, CooperativeStickyAssignor.class),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.MEDIUM,
                                        PARTITION_ASSIGNMENT_STRATEGY_DOC)
                                .define(METADATA_MAX_AGE_CONFIG,
                                        Type.LONG,
                                        5 * 60 * 1000,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METADATA_MAX_AGE_DOC)
                                .define(ENABLE_AUTO_COMMIT_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.MEDIUM,
                                        ENABLE_AUTO_COMMIT_DOC)
                                .define(AUTO_COMMIT_INTERVAL_MS_CONFIG,
                                        Type.INT,
                                        5000,
                                        atLeast(0),
                                        Importance.LOW,
                                        AUTO_COMMIT_INTERVAL_MS_DOC)
                                .define(CLIENT_ID_CONFIG,
                                        Type.STRING,
                                        "",
                                        Importance.LOW,
                                        CommonClientConfigs.CLIENT_ID_DOC)
                                .define(CLIENT_RACK_CONFIG,
                                        Type.STRING,
                                        DEFAULT_CLIENT_RACK,
                                        Importance.LOW,
                                        CommonClientConfigs.CLIENT_RACK_DOC)
                                .define(MAX_PARTITION_FETCH_BYTES_CONFIG,
                                        Type.INT,
                                        DEFAULT_MAX_PARTITION_FETCH_BYTES,
                                        atLeast(0),
                                        Importance.HIGH,
                                        MAX_PARTITION_FETCH_BYTES_DOC)
                                .define(SEND_BUFFER_CONFIG,
                                        Type.INT,
                                        128 * 1024,
                                        atLeast(CommonClientConfigs.SEND_BUFFER_LOWER_BOUND),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SEND_BUFFER_DOC)
                                .define(RECEIVE_BUFFER_CONFIG,
                                        Type.INT,
                                        64 * 1024,
                                        atLeast(CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.RECEIVE_BUFFER_DOC)
                                .define(FETCH_MIN_BYTES_CONFIG,
                                        Type.INT,
                                        DEFAULT_FETCH_MIN_BYTES,
                                        atLeast(0),
                                        Importance.HIGH,
                                        FETCH_MIN_BYTES_DOC)
                                .define(FETCH_MAX_BYTES_CONFIG,
                                        Type.INT,
                                        DEFAULT_FETCH_MAX_BYTES,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        FETCH_MAX_BYTES_DOC)
                                .define(FETCH_MAX_WAIT_MS_CONFIG,
                                        Type.INT,
                                        DEFAULT_FETCH_MAX_WAIT_MS,
                                        atLeast(0),
                                        Importance.LOW,
                                        FETCH_MAX_WAIT_MS_DOC)
                                .define(RECONNECT_BACKOFF_MS_CONFIG,
                                        Type.LONG,
                                        50L,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                                .define(RECONNECT_BACKOFF_MAX_MS_CONFIG,
                                        Type.LONG,
                                        1000L,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
                                .define(RETRY_BACKOFF_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MS,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                                .define(RETRY_BACKOFF_MAX_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MAX_MS,
                                        atLeast(0L),
                                        Importance.LOW,
                                        CommonClientConfigs.RETRY_BACKOFF_MAX_MS_DOC)
                                .define(ENABLE_METRICS_PUSH_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.LOW,
                                        ENABLE_METRICS_PUSH_DOC)
                                .define(AUTO_OFFSET_RESET_CONFIG,
                                        Type.STRING,
                                        AutoOffsetResetStrategy.LATEST.name(),
                                        new AutoOffsetResetStrategy.Validator(),
                                        Importance.MEDIUM,
                                        AUTO_OFFSET_RESET_DOC)
                                .define(CHECK_CRCS_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.LOW,
                                        CHECK_CRCS_DOC)
                                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                                        Type.LONG,
                                        30000,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                                .define(METRICS_NUM_SAMPLES_CONFIG,
                                        Type.INT,
                                        2,
                                        atLeast(1),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                                .define(METRICS_RECORDING_LEVEL_CONFIG,
                                        Type.STRING,
                                        Sensor.RecordingLevel.INFO.toString(),
                                        in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString(), Sensor.RecordingLevel.TRACE.toString()),
                                        Importance.LOW,
                                        CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
                                .define(METRIC_REPORTER_CLASSES_CONFIG,
                                        Type.LIST,
                                        JmxReporter.class.getName(),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.LOW,
                                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                                .define(KEY_DESERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        KEY_DESERIALIZER_CLASS_DOC)
                                .define(VALUE_DESERIALIZER_CLASS_CONFIG,
                                        Type.CLASS,
                                        Importance.HIGH,
                                        VALUE_DESERIALIZER_CLASS_DOC)
                                .define(REQUEST_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        30000,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        REQUEST_TIMEOUT_MS_DOC)
                                .define(DEFAULT_API_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        60 * 1000,
                                        atLeast(0),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_DOC)
                                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
                                .define(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
                                /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
                                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                                        Type.LONG,
                                        9 * 60 * 1000,
                                        Importance.MEDIUM,
                                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                                .define(INTERCEPTOR_CLASSES_CONFIG,
                                        Type.LIST,
                                        Collections.emptyList(),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.LOW,
                                        INTERCEPTOR_CLASSES_DOC)
                                .define(MAX_POLL_RECORDS_CONFIG,
                                        Type.INT,
                                        DEFAULT_MAX_POLL_RECORDS,
                                        atLeast(1),
                                        Importance.MEDIUM,
                                        MAX_POLL_RECORDS_DOC)
                                .define(MAX_POLL_INTERVAL_MS_CONFIG,
                                        Type.INT,
                                        300000,
                                        atLeast(1),
                                        Importance.MEDIUM,
                                        MAX_POLL_INTERVAL_MS_DOC)
                                .define(EXCLUDE_INTERNAL_TOPICS_CONFIG,
                                        Type.BOOLEAN,
                                        DEFAULT_EXCLUDE_INTERNAL_TOPICS,
                                        Importance.MEDIUM,
                                        EXCLUDE_INTERNAL_TOPICS_DOC)
                                .defineInternal(LEAVE_GROUP_ON_CLOSE_CONFIG,
                                        Type.BOOLEAN,
                                        true,
                                        Importance.LOW)
                                .define(ISOLATION_LEVEL_CONFIG,
                                        Type.STRING,
                                        DEFAULT_ISOLATION_LEVEL,
                                        in(IsolationLevel.READ_COMMITTED.toString(), IsolationLevel.READ_UNCOMMITTED.toString()),
                                        Importance.MEDIUM,
                                        ISOLATION_LEVEL_DOC)
                                .define(ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                                        Type.BOOLEAN,
                                        DEFAULT_ALLOW_AUTO_CREATE_TOPICS,
                                        Importance.MEDIUM,
                                        ALLOW_AUTO_CREATE_TOPICS_DOC)
                                .define(GROUP_PROTOCOL_CONFIG,
                                        Type.STRING,
                                        DEFAULT_GROUP_PROTOCOL,
                                        ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(GroupProtocol.class)),
                                        Importance.HIGH,
                                        GROUP_PROTOCOL_DOC)
                                .define(GROUP_REMOTE_ASSIGNOR_CONFIG,
                                        Type.STRING,
                                        DEFAULT_GROUP_REMOTE_ASSIGNOR,
                                        Importance.MEDIUM,
                                        GROUP_REMOTE_ASSIGNOR_DOC)
                                // security support
                                .define(SECURITY_PROVIDERS_CONFIG,
                                        Type.STRING,
                                        null,
                                        Importance.LOW,
                                        SECURITY_PROVIDERS_DOC)
                                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                                        Type.STRING,
                                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                                        ConfigDef.CaseInsensitiveValidString
                                                .in(Utils.enumOptions(SecurityProtocol.class)),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                                .withClientSslSupport()
                                .withClientSaslSupport()
                                .define(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG,
                                        Type.STRING,
                                        CommonClientConfigs.DEFAULT_METADATA_RECOVERY_STRATEGY,
                                        ConfigDef.CaseInsensitiveValidString
                                                .in(Utils.enumOptions(MetadataRecoveryStrategy.class)),
                                        Importance.LOW,
                                        CommonClientConfigs.METADATA_RECOVERY_STRATEGY_DOC)
                                .define(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG,
                                        Type.LONG,
                                        CommonClientConfigs.DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS,
                                        atLeast(0),
                                        Importance.LOW,
                                        CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC);

    }

    /**
     * 配置后处理方法
     * 处理解析后的配置值
     * 
     * 设计原理：
     * 1. 配置值的后期处理
     * 2. 特殊规则应用
     * 3. 配置一致性检查
     * 
     * 处理内容：
     * 1. SASL机制验证
     * 2. 指数退避配置检查
     * 3. 客户端ID处理
     * 4. 自动提交配置处理
     * 
     * 注意事项：
     * 1. 保持向后兼容
     * 2. 处理配置依赖
     * 3. 验证配置有效性
     */
    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        CommonClientConfigs.postValidateSaslMechanismConfig(this);
        CommonClientConfigs.warnDisablingExponentialBackoff(this);
        Map<String, Object> refinedConfigs = CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
        maybeOverrideClientId(refinedConfigs);
        maybeOverrideEnableAutoCommit(refinedConfigs);
        checkUnsupportedConfigs();
        return refinedConfigs;
    }

    /**
     * 客户端ID重写方法
     * 处理消费者客户端ID的生成和重写
     * 
     * 设计原理：
     * 1. 确保每个消费者实例有唯一标识
     * 2. 支持自动生成和手动指定
     * 3. 便于监控和问题排查
     * 
     * 生成规则：
     * 1. 优先使用用户指定的clientId
     * 2. 如果未指定，则自动生成
     * 3. 自动生成格式：consumer-{groupId}-{序号/实例ID}
     * 
     * 实现细节：
     * 1. 检查现有clientId
     * 2. 获取groupId和实例ID
     * 3. 生成新的clientId
     * 4. 更新配置
     */
    private void maybeOverrideClientId(Map<String, Object> configs) {
        final String clientId = this.getString(CLIENT_ID_CONFIG);
        if (clientId == null || clientId.isEmpty()) {
            final String groupId = this.getString(GROUP_ID_CONFIG);
            String groupInstanceId = this.getString(GROUP_INSTANCE_ID_CONFIG);
            if (groupInstanceId != null)
                JoinGroupRequest.validateGroupInstanceId(groupInstanceId);

            String groupInstanceIdPart = groupInstanceId != null ? groupInstanceId : CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement() + "";
            String generatedClientId = String.format("consumer-%s-%s", groupId, groupInstanceIdPart);
            configs.put(CLIENT_ID_CONFIG, generatedClientId);
        }
    }

    /**
     * 反序列化器配置追加方法
     * 将反序列化器添加到现有配置中
     * 
     * 设计原理：
     * 1. 支持动态配置反序列化器
     * 2. 验证反序列化器有效性
     * 3. 确保配置完整性
     * 
     * 处理流程：
     * 1. 复制现有配置
     * 2. 添加键反序列化器
     * 3. 添加值反序列化器
     * 4. 验证配置完整性
     * 
     * 参数说明：
     * @param configs 原始配置
     * @param keyDeserializer 键反序列化器
     * @param valueDeserializer 值反序列化器
     * @return 更新后的配置
     */
    public static Map<String, Object> appendDeserializerToConfig(Map<String, Object> configs,
                                                                 Deserializer<?> keyDeserializer,
                                                                 Deserializer<?> valueDeserializer) {
        // validate deserializer configuration, if the passed deserializer instance is null, the user must explicitly set a valid deserializer configuration value
        Map<String, Object> newConfigs = new HashMap<>(configs);
        if (keyDeserializer != null)
            newConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        else if (newConfigs.get(KEY_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(KEY_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        if (valueDeserializer != null)
            newConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        else if (newConfigs.get(VALUE_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(VALUE_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        return newConfigs;
    }

    /**
     * 自动提交配置重写方法
     * 处理自动提交配置的特殊逻辑
     * 
     * 设计原理：
     * 1. 处理默认消费者组的特殊情况
     * 2. 确保配置安全性
     * 3. 防止数据丢失
     * 
     * 处理规则：
     * 1. 对于默认组ID（null），禁用自动提交
     * 2. 验证配置合法性
     * 3. 更新配置值
     * 
     * 异常处理：
     * 1. 检查配置冲突
     * 2. 抛出配置异常
     * 3. 提供错误信息
     */
    private void maybeOverrideEnableAutoCommit(Map<String, Object> configs) {
        Optional<String> groupId = Optional.ofNullable(getString(CommonClientConfigs.GROUP_ID_CONFIG));
        Map<String, Object> originals = originals();
        boolean enableAutoCommit = originals.containsKey(ENABLE_AUTO_COMMIT_CONFIG) ? getBoolean(ENABLE_AUTO_COMMIT_CONFIG) : false;
        if (groupId.isEmpty()) {
            if (!originals.containsKey(ENABLE_AUTO_COMMIT_CONFIG)) {
                configs.put(ENABLE_AUTO_COMMIT_CONFIG, false);
            } else if (enableAutoCommit) {
                throw new InvalidConfigurationException(ENABLE_AUTO_COMMIT_CONFIG + " 在使用默认消费者组（null）时不能设置为true。");
            }
        }
    }

    /**
     * 不支持配置检查方法
     * 验证协议特定的配置兼容性
     * 
     * 设计原理：
     * 1. 确保协议兼容性
     * 2. 防止配置错误
     * 3. 提供清晰的错误信息
     * 
     * 检查内容：
     * 1. CLASSIC协议的限制
     * 2. CONSUMER协议的限制
     * 3. 配置值有效性
     * 
     * 异常处理：
     * 1. 检测无效配置
     * 2. 抛出配置异常
     * 3. 提供详细错误信息
     */
    private void checkUnsupportedConfigs() {
        String groupProtocol = getString(GROUP_PROTOCOL_CONFIG);
        if (GroupProtocol.CLASSIC.name().equalsIgnoreCase(groupProtocol)) {
            checkUnsupportedConfigs(GroupProtocol.CLASSIC, CLASSIC_PROTOCOL_UNSUPPORTED_CONFIGS);
        } else if (GroupProtocol.CONSUMER.name().equalsIgnoreCase(groupProtocol)) {
            checkUnsupportedConfigs(GroupProtocol.CONSUMER, CONSUMER_PROTOCOL_UNSUPPORTED_CONFIGS);
        }
    }

    /**
     * 协议特定配置检查方法
     * 检查特定协议下不支持的配置项
     * 
     * 设计原理：
     * 1. 分离协议特定的验证逻辑
     * 2. 提供精确的错误信息
     * 3. 支持不同协议的验证
     * 
     * 验证流程：
     * 1. 检查协议类型
     * 2. 验证配置项
     * 3. 收集无效配置
     * 
     * 参数说明：
     * @param groupProtocol 消费者组协议
     * @param unsupportedConfigs 不支持的配置列表
     */
    private void checkUnsupportedConfigs(GroupProtocol groupProtocol, List<String> unsupportedConfigs) {
        if (getString(GROUP_PROTOCOL_CONFIG).equalsIgnoreCase(groupProtocol.name())) {
            List<String> invalidConfigs = new ArrayList<>();
            unsupportedConfigs.forEach(configName -> {
                Object config = originals().get(configName);
                if (config != null && !Utils.isBlank(config.toString())) {
                    invalidConfigs.add(configName);
                }
            });
            if (!invalidConfigs.isEmpty()) {
                throw new ConfigException(String.join(", ", invalidConfigs) +
                        " 在 " + GROUP_PROTOCOL_CONFIG + "=" + groupProtocol.name() + " 时不能设置");
            }
        }
    }

    /**
     * 构造函数 - Properties版本
     * 使用Properties对象初始化配置
     * 
     * @param props 配置属性
     */
    public ConsumerConfig(Properties props) {
        super(CONFIG, props);
    }

    /**
     * 构造函数 - Map版本
     * 使用Map对象初始化配置
     * 
     * @param props 配置映射
     */
    public ConsumerConfig(Map<String, Object> props) {
        super(CONFIG, props);
    }

    /**
     * 构造函数 - 带日志控制的Map版本
     * 使用Map对象初始化配置，可控制是否记录日志
     * 
     * @param props 配置映射
     * @param doLog 是否记录日志
     */
    protected ConsumerConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    /**
     * 获取所有配置名称
     * 返回所有已定义的配置项名称
     * 
     * @return 配置名称集合
     */
    public static Set<String> configNames() {
        return CONFIG.names();
    }

    /**
     * 获取配置定义
     * 返回配置定义的副本
     * 
     * @return 配置定义对象
     */
    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }

    /**
     * 主方法
     * 生成配置文档的HTML格式
     * 
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "consumerconfigs_" + config));
    }

}
