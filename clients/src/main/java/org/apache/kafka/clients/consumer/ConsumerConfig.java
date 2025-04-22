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
 * Kafka消费者配置类
 * 包含了所有Kafka消费者客户端的配置项
 * 
 * 设计目标：
 * 1. 提供统一的配置管理
 * 2. 支持消费者组的协调和管理
 * 3. 控制消费行为和性能参数
 * 
 * 主要功能：
 * 1. 消费者组配置：组ID、实例ID等
 * 2. 消费行为控制：批量大小、提交方式等
 * 3. 性能调优：缓冲区、超时时间等
 * 4. 分区分配策略：支持多种分配器
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
     * 
     * 设计原理：
     * 1. 支持多种分配策略，每种策略适用于不同场景
     * 2. 允许自定义分配策略以满足特殊需求
     * 3. 通过优先级列表实现平滑升级
     * 
     * 可用的分配策略：
     * 1. RangeAssignor：按主题分配分区，每个消费者负责连续的分区范围
     * 2. RoundRobinAssignor：轮询分配所有分区，确保均匀分布
     * 3. StickyAssignor：在均衡分配的同时尽量保持现有分配关系，减少分区迁移
     * 4. CooperativeStickyAssignor：在StickyAssignor基础上支持渐进式rebalance
     * 
     * 最佳实践：
     * 1. 默认使用[RangeAssignor, CooperativeStickyAssignor]组合
     * 2. 可以通过一次滚动升级切换到CooperativeStickyAssignor
     * 3. 对于特殊需求可以实现ConsumerPartitionAssignor接口自定义策略
     * 
     * <code>partition.assignment.strategy</code>
     */
    public static final String PARTITION_ASSIGNMENT_STRATEGY_CONFIG = "partition.assignment.strategy";
    private static final String PARTITION_ASSIGNMENT_STRATEGY_DOC = "A list of class names or class types, " +
        "ordered by preference, of supported partition assignment strategies that the client will use to distribute " +
        "partition ownership amongst consumer instances when group management is used. Available options are:" +
        "<ul>" +
        "<li><code>org.apache.kafka.clients.consumer.RangeAssignor</code>: Assigns partitions on a per-topic basis.</li>" +
        "<li><code>org.apache.kafka.clients.consumer.RoundRobinAssignor</code>: Assigns partitions to consumers in a round-robin fashion.</li>" +
        "<li><code>org.apache.kafka.clients.consumer.StickyAssignor</code>: Guarantees an assignment that is " +
        "maximally balanced while preserving as many existing partition assignments as possible.</li>" +
        "<li><code>org.apache.kafka.clients.consumer.CooperativeStickyAssignor</code>: Follows the same StickyAssignor " +
        "logic, but allows for cooperative rebalancing.</li>" +
        "</ul>" +
        "<p>The default assignor is [RangeAssignor, CooperativeStickyAssignor], which will use the RangeAssignor by default, " +
        "but allows upgrading to the CooperativeStickyAssignor with just a single rolling bounce that removes the RangeAssignor from the list.</p>" +
        "<p>Implementing the <code>org.apache.kafka.clients.consumer.ConsumerPartitionAssignor</code> " +
        "interface allows you to plug in a custom assignment strategy.</p>";

    /**
     * 自动偏移量重置配置
     * 
     * 设计原理：
     * 1. 处理消费者组首次消费或偏移量过期的场景
     * 2. 提供多种重置策略以满足不同业务需求
     * 3. 支持基于时间的偏移量重置
     * 
     * 使用场景：
     * 1. 新消费者组首次订阅主题
     * 2. 消费者组的偏移量已过期被删除
     * 3. 消费者请求的偏移量不存在
     * 
     * 可选值：
     * 1. earliest：自动重置到最早的偏移量
     *    - 适用于需要处理所有历史数据的场景
     *    - 可能导致大量积压消息的重新处理
     * 
     * 2. latest：自动重置到最新的偏移量
     *    - 适用于只关注最新数据的场景
     *    - 会跳过重置前的所有消息
     * 
     * 3. by_duration:<duration>：重置到距当前时间指定间隔的偏移量
     *    - 时间间隔必须使用ISO8601格式(PnDTnHnMn.nS)
     *    - 不允许使用负数时间间隔
     *    - 适用于需要从特定时间点开始处理的场景
     * 
     * 4. none：不自动重置，抛出异常
     *    - 适用于严格控制偏移量的场景
     *    - 需要手动处理偏移量重置
     * 
     * 注意事项：
     * 1. 设置为latest时增加分区可能导致消息丢失
     * 2. 生产者可能在消费者重置偏移量之前向新分区发送消息
     * 3. 重置策略会影响消息处理的完整性和顺序性
     * 
     * <code>auto.offset.reset</code>
     */
    public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";
    public static final String AUTO_OFFSET_RESET_DOC = "What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server " +
            "(e.g. because that data has been deleted): " +
            "<ul><li>earliest: automatically reset the offset to the earliest offset</li>" +
            "<li>latest: automatically reset the offset to the latest offset</li>" +
            "<li>by_duration:&lt;duration&gt;: automatically reset the offset to a configured &lt;duration&gt; from the current timestamp. &lt;duration&gt; must be specified in ISO8601 format (PnDTnHnMn.nS). " +
            "Negative duration is not allowed.</li>" +
            "<li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li>" +
            "<li>anything else: throw exception to the consumer.</li></ul>" +
            "<p>Note that altering partition numbers while setting this config to latest may cause message delivery loss since " +
            "producers could start to send messages to newly added partitions (i.e. no initial offsets exist yet) before consumers reset their offsets.";

    /**
     * 最小获取字节数配置
     * 
     * 设计原理：
     * 1. 控制服务端返回数据的最小量
     * 2. 平衡延迟和吞吐量
     * 3. 减少小数据量请求的频繁交互
     * 
     * 工作机制：
     * 1. 如果可用数据量小于此值，服务器会等待
     * 2. 等待期间积累更多数据
     * 3. 直到满足最小值或超时才返回
     * 
     * 性能影响：
     * 1. 值越大，单次传输效率越高
     * 2. 值越大，延迟可能增加
     * 3. 值越小，响应越及时
     * 
     * 最佳实践：
     * 1. 默认值1字节适合实时性要求高的场景
     * 2. 增大此值可以提高吞吐量
     * 3. 需要与fetch.max.wait.ms配合使用
     * 
     * <code>fetch.min.bytes</code>
     */
    public static final String FETCH_MIN_BYTES_CONFIG = "fetch.min.bytes";
    public static final int DEFAULT_FETCH_MIN_BYTES = 1;
    private static final String FETCH_MIN_BYTES_DOC = "The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait for that much data to accumulate before answering the request. The default setting of " + DEFAULT_FETCH_MIN_BYTES + " byte means that fetch requests are answered as soon as that many byte(s) of data is available or the fetch request times out waiting for data to arrive. Setting this to a larger value will cause the server to wait for larger amounts of data to accumulate which can improve server throughput a bit at the cost of some additional latency.";

    /**
     * 最大获取字节数配置
     * 
     * 设计原理：
     * 1. 限制单次请求返回的数据量
     * 2. 控制消费者端的内存使用
     * 3. 支持大消息的传输
     * 
     * 工作机制：
     * 1. 服务端按批次返回记录
     * 2. 即使单个批次超过此值也会返回
     * 3. 消费者支持并行获取多个分区
     * 
     * 注意事项：
     * 1. 这不是绝对的最大值限制
     * 2. 实际最大批次大小受broker和topic配置限制
     * 3. 需要考虑消费者的内存容量
     * 
     * 最佳实践：
     * 1. 默认值50MB适合大多数场景
     * 2. 根据消息大小和内存调整
     * 3. 考虑与broker端配置的协调
     * 
     * <code>fetch.max.bytes</code>
     */
    public static final String FETCH_MAX_BYTES_CONFIG = "fetch.max.bytes";
    private static final String FETCH_MAX_BYTES_DOC = "The maximum amount of data the server should return for a fetch request. " +
            "Records are fetched in batches by the consumer, and if the first record batch in the first non-empty partition of the fetch is larger than " +
            "this value, the record batch will still be returned to ensure that the consumer can make progress. As such, this is not a absolute maximum. " +
            "The maximum record batch size accepted by the broker is defined via <code>message.max.bytes</code> (broker config) or " +
            "<code>max.message.bytes</code> (topic config). Note that the consumer performs multiple fetches in parallel.";
    public static final int DEFAULT_FETCH_MAX_BYTES = 50 * 1024 * 1024;

    /**
     * 最大等待时间配置
     * 
     * 设计原理：
     * 1. 控制服务端等待数据的最长时间
     * 2. 避免因数据不足导致请求长时间阻塞
     * 3. 平衡实时性和批量效率
     * 
     * 工作机制：
     * 1. 当数据量不满足fetch.min.bytes时等待
     * 2. 超过等待时间后立即返回当前数据
     * 3. 仅用于本地日志获取
     * 
     * 调优建议：
     * 1. 默认值500ms适合一般场景
     * 2. 降低此值可以提高实时性
     * 3. 增加此值可以提高批量效率
     * 
     * 注意事项：
     * 1. 远程获取请求使用broker端配置
     * 2. 需要与fetch.min.bytes配合使用
     * 3. 影响消费延迟和吞吐量
     * 
     * <code>fetch.max.wait.ms</code>
     */
    public static final String FETCH_MAX_WAIT_MS_CONFIG = "fetch.max.wait.ms";
    private static final String FETCH_MAX_WAIT_MS_DOC = "The maximum amount of time the server will block before " +
            "answering the fetch request there isn't sufficient data to immediately satisfy the requirement given by " +
            "fetch.min.bytes. This config is used only for local log fetch. To tune the remote fetch maximum wait " +
            "time, please refer to 'remote.fetch.max.wait.ms' broker config";
    public static final int DEFAULT_FETCH_MAX_WAIT_MS = 500;

    /**
     * <code>metadata.max.age.ms</code>
     * 元数据最大有效期配置
     * 
     * 设计原理：
     * 1. 控制客户端缓存的集群元数据的有效期
     * 2. 即使没有分区变更也会强制刷新
     * 3. 平衡集群感知实时性和请求开销
     * 
     * 应用场景：
     * 1. 需要及时感知集群变化的场景
     * 2. 对元数据一致性要求高的环境
     * 3. 动态伸缩集群规模的场景
     */
    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;

    /**
     * <code>max.partition.fetch.bytes</code>
     * 单个分区获取数据的最大字节数配置
     * 
     * 设计原理：
     * 1. 控制单次请求从每个分区返回的最大数据量
     * 2. 平衡网络带宽使用和处理延迟
     * 3. 支持大消息的传输需求
     * 
     * 工作机制：
     * 1. 消费者按批次获取记录
     * 2. 如果第一个非空分区的第一个批次超过此限制，仍会返回该批次
     * 3. 确保消费者能够继续处理，避免死锁
     * 
     * 性能影响：
     * 1. 值越大，单次传输效率越高
     * 2. 值越大，内存占用越多
     * 3. 需要与broker端配置协调
     */
    public static final String MAX_PARTITION_FETCH_BYTES_CONFIG = "max.partition.fetch.bytes";
    private static final String MAX_PARTITION_FETCH_BYTES_DOC = "服务器对每个分区返回的最大数据量。消费者按批次获取记录。如果获取的第一个非空分区中的第一个记录批次大于此限制，该批次仍会被返回以确保消费者能够继续处理。broker接受的最大记录批次大小由<code>message.max.bytes</code>（broker配置）或<code>max.message.bytes</code>（主题配置）定义。参见" + FETCH_MAX_BYTES_CONFIG + "用于限制消费者请求大小。";
    public static final int DEFAULT_MAX_PARTITION_FETCH_BYTES = 1 * 1024 * 1024;

    /**
     * <code>send.buffer.bytes</code>
     * 发送缓冲区大小配置
     * 
     * 设计原理：
     * 1. 控制TCP发送缓冲区大小
     * 2. 影响网络吞吐量和延迟
     * 3. 适应不同网络环境
     */
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

    /**
     * <code>receive.buffer.bytes</code>
     * 接收缓冲区大小配置
     * 
     * 设计原理：
     * 1. 控制TCP接收缓冲区大小
     * 2. 影响数据接收性能
     * 3. 平衡内存使用和网络性能
     */
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

    /**
     * <code>client.id</code>
     * 客户端标识配置
     * 
     * 设计原理：
     * 1. 用于标识请求的来源
     * 2. 便于监控和调试
     * 3. 支持请求追踪
     */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

    /**
     * <code>client.rack</code>
     * 客户端机架配置
     * 
     * 设计原理：
     * 1. 支持机架感知的功能
     * 2. 优化跨数据中心场景
     * 3. 提供位置感知能力
     */
    public static final String CLIENT_RACK_CONFIG = CommonClientConfigs.CLIENT_RACK_CONFIG;
    public static final String DEFAULT_CLIENT_RACK = CommonClientConfigs.DEFAULT_CLIENT_RACK;

    /**
     * <code>reconnect.backoff.ms</code>
     * 重连退避时间配置
     * 
     * 设计原理：
     * 1. 控制重连尝试的时间间隔
     * 2. 避免频繁重连消耗资源
     * 3. 实现指数退避机制
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /**
     * <code>reconnect.backoff.max.ms</code>
     * 最大重连退避时间配置
     * 
     * 设计原理：
     * 1. 限制重连退避的最大等待时间
     * 2. 避免过长的重连等待
     * 3. 平衡重试频率和资源消耗
     */
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

    /**
     * <code>retry.backoff.ms</code>
     * 重试退避时间配置
     * 
     * 设计原理：
     * 1. 控制操作重试的时间间隔
     * 2. 避免立即重试导致资源浪费
     * 3. 支持渐进式重试策略
     */
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;

    /**
     * <code>enable.metrics.push</code>
     * 启用指标推送配置
     * 
     * 设计原理：
     * 1. 支持主动推送监控指标
     * 2. 提供实时监控能力
     * 3. 便于集中式监控
     */
    public static final String ENABLE_METRICS_PUSH_CONFIG = CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG;
    public static final String ENABLE_METRICS_PUSH_DOC = CommonClientConfigs.ENABLE_METRICS_PUSH_DOC;

    /**
     * <code>retry.backoff.max.ms</code>
     * 最大重试退避时间配置
     * 
     * 设计原理：
     * 1. 限制重试退避的最大等待时间
     * 2. 避免过长的重试等待
     * 3. 确保及时响应故障
     */
    public static final String RETRY_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MAX_MS_CONFIG;

    /**
     * <code>metrics.sample.window.ms</code>
     * 指标采样窗口配置
     * 
     * 设计原理：
     * 1. 控制指标统计的时间窗口
     * 2. 平衡精度和资源消耗
     * 3. 支持滑动窗口统计
     */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /**
     * <code>metrics.num.samples</code>
     * 指标样本数量配置
     * 
     * 设计原理：
     * 1. 控制每个指标保留的样本数
     * 2. 影响统计的准确性
     * 3. 平衡内存使用和统计精度
     */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

    /**
     * <code>metrics.log.level</code>
     * 指标记录级别配置
     * 
     * 设计原理：
     * 1. 控制指标记录的详细程度
     * 2. 平衡监控需求和性能开销
     * 3. 支持不同级别的监控
     */
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

    /**
     * <code>metric.reporters</code>
     * 指标报告器配置
     * 
     * 设计原理：
     * 1. 支持自定义指标报告
     * 2. 提供扩展监控能力
     * 3. 集成外部监控系统
     */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /**
     * <code>check.crcs</code>
     * CRC校验配置
     * 
     * 设计原理：
     * 1. 保证消息传输的完整性
     * 2. 检测传输过程中的数据损坏
     * 3. 在性能和可靠性间权衡
     * 
     * 应用场景：
     * 1. 对数据完整性要求高的场景
     * 2. 网络质量不稳定的环境
     * 3. 需要极致性能的场景可以禁用
     */
    public static final String CHECK_CRCS_CONFIG = "check.crcs";
    private static final String CHECK_CRCS_DOC = "自动检查消费的记录的CRC32校验和。这确保消息在传输或磁盘存储过程中没有发生损坏。此检查会增加一些开销，因此在追求极致性能的场景下可以禁用。";

    /**
     * 键反序列化器配置
     * 用于将消息键从字节数组反序列化为Java对象
     * 
     * 设计原理：
     * 1. 提供灵活的反序列化机制，支持自定义数据类型
     * 2. 与生产者的序列化器配对使用
     * 3. 通过接口实现可扩展性
     * 
     * 最佳实践：
     * 1. 使用与生产者对应的反序列化器
     * 2. 自定义反序列化器时需实现Deserializer接口
     * 3. 确保线程安全
     */
    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";
    public static final String KEY_DESERIALIZER_CLASS_DOC = "键反序列化器类，必须实现<code>org.apache.kafka.common.serialization.Deserializer</code>接口。";

    /**
     * 值反序列化器配置
     * 用于将消息值从字节数组反序列化为Java对象
     * 
     * 设计原理：
     * 1. 提供灵活的反序列化机制，支持自定义数据类型
     * 2. 与生产者的序列化器配对使用
     * 3. 通过接口实现可扩展性
     * 
     * 最佳实践：
     * 1. 使用与生产者对应的反序列化器
     * 2. 自定义反序列化器时需实现Deserializer接口
     * 3. 确保线程安全
     */
    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";
    public static final String VALUE_DESERIALIZER_CLASS_DOC = "值反序列化器类，必须实现<code>org.apache.kafka.common.serialization.Deserializer</code>接口。";

    /**
     * Socket连接建立超时配置
     * 控制建立TCP连接的超时时间
     * 
     * 设计原理：
     * 1. 避免连接建立过程阻塞过长时间
     * 2. 快速检测网络问题
     * 3. 支持重试机制
     */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG;

    /**
     * Socket连接建立最大超时配置
     * 在重试期间，连接建立的最大超时时间
     * 
     * 设计原理：
     * 1. 限制重试过程中的最大等待时间
     * 2. 防止无限期等待
     * 3. 在网络不稳定时提供更好的容错能力
     */
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG;

    /**
     * 连接最大空闲时间配置
     * 控制空闲连接的保持时间
     * 
     * 设计原理：
     * 1. 优化资源使用，及时释放空闲连接
     * 2. 减少服务端资源占用
     * 3. 支持连接池管理
     */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /**
     * 请求超时配置
     * 控制客户端请求的超时时间
     * 
     * 设计原理：
     * 1. 避免请求无限期等待
     * 2. 快速失败以便重试
     * 3. 控制系统响应时间
     */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;

    /**
     * 默认API超时配置
     * 控制消费者API调用的默认超时时间
     * 
     * 设计原理：
     * 1. 为所有API操作提供统一的超时控制
     * 2. 可被具体API的超时配置覆盖
     * 3. 确保操作的可控性
     */
    public static final String DEFAULT_API_TIMEOUT_MS_CONFIG = CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG;

    /**
     * 拦截器配置
     * 用于在消费消息时进行拦截处理
     * 
     * 设计原理：
     * 1. 提供消息处理的扩展点
     * 2. 支持消息的监控、转换等场景
     * 3. 通过链式调用实现多重处理
     * 
     * 使用场景：
     * 1. 消息审计和监控
     * 2. 消息格式转换
     * 3. 消息过滤
     */
    public static final String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
    public static final String INTERCEPTOR_CLASSES_DOC = "拦截器类列表配置。" +
            "通过实现<code>org.apache.kafka.clients.consumer.ConsumerInterceptor</code>接口，" +
            "可以拦截（并可能修改）消费者接收到的记录。默认情况下没有配置拦截器。";

    /**
     * 排除内部主题配置
     * 控制是否在订阅模式下排除Kafka内部主题
     * 
     * 设计原理：
     * 1. 避免意外消费系统内部主题
     * 2. 支持显式订阅内部主题
     * 3. 提供主题访问控制
     */
    public static final String EXCLUDE_INTERNAL_TOPICS_CONFIG = "exclude.internal.topics";
    private static final String EXCLUDE_INTERNAL_TOPICS_DOC = "是否在使用订阅模式时排除匹配的内部主题。" +
            "即使启用此配置，也可以通过显式订阅来消费内部主题。";
    public static final boolean DEFAULT_EXCLUDE_INTERNAL_TOPICS = true;

    /**
     * 关闭时离组配置
     * 控制消费者关闭时是否主动离开消费者组
     * 
     * 设计原理：
     * 1. 支持优雅关闭
     * 2. 控制rebalance触发时机
     * 3. 优化组成员管理
     * 
     * 注意：这是内部配置，未来可能发生不兼容变更
     */
    static final String LEAVE_GROUP_ON_CLOSE_CONFIG = "internal.leave.group.on.close";

    /**
     * 稳定偏移量不支持时抛出异常配置
     * 控制在不支持新的稳定偏移量特性时是否抛出异常
     * 
     * 设计原理：
     * 1. 防止broker降级导致的数据一致性问题
     * 2. 快速失败避免正确性问题
     * 3. 保护偏移量提交的完整性
     * 
     * 注意：这是内部配置，未来可能发生不兼容变更
     */
    static final String THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED = "internal.throw.on.fetch.stable.offset.unsupported";

    /**
     * 事务隔离级别配置
     * 控制如何读取事务性写入的消息
     * 
     * 设计原理：
     * 1. 提供事务消息的读取语义
     * 2. 支持不同的一致性级别
     * 3. 保证消息顺序性
     * 
     * 可选值：
     * 1. read_committed：只读取已提交的事务消息
     *    - 提供强一致性保证
     *    - 可能增加延迟
     *    - 适用于要求数据一致性的场景
     * 
     * 2. read_uncommitted：读取所有消息
     *    - 包括未提交和已中止的事务消息
     *    - 延迟较低
     *    - 适用于对一致性要求不高的场景
     * 
     * 实现机制：
     * 1. read_committed模式下：
     *    - 只返回LSO（最后稳定偏移量）之前的消息
     *    - 等待进行中事务完成才返回后续消息
     *    - seekToEnd返回LSO而不是高水位
     * 
     * 2. read_uncommitted模式下：
     *    - 返回所有可见消息
     *    - 不考虑事务状态
     *    - 可能读取到未提交的事务消息
     */
    public static final String ISOLATION_LEVEL_CONFIG = "isolation.level";
    public static final String ISOLATION_LEVEL_DOC = "控制如何读取事务性写入的消息。如果设置为<code>read_committed</code>，consumer.poll()将只返回" +
            "已提交的事务消息。如果设置为<code>read_uncommitted</code>（默认值），consumer.poll()将返回所有消息，包括已中止的事务消息。" +
            "非事务性消息在任何模式下都会无条件返回。<p>消息总是按照偏移量顺序返回。因此，在" +
            "<code>read_committed</code>模式下，consumer.poll()只会返回最后稳定偏移量（LSO）之前的消息，LSO是第一个开放事务的偏移量减一。" +
            "特别是，在进行中事务之后的任何消息都将被暂时保留，直到相关事务完成。因此，<code>read_committed</code>" +
            "模式的消费者在有进行中的事务时将无法读取到高水位标记。</p><p>此外，在<code>read_committed</code>模式下，seekToEnd方法将" +
            "返回LSO</p>";

    public static final String DEFAULT_ISOLATION_LEVEL = IsolationLevel.READ_UNCOMMITTED.toString();

    /**
     * 允许自动创建主题配置
     * 控制在订阅或分配不存在的主题时是否自动创建
     * 
     * 设计原理：
     * 1. 简化主题管理
     * 2. 支持动态创建主题
     * 3. 向后兼容旧版本
     * 
     * 使用场景：
     * 1. 开发测试环境自动创建主题
     * 2. 简化应用程序部署
     * 3. 支持动态扩展
     * 
     * 注意事项：
     * 1. 生产环境建议禁用此功能
     * 2. 需要broker支持自动创建主题
     * 3. 0.11.0之前的broker必须启用此配置
     */
    public static final String ALLOW_AUTO_CREATE_TOPICS_CONFIG = "allow.auto.create.topics";
    private static final String ALLOW_AUTO_CREATE_TOPICS_DOC = "在订阅或分配主题时允许在broker上自动创建主题。" +
            "只有当broker允许自动创建主题（通过`auto.create.topics.enable`配置）时，被订阅的主题才会被自动创建。" +
            "使用0.11.0版本之前的broker时，此配置必须设置为`true`";
    public static final boolean DEFAULT_ALLOW_AUTO_CREATE_TOPICS = true;

    /**
     * 安全提供者配置
     * 配置安全实现的提供者
     * 
     * 设计原理：
     * 1. 支持自定义安全实现
     * 2. 提供安全机制的可扩展性
     * 3. 集成不同的安全方案
     */
    public static final String SECURITY_PROVIDERS_CONFIG = SecurityConfig.SECURITY_PROVIDERS_CONFIG;
    private static final String SECURITY_PROVIDERS_DOC = SecurityConfig.SECURITY_PROVIDERS_DOC;

    /**
     * 消费者客户端ID序列生成器
     * 用于生成唯一的客户端标识符
     * 
     * 设计原理：
     * 1. 保证客户端ID的唯一性
     * 2. 支持多实例部署
     * 3. 便于监控和调试
     */
    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * A list of configuration keys not supported for CLASSIC protocol.
     */
    private static final List<String> CLASSIC_PROTOCOL_UNSUPPORTED_CONFIGS = Collections.singletonList(
            GROUP_REMOTE_ASSIGNOR_CONFIG
    );

    /**
     * A list of configuration keys not supported for CONSUMER protocol.
     */
    private static final List<String> CONSUMER_PROTOCOL_UNSUPPORTED_CONFIGS = List.of(
            PARTITION_ASSIGNMENT_STRATEGY_CONFIG, 
            HEARTBEAT_INTERVAL_MS_CONFIG, 
            SESSION_TIMEOUT_MS_CONFIG
    );

    /**
     * 静态初始化块，用于定义所有Kafka消费者配置项
     * 
     * 设计原理：
     * 1. 使用ConfigDef统一管理所有配置项
     * 2. 为每个配置项定义类型、默认值、验证器和重要性级别
     * 3. 支持配置项的动态验证和转换
     * 
     * 配置项分类：
     * 1. 网络相关：bootstrap.servers, client.dns.lookup等
     * 2. 消费者组：group.id, group.instance.id等
     * 3. 消费行为：auto.offset.reset, enable.auto.commit等
     * 4. 性能调优：fetch.min.bytes, fetch.max.bytes等
     * 5. 安全配置：security.protocol等
     */
    static {
        // 初始化配置定义对象
        CONFIG = new ConfigDef()
                // 定义bootstrap.servers配置项
                // 这是连接Kafka集群的必需配置，指定broker列表
                .define(BOOTSTRAP_SERVERS_CONFIG,
                                        Type.LIST,
                                        Collections.emptyList(),
                                        new ConfigDef.NonNullValidator(),
                                        Importance.HIGH,
                                        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                                                                // 定义client.dns.lookup配置项
                                // 控制DNS查找行为，可以选择使用所有IP或只解析规范主机名
                                .define(CLIENT_DNS_LOOKUP_CONFIG,
                                        Type.STRING,
                                        ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                        in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                           ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                                        Importance.MEDIUM,
                                        CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
                                // 定义group.id配置项
                                // 指定消费者所属的消费者组，用于实现消费者的水平扩展和故障转移
                                .define(GROUP_ID_CONFIG, Type.STRING, null, Importance.HIGH, GROUP_ID_DOC)
                                // 定义group.instance.id配置项
                                // 为消费者提供静态成员机制，使其在重启后保持相同的分区分配
                                .define(GROUP_INSTANCE_ID_CONFIG,
                                        Type.STRING,
                                        null,
                                        new ConfigDef.NonEmptyString(),
                                        Importance.MEDIUM,
                                        GROUP_INSTANCE_ID_DOC)
                                // 定义session.timeout.ms配置项
                                // 消费者会话超时时间，用于检测消费者故障
                                .define(SESSION_TIMEOUT_MS_CONFIG,
                                        Type.INT,
                                        45000, // 默认45秒
                                        Importance.HIGH,
                                        SESSION_TIMEOUT_MS_DOC)
                                // 定义heartbeat.interval.ms配置项
                                // 心跳发送间隔，通常设置为session.timeout.ms的1/3
                                .define(HEARTBEAT_INTERVAL_MS_CONFIG,
                                        Type.INT,
                                        3000, // 默认3秒
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
                                .defineInternal(THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED,
                                        Type.BOOLEAN,
                                        false,
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
     * 配置解析后处理方法
     * 
     * 设计原理：
     * 1. 对解析后的配置进行验证和调整
     * 2. 确保配置的一致性和正确性
     * 3. 处理特殊配置项的覆盖逻辑
     * 
     * 实现细节：
     * 1. 验证SASL机制配置
     * 2. 检查指数退避配置
     * 3. 处理重连退避配置
     * 4. 根据需要覆盖客户端ID
     * 5. 处理自动提交配置
     * 6. 检查不支持的配置组合
     * 
     * @param parsedValues 已解析的配置值映射
     * @return 处理后的配置映射
     */
    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        // 验证SASL认证机制配置
        CommonClientConfigs.postValidateSaslMechanismConfig(this);
        // 检查是否禁用了指数退避，并发出警告
        CommonClientConfigs.warnDisablingExponentialBackoff(this);
        // 处理重连退避相关配置
        Map<String, Object> refinedConfigs = CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
        // 根据需要覆盖客户端ID
        maybeOverrideClientId(refinedConfigs);
        // 处理自动提交配置的覆盖
        maybeOverrideEnableAutoCommit(refinedConfigs);
        // 检查不支持的配置组合
        checkUnsupportedConfigs();
        return refinedConfigs;
    }

    /**
     * 客户端ID生成和覆盖方法
     * 
     * 设计原理：
     * 1. 确保每个消费者实例有唯一的客户端标识
     * 2. 支持静态成员ID机制
     * 3. 在未指定时自动生成有意义的ID
     * 
     * 实现细节：
     * 1. 检查是否已配置客户端ID
     * 2. 使用消费者组ID和实例ID生成唯一标识
     * 3. 支持自动序列号生成
     * 
     * 生成规则：
     * - 如果已配置客户端ID，则保持不变
     * - 否则，格式为：consumer-{groupId}-{groupInstanceId或序列号}
     * 
     * @param configs 配置映射，用于存储生成的客户端ID
     */
    private void maybeOverrideClientId(Map<String, Object> configs) {
        // 获取当前配置的客户端ID
        final String clientId = this.getString(CLIENT_ID_CONFIG);
        // 如果客户端ID未配置或为空
        if (clientId == null || clientId.isEmpty()) {
            // 获取消费者组ID
            final String groupId = this.getString(GROUP_ID_CONFIG);
            // 获取消费者组实例ID（用于静态成员机制）
            String groupInstanceId = this.getString(GROUP_INSTANCE_ID_CONFIG);
            // 如果指定了组实例ID，验证其有效性
            if (groupInstanceId != null)
                JoinGroupRequest.validateGroupInstanceId(groupInstanceId);

            // 使用组实例ID或生成的序列号作为实例标识部分
            String groupInstanceIdPart = groupInstanceId != null ? groupInstanceId : CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement() + "";
            // 生成最终的客户端ID
            String generatedClientId = String.format("consumer-%s-%s", groupId, groupInstanceIdPart);
            // 将生成的客户端ID添加到配置中
            configs.put(CLIENT_ID_CONFIG, generatedClientId);
        }
    }

    /**
     * 将反序列化器添加到配置中的工具方法
     * 
     * 设计原理：
     * 1. 支持运行时动态配置反序列化器
     * 2. 确保必要的反序列化器配置存在
     * 3. 提供配置验证和错误处理
     * 
     * 实现细节：
     * 1. 创建配置的副本以避免修改原始配置
     * 2. 分别处理键和值的反序列化器
     * 3. 验证反序列化器的有效性
     * 
     * 使用场景：
     * - 在创建消费者实例时动态指定反序列化器
     * - 在运行时更改反序列化器配置
     * - 支持自定义反序列化器的注入
     *
     * @param configs 原始配置映射
     * @param keyDeserializer 键的反序列化器实例
     * @param valueDeserializer 值的反序列化器实例
     * @return 包含反序列化器配置的新配置映射
     * @throws ConfigException 当必要的反序列化器配置缺失时
     */
    public static Map<String, Object> appendDeserializerToConfig(Map<String, Object> configs,
                                                                 Deserializer<?> keyDeserializer,
                                                                 Deserializer<?> valueDeserializer) {
        // 创建配置的副本，避免修改原始配置
        Map<String, Object> newConfigs = new HashMap<>(configs);
        
        // 处理键的反序列化器配置
        if (keyDeserializer != null)
            // 如果提供了反序列化器实例，使用其类作为配置值
            newConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        else if (newConfigs.get(KEY_DESERIALIZER_CLASS_CONFIG) == null)
            // 如果没有提供实例且配置中也没有指定，抛出异常
            throw new ConfigException(KEY_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
            
        // 处理值的反序列化器配置
        if (valueDeserializer != null)
            // 如果提供了反序列化器实例，使用其类作为配置值
            newConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        else if (newConfigs.get(VALUE_DESERIALIZER_CLASS_CONFIG) == null)
            // 如果没有提供实例且配置中也没有指定，抛出异常
            throw new ConfigException(VALUE_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
            
        return newConfigs;
    }

    /**
     * 自动提交配置覆盖方法
     * 
     * 设计原理：
     * 1. 确保消费者组行为的一致性
     * 2. 防止无组ID时的自动提交
     * 3. 提供合理的默认配置
     * 
     * 实现细节：
     * 1. 检查消费者组ID的存在性
     * 2. 处理自动提交配置的覆盖逻辑
     * 3. 验证配置的有效性
     * 
     * 使用场景：
     * - 消费者未指定组ID时的配置处理
     * - 防止配置错误导致的数据丢失
     * - 确保消费者行为的可预测性
     *
     * @param configs 配置映射，用于存储可能被覆盖的配置
     * @throws InvalidConfigurationException 当在无组ID时启用自动提交
     */
    private void maybeOverrideEnableAutoCommit(Map<String, Object> configs) {
        // 获取消费者组ID，使用Optional处理可能为null的情况
        Optional<String> groupId = Optional.ofNullable(getString(CommonClientConfigs.GROUP_ID_CONFIG));
        // 获取原始配置，用于检查用户是否显式设置了配置项
        Map<String, Object> originals = originals();
        // 确定是否启用自动提交，默认为false
        boolean enableAutoCommit = originals.containsKey(ENABLE_AUTO_COMMIT_CONFIG) ? getBoolean(ENABLE_AUTO_COMMIT_CONFIG) : false;
        
        // 当消费者组ID为空时的特殊处理
        if (groupId.isEmpty()) {
            // 如果用户未显式配置自动提交，则默认设置为false
            if (!originals.containsKey(ENABLE_AUTO_COMMIT_CONFIG)) {
                configs.put(ENABLE_AUTO_COMMIT_CONFIG, false);
            }
            // 如果用户启用了自动提交但没有组ID，抛出异常
            else if (enableAutoCommit) {
                throw new InvalidConfigurationException(ENABLE_AUTO_COMMIT_CONFIG + " cannot be set to true when default group id (null) is used.");
            }
        }
    }

    /**
     * 检查不支持的配置组合
     * 
     * 设计原理：
     * 1. 确保配置的兼容性
     * 2. 防止不同协议版本的配置冲突
     * 3. 提供清晰的错误提示
     * 
     * 实现细节：
     * 1. 获取当前使用的消费者组协议
     * 2. 根据协议类型检查不支持的配置
     * 3. 验证配置的有效性
     */
    private void checkUnsupportedConfigs() {
        // 获取配置的消费者组协议类型
        String groupProtocol = getString(GROUP_PROTOCOL_CONFIG);
        // 根据协议类型检查不支持的配置
        if (GroupProtocol.CLASSIC.name().equalsIgnoreCase(groupProtocol)) {
            // 检查经典协议不支持的配置
            checkUnsupportedConfigs(GroupProtocol.CLASSIC, CLASSIC_PROTOCOL_UNSUPPORTED_CONFIGS);
        } else if (GroupProtocol.CONSUMER.name().equalsIgnoreCase(groupProtocol)) {
            // 检查新版消费者协议不支持的配置
            checkUnsupportedConfigs(GroupProtocol.CONSUMER, CONSUMER_PROTOCOL_UNSUPPORTED_CONFIGS);
        }
    }

    /**
     * 检查特定协议下不支持的配置
     * 
     * 设计原理：
     * 1. 隔离不同协议版本的配置验证逻辑
     * 2. 提供详细的配置冲突信息
     * 3. 支持配置的可扩展性
     * 
     * 实现细节：
     * 1. 验证当前协议类型
     * 2. 收集所有无效的配置项
     * 3. 生成错误信息
     *
     * @param groupProtocol 消费者组协议类型
     * @param unsupportedConfigs 该协议不支持的配置列表
     * @throws ConfigException 当发现不支持的配置时
     */
    private void checkUnsupportedConfigs(GroupProtocol groupProtocol, List<String> unsupportedConfigs) {
        // 确认当前使用的是指定的协议
        if (getString(GROUP_PROTOCOL_CONFIG).equalsIgnoreCase(groupProtocol.name())) {
            // 用于收集发现的无效配置
            List<String> invalidConfigs = new ArrayList<>();
            // 检查每个不支持的配置项
            unsupportedConfigs.forEach(configName -> {
                // 获取配置值
                Object config = originals().get(configName);
                // 如果配置存在且不为空，则添加到无效配置列表
                if (config != null && !Utils.isBlank(config.toString())) {
                    invalidConfigs.add(configName);
                }
            });
            // 如果发现任何无效配置，抛出异常
            if (!invalidConfigs.isEmpty()) {
                throw new ConfigException(String.join(", ", invalidConfigs) +
                        " cannot be set when " + GROUP_PROTOCOL_CONFIG + "=" + groupProtocol.name());
            }
        }
    }

    public ConsumerConfig(Properties props) {
        super(CONFIG, props);
    }

    public ConsumerConfig(Map<String, Object> props) {
        super(CONFIG, props);
    }

    protected ConsumerConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "consumerconfigs_" + config));
    }

}
