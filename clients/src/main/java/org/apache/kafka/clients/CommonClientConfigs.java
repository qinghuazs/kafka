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
package org.apache.kafka.clients;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Kafka客户端应用程序（生产者、消费者、连接器等）共享的配置类。
 * 该类定义了所有Kafka客户端通用的配置项，包括：
 * 1. 集群连接配置：引导服务器、DNS查找、连接超时等
 * 2. 网络通信配置：发送/接收缓冲区、请求超时等
 * 3. 客户端标识配置：客户端ID、机架位置等
 * 4. 重试和重连机制：退避策略、最大重试次数等
 * 5. 度量指标配置：采样窗口、记录级别等
 * 6. 安全配置：安全协议等
 * 7. 消费者组管理：组ID、实例ID、会话超时等
 */
public class CommonClientConfigs {
    // 日志记录器
    private static final Logger log = LoggerFactory.getLogger(CommonClientConfigs.class);

    /*
     * 注意：不要修改任何配置项的名称，因为这些名称是公共API的一部分，修改会破坏用户代码。
     */

    // 引导服务器配置，用于建立与Kafka集群的初始连接
    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    public static final String BOOTSTRAP_SERVERS_DOC = "用于建立与Kafka集群初始连接的主机/端口对列表。"
                                                        + "Clients use this list to bootstrap and discover the full set of Kafka brokers. "
                                                        + "While the order of servers in the list does not matter, we recommend including more than one server to ensure resilience if any servers are down. "
                                                        + "This list does not need to contain the entire set of brokers, as Kafka clients automatically manage and update connections to the cluster efficiently. "
                                                        + "This list must be in the form <code>host1:port1,host2:port2,...</code>.";
    // 客户端DNS查找配置，控制客户端如何使用DNS查找
    public static final String CLIENT_DNS_LOOKUP_CONFIG = "client.dns.lookup";
    public static final String CLIENT_DNS_LOOKUP_DOC = "控制客户端如何使用DNS查找。"
                                                       + "If set to <code>use_all_dns_ips</code>, connect to each returned IP "
                                                       + "address in sequence until a successful connection is established. "
                                                       + "After a disconnection, the next IP is used. Once all IPs have been "
                                                       + "used once, the client resolves the IP(s) from the hostname again "
                                                       + "(both the JVM and the OS cache DNS name lookups, however). "
                                                       + "If set to <code>resolve_canonical_bootstrap_servers_only</code>, "
                                                       + "resolve each bootstrap address into a list of canonical names. After "
                                                       + "the bootstrap phase, this behaves the same as <code>use_all_dns_ips</code>.";

    // 元数据最大年龄配置，控制元数据刷新的时间间隔
    public static final String METADATA_MAX_AGE_CONFIG = "metadata.max.age.ms";
    public static final String METADATA_MAX_AGE_DOC = "即使没有发现任何分区领导权变化，强制刷新元数据的时间间隔（毫秒）。这样可以主动发现新的代理或分区。";

    // TCP发送缓冲区大小配置
    public static final String SEND_BUFFER_CONFIG = "send.buffer.bytes";
    public static final String SEND_BUFFER_DOC = "发送数据时使用的TCP发送缓冲区（SO_SNDBUF）大小。如果值为-1，将使用操作系统默认值。";
    public static final int SEND_BUFFER_LOWER_BOUND = -1;

    // TCP接收缓冲区大小配置
    public static final String RECEIVE_BUFFER_CONFIG = "receive.buffer.bytes";
    public static final String RECEIVE_BUFFER_DOC = "读取数据时使用的TCP接收缓冲区（SO_RCVBUF）大小。如果值为-1，将使用操作系统默认值。";
    public static final int RECEIVE_BUFFER_LOWER_BOUND = -1;

    // 客户端ID配置
    public static final String CLIENT_ID_CONFIG = "client.id";
    public static final String CLIENT_ID_DOC = "发送请求时传递给服务器的ID字符串。目的是通过在服务器端请求日志中包含逻辑应用程序名称，能够追踪请求的来源，而不仅仅是IP/端口。";

    // 客户端机架标识配置
    public static final String CLIENT_RACK_CONFIG = "client.rack";
    public static final String CLIENT_RACK_DOC = "客户端的机架标识符。可以是任何表示客户端物理位置的字符串值。与代理配置'broker.rack'相对应。";
    public static final String DEFAULT_CLIENT_RACK = "";

    // 重连退避时间配置
    public static final String RECONNECT_BACKOFF_MS_CONFIG = "reconnect.backoff.ms";
    public static final String RECONNECT_BACKOFF_MS_DOC = "尝试重新连接到指定主机之前等待的基础时间。" +
        "This avoids repeatedly connecting to a host in a tight loop. This backoff applies to all connection attempts by the client to a broker. " +
        "This value is the initial backoff value and will increase exponentially for each consecutive connection failure, up to the <code>reconnect.backoff.max.ms</code> value.";

    // 最大重连退避时间配置
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = "reconnect.backoff.max.ms";
    public static final String RECONNECT_BACKOFF_MAX_MS_DOC = "重新连接到反复连接失败的代理时要等待的最大时间（毫秒）。" +
        "If provided, the backoff per host will increase exponentially for each consecutive connection failure, up to this maximum. After calculating the backoff increase, 20% random jitter is added to avoid connection storms.";

    // 重试次数配置
    public static final String RETRIES_CONFIG = "retries";
    public static final String RETRIES_DOC = "设置大于零的值将导致客户端重新发送任何可能因暂时性错误而失败的请求。" +
        " It is recommended to set the value to either zero or `MAX_VALUE` and use corresponding timeout parameters to control how long a client should retry a request.";

    // 重试退避时间配置
    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    public static final String RETRY_BACKOFF_MS_DOC = "在尝试重试对指定主题分区的失败请求之前等待的时间。" +
        "This avoids repeatedly sending requests in a tight loop under some failure scenarios. This value is the initial backoff value and will increase exponentially for each failed request, " +
        "up to the <code>retry.backoff.max.ms</code> value.";
    public static final Long DEFAULT_RETRY_BACKOFF_MS = 100L;

    // 最大重试退避时间配置
    public static final String RETRY_BACKOFF_MAX_MS_CONFIG = "retry.backoff.max.ms";
    public static final String RETRY_BACKOFF_MAX_MS_DOC = "重试向反复失败的代理发送请求时要等待的最大时间（毫秒）。" +
        "If provided, the backoff per client will increase exponentially for each failed request, up to this maximum. To prevent all clients from being synchronized upon retry, " +
        "a randomized jitter with a factor of 0.2 will be applied to the backoff, resulting in the backoff falling within a range between 20% below and 20% above the computed value. " +
        "If <code>retry.backoff.ms</code> is set to be higher than <code>retry.backoff.max.ms</code>, then <code>retry.backoff.max.ms</code> will be used as a constant backoff from the beginning without any exponential increase";
    public static final Long DEFAULT_RETRY_BACKOFF_MAX_MS = 1000L;

    public static final int RETRY_BACKOFF_EXP_BASE = 2;
    public static final double RETRY_BACKOFF_JITTER = 0.2;

    // 启用指标推送配置
    public static final String ENABLE_METRICS_PUSH_CONFIG = "enable.metrics.push";
    public static final String ENABLE_METRICS_PUSH_DOC = "是否启用将客户端指标推送到集群，前提是集群有与此客户端匹配的客户端指标订阅。";

    // 指标采样窗口配置
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = "metrics.sample.window.ms";
    public static final String METRICS_SAMPLE_WINDOW_MS_DOC = "计算指标样本的时间窗口。";

    // 指标样本数量配置
    public static final String METRICS_NUM_SAMPLES_CONFIG = "metrics.num.samples";
    public static final String METRICS_NUM_SAMPLES_DOC = "用于计算指标的样本数量。";

    // 指标记录级别配置
    public static final String METRICS_RECORDING_LEVEL_CONFIG = "metrics.recording.level";
    public static final String METRICS_RECORDING_LEVEL_DOC = "指标记录的最高级别。有三个级别用于记录指标 - info、debug和trace。\n" +
            " \n" +
            "INFO level records only essential metrics necessary for monitoring system performance and health. It collects vital data without gathering too much detail, making it suitable for production environments where minimal overhead is desired.\n" +
            "\n" +
            "DEBUG level records most metrics, providing more detailed information about the system's operation. It's useful for development and testing environments where you need deeper insights to debug and fine-tune the application.\n" +
            "\n" +
            "TRACE level records all possible metrics, capturing every detail about the system's performance and operation. It's best for controlled environments where in-depth analysis is required, though it can introduce significant overhead.";
    // 指标报告器类配置
    public static final String METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters";
    public static final String METRIC_REPORTER_CLASSES_DOC = "用作指标报告器的类列表。实现<code>org.apache.kafka.common.metrics.MetricsReporter</code>接口允许插入在创建新指标时会收到通知的类。";

    // 指标上下文前缀
    public static final String METRICS_CONTEXT_PREFIX = "metrics.context.";

    // 安全协议配置
    public static final String SECURITY_PROTOCOL_CONFIG = "security.protocol";
    public static final String SECURITY_PROTOCOL_DOC = "与代理通信时使用的协议。";
    public static final String DEFAULT_SECURITY_PROTOCOL = "PLAINTEXT";

    // 套接字连接设置超时配置
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = "socket.connection.setup.timeout.ms";
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC = "客户端等待套接字连接建立的时间。" +
        "If the connection is not built before the timeout elapses, clients will close the socket channel. " +
        "This value is the initial backoff value and will increase exponentially for each consecutive connection failure, " +
        "up to the <code>socket.connection.setup.timeout.max.ms</code> value.";
    public static final Long DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS = 10 * 1000L;

    // 最大套接字连接设置超时配置
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = "socket.connection.setup.timeout.max.ms";
    public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC = "客户端等待套接字连接建立的最大时间。" +
        "The connection setup timeout will increase exponentially for each consecutive connection failure up to this maximum. To avoid connection storms, " +
        "a randomization factor of 0.2 will be applied to the timeout resulting in a random range between 20% below and 20% above the computed value.";
    public static final Long DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS = 30 * 1000L;

    // 连接最大空闲时间配置
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = "connections.max.idle.ms";
    public static final String CONNECTIONS_MAX_IDLE_MS_DOC = "在此配置指定的毫秒数后关闭空闲连接。";

    // 请求超时配置
    public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
    public static final String REQUEST_TIMEOUT_MS_DOC = "此配置控制客户端等待请求响应的最大时间。"
                                                         + "for the response of a request. If the response is not received before the timeout "
                                                         + "elapses the client will resend the request if necessary or fail the request if "
                                                         + "retries are exhausted.";

    public static final String DEFAULT_LIST_KEY_SERDE_INNER_CLASS = "default.list.key.serde.inner";
    public static final String DEFAULT_LIST_KEY_SERDE_INNER_CLASS_DOC = "Default inner class of list serde for key that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface. "
            + "This configuration will be read if and only if <code>default.key.serde</code> configuration is set to <code>org.apache.kafka.common.serialization.Serdes.ListSerde</code>";

    public static final String DEFAULT_LIST_VALUE_SERDE_INNER_CLASS = "default.list.value.serde.inner";
    public static final String DEFAULT_LIST_VALUE_SERDE_INNER_CLASS_DOC = "Default inner class of list serde for value that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface. "
            + "This configuration will be read if and only if <code>default.value.serde</code> configuration is set to <code>org.apache.kafka.common.serialization.Serdes.ListSerde</code>";

    public static final String DEFAULT_LIST_KEY_SERDE_TYPE_CLASS = "default.list.key.serde.type";
    public static final String DEFAULT_LIST_KEY_SERDE_TYPE_CLASS_DOC = "Default class for key that implements the <code>java.util.List</code> interface. "
            + "This configuration will be read if and only if <code>default.key.serde</code> configuration is set to <code>org.apache.kafka.common.serialization.Serdes.ListSerde</code> "
            + "Note when list serde class is used, one needs to set the inner serde class that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface via '"
            + DEFAULT_LIST_KEY_SERDE_INNER_CLASS + "'";

    public static final String DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS = "default.list.value.serde.type";
    public static final String DEFAULT_LIST_VALUE_SERDE_TYPE_CLASS_DOC = "Default class for value that implements the <code>java.util.List</code> interface. "
            + "This configuration will be read if and only if <code>default.value.serde</code> configuration is set to <code>org.apache.kafka.common.serialization.Serdes.ListSerde</code> "
            + "Note when list serde class is used, one needs to set the inner serde class that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface via '"
            + DEFAULT_LIST_VALUE_SERDE_INNER_CLASS + "'";

    // 消费者组ID配置
    public static final String GROUP_ID_CONFIG = "group.id";
    public static final String GROUP_ID_DOC = "标识此消费者所属消费者组的唯一字符串。如果消费者使用<code>subscribe(topic)</code>的组管理功能或基于Kafka的偏移量管理策略，则此属性是必需的。";

    // 消费者组实例ID配置
    public static final String GROUP_INSTANCE_ID_CONFIG = "group.instance.id";
    public static final String GROUP_INSTANCE_ID_DOC = "最终用户提供的消费者实例的唯一标识符。"
                                                       + "Only non-empty strings are permitted. If set, the consumer is treated as a static member, "
                                                       + "which means that only one instance with this ID is allowed in the consumer group at any time. "
                                                       + "This can be used in combination with a larger session timeout to avoid group rebalances caused by transient unavailability "
                                                       + "(e.g. process restarts). If not set, the consumer will join the group as a dynamic member, which is the traditional behavior.";

    // 最大轮询间隔配置
    public static final String MAX_POLL_INTERVAL_MS_CONFIG = "max.poll.interval.ms";
    public static final String MAX_POLL_INTERVAL_MS_DOC = "使用消费者组管理时，poll()调用之间的最大延迟。"
                                                          + "consumer group management. This places an upper bound on the amount of time that the consumer can be idle "
                                                          + "before fetching more records. If poll() is not called before expiration of this timeout, then the consumer "
                                                          + "is considered failed and the group will rebalance in order to reassign the partitions to another member. "
                                                          + "For consumers using a non-null <code>group.instance.id</code> which reach this timeout, partitions will not be immediately reassigned. "
                                                          + "Instead, the consumer will stop sending heartbeats and partitions will be reassigned "
                                                          + "after expiration of <code>session.timeout.ms</code>. This mirrors the behavior of a static consumer which has shutdown.";

    // 重平衡超时配置
    public static final String REBALANCE_TIMEOUT_MS_CONFIG = "rebalance.timeout.ms";
    public static final String REBALANCE_TIMEOUT_MS_DOC = "每个工作进程加入组的最大允许时间。"
                                                          + "once a rebalance has begun. This is basically a limit on the amount of time needed for all tasks to "
                                                          + "flush any pending data and commit offsets. If the timeout is exceeded, then the worker will be removed "
                                                          + "from the group, which will cause offset commit failures.";

    // 会话超时配置，用于检测客户端故障
    public static final String SESSION_TIMEOUT_MS_CONFIG = "session.timeout.ms";
    public static final String SESSION_TIMEOUT_MS_DOC = "使用Kafka的组管理功能时用于检测客户端故障的超时时间。"
                                                        + "客户端会定期向代理发送心跳以表明其存活状态。如果代理在会话超时到期前没有收到任何心跳，"
                                                        + "则代理会将该客户端从组中移除并启动重平衡。请注意，该值必须在代理配置中"
                                                        + "<code>group.min.session.timeout.ms</code>和<code>group.max.session.timeout.ms</code>允许的范围内。"
                                                        + "另外，当<code>group.protocol</code>设置为\"consumer\"时，此配置不受支持。";


    // 心跳间隔配置，用于维持消费者会话活跃状态
    public static final String HEARTBEAT_INTERVAL_MS_CONFIG = "heartbeat.interval.ms";
    public static final String HEARTBEAT_INTERVAL_MS_DOC = "使用Kafka的组管理功能时，向消费者协调器发送心跳的预期时间间隔。"
                                                           + "心跳用于确保消费者会话保持活跃状态，并在新消费者加入或离开组时促进重平衡。"
                                                           + "该值必须小于<code>session.timeout.ms</code>，通常应设置为不超过该值的1/3。"
                                                           + "可以设置得更低以控制正常重平衡的预期时间。";


    // 默认API超时配置，为客户端API操作设置默认超时时间
    public static final String DEFAULT_API_TIMEOUT_MS_CONFIG = "default.api.timeout.ms";
    public static final String DEFAULT_API_TIMEOUT_MS_DOC = "指定客户端API的超时时间（毫秒）。" +
            "此配置用作所有未指定<code>timeout</code>参数的客户端操作的默认超时时间。";


    // 元数据恢复策略配置，控制客户端在所有已知代理不可用时如何恢复
    public static final String METADATA_RECOVERY_STRATEGY_CONFIG = "metadata.recovery.strategy";
    public static final String METADATA_RECOVERY_STRATEGY_DOC = "控制当客户端已知的所有代理都不可用时如何恢复。" +
            "如果设置为<code>none</code>，客户端将失败。如果设置为<code>rebootstrap</code>，" +
            "客户端将使用<code>bootstrap.servers</code>重新执行引导过程。" +
            "当客户端与代理通信非常不频繁，以至于在客户端刷新元数据之前代理集可能完全改变时，重新引导特别有用。" +
            "当所有最后已知的代理同时显示为不可用时，将触发元数据恢复。" +
            "当代理断开连接且当前没有重试尝试正在进行时，代理显示为不可用。" +
            "建议增加客户端的<code>reconnect.backoff.ms</code>和<code>reconnect.backoff.max.ms</code>值，" +
            "并减少<code>socket.connection.setup.timeout.ms</code>和<code>socket.connection.setup.timeout.max.ms</code>值。" +
            "如果在<code>metadata.recovery.rebootstrap.trigger.ms</code>毫秒内无法连接到任何代理，" +
            "或者服务器请求重新引导时，也会触发重新引导。";

    public static final String DEFAULT_METADATA_RECOVERY_STRATEGY = MetadataRecoveryStrategy.REBOOTSTRAP.name;

    // 元数据恢复重新引导触发时间配置
    public static final String METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG = "metadata.recovery.rebootstrap.trigger.ms";
    public static final String METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_DOC = "如果配置为使用<code>metadata.recovery.strategy=rebootstrap</code>的客户端" +
            "在此时间间隔内无法从最后已知元数据中的任何代理获取元数据，客户端将使用<code>bootstrap.servers</code>配置重复引导过程。";
    // 默认重新引导触发时间为300秒
    public static final long DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS = 300 * 1000;


    /**
     * 后处理重连退避配置，当显式配置了重连退避时间但未配置最大重连退避时间时，禁用指数退避机制。
     * 这种情况下，为了保持向后兼容性，将使用固定的重连退避时间而不是指数增长。
     *
     * @param config                    配置对象，包含所有的配置项
     * @param parsedValues              已解析的配置值，用于后处理配置
     *
     * @return                          返回需要更新的配置值映射
     */
    public static Map<String, Object> postProcessReconnectBackoffConfigs(AbstractConfig config,
                                                                         Map<String, Object> parsedValues) {
        // 创建返回值Map用于存储需要更新的配置
        HashMap<String, Object> rval = new HashMap<>();
        // 获取原始配置
        Map<String, Object> originalConfig = config.originals();
        // 检查是否只配置了重连退避时间而没有配置最大重连退避时间
        if ((!originalConfig.containsKey(RECONNECT_BACKOFF_MAX_MS_CONFIG)) &&
            originalConfig.containsKey(RECONNECT_BACKOFF_MS_CONFIG)) {
            // 记录警告日志，提示禁用指数退避机制
            log.warn("Disabling exponential reconnect backoff because {} is set, but {} is not.",
                    RECONNECT_BACKOFF_MS_CONFIG, RECONNECT_BACKOFF_MAX_MS_CONFIG);
            // 将最大重连退避时间设置为与重连退避时间相同的值，实现固定退避时间
            rval.put(RECONNECT_BACKOFF_MAX_MS_CONFIG, parsedValues.get(RECONNECT_BACKOFF_MS_CONFIG));
        }
        return rval;
    }

    /**
     * 当初始退避值大于最大退避值时，记录警告日志提示指数退避被禁用。
     * 这种情况下会使用固定的退避时间，而不是指数增长的退避时间。
     * 该方法检查两种场景：重试退避和连接设置超时。
     *
     * @param config                    配置对象，包含所有的配置项
     */
    public static void warnDisablingExponentialBackoff(AbstractConfig config) {
        // 获取重试退避时间配置值
        long retryBackoffMs = config.getLong(RETRY_BACKOFF_MS_CONFIG);
        // 获取最大重试退避时间配置值
        long retryBackoffMaxMs = config.getLong(RETRY_BACKOFF_MAX_MS_CONFIG);
        // 检查重试退避时间是否大于最大重试退避时间
        if (retryBackoffMs > retryBackoffMaxMs) {
            // 记录警告日志，提示将使用固定的退避时间
            log.warn("Configuration '{}' with value '{}' is greater than configuration '{}' with value '{}'. " +
                    "A static backoff with value '{}' will be applied.",
                RETRY_BACKOFF_MS_CONFIG, retryBackoffMs,
                RETRY_BACKOFF_MAX_MS_CONFIG, retryBackoffMaxMs, retryBackoffMaxMs);
        }

        // 获取连接设置超时配置值
        long connectionSetupTimeoutMs = config.getLong(SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG);
        // 获取最大连接设置超时配置值
        long connectionSetupTimeoutMaxMs = config.getLong(SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG);
        // 检查连接设置超时是否大于最大连接设置超时
        if (connectionSetupTimeoutMs > connectionSetupTimeoutMaxMs) {
            // 记录警告日志，提示将使用固定的连接设置超时
            log.warn("Configuration '{}' with value '{}' is greater than configuration '{}' with value '{}'. " +
                    "A static connection setup timeout with value '{}' will be applied.",
                SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, connectionSetupTimeoutMs,
                SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, connectionSetupTimeoutMaxMs, connectionSetupTimeoutMaxMs);
        }
    }

    /**
     * 验证SASL机制配置的有效性。
     * 当安全协议配置为SASL_PLAINTEXT或SASL_SSL时，必须指定有效的SASL机制。
     *
     * @param config                    配置对象，包含所有的配置项
     * @throws ConfigException          当SASL启用但未指定机制时抛出配置异常
     */
    public static void postValidateSaslMechanismConfig(AbstractConfig config) {
        // 获取安全协议配置
        SecurityProtocol securityProtocol = SecurityProtocol.forName(config.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        // 获取客户端SASL机制配置
        String clientSaslMechanism = config.getString(SaslConfigs.SASL_MECHANISM);
        // 检查是否使用了SASL安全协议
        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
            // 当使用SASL时，验证是否指定了有效的SASL机制
            if (clientSaslMechanism == null || clientSaslMechanism.isEmpty()) {
                // 如果未指定SASL机制，抛出配置异常
                throw new ConfigException(SaslConfigs.SASL_MECHANISM, null, "When the " + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG +
                        " configuration enables SASL, mechanism must be non-null and non-empty string.");
            }
        }
    }

    /**
     * 获取指标报告器实例列表，不指定客户端ID。
     *
     * @param config                    配置对象
     * @return                          指标报告器实例列表
     */
    public static List<MetricsReporter> metricsReporters(AbstractConfig config) {
        // 使用空Map调用重载方法
        return metricsReporters(Collections.emptyMap(), config);
    }

    /**
     * 获取指标报告器实例列表，使用指定的客户端ID。
     *
     * @param clientId                  客户端ID
     * @param config                    配置对象
     * @return                          指标报告器实例列表
     */
    public static List<MetricsReporter> metricsReporters(String clientId, AbstractConfig config) {
        // 将客户端ID封装在Map中调用重载方法
        return metricsReporters(Collections.singletonMap(CommonClientConfigs.CLIENT_ID_CONFIG, clientId), config);
    }

    /**
     * 获取指标报告器实例列表，支持覆盖客户端ID配置。
     *
     * @param clientIdOverride          用于覆盖默认客户端ID的配置Map
     * @param config                    配置对象
     * @return                          指标报告器实例列表
     */
    public static List<MetricsReporter> metricsReporters(Map<String, Object> clientIdOverride, AbstractConfig config) {
        // 根据配置创建指标报告器实例
        return config.getConfiguredInstances(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class, clientIdOverride);
    }

    /**
     * 创建遥测报告器实例。
     * 只有当启用了指标推送功能时才会创建遥测报告器。
     *
     * @param clientId                  客户端ID
     * @param config                    配置对象
     * @return                          遥测报告器实例的Optional包装
     */
    public static Optional<ClientTelemetryReporter> telemetryReporter(String clientId, AbstractConfig config) {
        // 检查是否启用了指标推送功能
        if (!config.getBoolean(CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG)) {
            // 如果未启用，返回空Optional
            return Optional.empty();
        }

        // 创建遥测报告器实例，使用系统时间
        ClientTelemetryReporter telemetryReporter = new ClientTelemetryReporter(Time.SYSTEM);
        // 配置遥测报告器，将客户端ID添加到配置中
        telemetryReporter.configure(config.originals(Collections.singletonMap(CommonClientConfigs.CLIENT_ID_CONFIG, clientId)));
        // 返回包含遥测报告器的Optional
        return Optional.of(telemetryReporter);
    }
}
