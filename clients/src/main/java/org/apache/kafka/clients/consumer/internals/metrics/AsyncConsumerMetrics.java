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
package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.Arrays;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;

/**
 * 异步消费者指标类，继承自 KafkaConsumerMetrics 并实现 AutoCloseable 接口。
 * 用于收集和报告与异步消费者操作相关的各种指标，例如事件队列大小、处理时间等。
 * 这些指标有助于监控消费者的健康状况和性能。
 */
public class AsyncConsumerMetrics extends KafkaConsumerMetrics implements AutoCloseable {
    // Metrics 对象，用于注册和管理所有指标
    private final Metrics metrics;

    // 用于记录网络线程两次轮询之间时间的传感器的名称
    public static final String TIME_BETWEEN_NETWORK_THREAD_POLL_SENSOR_NAME = "time-between-network-thread-poll";
    // 用于记录应用程序事件队列大小的传感器的名称
    public static final String APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME = "application-event-queue-size";
    // 用于记录应用程序事件在队列中等待时间的传感器的名称
    public static final String APPLICATION_EVENT_QUEUE_TIME_SENSOR_NAME = "application-event-queue-time";
    // 用于记录应用程序事件队列处理时间的传感器的名称
    public static final String APPLICATION_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME = "application-event-queue-processing-time";
    // 用于记录已过期应用程序事件数量的传感器的名称
    public static final String APPLICATION_EVENT_EXPIRED_SIZE_SENSOR_NAME = "application-events-expired-count";
    // 用于记录后台事件队列大小的传感器的名称
    public static final String BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME = "background-event-queue-size";
    // 用于记录后台事件在队列中等待时间的传感器的名称
    public static final String BACKGROUND_EVENT_QUEUE_TIME_SENSOR_NAME = "background-event-queue-time";
    // 用于记录后台事件队列处理时间的传感器的名称
    public static final String BACKGROUND_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME = "background-event-queue-processing-time";
    // 用于记录未发送请求队列大小的传感器的名称
    public static final String UNSENT_REQUESTS_QUEUE_SIZE_SENSOR_NAME = "unsent-requests-queue-size";
    // 用于记录未发送请求在队列中等待时间的传感器的名称
    public static final String UNSENT_REQUESTS_QUEUE_TIME_SENSOR_NAME = "unsent-requests-queue-time";
    // 记录网络线程两次轮询之间时间的传感器
    private final Sensor timeBetweenNetworkThreadPollSensor;
    // 记录应用程序事件队列大小的传感器
    private final Sensor applicationEventQueueSizeSensor;
    // 记录应用程序事件在队列中等待时间的传感器
    private final Sensor applicationEventQueueTimeSensor;
    // 记录应用程序事件队列处理时间的传感器
    private final Sensor applicationEventQueueProcessingTimeSensor;
    // 记录已过期应用程序事件数量的传感器
    private final Sensor applicationEventExpiredSizeSensor;
    // 记录后台事件队列大小的传感器
    private final Sensor backgroundEventQueueSizeSensor;
    // 记录后台事件在队列中等待时间的传感器
    private final Sensor backgroundEventQueueTimeSensor;
    // 记录后台事件队列处理时间的传感器
    private final Sensor backgroundEventQueueProcessingTimeSensor;
    // 记录未发送请求队列大小的传感器
    private final Sensor unsentRequestsQueueSizeSensor;
    // 记录未发送请求在队列中等待时间的传感器
    private final Sensor unsentRequestsQueueTimeSensor;

    /**
     * AsyncConsumerMetrics 的构造函数。
     * @param metrics Metrics 对象，用于注册和管理所有指标。
     */
    public AsyncConsumerMetrics(Metrics metrics) {
        // 调用父类 KafkaConsumerMetrics 的构造函数，传递 metrics 对象和消费者指标组前缀
        // 这是为了初始化父类中定义的通用消费者指标
        super(metrics, CONSUMER_METRIC_GROUP_PREFIX);

        // 将传入的 Metrics 对象赋值给当前类的 metrics 字段
        // 这个 Metrics 对象将用于创建和注册此类特有的指标
        this.metrics = metrics;
        // 初始化用于记录网络线程两次轮询之间时间的传感器
        // metrics.sensor() 方法会查找或创建一个具有指定名称的 Sensor 对象
        this.timeBetweenNetworkThreadPollSensor = metrics.sensor(TIME_BETWEEN_NETWORK_THREAD_POLL_SENSOR_NAME);
        // 为 timeBetweenNetworkThreadPollSensor 添加一个平均值指标
        // 这个指标将报告网络线程每次轮询之间的平均时间（毫秒）
        // 为 timeBetweenNetworkThreadPollSensor 添加一个最大值指标
        // 这个指标将报告网络线程每次轮询之间的最大时间（毫秒）
        this.timeBetweenNetworkThreadPollSensor.add(
            metrics.metricName(
                "time-between-network-thread-poll-avg",
                CONSUMER_METRIC_GROUP,
                "网络线程每次轮询之间的平均时间（毫秒）。"
            ),
            new Avg()
        );
        // 为 timeBetweenNetworkThreadPollSensor 添加一个平均值指标
        // 这个指标将报告网络线程每次轮询之间的平均时间（毫秒）
        // 为 timeBetweenNetworkThreadPollSensor 添加一个最大值指标
        // 这个指标将报告网络线程每次轮询之间的最大时间（毫秒）
        this.timeBetweenNetworkThreadPollSensor.add(
            metrics.metricName(
                "time-between-network-thread-poll-max",
                CONSUMER_METRIC_GROUP,
                "网络线程每次轮询之间的最大时间（毫秒）。"
            ),
            new Max()
        );

        // 初始化用于记录应用程序事件队列大小的传感器
        this.applicationEventQueueSizeSensor = metrics.sensor(APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME);
        // 为 applicationEventQueueSizeSensor 添加一个当前值指标
        // 这个指标将报告从应用程序线程发送到后台线程的队列中当前的事件数量
        this.applicationEventQueueSizeSensor.add(
            metrics.metricName(
                APPLICATION_EVENT_QUEUE_SIZE_SENSOR_NAME,
                CONSUMER_METRIC_GROUP,
                "从应用程序线程发送到后台线程的队列中当前的事件数量。"
            ),
            new Value()
        );

        // 初始化用于记录应用程序事件在队列中等待时间的传感器
        this.applicationEventQueueTimeSensor = metrics.sensor(APPLICATION_EVENT_QUEUE_TIME_SENSOR_NAME);
        // 为 applicationEventQueueTimeSensor 添加一个平均值指标
        // 这个指标将报告应用程序事件出队的平均时间（毫秒）
        // 为 applicationEventQueueTimeSensor 添加一个最大值指标
        // 这个指标将报告应用程序事件出队的最大时间（毫秒）
        this.applicationEventQueueTimeSensor.add(
            metrics.metricName(
                "application-event-queue-time-avg",
                CONSUMER_METRIC_GROUP,
                "应用程序事件出队的平均时间（毫秒）。"
            ),
            new Avg()
        );
        // 为 applicationEventQueueTimeSensor 添加一个平均值指标
        // 这个指标将报告应用程序事件出队的平均时间（毫秒）
        // 为 applicationEventQueueTimeSensor 添加一个最大值指标
        // 这个指标将报告应用程序事件出队的最大时间（毫秒）
        this.applicationEventQueueTimeSensor.add(
            metrics.metricName(
                "application-event-queue-time-max",
                CONSUMER_METRIC_GROUP,
                "应用程序事件出队的最大时间（毫秒）。"
            ),
            new Max()
        );

        // 初始化用于记录应用程序事件队列处理时间的传感器
        this.applicationEventQueueProcessingTimeSensor = metrics.sensor(APPLICATION_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME);
        // 为 applicationEventQueueProcessingTimeSensor 添加一个平均值指标
        // 这个指标将报告后台线程处理所有可用应用程序事件的平均时间（毫秒）
        // 为 applicationEventQueueProcessingTimeSensor 添加一个最大值指标
        // 这个指标将报告后台线程处理所有可用应用程序事件的最大时间（毫秒）
        this.applicationEventQueueProcessingTimeSensor.add(
            metrics.metricName(
                "application-event-queue-processing-time-avg",
                CONSUMER_METRIC_GROUP,
                "后台线程处理所有可用应用程序事件的平均时间（毫秒）。"
            ),
            new Avg()
        );
        // 为 applicationEventQueueProcessingTimeSensor 添加一个平均值指标
        // 这个指标将报告后台线程处理所有可用应用程序事件的平均时间（毫秒）
        // 为 applicationEventQueueProcessingTimeSensor 添加一个最大值指标
        // 这个指标将报告后台线程处理所有可用应用程序事件的最大时间（毫秒）
        this.applicationEventQueueProcessingTimeSensor.add(
            metrics.metricName("application-event-queue-processing-time-max",
                CONSUMER_METRIC_GROUP,
                "后台线程处理所有可用应用程序事件的最大时间（毫秒）。"
            ),
            new Max()
        );

        // 初始化用于记录已过期应用程序事件数量的传感器
        this.applicationEventExpiredSizeSensor = metrics.sensor(APPLICATION_EVENT_EXPIRED_SIZE_SENSOR_NAME);
        // 为 applicationEventExpiredSizeSensor 添加一个当前值指标
        // 这个指标将报告当前已过期的应用程序事件的数量
        this.applicationEventExpiredSizeSensor.add(
            metrics.metricName(
                APPLICATION_EVENT_EXPIRED_SIZE_SENSOR_NAME,
                CONSUMER_METRIC_GROUP,
                "当前已过期的应用程序事件的数量。"
            ),
            new Value()
        );

        // 初始化用于记录未发送请求队列大小的传感器
        this.unsentRequestsQueueSizeSensor = metrics.sensor(UNSENT_REQUESTS_QUEUE_SIZE_SENSOR_NAME);
        // 为 unsentRequestsQueueSizeSensor 添加一个当前值指标
        // 这个指标将报告后台线程中当前未发送请求的数量
        this.unsentRequestsQueueSizeSensor.add(
            metrics.metricName(
                UNSENT_REQUESTS_QUEUE_SIZE_SENSOR_NAME,
                CONSUMER_METRIC_GROUP,
                "后台线程中当前未发送请求的数量。"
            ),
            new Value()
        );

        // 初始化用于记录未发送请求在队列中等待时间的传感器
        this.unsentRequestsQueueTimeSensor = metrics.sensor(UNSENT_REQUESTS_QUEUE_TIME_SENSOR_NAME);
        // 为 unsentRequestsQueueTimeSensor 添加一个平均值指标
        // 这个指标将报告后台线程中请求发送的平均时间（毫秒）
        // 为 unsentRequestsQueueTimeSensor 添加一个最大值指标
        // 这个指标将报告后台线程中请求保持未发送状态的最长时间（毫秒）
        this.unsentRequestsQueueTimeSensor.add(
            metrics.metricName(
                "unsent-requests-queue-time-avg",
                CONSUMER_METRIC_GROUP,
                "后台线程中请求发送的平均时间（毫秒）。"
            ),
            new Avg()
        );
        // 为 unsentRequestsQueueTimeSensor 添加一个平均值指标
        // 这个指标将报告后台线程中请求发送的平均时间（毫秒）
        // 为 unsentRequestsQueueTimeSensor 添加一个最大值指标
        // 这个指标将报告后台线程中请求保持未发送状态的最长时间（毫秒）
        this.unsentRequestsQueueTimeSensor.add(
            metrics.metricName(
                "unsent-requests-queue-time-max",
                CONSUMER_METRIC_GROUP,
                "后台线程中请求保持未发送状态的最长时间（毫秒）。"
            ),
            new Max()
        );

        // 初始化用于记录后台事件队列大小的传感器
        this.backgroundEventQueueSizeSensor = metrics.sensor(BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME);
        // 为 backgroundEventQueueSizeSensor 添加一个当前值指标
        // 这个指标将报告从后台线程发送到应用程序线程的队列中当前的事件数量
        this.backgroundEventQueueSizeSensor.add(
            metrics.metricName(
                BACKGROUND_EVENT_QUEUE_SIZE_SENSOR_NAME,
                CONSUMER_METRIC_GROUP,
                "从后台线程发送到应用程序线程的队列中当前的事件数量。"
            ),
            new Value()
        );

        // 初始化用于记录后台事件在队列中等待时间的传感器
        this.backgroundEventQueueTimeSensor = metrics.sensor(BACKGROUND_EVENT_QUEUE_TIME_SENSOR_NAME);
        // 为 backgroundEventQueueTimeSensor 添加一个平均值指标
        // 这个指标将报告后台事件出队的平均时间（毫秒）
        // 为 backgroundEventQueueTimeSensor 添加一个最大值指标
        // 这个指标将报告后台事件出队的最大时间（毫秒）
        this.backgroundEventQueueTimeSensor.add(
            metrics.metricName(
                "background-event-queue-time-avg",
                CONSUMER_METRIC_GROUP,
                "后台事件出队的平均时间（毫秒）。"
            ),
            new Avg()
        );
        // 为 backgroundEventQueueTimeSensor 添加一个平均值指标
        // 这个指标将报告后台事件出队的平均时间（毫秒）
        // 为 backgroundEventQueueTimeSensor 添加一个最大值指标
        // 这个指标将报告后台事件出队的最大时间（毫秒）
        this.backgroundEventQueueTimeSensor.add(
            metrics.metricName(
                "background-event-queue-time-max",
                CONSUMER_METRIC_GROUP,
                "后台事件出队的最大时间（毫秒）。"
            ),
            new Max()
        );

        // 初始化用于记录后台事件队列处理时间的传感器
        this.backgroundEventQueueProcessingTimeSensor = metrics.sensor(BACKGROUND_EVENT_QUEUE_PROCESSING_TIME_SENSOR_NAME);
        // 为 backgroundEventQueueProcessingTimeSensor 添加一个平均值指标
        // 这个指标将报告消费者处理所有可用后台事件的平均时间（毫秒）
        // 为 backgroundEventQueueProcessingTimeSensor 添加一个最大值指标
        // 这个指标将报告消费者处理所有可用后台事件的最大时间（毫秒）
        this.backgroundEventQueueProcessingTimeSensor.add(
            metrics.metricName(
                "background-event-queue-processing-time-avg",
                CONSUMER_METRIC_GROUP,
                "消费者处理所有可用后台事件的平均时间（毫秒）。"
            ),
            new Avg()
        );
        // 为 backgroundEventQueueProcessingTimeSensor 添加一个平均值指标
        // 这个指标将报告消费者处理所有可用后台事件的平均时间（毫秒）
        // 为 backgroundEventQueueProcessingTimeSensor 添加一个最大值指标
        // 这个指标将报告消费者处理所有可用后台事件的最大时间（毫秒）
        this.backgroundEventQueueProcessingTimeSensor.add(
            metrics.metricName(
                "background-event-queue-processing-time-max",
                CONSUMER_METRIC_GROUP,
                "消费者处理所有可用后台事件的最大时间（毫秒）。"
            ),
            new Max()
        );
    }

    /**
     * 记录网络线程两次轮询之间的时间。
     * <p>
     * 应用场景：监控网络线程的轮询频率，过长的时间间隔可能表示网络线程繁忙或存在瓶颈。
     * 实现细节：调用 {@code timeBetweenNetworkThreadPollSensor} 的 {@code record} 方法记录时间。
     * 设计考虑：提供此指标有助于诊断消费者与 Kafka 集群之间的网络交互性能。
     * </p>
     * @param timeBetweenNetworkThreadPoll 网络线程两次轮询之间的时间（毫秒）。
     */
    public void recordTimeBetweenNetworkThreadPoll(long timeBetweenNetworkThreadPoll) {
        // 使用 timeBetweenNetworkThreadPollSensor 传感器记录网络线程两次轮询之间的时间值
        this.timeBetweenNetworkThreadPollSensor.record(timeBetweenNetworkThreadPoll);
    }

    /**
     * 记录应用程序事件队列的大小。
     * <p>
     * 应用场景：监控应用程序事件队列的积压情况，队列过大可能表示后台处理能力不足或应用程序提交事件过快。
     * 实现细节：调用 {@code applicationEventQueueSizeSensor} 的 {@code record} 方法记录队列大小。
     * 设计考虑：帮助用户了解应用程序与后台线程之间的事件缓冲情况。
     * </p>
     * @param size 应用程序事件队列的大小。
     */
    public void recordApplicationEventQueueSize(int size) {
        // 使用 applicationEventQueueSizeSensor 传感器记录应用程序事件队列的当前大小
        this.applicationEventQueueSizeSensor.record(size);
    }

    /**
     * 记录应用程序事件在队列中的等待时间。
     * <p>
     * 应用场景：监控应用程序事件在被处理前在队列中等待的时间，较长的等待时间可能影响消费者的实时性。
     * 实现细节：调用 {@code applicationEventQueueTimeSensor} 的 {@code record} 方法记录等待时间。
     * 设计考虑：提供此指标有助于评估事件处理的延迟。
     * </p>
     * @param time 应用程序事件在队列中的等待时间（毫秒）。
     */
    public void recordApplicationEventQueueTime(long time) {
        // 使用 applicationEventQueueTimeSensor 传感器记录应用程序事件在队列中的等待时间
        this.applicationEventQueueTimeSensor.record(time);
    }

    /**
     * 记录应用程序事件队列的处理时间。
     * <p>
     * 应用场景：监控后台线程处理应用程序事件所花费的时间，较长的处理时间可能表明事件处理逻辑复杂或资源不足。
     * 实现细节：调用 {@code applicationEventQueueProcessingTimeSensor} 的 {@code record} 方法记录处理时间。
     * 设计考虑：帮助分析后台线程处理事件的效率。
     * </p>
     * @param processingTime 应用程序事件队列的处理时间（毫秒）。
     */
    public void recordApplicationEventQueueProcessingTime(long processingTime) {
        // 使用 applicationEventQueueProcessingTimeSensor 传感器记录处理应用程序事件队列所需的时间
        this.applicationEventQueueProcessingTimeSensor.record(processingTime);
    }

    /**
     * 记录已过期的应用程序事件数量。
     * <p>
     * 应用场景：监控因超时等原因未能及时处理而被丢弃的应用程序事件数量，过多的过期事件可能表示系统处理能力不足或事件配置不当。
     * 实现细节：调用 {@code applicationEventExpiredSizeSensor} 的 {@code record} 方法记录过期事件数量。
     * 设计考虑：帮助用户识别和诊断事件丢失问题。
     * </p>
     * @param size 已过期的应用程序事件数量。
     */
    public void recordApplicationEventExpiredSize(long size) {
        // 使用 applicationEventExpiredSizeSensor 传感器记录已过期的应用程序事件的数量
        this.applicationEventExpiredSizeSensor.record(size);
    }

    /**
     * 记录未发送请求队列的大小。
     * <p>
     * 应用场景：监控等待发送到 Kafka 服务器的请求队列大小，队列积压可能表示网络问题或服务器响应缓慢。
     * 实现细节：调用 {@code unsentRequestsQueueSizeSensor} 的 {@code record} 方法记录队列大小和当前时间。
     * 设计考虑：提供对出站请求缓冲情况的可见性。
     * </p>
     * @param size 未发送请求队列的大小。
     * @param timeMs 当前时间戳（毫秒），用于某些传感器类型进行速率计算或窗口统计。
     */
    public void recordUnsentRequestsQueueSize(int size, long timeMs) {
        // 使用 unsentRequestsQueueSizeSensor 传感器记录未发送请求队列的大小，并提供当前时间戳
        this.unsentRequestsQueueSizeSensor.record(size, timeMs);
    }

    /**
     * 记录未发送请求在队列中的等待时间。
     * <p>
     * 应用场景：监控请求在发送前在队列中等待的时间，较长的等待时间可能影响请求的及时性。
     * 实现细节：调用 {@code unsentRequestsQueueTimeSensor} 的 {@code record} 方法记录等待时间。
     * 设计考虑：帮助评估请求发送的延迟。
     * </p>
     * @param time 未发送请求在队列中的等待时间（毫秒）。
     */
    public void recordUnsentRequestsQueueTime(long time) {
        // 使用 unsentRequestsQueueTimeSensor 传感器记录未发送请求在队列中的等待时间
        this.unsentRequestsQueueTimeSensor.record(time);
    }

    /**
     * 记录后台事件队列的大小。
     * <p>
     * 应用场景：监控后台事件队列的积压情况，这通常是内部事件队列，用于消费者组件间的通信。
     * 实现细节：调用 {@code backgroundEventQueueSizeSensor} 的 {@code record} 方法记录队列大小。
     * 设计考虑：帮助了解内部事件处理的负载情况。
     * </p>
     * @param size 后台事件队列的大小。
     */
    public void recordBackgroundEventQueueSize(int size) {
        // 使用 backgroundEventQueueSizeSensor 传感器记录后台事件队列的当前大小
        this.backgroundEventQueueSizeSensor.record(size);
    }

    /**
     * 记录后台事件在队列中的等待时间。
     * <p>
     * 应用场景：监控后台事件在被处理前在队列中等待的时间。
     * 实现细节：调用 {@code backgroundEventQueueTimeSensor} 的 {@code record} 方法记录等待时间。
     * 设计考虑：提供对内部事件处理延迟的洞察。
     * </p>
     * @param time 后台事件在队列中的等待时间（毫秒）。
     */
    public void recordBackgroundEventQueueTime(long time) {
        // 使用 backgroundEventQueueTimeSensor 传感器记录后台事件在队列中的等待时间
        this.backgroundEventQueueTimeSensor.record(time);
    }

    /**
     * 记录后台事件队列的处理时间。
     * <p>
     * 应用场景：监控处理后台事件所花费的时间。
     * 实现细节：调用 {@code backgroundEventQueueProcessingTimeSensor} 的 {@code record} 方法记录处理时间。
     * 设计考虑：帮助分析内部事件处理的效率。
     * </p>
     * @param processingTime 后台事件队列的处理时间（毫秒）。
     */
    public void recordBackgroundEventQueueProcessingTime(long processingTime) {
        // 使用 backgroundEventQueueProcessingTimeSensor 传感器记录处理后台事件队列所需的时间
        this.backgroundEventQueueProcessingTimeSensor.record(processingTime);
    }

    /**
     * 关闭 {@link AsyncConsumerMetrics} 并移除所有相关的传感器。
     * <p>
     * 应用场景：当消费者关闭或不再需要这些指标时调用此方法，以释放资源并防止指标泄漏。
     * 实现细节：
     * 1. 创建一个包含所有此类定义的传感器名称的列表。
     * 2. 遍历列表中的每个传感器名称，并调用 {@code metrics.removeSensor()} 方法将其从 {@link Metrics} 实例中移除。
     * 3. 调用父类 {@code KafkaConsumerMetrics} 的 {@code close()} 方法，以关闭父类中定义的传感器。
     * 设计考虑：确保在组件生命周期结束时正确清理所有注册的指标，避免资源浪费和潜在的内存问题。
     * </p>
     */
    @Override
    public void close() {
        // 创建一个包含所有在此类中定义的传感器名称的列表
        Arrays.asList(
            // 获取网络线程轮询间隔传感器的名称
            timeBetweenNetworkThreadPollSensor.name(),
            // 获取应用程序事件队列大小传感器的名称
            applicationEventQueueSizeSensor.name(),
            // 获取应用程序事件队列等待时间传感器的名称
            applicationEventQueueTimeSensor.name(),
            // 获取应用程序事件队列处理时间传感器的名称
            applicationEventQueueProcessingTimeSensor.name(),
            // 获取应用程序过期事件数量传感器的名称
            applicationEventExpiredSizeSensor.name(),
            // 获取后台事件队列大小传感器的名称
            backgroundEventQueueSizeSensor.name(),
            // 获取后台事件队列等待时间传感器的名称
            backgroundEventQueueTimeSensor.name(),
            // 获取后台事件队列处理时间传感器的名称
            backgroundEventQueueProcessingTimeSensor.name(),
            // 获取未发送请求队列大小传感器的名称
            unsentRequestsQueueSizeSensor.name(),
            // 获取未发送请求队列等待时间传感器的名称
            unsentRequestsQueueTimeSensor.name()
        ).forEach(metrics::removeSensor); // 遍历列表中的每个传感器名称，并从 Metrics 对象中移除该传感器
        // 调用父类的 close 方法，以清理父类中注册的传感器
        super.close();
    }
}
