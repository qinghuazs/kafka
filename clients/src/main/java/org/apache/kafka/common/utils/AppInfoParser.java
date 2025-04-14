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
package org.apache.kafka.common.utils;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Properties;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Kafka应用程序信息解析器，负责管理和暴露应用程序的元数据信息
 * 主要功能包括：
 * 1. 读取和提供Kafka版本信息和提交ID
 * 2. 注册和注销JMX MBean，用于监控应用程序信息
 * 3. 管理应用程序相关的度量指标
 */
public class AppInfoParser {
    // 日志记录器
    private static final Logger log = LoggerFactory.getLogger(AppInfoParser.class);
    // Kafka版本号
    private static final String VERSION;
    // Kafka代码的提交ID
    private static final String COMMIT_ID;

    // 当无法获取版本信息时的默认值
    protected static final String DEFAULT_VALUE = "unknown";

    // 静态初始化块：加载Kafka版本信息
    static {
        Properties props = new Properties();
        try (InputStream resourceStream = AppInfoParser.class.getResourceAsStream("/kafka/kafka-version.properties")) {
            // 从配置文件加载版本信息
            props.load(resourceStream);
        } catch (Exception e) {
            // 如果加载失败，记录警告日志
            log.warn("Error while loading kafka-version.properties: {}", e.getMessage());
        }
        // 获取版本号，如果不存在则使用默认值
        VERSION = props.getProperty("version", DEFAULT_VALUE).trim();
        // 获取提交ID，如果不存在则使用默认值
        COMMIT_ID = props.getProperty("commitId", DEFAULT_VALUE).trim();
    }

    /**
     * 获取Kafka版本号
     * @return Kafka版本号字符串
     */
    public static String getVersion() {
        return VERSION;
    }

    /**
     * 获取Kafka代码的提交ID
     * @return 提交ID字符串
     */
    public static String getCommitId() {
        return COMMIT_ID;
    }

    /**
     * 注册应用程序信息到JMX并添加相关度量指标
     * @param prefix JMX MBean名称前缀
     * @param id 应用程序ID
     * @param metrics 度量指标收集器
     * @param nowMs 当前时间戳（毫秒）
     */
    public static synchronized void registerAppInfo(String prefix, String id, Metrics metrics, long nowMs) {
        try {
            // 创建JMX ObjectName，格式为：prefix:type=app-info,id=sanitized_id
            ObjectName name = new ObjectName(prefix + ":type=app-info,id=" + Sanitizer.jmxSanitize(id));
            // 获取JMX MBean服务器
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            // 检查是否已经注册了相同名称的MBean
            if (server.isRegistered(name)) {
                log.info("The mbean of App info: [{}], id: [{}] already exists, so skipping a new mbean creation.", prefix, id);
                return;
            }
            // 创建新的AppInfo MBean实例
            AppInfo mBean = new AppInfo(nowMs);
            // 注册MBean到JMX服务器
            server.registerMBean(mBean, name);

            // 注册相关的度量指标，前缀将由JmxReporter稍后添加
            registerMetrics(metrics, mBean);
        } catch (JMException e) {
            log.warn("Error registering AppInfo mbean", e);
        }
    }

    /**
     * 从JMX注销应用程序信息并移除相关度量指标
     * @param prefix JMX MBean名称前缀
     * @param id 应用程序ID
     * @param metrics 度量指标收集器
     */
    public static synchronized void unregisterAppInfo(String prefix, String id, Metrics metrics) {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName(prefix + ":type=app-info,id=" + Sanitizer.jmxSanitize(id));
            if (server.isRegistered(name))
                server.unregisterMBean(name);

            unregisterMetrics(metrics);
        } catch (JMException e) {
            log.warn("Error unregistering AppInfo mbean", e);
        } finally {
            log.info("App info {} for {} unregistered", prefix, id);
        }
    }

    /**
     * 创建度量指标名称
     * @param metrics 度量指标收集器
     * @param name 指标名称
     * @return 完整的度量指标名称对象
     */
    private static MetricName metricName(Metrics metrics, String name) {
        return metrics.metricName(name, "app-info", "Metric indicating " + name);
    }

    /**
     * 注册应用程序相关的度量指标
     * @param metrics 度量指标收集器
     * @param appInfo 应用程序信息对象
     */
    private static void registerMetrics(Metrics metrics, AppInfo appInfo) {
        if (metrics != null) {
            // 添加版本号度量指标
            metrics.addMetric(metricName(metrics, "version"), new ImmutableValue<>(appInfo.getVersion()));
            // 添加提交ID度量指标
            metrics.addMetric(metricName(metrics, "commit-id"), new ImmutableValue<>(appInfo.getCommitId()));
            // 添加启动时间度量指标
            metrics.addMetric(metricName(metrics, "start-time-ms"), new ImmutableValue<>(appInfo.getStartTimeMs()));
        }
    }

    /**
     * 注销应用程序相关的度量指标
     * @param metrics 度量指标收集器
     */
    private static void unregisterMetrics(Metrics metrics) {
        if (metrics != null) {
            // 移除版本号度量指标
            metrics.removeMetric(metricName(metrics, "version"));
            // 移除提交ID度量指标
            metrics.removeMetric(metricName(metrics, "commit-id"));
            // 移除启动时间度量指标
            metrics.removeMetric(metricName(metrics, "start-time-ms"));
        }
    }

    /**
     * JMX MBean接口，定义了可以通过JMX暴露的应用程序信息
     */
    public interface AppInfoMBean {
        String getVersion();
        String getCommitId();
        Long getStartTimeMs();
    }

    /**
     * 应用程序信息实现类，实现了JMX MBean接口
     */
    public static class AppInfo implements AppInfoMBean {

        private final Long startTimeMs;

        public AppInfo(long startTimeMs) {
            this.startTimeMs = startTimeMs;
            log.info("Kafka version: {}", AppInfoParser.getVersion());
            log.info("Kafka commitId: {}", AppInfoParser.getCommitId());
            log.info("Kafka startTimeMs: {}", startTimeMs);
        }

        @Override
        public String getVersion() {
            return AppInfoParser.getVersion();
        }

        @Override
        public String getCommitId() {
            return AppInfoParser.getCommitId();
        }

        @Override
        public Long getStartTimeMs() {
            return startTimeMs;
        }

    }

    /**
     * 不可变的度量指标值包装类
     * @param <T> 度量指标值的类型
     */
    static class ImmutableValue<T> implements Gauge<T> {
        private final T value;

        public ImmutableValue(T value) {
            this.value = value;
        }

        @Override
        public T value(MetricConfig config, long now) {
            return value;
        }
    }
}
