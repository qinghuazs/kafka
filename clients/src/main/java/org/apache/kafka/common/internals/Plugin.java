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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Monitorable;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Plugin类是Kafka的插件包装器，用于管理插件实例的生命周期和度量指标。
 * 该类实现了Supplier接口以提供插件实例访问，同时实现AutoCloseable接口以确保资源的正确释放。
 */
public class Plugin<T> implements Supplier<T>, AutoCloseable {

    // 被包装的插件实例
    private final T instance;
    // 插件的度量指标实现，使用Optional包装以处理可能为空的情况
    private final Optional<PluginMetricsImpl> pluginMetrics;

    /**
     * 私有构造函数，用于创建Plugin实例
     * @param instance 要包装的插件实例
     * @param pluginMetrics 插件的度量指标实现
     */
    private Plugin(T instance, PluginMetricsImpl pluginMetrics) {
        this.instance = instance;
        this.pluginMetrics = Optional.ofNullable(pluginMetrics);
    }

    /**
     * 包装插件实例的静态工厂方法
     * @param instance 要包装的插件实例
     * @param metrics 度量指标系统
     * @param key 配置键，用于标识插件
     * @return 包装后的Plugin实例
     */
    public static <T> Plugin<T> wrapInstance(T instance, Metrics metrics, String key) {
        // 使用lambda表达式生成标签供应器
        return wrapInstance(instance, metrics, () -> tags(key, instance));
    }

    /**
     * 创建插件标签的私有辅助方法
     * @param key 配置键
     * @param instance 插件实例
     * @return 包含配置键和类名的标签Map
     */
    private static <T> Map<String, String> tags(String key, T instance) {
        Map<String, String> tags = new LinkedHashMap<>();
        // 添加配置键作为标签
        tags.put("config", key);
        // 添加实例的类名作为标签
        tags.put("class", instance.getClass().getSimpleName());
        return tags;
    }

    /**
     * 批量包装多个插件实例的静态工厂方法
     * @param instances 要包装的插件实例列表
     * @param metrics 度量指标系统
     * @param key 配置键
     * @return 包装后的Plugin实例列表
     */
    public static <T> List<Plugin<T>> wrapInstances(List<T> instances, Metrics metrics, String key) {
        List<Plugin<T>> plugins = new ArrayList<>();
        // 遍历每个实例并进行包装
        for (T instance : instances) {
            plugins.add(wrapInstance(instance, metrics, key));
        }
        return plugins;
    }

    /**
     * 使用自定义标签供应器包装插件实例的静态工厂方法
     * @param instance 要包装的插件实例
     * @param metrics 度量指标系统
     * @param tagsSupplier 提供标签的供应器
     * @return 包装后的Plugin实例
     */
    public static <T> Plugin<T> wrapInstance(T instance, Metrics metrics, Supplier<Map<String, String>> tagsSupplier) {
        PluginMetricsImpl pluginMetrics = null;
        // 如果实例支持监控且提供了度量指标系统
        if (instance instanceof Monitorable && metrics != null) {
            // 创建插件度量指标实现
            pluginMetrics = new PluginMetricsImpl(metrics, tagsSupplier.get());
            // 将度量指标关联到可监控的实例
            ((Monitorable) instance).withPluginMetrics(pluginMetrics);
        }
        return new Plugin<>(instance, pluginMetrics);
    }

    /**
     * 实现Supplier接口的get方法，返回包装的插件实例
     * @return 原始的插件实例
     */
    @Override
    public T get() {
        return instance;
    }

    /**
     * 实现AutoCloseable接口的close方法，负责清理资源
     * 包括关闭插件实例（如果实现了AutoCloseable）和度量指标
     * @throws Exception 如果关闭过程中发生错误
     */
    @Override
    public void close() throws Exception {
        // 用于存储第一个发生的异常
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        // 如果实例支持自动关闭，则安全地关闭它
        if (instance instanceof AutoCloseable) {
            Utils.closeQuietly((AutoCloseable) instance, instance.getClass().getSimpleName(), firstException);
        }
        // 如果存在度量指标，则安全地关闭它
        pluginMetrics.ifPresent(metrics -> Utils.closeQuietly(metrics, "pluginMetrics", firstException));
        // 检查是否发生了异常
        Throwable throwable = firstException.get();
        if (throwable != null) throw new KafkaException("failed closing plugin", throwable);
    }
}
