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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.SampledStat;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 传感器构建器类
 * 用于简化创建{@link Sensor sensors}以记录{@link Metric metrics}的过程
 * 
 * 应用场景：
 * 1. 创建性能监控传感器
 * 2. 构建指标收集器
 * 3. 配置度量统计方式
 * 4. 管理指标标签
 */
public class SensorBuilder {

    /**
     * 指标管理器
     * 用于创建和管理传感器及其相关指标
     */
    private final Metrics metrics;

    /**
     * 传感器实例
     * 用于记录和跟踪指标数据
     */
    private final Sensor sensor;

    /**
     * 是否为预先存在的传感器
     * 用于控制是否需要添加新的指标配置
     */
    private final boolean preexisting;

    /**
     * 指标标签映射
     * 用于标识和分类指标
     */
    private final Map<String, String> tags;

    /**
     * 构造函数
     * 使用默认的空标签映射创建传感器构建器
     *
     * @param metrics 指标管理器
     * @param name 传感器名称
     */
    public SensorBuilder(Metrics metrics, String name) {
        // 调用带有标签供应商的构造函数，使用空映射作为默认标签
        this(metrics, name, Collections::emptyMap);
    }

    /**
     * 构造函数
     * 创建一个新的传感器构建器实例
     *
     * @param metrics 指标管理器
     * @param name 传感器名称
     * @param tagsSupplier 标签映射的供应商
     */
    public SensorBuilder(Metrics metrics, String name, Supplier<Map<String, String>> tagsSupplier) {
        // 初始化指标管理器
        this.metrics = metrics;
        // 尝试获取已存在的传感器
        Sensor s = metrics.getSensor(name);

        // 如果传感器已存在
        if (s != null) {
            // 使用现有传感器
            sensor = s;
            // 使用空标签映射
            tags = Collections.emptyMap();
            // 标记为预先存在
            preexisting = true;
        } else {
            // 创建新的传感器
            sensor = metrics.sensor(name);
            // 从供应商获取标签
            tags = tagsSupplier.get();
            // 标记为新创建
            preexisting = false;
        }
    }

    /**
     * 添加平均值统计
     *
     * @param name 指标名称模板
     * @return 构建器实例，支持链式调用
     */
    SensorBuilder withAvg(MetricNameTemplate name) {
        // 只有对新创建的传感器才添加统计
        if (!preexisting)
            // 添加平均值统计器
            sensor.add(metrics.metricInstance(name, tags), new Avg());

        return this;
    }

    /**
     * 添加最小值统计
     *
     * @param name 指标名称模板
     * @return 构建器实例，支持链式调用
     */
    SensorBuilder withMin(MetricNameTemplate name) {
        // 只有对新创建的传感器才添加统计
        if (!preexisting)
            // 添加最小值统计器
            sensor.add(metrics.metricInstance(name, tags), new Min());

        return this;
    }

    /**
     * 添加最大值统计
     *
     * @param name 指标名称模板
     * @return 构建器实例，支持链式调用
     */
    SensorBuilder withMax(MetricNameTemplate name) {
        // 只有对新创建的传感器才添加统计
        if (!preexisting)
            // 添加最大值统计器
            sensor.add(metrics.metricInstance(name, tags), new Max());

        return this;
    }

    /**
     * 添加值统计
     *
     * @param name 指标名称模板
     * @return 构建器实例，支持链式调用
     */
    SensorBuilder withValue(MetricNameTemplate name) {
        // 只有对新创建的传感器才添加统计
        if (!preexisting)
            // 添加值统计器
            sensor.add(metrics.metricInstance(name, tags), new Value());

        return this;
    }

    /**
     * 添加计量器统计
     *
     * @param rateName 速率指标名称模板
     * @param totalName 总量指标名称模板
     * @return 构建器实例，支持链式调用
     */
    SensorBuilder withMeter(MetricNameTemplate rateName, MetricNameTemplate totalName) {
        // 只有对新创建的传感器才添加统计
        if (!preexisting) {
            // 添加计量器，包含速率和总量统计
            sensor.add(new Meter(metrics.metricInstance(rateName, tags), metrics.metricInstance(totalName, tags)));
        }

        return this;
    }

    /**
     * 添加带有采样统计的计量器
     *
     * @param sampledStat 采样统计器
     * @param rateName 速率指标名称模板
     * @param totalName 总量指标名称模板
     * @return 构建器实例，支持链式调用
     */
    SensorBuilder withMeter(SampledStat sampledStat, MetricNameTemplate rateName, MetricNameTemplate totalName) {
        // 只有对新创建的传感器才添加统计
        if (!preexisting) {
            // 添加带有采样统计的计量器
            sensor.add(new Meter(sampledStat, metrics.metricInstance(rateName, tags), metrics.metricInstance(totalName, tags)));
        }

        return this;
    }

    /**
     * 构建并返回传感器实例
     *
     * @return 配置完成的传感器实例
     */
    Sensor build() {
        // 返回已配置的传感器
        return sensor;
    }
}
