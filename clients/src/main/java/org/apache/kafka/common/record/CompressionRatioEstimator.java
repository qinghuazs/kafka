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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * 该类用于估算每个主题和压缩类型组合的压缩比率。
 * 通过动态调整压缩比率估算值来优化Kafka的消息压缩效果。
 * 使用线程安全的并发Map来存储每个主题的压缩比率数组。
 */
public class CompressionRatioEstimator {
    // 当批次压缩效果比预期好时，提高压缩比率的固定步长
    // 使用较小的步长(0.005)来确保压缩比率的平稳改善
    public static final float COMPRESSION_RATIO_IMPROVING_STEP = 0.005f;
    // 当批次压缩效果比预期差时，降低压缩比率的最小步长
    // 使用较大的步长(0.05)来快速适应压缩效果的恶化
    public static final float COMPRESSION_RATIO_DETERIORATE_STEP = 0.05f;
    // 存储每个主题的压缩比率数组的并发Map
    // Key为主题名称，Value为该主题下不同压缩类型的压缩比率数组
    private static final ConcurrentMap<String, float[]> COMPRESSION_RATIO = new ConcurrentHashMap<>();

    /**
     * 更新指定主题和压缩类型的压缩比率估算值。
     * 实现步骤：
     * 1. 获取主题的压缩比率数组（不存在则创建）
     * 2. 获取当前压缩类型的估算值
     * 3. 根据观察到的压缩比率调整估算值：
     *    - 如果观察值大于估算值，使用较大步长提高估算值
     *    - 如果观察值小于估算值，使用较小步长降低估算值
     * 4. 返回更新后的估算值
     *
     * @param topic         要更新压缩比率估算的主题
     * @param type          压缩类型
     * @param observedRatio 观察到的实际压缩比率
     * @return 更新后的压缩比率估算值
     */
    public static float updateEstimation(String topic, CompressionType type, float observedRatio) {
        // 获取或创建主题的压缩比率数组
        float[] compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic);
        // 获取当前压缩类型的估算值
        float currentEstimation = compressionRatioForTopic[type.id];
        // 使用同步块确保线程安全
        synchronized (compressionRatioForTopic) {
            if (observedRatio > currentEstimation)
                // 压缩效果变差，使用较大步长调整
                compressionRatioForTopic[type.id] = Math.max(currentEstimation + COMPRESSION_RATIO_DETERIORATE_STEP, observedRatio);
            else if (observedRatio < currentEstimation) {
                // 压缩效果改善，使用较小步长调整
                compressionRatioForTopic[type.id] = Math.max(currentEstimation - COMPRESSION_RATIO_IMPROVING_STEP, observedRatio);
            }
        }
        return compressionRatioForTopic[type.id];
    }

    /**
     * 获取指定主题和压缩类型的压缩比率估算值。
     * 如果主题不存在，会创建一个新的压缩比率数组并初始化默认值。
     *
     * @param topic 要查询的主题
     * @param type  压缩类型
     * @return 当前的压缩比率估算值
     */
    public static float estimation(String topic, CompressionType type) {
        // 获取主题的压缩比率数组，如果不存在则创建
        float[] compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic);
        // 返回指定压缩类型的估算值
        return compressionRatioForTopic[type.id];
    }

    /**
     * 重置指定主题的所有压缩类型的压缩比率估算值为初始值。
     * 实现步骤：
     * 1. 获取主题的压缩比率数组
     * 2. 在同步块中重置所有压缩类型的估算值
     * 3. 使用每种压缩类型的默认压缩率
     *
     * @param topic 要重置的主题
     */
    public static void resetEstimation(String topic) {
        // 获取主题的压缩比率数组
        float[] compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic);
        // 使用同步块确保线程安全
        synchronized (compressionRatioForTopic) {
            // 遍历所有压缩类型，重置为默认压缩率
            for (CompressionType type : CompressionType.values()) {
                compressionRatioForTopic[type.id] = type.rate;
            }
        }
    }

    /**
     * 设置指定主题和压缩类型的压缩比率估算值。
     * 此方法主要用于单元测试目的，允许直接设置压缩比率。
     *
     * @param topic 要设置的主题
     * @param type  压缩类型
     * @param ratio 要设置的压缩比率值
     */
    public static void setEstimation(String topic, CompressionType type, float ratio) {
        // 获取主题的压缩比率数组
        float[] compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic);
        // 使用同步块确保线程安全
        synchronized (compressionRatioForTopic) {
            // 直接设置指定压缩类型的压缩比率
            compressionRatioForTopic[type.id] = ratio;
        }
    }

    /**
     * 获取指定主题的压缩比率数组，如果不存在则创建新的数组。
     * 实现步骤：
     * 1. 尝试从并发Map中获取主题的压缩比率数组
     * 2. 如果不存在，创建新的压缩比率数组
     * 3. 使用putIfAbsent确保线程安全的创建
     * 4. 返回已存在或新创建的压缩比率数组
     *
     * @param topic 主题名称
     * @return 主题对应的压缩比率数组
     */
    private static float[] getAndCreateEstimationIfAbsent(String topic) {
        // 尝试获取主题的压缩比率数组
        float[] compressionRatioForTopic = COMPRESSION_RATIO.get(topic);
        if (compressionRatioForTopic == null) {
            // 创建新的压缩比率数组
            compressionRatioForTopic = initialCompressionRatio();
            // 尝试将新创建的数组放入Map中
            float[] existingCompressionRatio = COMPRESSION_RATIO.putIfAbsent(topic, compressionRatioForTopic);
            // 如果其他线程已经创建了数组，使用已存在的数组
            if (existingCompressionRatio != null)
                return existingCompressionRatio;
        }
        return compressionRatioForTopic;
    }

    /**
     * 创建并初始化压缩比率数组。
     * 实现步骤：
     * 1. 创建与压缩类型数量相同长度的数组
     * 2. 使用每种压缩类型的默认压缩率初始化数组
     *
     * @return 初始化后的压缩比率数组
     */
    private static float[] initialCompressionRatio() {
        // 创建数组，大小为压缩类型的数量
        float[] compressionRatio = new float[CompressionType.values().length];
        // 使用每种压缩类型的默认压缩率初始化数组
        for (CompressionType type : CompressionType.values()) {
            compressionRatio[type.id] = type.rate;
        }
        return compressionRatio;
    }
}
