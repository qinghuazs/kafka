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

import java.util.concurrent.ThreadLocalRandom;

/**
 * 指数退避算法工具类，用于计算重试、重连等场景的延迟时间。
 * 该类提供了一个具有抖动（jitter）的指数退避实现，可以避免多个客户端在同一时间重试导致的"惊群效应"。
 * <p>
 * 退避时间计算公式：
 * <pre>Backoff(attempts) = random(1 - jitter, 1 + jitter) * initialInterval * multiplier ^ attempts</pre>
 * 其中：
 * - attempts: 重试次数
 * - jitter: 随机抖动因子，用于在退避时间上增加随机性
 * - initialInterval: 初始退避时间
 * - multiplier: 退避时间的递增倍数
 * <p>
 * 特殊情况：
 * 1. 如果maxInterval小于initialInterval，将始终返回maxInterval作为固定的退避时间
 * 2. 即使有随机抖动，最终的退避时间也不会超过maxInterval
 * <p>
 * 该类是线程安全的。
 */
public class ExponentialBackoff {
    // 初始退避时间（毫秒）
    private final long initialInterval;
    // 退避时间的递增倍数
    private final int multiplier;
    // 最大退避时间（毫秒）
    private final long maxInterval;
    // 随机抖动因子（0表示无抖动，0.2表示在退避时间的±20%范围内随机）
    private final double jitter;
    // 达到maxInterval所需的最大指数值，用于优化计算
    private final double expMax;

    /**
     * 创建一个指数退避算法实例
     * @param initialInterval 初始退避时间（毫秒）
     * @param multiplier 退避时间的递增倍数
     * @param maxInterval 最大退避时间（毫秒）
     * @param jitter 随机抖动因子（0-1之间的小数）
     */
    public ExponentialBackoff(long initialInterval, int multiplier, long maxInterval, double jitter) {
        // 确保初始退避时间不超过最大退避时间
        this.initialInterval = Math.min(maxInterval, initialInterval);
        this.multiplier = multiplier;
        this.maxInterval = maxInterval;
        this.jitter = jitter;
        // 计算达到maxInterval所需的最大指数值
        // 如果maxInterval <= initialInterval，则expMax = 0，表示将始终返回固定值
        this.expMax = maxInterval > initialInterval ?
                Math.log(maxInterval / (double) Math.max(initialInterval, 1)) / Math.log(multiplier) : 0;
    }

    /**
     * 获取初始退避时间
     * @return 初始退避时间（毫秒）
     */
    public long initialInterval() {
        return initialInterval;
    }

    /**
     * 计算当前重试次数下的退避时间
     * @param attempts 当前重试次数
     * @return 计算得到的退避时间（毫秒）
     */
    public long backoff(long attempts) {
        // 如果expMax为0，说明maxInterval <= initialInterval，直接返回固定值
        if (expMax == 0) {
            return initialInterval;
        }
        // 限制指数增长的最大值，避免溢出
        double exp = Math.min(attempts, this.expMax);
        // 计算基础退避时间：initialInterval * multiplier^exp
        double term = initialInterval * Math.pow(multiplier, exp);
        // 计算随机因子：如果jitter接近0则不使用随机抖动
        double randomFactor = jitter < Double.MIN_NORMAL ? 1.0 :
            ThreadLocalRandom.current().nextDouble(1 - jitter, 1 + jitter);
        // 应用随机因子并转换为长整型
        long backoffValue = (long) (randomFactor * term);
        // 确保不超过最大退避时间
        return Math.min(backoffValue, maxInterval);
    }

    @Override
    public String toString() {
        return "ExponentialBackoff{" +
                "multiplier=" + multiplier +
                ", expMax=" + expMax +
                ", initialInterval=" + initialInterval +
                ", jitter=" + jitter +
                '}';
    }
}
