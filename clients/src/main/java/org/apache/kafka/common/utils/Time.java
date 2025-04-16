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

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * 时钟抽象接口，用于在单元测试中模拟和控制时间。
 * 该接口提供了一系列时间相关的操作，包括获取当前时间、休眠、等待条件等功能。
 * 
 * 所有实现该接口的类都必须保证线程安全。
 */
public interface Time {

    /**
     * 系统默认的Time实现实例，使用系统时钟
     */
    Time SYSTEM = SystemTime.getSystemTime();

    /**
     * 获取当前时间的毫秒值
     * 
     * @return 返回自1970年1月1日00:00:00 UTC以来的毫秒数
     */
    long milliseconds();

    /**
     * 获取高精度时钟的毫秒值
     * 
     * @return 将纳秒时间转换为毫秒返回
     */
    default long hiResClockMs() {
        // 将纳秒转换为毫秒
        return TimeUnit.NANOSECONDS.toMillis(nanoseconds());
    }

    /**
     * 获取JVM的高精度时间源的当前值（纳秒）
     * 
     * <p>此方法仅用于测量时间间隔，与系统时间或挂钟时间无关。
     * 返回值表示从某个固定但任意的<i>原点</i>时间开始的纳秒数
     * （该原点可能在将来，因此值可能为负）。
     * 在同一个Java虚拟机实例中的所有调用都使用相同的原点；
     * 不同的虚拟机实例可能使用不同的原点。
     * 
     * @return 返回高精度计时器的当前值，单位为纳秒
     */
    long nanoseconds();

    /**
     * 使当前线程休眠指定的毫秒数
     * 
     * @param ms 要休眠的毫秒数
     */
    void sleep(long ms);

    /**
     * 使用给定对象的监视器等待条件满足
     * 这种方式避免了直接调用{@link Object#wait()}时对系统时间的隐式依赖
     *
     * @param obj 用于等待的对象，将使用其{@link Object#wait()}方法
     *            注意：调用者负责在条件满足时调用该对象的notify方法
     * @param condition 要等待的条件（返回true表示条件满足）
     * @param deadlineMs 超时截止时间戳，超过这个时间将抛出超时异常
     *
     * @throws InterruptedException 如果等待过程中线程被中断
     * @throws org.apache.kafka.common.errors.TimeoutException 如果在条件满足前达到超时时间
     */
    void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) throws InterruptedException;

    /**
     * 创建一个绑定到当前时间实例的定时器
     * 
     * @param timeoutMs 定时器超时时间（毫秒）
     * @return 返回新创建的定时器实例
     */
    default Timer timer(long timeoutMs) {
        // 创建并返回一个新的Timer实例
        return new Timer(this, timeoutMs);
    }

    /**
     * 获取一个绑定到当前时间实例的定时器，使用Duration指定超时时间。
     * 
     * @param timeout 超时时间（Duration类型）
     * @return 新创建的定时器实例
     */
    default Timer timer(Duration timeout) {
        return timer(timeout.toMillis());
    }

    /**
     * 等待Future完成或超时
     *
     * @param future     要等待的Future对象
     * @param deadlineNs 超时截止时间（单调递增的纳秒时间）
     * @return          Future的执行结果
     * @param <T>       Future的结果类型
     * @throws TimeoutException 如果等待超时
     * @throws InterruptedException 如果等待过程中线程被中断
     * @throws ExecutionException 如果Future执行过程中发生异常
     */
    default <T> T waitForFuture(
        Future<T> future,
        long deadlineNs
    ) throws TimeoutException, InterruptedException, ExecutionException  {
        // 记录最后一次超时异常
        TimeoutException timeoutException = null;
        while (true) {
            // 获取当前时间（纳秒）
            long nowNs = nanoseconds();
            // 检查是否已超时
            if (deadlineNs <= nowNs) {
                throw (timeoutException == null) ? new TimeoutException() : timeoutException;
            }
            // 计算剩余等待时间
            long deltaNs = deadlineNs - nowNs;
            try {
                // 尝试在剩余时间内获取Future的结果
                return future.get(deltaNs, TimeUnit.NANOSECONDS);
            } catch (TimeoutException t) {
                // 记录超时异常，继续尝试
                timeoutException = t;
            }
        }
    }
}
