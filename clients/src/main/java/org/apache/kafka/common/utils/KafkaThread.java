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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka线程包装类，用于优雅地设置和管理线程
 * 
 * 该类继承自Java标准库的Thread类，提供了以下增强功能：
 * 1. 统一的线程命名机制
 * 2. 守护线程和非守护线程的快速创建
 * 3. 统一的未捕获异常处理
 * 
 * 应用场景：
 * - 在Kafka中创建各种后台服务线程
 * - 需要统一管理线程生命周期的场景
 * - 需要统一处理线程异常的场景
 */
public class KafkaThread extends Thread {

    /**
     * 日志记录器，用于记录线程相关的日志信息，特别是未捕获的异常
     */
    private static final Logger log = LoggerFactory.getLogger(KafkaThread.class);
    
    /**
     * 创建一个守护线程
     * 
     * @param name 线程名称，用于标识和日志记录
     * @param runnable 线程要执行的任务
     * @return 配置好的KafkaThread实例
     */
    public static KafkaThread daemon(final String name, Runnable runnable) {
        // 创建一个新的守护线程实例，daemon参数设置为true
        return new KafkaThread(name, runnable, true);
    }

    /**
     * 创建一个非守护线程
     * 
     * @param name 线程名称，用于标识和日志记录
     * @param runnable 线程要执行的任务
     * @return 配置好的KafkaThread实例
     */
    public static KafkaThread nonDaemon(final String name, Runnable runnable) {
        // 创建一个新的非守护线程实例，daemon参数设置为false
        return new KafkaThread(name, runnable, false);
    }

    /**
     * 创建一个KafkaThread实例，只指定名称和是否为守护线程
     * 
     * @param name 线程名称
     * @param daemon 是否为守护线程
     */
    @SuppressWarnings("this-escape")
    public KafkaThread(final String name, boolean daemon) {
        // 调用父类构造函数设置线程名称
        super(name);
        // 配置线程的守护状态和异常处理器
        configureThread(name, daemon);
    }

    /**
     * 创建一个KafkaThread实例，指定名称、运行任务和是否为守护线程
     * 
     * @param name 线程名称
     * @param runnable 要执行的任务
     * @param daemon 是否为守护线程
     */
    @SuppressWarnings("this-escape")
    public KafkaThread(final String name, Runnable runnable, boolean daemon) {
        // 调用父类构造函数设置线程的运行任务和名称
        super(runnable, name);
        // 配置线程的守护状态和异常处理器
        configureThread(name, daemon);
    }

    /**
     * 配置线程的属性，包括守护状态和异常处理
     * 
     * @param name 线程名称，用于异常日志记录
     * @param daemon 是否设置为守护线程
     */
    private void configureThread(final String name, boolean daemon) {
        // 设置线程的守护状态
        setDaemon(daemon);
        // 设置未捕获异常处理器，将异常信息记录到日志中
        setUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception in thread '{}':", name, e));
    }

}
