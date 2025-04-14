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
package org.apache.kafka.clients.producer.internals;


import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 事务性请求结果的封装类
 * 用于管理Kafka生产者的事务性操作的执行状态、完成情况和错误处理
 * 通过CountDownLatch实现异步等待机制，支持超时和中断处理
 */
public final class TransactionalRequestResult {
    // 用于实现异步等待的闭锁，初始计数为1
    private final CountDownLatch latch;
    // 存储执行过程中发生的运行时异常，使用volatile保证多线程可见性
    private volatile RuntimeException error = null;
    // 当前正在执行的事务操作的描述信息
    private final String operation;
    // 标记请求是否已经被确认，使用volatile保证多线程可见性
    private volatile boolean isAcked = false;

    /**
     * 创建一个新的事务请求结果对象
     * @param operation 事务操作的描述信息
     */
    public TransactionalRequestResult(String operation) {
        // 调用私有构造函数，创建计数为1的CountDownLatch
        this(new CountDownLatch(1), operation);
    }

    /**
     * 私有构造函数，用于创建事务请求结果对象
     * @param latch 用于同步的CountDownLatch对象
     * @param operation 事务操作的描述信息
     */
    private TransactionalRequestResult(CountDownLatch latch, String operation) {
        this.latch = latch;
        this.operation = operation;
    }

    /**
     * 标记事务请求失败
     * @param error 导致失败的运行时异常
     */
    public void fail(RuntimeException error) {
        // 记录错误信息
        this.error = error;
        // 释放闭锁，允许等待的线程继续执行
        this.latch.countDown();
    }

    /**
     * 标记事务请求完成
     * 不带错误的正常完成处理
     */
    public void done() {
        // 释放闭锁，允许等待的线程继续执行
        this.latch.countDown();
    }

    /**
     * 等待事务请求完成
     * 使用最大超时时间进行等待
     */
    public void await() {
        // 调用带超时参数的await方法，使用最大长整型值作为超时时间
        this.await(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /**
     * 等待事务请求完成，支持超时机制
     * @param timeout 超时时间值
     * @param unit 超时时间单位
     * @throws TimeoutException 如果等待超时
     * @throws InterruptException 如果等待过程被中断
     * @throws RuntimeException 如果事务执行过程中发生错误
     */
    public void await(long timeout, TimeUnit unit) {
        try {
            // 等待闭锁计数归零，带有超时机制
            boolean success = latch.await(timeout, unit);
            if (!success) {
                // 超时未完成，抛出超时异常
                throw new TimeoutException("Timeout expired after " + unit.toMillis(timeout) +
                    "ms while awaiting " + operation);
            }

            // 标记请求已被确认
            isAcked = true;
            // 如果存在错误，抛出保存的异常
            if (error != null) {
                throw error;
            }
        } catch (InterruptedException e) {
            // 等待过程被中断，包装为InterruptException并抛出
            throw new InterruptException("Received interrupt while awaiting " + operation, e);
        }
    }

    /**
     * 获取事务执行过程中的错误信息
     * @return 如果有错误返回RuntimeException，否则返回null
     */
    public RuntimeException error() {
        return error;
    }

    /**
     * 检查事务是否成功完成
     * @return 如果事务完成且没有错误返回true，否则返回false
     */
    public boolean isSuccessful() {
        // 检查是否完成且没有错误发生
        return isCompleted() && error == null;
    }

    /**
     * 检查事务是否已完成（不论成功还是失败）
     * @return 如果事务已完成返回true，否则返回false
     */
    public boolean isCompleted() {
        // 通过检查闭锁计数是否为0判断是否完成
        return latch.getCount() == 0L;
    }

    /**
     * 检查事务请求是否已被确认
     * @return 如果请求已被确认返回true，否则返回false
     */
    public boolean isAcked() {
        return isAcked;
    }

}
