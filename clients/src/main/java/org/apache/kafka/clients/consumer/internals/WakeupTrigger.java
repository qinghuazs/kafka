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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 确保阻塞的API可以被consumer.wakeup()方法唤醒。
 * 该类实现了Kafka消费者的唤醒机制，用于管理和控制阻塞操作的生命周期。
 * 主要用于以下场景：
 * 1. 消费者在长时间阻塞操作（如poll）时需要被中断
 * 2. 多线程环境下安全地中断消费者操作
 * 3. 处理不同类型任务（Future任务、Fetch操作等）的唤醒
 */
public class WakeupTrigger {
    // 使用原子引用存储当前待处理的任务，确保线程安全
    // 可以是ActiveFuture、WakeupFuture、FetchAction、ShareFetchAction或DisabledWakeups类型
    private final AtomicReference<Wakeupable> pendingTask = new AtomicReference<>(null);

    /**
     * 唤醒待处理的任务。
     * 该方法的主要作用是中断当前正在执行的阻塞操作，具体行为如下：
     * 1. 如果没有待处理任务，创建WakeupFuture标记已调用唤醒
     * 2. 如果存在活跃的Future任务，用WakeupException异常完成该任务
     * 3. 如果是Fetch相关任务，调用其wakeup方法进行唤醒
     * 4. 如果任务已被唤醒，则不做任何操作
     * 
     * 该方法在多线程环境下是线程安全的，通过AtomicReference保证原子性
     */
    public void wakeup() {
        // 原子地更新pendingTask的值，确保线程安全
        pendingTask.getAndUpdate(task -> {
            // 如果当前没有待处理任务，创建WakeupFuture标记已调用唤醒
            if (task == null) {
                return new WakeupFuture();
            } 
            // 如果是活跃的Future任务，尝试用WakeupException完成该任务
            else if (task instanceof ActiveFuture) {
                ActiveFuture active = (ActiveFuture) task;
                // 尝试以WakeupException异常完成Future，返回是否成功触发
                boolean wasTriggered = active.future().completeExceptionally(new WakeupException());

                // 如果Future已经完成，completeExceptionally调用会返回false
                // 此时需要返回新的WakeupFuture，确保下一次setActiveTask能感知到唤醒操作
                // 如果成功触发了异常（返回true），则返回null清除pendingTask
                return wasTriggered ? null : new WakeupFuture();
            } 
            // 如果是Fetch操作任务，调用其缓冲区的wakeup方法
            else if (task instanceof FetchAction) {
                FetchAction fetchAction = (FetchAction) task;
                fetchAction.fetchBuffer().wakeup();
                // 返回WakeupFuture标记已调用唤醒
                return new WakeupFuture();
            } 
            // 如果是共享Fetch操作任务，处理方式与FetchAction相同
            else if (task instanceof ShareFetchAction) {
                ShareFetchAction shareFetchAction = (ShareFetchAction) task;
                shareFetchAction.fetchBuffer().wakeup();
                // 返回WakeupFuture标记已调用唤醒
                return new WakeupFuture();
            } 
            // 对于其他类型的任务（如DisabledWakeups），保持原状
            else {
                return task;
            }
        });
    }

    /**
     * 设置一个活跃任务。
     * 该方法用于注册一个需要被监控的异步任务，具体处理逻辑如下：
     * 1. 如果当前没有待处理任务，将传入的任务包装为ActiveFuture并设置为待处理任务
     * 2. 如果之前已调用过wakeup（即存在WakeupFuture），立即用WakeupException完成当前任务
     * 3. 如果唤醒功能已禁用（DisabledWakeups），保持当前状态
     * 4. 如果已存在活跃任务，抛出KafkaException异常
     * 
     * @param currentTask 要设置为活跃状态的CompletableFuture任务
     * @param <T> Future的结果类型
     * @return 传入的CompletableFuture任务
     * @throws KafkaException 当已存在活跃任务时抛出
     */
    public <T> CompletableFuture<T> setActiveTask(final CompletableFuture<T> currentTask) {
        // 检查入参不能为null
        Objects.requireNonNull(currentTask, "currentTask cannot be null");
        // 原子地更新pendingTask的值
        pendingTask.getAndUpdate(task -> {
            // 如果当前没有待处理任务，将当前任务包装为ActiveFuture
            if (task == null) {
                return new ActiveFuture(currentTask);
            } 
            // 如果存在WakeupFuture（表示之前调用过wakeup），
            // 立即以WakeupException完成当前任务，并清除pendingTask
            else if (task instanceof WakeupFuture) {
                currentTask.completeExceptionally(new WakeupException());
                return null;
            } 
            // 如果唤醒功能已禁用，保持当前状态
            else if (task instanceof DisabledWakeups) {
                return task;
            }
            // 如果已存在其他活跃任务，抛出异常
            // 这种情况通常表示程序逻辑错误，不应该同时存在多个活跃任务
            throw new KafkaException("Last active task is still active");
        });
        return currentTask;
    }

    /**
     * 设置一个Fetch操作任务。
     * 用于注册一个消费者的Fetch操作，该操作可以被唤醒机制打断。
     * 处理逻辑与setActiveTask类似，但专门处理Fetch操作的场景。
     * 
     * @param fetchBuffer Fetch操作的缓冲区
     * @throws IllegalStateException 当已存在活跃任务时抛出
     * @throws WakeupException 当之前已调用wakeup时抛出
     */
    public void setFetchAction(final FetchBuffer fetchBuffer) {
        // 用于标记是否需要抛出WakeupException的原子布尔值
        final AtomicBoolean throwWakeupException = new AtomicBoolean(false);
        // 原子地更新pendingTask的值
        pendingTask.getAndUpdate(task -> {
            // 如果当前没有待处理任务，创建新的FetchAction
            if (task == null) {
                return new FetchAction(fetchBuffer);
            } 
            // 如果存在WakeupFuture，标记需要抛出WakeupException
            // 并清除pendingTask
            else if (task instanceof WakeupFuture) {
                throwWakeupException.set(true);
                return null;
            } 
            // 如果唤醒功能已禁用，保持当前状态
            else if (task instanceof DisabledWakeups) {
                return task;
            }
            // 如果已存在其他活跃任务，抛出异常
            throw new IllegalStateException("Last active task is still active");
        });
        // 如果之前存在WakeupFuture，抛出WakeupException
        if (throwWakeupException.get()) {
            throw new WakeupException();
        }
    }

    /**
     * 设置一个共享的Fetch操作任务。
     * 用于注册一个共享的消费者Fetch操作，支持多个消费者共享同一个Fetch缓冲区。
     * 处理逻辑与setFetchAction相同，但使用共享的FetchBuffer。
     * 
     * @param fetchBuffer 共享的Fetch操作缓冲区
     * @throws IllegalStateException 当已存在活跃任务时抛出
     * @throws WakeupException 当之前已调用wakeup时抛出
     */
    public void setShareFetchAction(final ShareFetchBuffer fetchBuffer) {
        final AtomicBoolean throwWakeupException = new AtomicBoolean(false);
        pendingTask.getAndUpdate(task -> {
            if (task == null) {
                return new ShareFetchAction(fetchBuffer);
            } else if (task instanceof WakeupFuture) {
                throwWakeupException.set(true);
                return null;
            } else if (task instanceof DisabledWakeups) {
                return task;
            }
            // last active state is still active
            throw new IllegalStateException("Last active task is still active");
        });
        if (throwWakeupException.get()) {
            throw new WakeupException();
        }
    }

    /**
     * 禁用唤醒功能。
     * 设置DisabledWakeups状态，阻止新的唤醒操作和待处理任务的注册。
     * 通常在需要暂时禁用唤醒机制时使用，比如在进行某些不能被中断的操作时。
     */
    public void disableWakeups() {
        pendingTask.set(new DisabledWakeups());
    }

    /**
     * 清除当前的待处理任务。
     * 仅清除ActiveFuture、FetchAction或ShareFetchAction类型的任务，
     * 保留其他类型的任务状态（如WakeupFuture或DisabledWakeups）。
     */
    public void clearTask() {
        // 原子地更新pendingTask的值
        pendingTask.getAndUpdate(task -> {
            // 如果没有待处理任务，返回null
            if (task == null) {
                return null;
            } 
            // 如果是活跃任务、Fetch操作或共享Fetch操作，清除任务（返回null）
            // 这样可以释放这些任务占用的资源
            else if (task instanceof ActiveFuture || task instanceof FetchAction || task instanceof ShareFetchAction) {
                return null;
            }
            // 对于其他类型的任务（如WakeupFuture或DisabledWakeups），保持原状
            return task;
        });
    }

    /**
     * 检查并可能触发唤醒操作。
     * 如果存在WakeupFuture（表示之前调用过wakeup），则抛出WakeupException。
     * 用于在执行某些操作前检查是否应该被唤醒。
     * 
     * @throws WakeupException 当存在待处理的唤醒操作时抛出
     */
    public void maybeTriggerWakeup() {
        // 用于标记是否需要抛出WakeupException的原子布尔值
        final AtomicBoolean throwWakeupException = new AtomicBoolean(false);
        // 原子地更新pendingTask的值
        pendingTask.getAndUpdate(task -> {
            // 如果没有待处理任务，保持为null
            if (task == null) {
                return null;
            } 
            // 如果存在WakeupFuture，标记需要抛出WakeupException
            // 并清除pendingTask
            else if (task instanceof WakeupFuture) {
                throwWakeupException.set(true);
                return null;
            } 
            // 对于其他类型的任务，保持原状
            else {
                return task;
            }
        });
        // 如果之前存在WakeupFuture，抛出WakeupException
        if (throwWakeupException.get()) {
            throw new WakeupException();
        }
    }

    Wakeupable getPendingTask() {
        return pendingTask.get();
    }

    /**
     * 可唤醒接口，作为所有可被唤醒任务类型的标记接口。
     * 包括ActiveFuture、WakeupFuture、FetchAction、ShareFetchAction和DisabledWakeups等实现类。
     */
    interface Wakeupable { }

    /**
     * 禁用唤醒状态类。
     * 用于标记唤醒机制被禁用的状态，阻止唤醒操作和新任务的注册。
     */
    static class DisabledWakeups implements Wakeupable { }

    /**
     * 活跃Future任务类。
     * 包装了一个正在执行的CompletableFuture任务，
     * 使其可以被唤醒机制管理和控制。
     */
    static class ActiveFuture implements Wakeupable {
        private final CompletableFuture<?> future;

        public ActiveFuture(final CompletableFuture<?> future) {
            this.future = future;
        }

        public CompletableFuture<?> future() {
            return future;
        }
    }

    /**
     * 唤醒Future标记类。
     * 用于标记已经调用过wakeup方法的状态，
     * 确保后续的任务注册能感知到之前的唤醒操作。
     */
    static class WakeupFuture implements Wakeupable { }

    /**
     * Fetch操作任务类。
     * 封装了消费者的Fetch操作，使其可以被唤醒机制管理，
     * 包含对FetchBuffer的引用，用于在需要时唤醒Fetch操作。
     */
    static class FetchAction implements Wakeupable {

        private final FetchBuffer fetchBuffer;

        public FetchAction(FetchBuffer fetchBuffer) {
            this.fetchBuffer = fetchBuffer;
        }

        public FetchBuffer fetchBuffer() {
            return fetchBuffer;
        }
    }

    /**
     * 共享Fetch操作任务类。
     * 类似于FetchAction，但使用共享的FetchBuffer，
     * 支持多个消费者共享同一个Fetch缓冲区的场景。
     */
    static class ShareFetchAction implements Wakeupable {

        private final ShareFetchBuffer fetchBuffer;

        public ShareFetchAction(ShareFetchBuffer fetchBuffer) {
            this.fetchBuffer = fetchBuffer;
        }

        public ShareFetchBuffer fetchBuffer() {
            return fetchBuffer;
        }
    }
}
