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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * {@code IdempotentCloser} 封装了一些基本逻辑，以确保给定的资源只被关闭一次。
 * 底层机制通过 {@link AtomicBoolean#compareAndSet(boolean, boolean)} 来确保关闭操作只发生一次，并且是线程安全的。
 * 用户可以提供回调函数（通过可选的 {@link Runnable}），分别用于处理首次关闭和后续关闭的情况。
 * 
 * 应用场景：
 * 1. 资源管理：用于管理需要安全关闭的资源，如文件句柄、网络连接、数据库连接等
 * 2. 并发环境：在多线程环境下确保资源只被关闭一次
 * 3. 状态监控：通过回调机制实现资源关闭时的日志记录、清理操作等
 *
 * <p/>
 *
 * 示例代码：
 *
 * <pre>
 *
 * public class MyDataFile implements Closeable {
 *
 *     private final IdempotentCloser closer = new IdempotentCloser();
 *
 *     private final File file;
 *
 *     . . .
 *
 *     public boolean write() {
 *         closer.assertOpen(() -> String.format("Data file %s already closed!", file));
 *         writeToFile();
 *     }
 *
 *     public boolean isClosed() {
 *         return closer.isClosed();
 *     }
 *
 *     &#064;Override
 *     public void close() {
 *         Runnable onInitialClose = () -> {
 *             cleanUpFile(file);
 *             log.debug("Data file {} closed", file);
 *         };
 *         Runnable onSubsequentClose = () -> {
 *             log.warn("Data file {} already closed!", file);
 *         };
 *         closer.close(onInitialClose, onSubsequentClose);
 *     }
 * }
 * </pre>
 */
public class IdempotentCloser implements AutoCloseable {

    /**
     * 使用AtomicBoolean来跟踪资源的关闭状态
     * - true: 资源已关闭
     * - false: 资源未关闭
     * 使用AtomicBoolean而不是普通boolean的原因是需要确保在并发环境下的线程安全性
     */
    private final AtomicBoolean isClosed;

    /**
     * 创建一个未关闭状态的 {@code IdempotentCloser} 实例
     */
    public IdempotentCloser() {
        this(false);
    }

    /**
     * 创建一个具有指定初始状态的 {@code IdempotentCloser} 实例
     * 这个构造函数主要用于特殊场景，比如：
     * 1. 资源已经被其他方式关闭，需要创建一个已关闭状态的实例
     * 2. 在资源恢复场景中，需要根据持久化的状态创建实例
     *
     * @param isClosed 初始状态值，true表示已关闭，false表示未关闭
     */
    public IdempotentCloser(boolean isClosed) {
        this.isClosed = new AtomicBoolean(isClosed);
    }

    /**
     * 断言检查当前资源是否处于打开状态
     * 实现细节：
     * 1. 通过Supplier延迟构造错误消息，只有在抛出异常时才会生成消息，提高性能
     * 2. 使用AtomicBoolean的get方法检查状态，确保线程安全
     * 3. 如果资源已关闭，抛出IllegalStateException异常
     *
     * @param message 异常消息的提供者，使用Supplier实现延迟计算
     * @throws IllegalStateException 当资源已经关闭时抛出此异常
     */
    public void assertOpen(Supplier<String> message) {
        if (isClosed.get())
            throw new IllegalStateException(message.get());
    }

    /**
     * 断言检查当前资源是否处于打开状态
     * 这是{@link #assertOpen(Supplier)}的简化版本，直接接受错误消息字符串
     * 实现细节：
     * 1. 直接使用提供的消息字符串，适用于简单的错误提示场景
     * 2. 使用AtomicBoolean的get方法检查状态，确保线程安全
     * 3. 如果资源已关闭，抛出IllegalStateException异常
     *
     * @param message 用于异常的错误消息
     * @throws IllegalStateException 当资源已经关闭时抛出此异常
     */
    public void assertOpen(String message) {
        if (isClosed.get())
            throw new IllegalStateException(message);
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    /**
     * 以线程安全的方式关闭资源
     * 这是{@link #close(Runnable, Runnable)}的简化版本，不提供任何回调函数
     * 
     * 实现细节：
     * 1. 调用完整版本的close方法，传入null作为回调函数
     * 2. 关闭操作完成后，{@link #isClosed()}将返回true
     * 3. 关闭后调用{@link #assertOpen(String)}或{@link #assertOpen(Supplier)}将抛出异常
     *
     * @throws IllegalStateException 当资源已关闭且调用assertOpen方法时抛出
     */
    @Override
    public void close() {
        close(null, null);
    }

    /**
     * 以线程安全的方式关闭资源，并在首次关闭时执行指定的回调函数
     * 这是{@link #close(Runnable, Runnable)}的简化版本，只提供首次关闭的回调
     * 
     * 实现细节：
     * 1. 调用完整版本的close方法，将onInitialClose作为首次关闭回调
     * 2. 不提供后续关闭的回调（传入null）
     * 3. 即使回调执行过程中抛出异常，资源也会被标记为已关闭
     *
     * @param onInitialClose 资源首次关闭时执行的回调函数，可以为null
     *                      常用于执行资源清理、日志记录等操作
     * @throws IllegalStateException 当资源已关闭且调用assertOpen方法时抛出
     */
    public void close(final Runnable onInitialClose) {
        close(onInitialClose, null);
    }

    /**
     * 以线程安全的方式关闭资源，支持首次关闭和后续关闭的不同处理逻辑
     * 
     * 实现细节：
     * 1. 使用AtomicBoolean的compareAndSet方法确保线程安全
     * 2. 首次关闭时执行onInitialClose回调
     * 3. 后续关闭时执行onSubsequentClose回调
     * 4. 回调执行过程中的异常不会影响资源的关闭状态
     *
     * 设计考虑：
     * 1. 线程安全：使用CAS操作确保在并发环境下的正确性
     * 2. 异常处理：回调异常不影响状态，避免资源状态不一致
     * 3. 灵活性：支持不同场景下的自定义处理逻辑
     *
     * @param onInitialClose    资源首次关闭时执行的回调函数，可以为null
     *                          用于执行必要的清理操作，如关闭文件、释放连接等
     * @param onSubsequentClose 资源重复关闭时执行的回调函数，可以为null
     *                          用于处理重复关闭的情况，如记录警告日志等
     * @throws IllegalStateException 当资源已关闭且调用assertOpen方法时抛出
     */
    public void close(final Runnable onInitialClose, final Runnable onSubsequentClose) {
        if (isClosed.compareAndSet(false, true)) {
            if (onInitialClose != null)
                onInitialClose.run();
        } else {
            if (onSubsequentClose != null)
                onSubsequentClose.run();
        }
    }

    @Override
    public String toString() {
        return "IdempotentCloser{" +
                "isClosed=" + isClosed +
                '}';
    }
}