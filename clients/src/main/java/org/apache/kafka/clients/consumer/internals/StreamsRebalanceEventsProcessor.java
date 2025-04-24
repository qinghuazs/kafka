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

import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksRevokedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksRevokedCallbackNeededEvent;
import org.apache.kafka.common.KafkaException;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 处理来自Streams重平衡协议的事件。
 * <p>
 * Streams重平衡处理器从异步消费者的后台线程（具体来说是Streams成员管理器）接收事件并处理它们。
 * 例如，这些事件包括请求调用任务分配和任务撤销的回调。
 * 事件处理的结果会被传回后台线程。
 * 
 * 应用场景：
 * 1. 在Kafka Streams应用程序中，当消费者组成员发生变化时（如新成员加入或现有成员离开）
 * 2. 当需要重新分配流处理任务以确保工作负载均衡时
 * 3. 在系统恢复或错误处理过程中，需要重新分配或撤销任务时
 */
public class StreamsRebalanceEventsProcessor {

    /**
     * 用于存储待处理的回调请求事件的阻塞队列
     * 这个队列接收来自后台线程的各种事件（如任务分配、撤销等）
     */
    private final BlockingQueue<BackgroundEvent> onCallbackRequests = new LinkedBlockingQueue<>();

    /**
     * 应用事件处理器，负责将回调结果发送回后台线程
     * 这个处理器在处理完事件后，会通知后台线程处理的结果
     */
    private ApplicationEventHandler applicationEventHandler = null;

    /**
     * Streams组重平衡回调接口，包含任务分配、撤销等回调方法
     * 这个接口定义了在重平衡过程中需要执行的各种回调操作
     */
    private final StreamsGroupRebalanceCallbacks rebalanceCallbacks;

    /**
     * 存储Streams重平衡相关的数据，如任务分配信息等
     * 用于维护重平衡过程中的状态和数据
     */
    private final StreamsRebalanceData streamsRebalanceData;

    /**
     * 构造Streams重平衡处理器
     *
     * @param streamsRebalanceData 重平衡数据对象，包含任务分配等信息
     * @param rebalanceCallbacks 重平衡回调接口，处理任务分配和撤销的回调
     */
    public StreamsRebalanceEventsProcessor(StreamsRebalanceData streamsRebalanceData,
                                           StreamsGroupRebalanceCallbacks rebalanceCallbacks) {
        this.streamsRebalanceData = streamsRebalanceData;
        this.rebalanceCallbacks = rebalanceCallbacks;
    }

    /**
     * 请求调用任务分配回调
     * 
     * 当Streams组需要为成员分配新的任务时调用此方法。这通常发生在：
     * 1. 消费者组重平衡后需要分配新任务
     * 2. 现有任务需要重新分配时
     * 3. 系统扩展或缩容时的任务重新分配
     *
     * @param assignment 要分配给Streams组成员的任务集合
     * @return 一个CompletableFuture，当回调被调用完成时，这个Future会完成
     */
    public CompletableFuture<Void> requestOnTasksAssignedCallbackInvocation(final StreamsRebalanceData.Assignment assignment) {
        // 创建一个新的任务分配回调事件，包含要分配的任务信息
        final StreamsOnTasksAssignedCallbackNeededEvent onTasksAssignedCallbackNeededEvent = new StreamsOnTasksAssignedCallbackNeededEvent(assignment);
        // 将事件添加到回调请求队列中等待处理
        onCallbackRequests.add(onTasksAssignedCallbackNeededEvent);
        // 返回事件的Future，用于跟踪回调的完成状态
        return onTasksAssignedCallbackNeededEvent.future();
    }

    /**
     * 请求调用任务撤销回调
     * 
     * 在以下情况下会调用此方法：
     * 1. 需要从Streams组成员撤销任务时（如重平衡前的准备）
     * 2. 成员离开组时需要撤销其任务
     * 3. 任务重新分配前的清理工作
     *
     * @param activeTasksToRevoke 要从Streams组成员撤销的活跃任务集合
     * @return 一个CompletableFuture，当回调被调用完成时，这个Future会完成
     */
    public CompletableFuture<Void> requestOnTasksRevokedCallbackInvocation(final Set<StreamsRebalanceData.TaskId> activeTasksToRevoke) {
        // 创建一个新的任务撤销回调事件，包含要撤销的任务ID集合
        final StreamsOnTasksRevokedCallbackNeededEvent onTasksRevokedCallbackNeededEvent = new StreamsOnTasksRevokedCallbackNeededEvent(activeTasksToRevoke);
        // 将事件添加到回调请求队列中等待处理
        onCallbackRequests.add(onTasksRevokedCallbackNeededEvent);
        // 返回事件的Future，用于跟踪回调的完成状态
        return onTasksRevokedCallbackNeededEvent.future();
    }

    /**
     * 请求调用所有任务丢失的回调
     * 
     * 在以下场景中使用：
     * 1. 发生严重错误导致所有任务丢失时
     * 2. 消费者组成员与协调器失去连接时
     * 3. 需要重置所有任务状态时
     *
     * @return 一个CompletableFuture，当回调被调用完成时，这个Future会完成
     */
    public CompletableFuture<Void> requestOnAllTasksLostCallbackInvocation() {
        // 创建一个新的所有任务丢失回调事件
        final StreamsOnAllTasksLostCallbackNeededEvent onAllTasksLostCallbackNeededEvent = new StreamsOnAllTasksLostCallbackNeededEvent();
        // 将事件添加到回调请求队列中等待处理
        onCallbackRequests.add(onAllTasksLostCallbackNeededEvent);
        // 返回事件的Future，用于跟踪回调的完成状态
        return onAllTasksLostCallbackNeededEvent.future();
    }

    /**
     * 设置应用程序事件处理器
     * 
     * 应用程序处理器负责：
     * 1. 将回调的执行结果发送回后台线程
     * 2. 处理回调过程中产生的事件
     * 3. 确保事件处理的可靠性和顺序性
     *
     * @param applicationEventHandler 应用程序事件处理器实例
     */
    public void setApplicationEventHandler(final ApplicationEventHandler applicationEventHandler) {
        // 设置应用程序事件处理器，用于处理回调结果
        this.applicationEventHandler = applicationEventHandler;
    }

    /**
     * 处理后台事件
     * 
     * 根据事件类型执行相应的处理逻辑：
     * 1. 错误事件：直接抛出异常
     * 2. 任务撤销事件：处理任务撤销回调
     * 3. 任务分配事件：处理任务分配回调
     * 4. 任务丢失事件：处理所有任务丢失回调
     * 
     * @param event 要处理的后台事件
     */
    private void process(final BackgroundEvent event) {
        switch (event.type()) {
            case ERROR:
                // 如果是错误事件，直接抛出异常
                throw ((ErrorEvent) event).error();

            case STREAMS_ON_TASKS_REVOKED_CALLBACK_NEEDED:
                // 处理任务撤销回调事件
                processStreamsOnTasksRevokedCallbackNeededEvent((StreamsOnTasksRevokedCallbackNeededEvent) event);
                break;

            case STREAMS_ON_TASKS_ASSIGNED_CALLBACK_NEEDED:
                // 处理任务分配回调事件
                processStreamsOnTasksAssignedCallbackNeededEvent((StreamsOnTasksAssignedCallbackNeededEvent) event);
                break;

            case STREAMS_ON_ALL_TASKS_LOST_CALLBACK_NEEDED:
                // 处理所有任务丢失回调事件
                processStreamsOnAllTasksLostCallbackNeededEvent((StreamsOnAllTasksLostCallbackNeededEvent) event);
                break;

            default:
                // 对于未知的事件类型，抛出异常
                throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");
        }
    }

    /**
     * 处理任务撤销回调需求事件
     * 
     * 执行流程：
     * 1. 调用任务撤销回调
     * 2. 将完成事件添加到应用程序事件处理器
     * 3. 检查是否有错误发生
     * 
     * @param event 任务撤销回调需求事件
     */
    private void processStreamsOnTasksRevokedCallbackNeededEvent(final StreamsOnTasksRevokedCallbackNeededEvent event) {
        // 调用任务撤销回调并获取完成事件
        StreamsOnTasksRevokedCallbackCompletedEvent invokedEvent = invokeOnTasksRevokedCallback(event.activeTasksToRevoke(), event.future());
        // 将完成事件添加到应用程序事件处理器
        applicationEventHandler.add(invokedEvent);
        // 如果回调执行过程中发生错误，抛出异常
        if (invokedEvent.error().isPresent()) {
            throw invokedEvent.error().get();
        }
    }

    /**
     * 处理任务分配回调需求事件
     * 
     * 执行流程：
     * 1. 调用任务分配回调
     * 2. 将完成事件添加到应用程序事件处理器
     * 3. 检查是否有错误发生
     * 
     * @param event 任务分配回调需求事件
     */
    private void processStreamsOnTasksAssignedCallbackNeededEvent(final StreamsOnTasksAssignedCallbackNeededEvent event) {
        // 调用任务分配回调并获取完成事件
        StreamsOnTasksAssignedCallbackCompletedEvent invokedEvent = invokeOnTasksAssignedCallback(event.assignment(), event.future());
        // 将完成事件添加到应用程序事件处理器
        applicationEventHandler.add(invokedEvent);
        // 如果回调执行过程中发生错误，抛出异常
        if (invokedEvent.error().isPresent()) {
            throw invokedEvent.error().get();
        }
    }

    /**
     * 处理所有任务丢失回调需求事件
     * 
     * 执行流程：
     * 1. 调用所有任务丢失回调
     * 2. 将完成事件添加到应用程序事件处理器
     * 3. 检查是否有错误发生
     * 
     * @param event 所有任务丢失回调需求事件
     */
    private void processStreamsOnAllTasksLostCallbackNeededEvent(final StreamsOnAllTasksLostCallbackNeededEvent event) {
        // 调用所有任务丢失回调并获取完成事件
        StreamsOnAllTasksLostCallbackCompletedEvent invokedEvent = invokeOnAllTasksLostCallback(event.future());
        // 将完成事件添加到应用程序事件处理器
        applicationEventHandler.add(invokedEvent);
        // 如果回调执行过程中发生错误，抛出异常
        if (invokedEvent.error().isPresent()) {
            throw invokedEvent.error().get();
        }
    }

    /**
     * 执行任务撤销回调
     * 
     * 执行流程：
     * 1. 调用重平衡回调接口的任务撤销方法
     * 2. 处理可能发生的异常
     * 3. 创建并返回完成事件
     * 
     * @param activeTasksToRevoke 要撤销的活跃任务集合
     * @param future 用于跟踪回调完成状态的Future
     * @return 任务撤销回调完成事件
     */
    private StreamsOnTasksRevokedCallbackCompletedEvent invokeOnTasksRevokedCallback(final Set<StreamsRebalanceData.TaskId> activeTasksToRevoke,
                                                                                     final CompletableFuture<Void> future) {
        final Optional<KafkaException> error;
        // 调用重平衡回调接口的任务撤销方法
        final Optional<Exception> exceptionFromCallback = rebalanceCallbacks.onTasksRevoked(activeTasksToRevoke);
        if (exceptionFromCallback.isPresent()) {
            // 如果回调抛出异常，将其包装为KafkaException
            error = Optional.of(ConsumerUtils.maybeWrapAsKafkaException(exceptionFromCallback.get(), "Task revocation callback throws an error"));
        } else {
            // 如果回调成功执行，不设置错误
            error = Optional.empty();
        }
        // 创建并返回完成事件
        return new StreamsOnTasksRevokedCallbackCompletedEvent(future, error);
    }

    /**
     * 执行任务分配回调
     * 
     * 执行流程：
     * 1. 调用重平衡回调接口的任务分配方法
     * 2. 处理可能发生的异常
     * 3. 如果成功，更新已协调的任务分配信息
     * 4. 创建并返回完成事件
     * 
     * @param assignment 要分配的任务
     * @param future 用于跟踪回调完成状态的Future
     * @return 任务分配回调完成事件
     */
    private StreamsOnTasksAssignedCallbackCompletedEvent invokeOnTasksAssignedCallback(final StreamsRebalanceData.Assignment assignment,
                                                                                       final CompletableFuture<Void> future) {
        final Optional<KafkaException> error;
        // 调用重平衡回调接口的任务分配方法
        final Optional<Exception> exceptionFromCallback = rebalanceCallbacks.onTasksAssigned(assignment);
        if (exceptionFromCallback.isPresent()) {
            // 如果回调抛出异常，将其包装为KafkaException
            error = Optional.of(ConsumerUtils.maybeWrapAsKafkaException(exceptionFromCallback.get(), "Task assignment callback throws an error"));
        } else {
            // 如果回调成功执行
            error = Optional.empty();
            // 更新已协调的任务分配信息
            streamsRebalanceData.setReconciledAssignment(assignment);
        }
        // 创建并返回完成事件
        return new StreamsOnTasksAssignedCallbackCompletedEvent(future, error);
    }

    /**
     * 执行所有任务丢失回调
     * 
     * 执行流程：
     * 1. 调用重平衡回调接口的所有任务丢失方法
     * 2. 处理可能发生的异常
     * 3. 如果成功，清空已协调的任务分配信息
     * 4. 创建并返回完成事件
     * 
     * @param future 用于跟踪回调完成状态的Future
     * @return 所有任务丢失回调完成事件
     */
    private StreamsOnAllTasksLostCallbackCompletedEvent invokeOnAllTasksLostCallback(final CompletableFuture<Void> future) {
        final Optional<KafkaException> error;
        // 调用重平衡回调接口的所有任务丢失方法
        final Optional<Exception> exceptionFromCallback = rebalanceCallbacks.onAllTasksLost();
        if (exceptionFromCallback.isPresent()) {
            // 如果回调抛出异常，将其包装为KafkaException
            error = Optional.of(ConsumerUtils.maybeWrapAsKafkaException(exceptionFromCallback.get(), "All tasks lost callback throws an error"));
        } else {
            // 如果回调成功执行
            error = Optional.empty();
            // 清空已协调的任务分配信息
            streamsRebalanceData.setReconciledAssignment(StreamsRebalanceData.Assignment.EMPTY);
        }
        // 创建并返回完成事件
        return new StreamsOnAllTasksLostCallbackCompletedEvent(future, error);
    }

    /**
     * 处理从后台线程接收到的所有事件
     * 
     * 执行流程：
     * 1. 将回调请求队列中的所有事件取出到一个临时列表中
     * 2. 依次处理每个事件
     * 
     * 应用场景：
     * 1. 定期处理积累的事件
     * 2. 在重平衡过程中批量处理事件
     * 3. 系统关闭前处理剩余事件
     */
    public void process() {
        // 创建一个临时列表存储待处理的事件
        LinkedList<BackgroundEvent> events = new LinkedList<>();
        // 将回调请求队列中的所有事件排空到临时列表中
        onCallbackRequests.drainTo(events);
        // 遍历并处理每个事件
        for (BackgroundEvent event : events) {
            process(event);
        }
    }

}
