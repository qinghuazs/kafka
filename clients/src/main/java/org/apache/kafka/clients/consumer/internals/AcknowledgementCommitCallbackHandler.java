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

import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.common.TopicIdPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 确认提交回调处理器类
 * 用于处理Kafka消费者的消息确认提交回调，管理回调的执行状态和异常处理
 */
public class AcknowledgementCommitCallbackHandler {

    /**
     * 日志记录器
     * 用于记录回调执行过程中的异常信息
     */
    private static final Logger LOG = LoggerFactory.getLogger(AcknowledgementCommitCallbackHandler.class);
    
    /**
     * 确认提交回调接口
     * 用于执行实际的确认提交操作
     */
    private final AcknowledgementCommitCallback acknowledgementCommitCallback;
    
    /**
     * 回调进入标志
     * 用于标记回调是否正在执行中
     */
    private boolean enteredCallback = false;

    /**
     * 构造函数
     * 初始化确认提交回调处理器
     *
     * @param acknowledgementCommitCallback 确认提交回调接口的实现
     */
    AcknowledgementCommitCallbackHandler(AcknowledgementCommitCallback acknowledgementCommitCallback) {
        // 初始化回调接口
        this.acknowledgementCommitCallback = acknowledgementCommitCallback;
    }

    /**
     * 检查回调是否已进入执行状态
     *
     * @return 如果回调正在执行返回true，否则返回false
     */
    public boolean hasEnteredCallback() {
        // 返回回调进入状态
        return enteredCallback;
    }

    /**
     * 完成回调处理
     * 处理确认提交的完成操作，包括异常处理和状态管理
     *
     * @param acknowledgementsMapList 确认信息映射列表，包含主题分区和对应的确认信息
     */
    void onComplete(List<Map<TopicIdPartition, Acknowledgements>> acknowledgementsMapList) {
        // 创建异常列表，用于收集回调执行过程中的异常
        final ArrayList<Throwable> exceptions = new ArrayList<>();
        
        // 遍历确认信息映射列表，处理每个分区的确认信息
        acknowledgementsMapList.forEach(acknowledgementsMap -> acknowledgementsMap.forEach((partition, acknowledgements) -> {
            // 初始化异常对象
            Exception exception = null;
            // 检查是否存在确认错误码
            if (acknowledgements.getAcknowledgeErrorCode() != null) {
                // 获取错误码对应的异常
                exception = acknowledgements.getAcknowledgeErrorCode().exception();
            }
            
            // 获取确认的偏移量集合
            Set<Long> offsets = acknowledgements.getAcknowledgementsTypeMap().keySet();
            // 创建不可修改的偏移量集合副本
            Set<Long> offsetsCopy = Collections.unmodifiableSet(offsets);
            // 设置回调进入标志
            enteredCallback = true;
            
            try {
                // 执行确认提交回调，传入分区和对应的偏移量集合
                acknowledgementCommitCallback.onComplete(Collections.singletonMap(partition, offsetsCopy), exception);
            } catch (Throwable e) {
                // 记录回调执行过程中的异常
                LOG.error("Exception thrown by acknowledgement commit callback", e);
                // 将异常添加到异常列表
                exceptions.add(e);
            } finally {
                // 重置回调进入标志
                enteredCallback = false;
            }
        }));
        
        // 如果存在异常，抛出第一个异常
        if (!exceptions.isEmpty()) {
            throw ConsumerUtils.maybeWrapAsKafkaException(exceptions.get(0), 
                "Exception thrown by acknowledgement commit callback");
        }
    }
}
