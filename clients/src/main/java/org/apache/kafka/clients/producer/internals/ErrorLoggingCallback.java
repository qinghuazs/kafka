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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Kafka生产者的错误日志回调实现类。
 * 当消息发送失败时，该回调会记录详细的错误信息，包括主题名称、消息的键值对等信息。
 */
public class ErrorLoggingCallback implements Callback {
    /** 用于记录错误日志的Logger实例 */
    private static final Logger log = LoggerFactory.getLogger(ErrorLoggingCallback.class);
    /** 消息发送的目标主题 */
    private final String topic;
    /** 消息的键（可以为null） */
    private final byte[] key;
    /** 消息值的长度，-1表示值为null */
    private final int valueLength;
    /** 是否以字符串形式记录消息的键值对 */
    private final boolean logAsString;
    /** 消息的值（仅当logAsString为true时才保存） */
    private byte[] value;

    /**
     * 创建一个新的错误日志回调实例
     * 
     * @param topic 目标主题名称
     * @param key 消息的键（可以为null）
     * @param value 消息的值（可以为null）
     * @param logAsString 如果为true，则以UTF-8字符串形式记录键值对；如果为false，则只记录字节长度
     */
    public ErrorLoggingCallback(String topic, byte[] key, byte[] value, boolean logAsString) {
        this.topic = topic;
        this.key = key;

        // 只有当需要以字符串形式记录时才保存value引用
        if (logAsString) {
            this.value = value;
        }

        // 记录value的长度，如果value为null则设为-1
        this.valueLength = value == null ? -1 : value.length;
        this.logAsString = logAsString;
    }

    /**
     * 当消息发送完成时的回调方法
     * 如果发送过程中发生错误（e不为null），则记录错误信息
     * 
     * @param metadata 消息的元数据信息（发送成功时不为null）
     * @param e 发送过程中的异常（如果发送成功则为null）
     */
    public void onCompletion(RecordMetadata metadata, Exception e) {
        if (e != null) {
            // 根据logAsString的设置决定如何格式化key
            String keyString = (key == null) ? "null" :
                    logAsString ? new String(key, StandardCharsets.UTF_8) : key.length + " bytes";
            // 根据logAsString的设置决定如何格式化value
            String valueString = (valueLength == -1) ? "null" :
                    logAsString ? new String(value, StandardCharsets.UTF_8) : valueLength + " bytes";
            // 记录错误信息，包括主题、键、值和异常详情
            log.error("Error when sending message to topic {} with key: {}, value: {} with error:",
                    topic, keyString, valueString, e);
        }
    }
}
