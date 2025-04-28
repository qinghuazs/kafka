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
package org.apache.kafka.common.network;

import org.apache.kafka.common.errors.AuthenticationException;

/**
 * KafkaChannel的状态类：
 * 该类定义了Kafka通道的各种状态和状态转换机制。每个通道在其生命周期中会经历不同的状态，
 * 状态的变化反映了通道的连接、认证和关闭过程。
 * 
 * <ul>
 *   <li>NOT_CONNECTED: 未连接状态
 *       - 通道创建时的初始状态
 *       - 当socket连接建立时（通过{@link TransportLayer#finishConnect()}），状态会更新
 *       - PLAINTEXT（明文）通道直接转换到READY状态
 *       - 其他类型通道转换到AUTHENTICATE状态
 *       - 在此状态下的连接失败通常表示远程端点不可用，可能是由于端点配置错误</li>
 *   
 *   <li>AUTHENTICATE: 认证状态
 *       - 适用于SSL、SASL_SSL和SASL_PLAINTEXT类型的通道
 *       - 在SSL和SASL握手过程中保持此状态
 *       - 在此状态下的连接断开可能表示SSL或SASL认证失败（broker版本 < 1.0.0）
 *       - 认证成功后转换到READY状态</li>
 *   
 *   <li>READY: 就绪状态
 *       - 表示通道已连接且认证完成
 *       - 可以从此状态转换到EXPIRED、FAILED_SEND或LOCAL_CLOSE状态</li>
 *   
 *   <li>EXPIRED: 过期状态
 *       - 空闲连接超时后转入此状态
 *       - 进入此状态后通道会被关闭</li>
 *   
 *   <li>FAILED_SEND: 发送失败状态
 *       - 当发送操作失败导致通道关闭时，从READY状态转换到此状态</li>
 *   
 *   <li>AUTHENTICATION_FAILED: 认证失败状态
 *       - 当请求的SASL机制在broker上未启用时进入此状态
 *       - 当broker（版本1.0.0及以上）在SASL认证过程中返回错误响应时也会进入此状态
 *       - 可通过{@link #exception()}方法获取broker提供的认证失败原因</li>
 *   
 *   <li>LOCAL_CLOSE: 本地关闭状态
 *       - 当本地主动调用close()方法时进入此状态</li>
 * </ul>
 * 
 * 远程端点关闭通道的处理：
 * 当远程端点关闭通道时，通道状态将保持在断开连接时的状态。
 * 这个状态信息对于诊断连接断开的原因很有帮助。
 * 
 * 典型的状态转换路径：
 * <ul>
 *   <li>PLAINTEXT正常路径: NOT_CONNECTED => READY => LOCAL_CLOSE</li>
 *   <li>SASL/SSL正常路径: NOT_CONNECTED => AUTHENTICATE => READY => LOCAL_CLOSE</li>
 *   <li>Bootstrap服务器配置错误: NOT_CONNECTED（在NOT_CONNECTED状态下断开）</li>
 *   <li>安全配置错误: NOT_CONNECTED => AUTHENTICATE => AUTHENTICATION_FAILED（在AUTHENTICATION_FAILED状态下断开）</li>
 *   <li>旧版broker的安全配置错误: NOT_CONNECTED => AUTHENTICATE（在AUTHENTICATE状态下断开）</li>
 * </ul>
 */
public class ChannelState {
    /**
     * Kafka通道状态枚举
     * 定义了通道在其生命周期中可能处于的所有状态
     */
    public enum State {
        /** 未连接状态：通道创建后的初始状态 */
        NOT_CONNECTED,
        /** 认证状态：进行SSL/SASL认证时的状态 */
        AUTHENTICATE,
        /** 就绪状态：连接建立且认证完成的状态 */
        READY,
        /** 过期状态：空闲超时后的状态 */
        EXPIRED,
        /** 发送失败状态：发送操作失败后的状态 */
        FAILED_SEND,
        /** 认证失败状态：认证过程中出现错误的状态 */
        AUTHENTICATION_FAILED,
        /** 本地关闭状态：本地主动关闭连接的状态 */
        LOCAL_CLOSE
    }

    // AUTHENTICATION_FAILED状态包含自定义异常信息
    // 其他状态都使用预定义的、可重用的ChannelState实例
    /** 预定义的未连接状态实例 */
    public static final ChannelState NOT_CONNECTED = new ChannelState(State.NOT_CONNECTED);
    /** 预定义的认证状态实例 */
    public static final ChannelState AUTHENTICATE = new ChannelState(State.AUTHENTICATE);
    /** 预定义的就绪状态实例 */
    public static final ChannelState READY = new ChannelState(State.READY);
    /** 预定义的过期状态实例 */
    public static final ChannelState EXPIRED = new ChannelState(State.EXPIRED);
    /** 预定义的发送失败状态实例 */
    public static final ChannelState FAILED_SEND = new ChannelState(State.FAILED_SEND);
    /** 预定义的本地关闭状态实例 */
    public static final ChannelState LOCAL_CLOSE = new ChannelState(State.LOCAL_CLOSE);

    /** 当前通道状态 */
    private final State state;
    /** 认证异常信息，主要用于AUTHENTICATION_FAILED状态 */
    private final AuthenticationException exception;
    /** 远程地址信息，用于标识连接的远程端点 */
    private final String remoteAddress;

    /**
     * 创建一个基本的通道状态实例
     * @param state 通道状态枚举值
     */
    public ChannelState(State state) {
        this(state, null, null);
    }

    /**
     * 创建带有远程地址信息的通道状态实例
     * @param state 通道状态枚举值
     * @param remoteAddress 远程端点地址
     */
    public ChannelState(State state, String remoteAddress) {
        this(state, null, remoteAddress);
    }
    
    /**
     * 创建完整的通道状态实例
     * @param state 通道状态枚举值
     * @param exception 认证异常信息（如果有）
     * @param remoteAddress 远程端点地址
     */
    public ChannelState(State state, AuthenticationException exception, String remoteAddress) {
        this.state = state;
        this.exception = exception;
        this.remoteAddress = remoteAddress;
    }

    /**
     * 获取当前通道状态
     * @return 返回当前的状态枚举值
     */
    public State state() {
        return state;
    }

    /**
     * 获取认证异常信息
     * @return 如果是认证失败状态，返回相关的异常信息；否则返回null
     */
    public AuthenticationException exception() {
        return exception;
    }

    /**
     * 获取远程端点地址
     * @return 返回远程端点的地址信息
     */
    public String remoteAddress() {
        return remoteAddress;
    }
}
