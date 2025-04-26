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

import java.io.IOException;

/**
 * 该接口用于建模数据发送过程中的状态和操作。
 * 
 * 在Kafka的网络通信中，Send接口是一个核心抽象，它表示一个正在进行的数据发送操作。
 * 主要实现类包括：
 * 1. ByteBufferSend - 用于发送字节缓冲区数据
 * 2. NetworkSend - 网络发送的封装，包含目标节点信息
 * 3. RecordsSend - 用于发送消息记录
 */
public interface Send {

    /**
     * 检查当前发送操作是否已完成
     * 
     * 实现说明：
     * 1. 当所有数据都已写入且没有待处理的写操作时返回true
     * 2. 如果还有数据未写入或者有待处理的写操作则返回false
     * 
     * @return 如果发送完成返回true，否则返回false
     */
    boolean completed();

    /**
     * 将当前发送操作中尚未写入的数据写入到指定的通道中
     * 
     * 实现说明：
     * 1. 该方法可能需要多次调用才能完成整个数据的写入
     * 2. 每次调用都会尝试写入尽可能多的数据
     * 3. 写入的字节数取决于通道的可用空间和当前剩余待写入的数据量
     * 
     * @param channel 要写入数据的目标通道，通常是一个网络Socket通道
     * @return 本次调用实际写入的字节数
     * @throws IOException 如果写入过程中发生IO错误，例如连接断开、通道关闭等
     */
    long writeTo(TransferableChannel channel) throws IOException;

    /**
     * 获取要发送的数据的总大小
     * 
     * 实现说明：
     * 1. 返回此发送操作需要传输的总字节数
     * 2. 这个值在发送过程中保持不变
     * 3. 可用于进度跟踪和缓冲区大小规划
     * 
     * @return 要发送的数据的总字节数
     */
    long size();

}
