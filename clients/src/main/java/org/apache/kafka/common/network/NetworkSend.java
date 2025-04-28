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
 * NetworkSend类是Kafka网络层中负责处理网络数据发送的核心组件
 * 该类实现了Send接口，采用装饰器模式对基础的Send实现进行包装
 * 主要用于在网络传输层面处理数据发送，并维护目标节点的标识信息
 */
public class NetworkSend implements Send {
    /**
     * 目标节点的唯一标识符
     * 用于标识网络请求要发送到的目标节点
     */
    private final String destinationId;

    /**
     * 实际执行发送操作的Send实例
     * 通常是ByteBufferSend的实例，负责底层的数据发送实现
     */
    private final Send send;

    /**
     * 构造一个新的NetworkSend实例
     * @param destinationId 目标节点的标识符
     * @param send 实际负责数据发送的Send实例
     * 实现细节：初始化目标节点ID和底层发送器
     */
    public NetworkSend(String destinationId, Send send) {
        this.destinationId = destinationId;
        this.send = send;
    }

    /**
     * 获取目标节点的标识符
     * @return 目标节点ID
     */
    public String destinationId() {
        return destinationId;
    }

    /**
     * 获取底层的Send实例
     * @return 实际负责数据发送的Send实例
     */
    public Send send() {
        return send;
    }

    /**
     * 检查数据发送是否已完成
     * @return 如果数据发送完成返回true，否则返回false
     * 实现细节：委托给底层send实例判断完成状态
     */
    @Override
    public boolean completed() {
        return send.completed();
    }

    /**
     * 将数据写入到指定的传输通道
     * @param channel 目标传输通道
     * @return 本次写入的字节数
     * @throws IOException 如果写入过程中发生IO错误
     * 实现细节：委托给底层send实例执行实际的写入操作
     */
    @Override
    public long writeTo(TransferableChannel channel) throws IOException {
        return send.writeTo(channel);
    }

    /**
     * 获取要发送的数据总大小
     * @return 待发送数据的总字节数
     * 实现细节：委托给底层send实例获取数据大小
     */
    @Override
    public long size() {
        return send.size();
    }

}
