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

package org.apache.kafka.clients.admin;

/**
 * 标识端点类型，根据KIP-919规范定义。
 * 用于区分Kafka集群中不同类型的网络端点，包括代理节点和控制器节点。
 */
public enum EndpointType {
    /**
     * 未知类型的端点，用于处理无法识别的端点类型
     * ID值为0
     */
    UNKNOWN((byte) 0),

    /**
     * 代理节点类型的端点，用于处理常规的Kafka代理节点
     * ID值为1
     */
    BROKER((byte) 1),

    /**
     * 控制器节点类型的端点，用于处理Kafka集群的控制器节点
     * ID值为2
     */
    CONTROLLER((byte) 2);

    // 存储端点类型的唯一标识符
    private final byte id;

    /**
     * 构造函数，初始化端点类型的ID
     * 
     * @param id 端点类型的唯一标识符
     */
    EndpointType(byte id) {
        // 将传入的ID值赋给实例变量
        this.id = id;
    }

    /**
     * 获取端点类型的ID值
     * 
     * @return 返回端点类型的唯一标识符
     */
    public byte id() {
        // 返回当前端点类型的ID值
        return id;
    }

    /**
     * 根据ID值获取对应的端点类型
     * 
     * @param id 要查找的端点类型ID
     * @return 返回对应的EndpointType枚举值，如果ID未知则返回UNKNOWN
     */
    public static EndpointType fromId(byte id) {
        // 如果ID匹配BROKER的ID，返回BROKER类型
        if (id == BROKER.id) {
            return BROKER;
        // 如果ID匹配CONTROLLER的ID，返回CONTROLLER类型
        } else if (id == CONTROLLER.id) {
            return CONTROLLER;
        // 如果ID不匹配任何已知类型，返回UNKNOWN
        } else {
            return UNKNOWN;
        }
    }
}
