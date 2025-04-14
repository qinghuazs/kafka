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
package org.apache.kafka.common;

import java.util.Objects;

/**
 * Kafka节点信息类
 * 该类用于表示Kafka集群中的一个节点的所有相关信息，包括节点ID、主机名、端口号、机架位置等
 * 在整个Kafka集群中，每个Broker都会被表示为一个Node实例
 */
public class Node {

    // 表示空节点的常量，用于表示不存在的节点，ID为-1
    private static final Node NO_NODE = new Node(-1, "", -1);

    // 节点的唯一标识符
    private final int id;
    // 节点ID的字符串表示形式，用于网络客户端代码中的标识
    private final String idString;
    // 节点的主机名或IP地址
    private final String host;
    // 节点监听的端口号
    private final int port;
    // 节点所在的机架标识，用于机架感知功能
    private final String rack;
    // 标识节点是否被隔离（fenced），用于控制节点的可用性
    private final boolean isFenced;

    // 缓存hashCode值，因为在性能敏感的代码部分会频繁调用（如RecordAccumulator.ready）
    private Integer hash;

    /**
     * 创建一个基本的Node实例
     * @param id 节点的唯一标识符
     * @param host 节点的主机名或IP地址
     * @param port 节点的端口号
     */
    public Node(int id, String host, int port) {
        // 调用完整的构造函数，默认rack为null，isFenced为false
        this(id, host, port, null, false);
    }

    /**
     * 创建一个指定机架位置的Node实例
     * @param id 节点的唯一标识符
     * @param host 节点的主机名或IP地址
     * @param port 节点的端口号
     * @param rack 节点所在的机架标识
     */
    public Node(int id, String host, int port, String rack) {
        // 初始化节点的基本信息
        this.id = id;
        this.idString = Integer.toString(id);  // 将数字ID转换为字符串形式
        this.host = host;
        this.port = port;
        this.rack = rack;  // 设置机架位置
        this.isFenced = false;  // 默认节点未被隔离
    }

    /**
     * 创建一个完整的Node实例，包含所有可配置的属性
     * @param id 节点的唯一标识符
     * @param host 节点的主机名或IP地址
     * @param port 节点的端口号
     * @param rack 节点所在的机架标识
     * @param isFenced 节点是否被隔离
     */
    public Node(int id, String host, int port, String rack, boolean isFenced) {
        // 初始化节点的所有属性
        this.id = id;
        this.idString = Integer.toString(id);  // 将数字ID转换为字符串形式
        this.host = host;
        this.port = port;
        this.rack = rack;  // 设置机架位置
        this.isFenced = isFenced;  // 设置节点的隔离状态
    }

    /**
     * 获取表示空节点的常量实例
     * @return 返回一个表示不存在节点的Node实例，其ID为-1
     */
    public static Node noNode() {
        return NO_NODE;  // 返回预定义的空节点常量
    }

    /**
     * 检查当前节点是否为空节点
     * 当节点作为错误响应中的占位符时，可能会是空节点
     * @return 如果节点的host为null或为空，或port小于0，则返回true
     */
    public boolean isEmpty() {
        // 检查节点的关键属性是否有效
        return host == null || host.isEmpty() || port < 0;
    }

    /**
     * The node id of this node
     */
    public int id() {
        return id;
    }

    /**
     * String representation of the node id.
     * Typically the integer id is used to serialize over the wire, the string representation is used as an identifier with NetworkClient code
     */
    public String idString() {
        return idString;
    }

    /**
     * The host name for this node
     */
    public String host() {
        return host;
    }

    /**
     * The port for this node
     */
    public int port() {
        return port;
    }

    /**
     * True if this node has a defined rack
     */
    public boolean hasRack() {
        return rack != null;
    }

    /**
     * The rack for this node
     */
    public String rack() {
        return rack;
    }

    /**
     * Whether if this node is fenced
     */
    public boolean isFenced() {
        return isFenced;
    }

    @Override
    public int hashCode() {
        // 获取缓存的哈希值
        Integer h = this.hash;
        if (h == null) {
            // 如果没有缓存，则计算哈希值
            int result = 31 + ((host == null) ? 0 : host.hashCode());  // 使用host的哈希值作为基础
            result = 31 * result + id;  // 组合节点ID
            result = 31 * result + port;  // 组合端口号
            result = 31 * result + ((rack == null) ? 0 : rack.hashCode());  // 组合机架信息
            result = 31 * result + Objects.hashCode(isFenced);  // 组合隔离状态
            this.hash = result;  // 缓存计算结果
            return result;
        } else {
            return h;  // 返回缓存的哈希值
        }
    }

    @Override
    public boolean equals(Object obj) {
        // 检查是否为同一个对象
        if (this == obj)
            return true;
        // 检查是否为null或类型不匹配
        if (obj == null || getClass() != obj.getClass())
            return false;
        // 转换为Node类型
        Node other = (Node) obj;
        // 比较所有关键属性
        return id == other.id &&  // 比较节点ID
            port == other.port &&  // 比较端口号
            Objects.equals(host, other.host) &&  // 比较主机名
            Objects.equals(rack, other.rack) &&  // 比较机架信息
            Objects.equals(isFenced, other.isFenced);  // 比较隔离状态
    }

    @Override
    public String toString() {
        return host + ":" + port + " (id: " + idString + " rack: " + rack + " isFenced: " + isFenced + ")";
    }

}
