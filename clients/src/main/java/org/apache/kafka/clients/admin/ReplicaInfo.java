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
 * 描述Kafka broker上特定副本的信息。
 * 
 * 该类用于表示Kafka分区副本的状态信息，包括：
 * 1. 副本的日志段大小
 * 2. 副本的偏移量延迟（相对于高水位或当前副本的LEO）
 * 3. 是否为未来将替换当前副本的新副本
 * 
 * 应用场景：
 * - 监控副本同步状态
 * - 跟踪副本迁移进度
 * - 评估副本存储使用情况
 */
public class ReplicaInfo {

    /**
     * 副本的日志段总大小（以字节为单位）
     * 用于跟踪副本占用的存储空间
     */
    private final long size;

    /**
     * 副本的偏移量延迟
     * - 对于当前副本：表示相对于分区高水位的延迟
     * - 对于未来副本：表示相对于当前副本LEO的延迟
     */
    private final long offsetLag;

    /**
     * 标识该副本是否为未来副本
     * - true：表示这是一个由AlterReplicaLogDirsRequest创建的新副本，将在未来替换当前副本
     * - false：表示这是当前活跃的副本
     */
    private final boolean isFuture;

    /**
     * 创建ReplicaInfo实例
     * 
     * @param size 副本的日志段总大小（字节）
     * @param offsetLag 副本的偏移量延迟
     * @param isFuture 是否为未来副本
     */
    public ReplicaInfo(long size, long offsetLag, boolean isFuture) {
        // 初始化副本信息的各个属性
        this.size = size;
        this.offsetLag = offsetLag;
        this.isFuture = isFuture;
    }

    /**
     * 获取副本的日志段总大小（以字节为单位）
     * 用于评估副本的存储使用情况和容量规划
     * 
     * @return 副本的日志段总大小（字节）
     */
    public long size() {
        return size;
    }

    /**
     * 获取副本的偏移量延迟
     * 
     * 对于当前副本：返回日志末端偏移量（LEO）相对于分区高水位的延迟
     * 对于未来副本：返回相对于当前副本LEO的延迟
     * 
     * 该指标用于：
     * 1. 监控副本同步状态
     * 2. 评估副本是否落后
     * 3. 帮助进行副本管理决策
     * 
     * @return 副本的偏移量延迟值
     */
    public long offsetLag() {
        return offsetLag;
    }

    /**
     * 判断该副本是否为未来副本
     * 
     * 未来副本是通过AlterReplicaLogDirsRequest请求创建的新副本
     * 它将在适当的时候替换当前的活跃副本
     * 
     * 应用场景：
     * - 副本迁移过程中的状态判断
     * - 副本管理操作的条件检查
     * 
     * @return true表示这是一个将在未来替换当前副本的新副本，false表示这是当前活跃的副本
     */
    public boolean isFuture() {
        return isFuture;
    }

    @Override
    public String toString() {
        return "ReplicaInfo(" +
                "size=" + size +
                ", offsetLag=" + offsetLag +
                ", isFuture=" + isFuture +
                ')';
    }
}
