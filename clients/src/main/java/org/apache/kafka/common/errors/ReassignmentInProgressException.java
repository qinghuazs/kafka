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

package org.apache.kafka.common.errors;

/**
 * 分区重分配进行中异常
 * 
 * 当尝试执行某些操作时，如果目标分区正在进行重分配，则会抛出此异常。
 * 
 * 分区重分配场景：
 * 1. 集群负载均衡
 * 2. Broker上下线
 * 3. 机架感知分配调整
 * 4. 手动触发的分区迁移
 * 
 * 影响的操作：
 * - 分区扩容或收缩
 * - 副本重新分配
 * - Leader副本选举
 * - 配置修改
 * 
 * 处理建议：
 * - 等待当前重分配任务完成
 * - 通过监控工具跟踪重分配进度
 * - 必要时可以考虑取消重分配操作
 * - 在业务低峰期执行重分配任务
 */
public class ReassignmentInProgressException extends ApiException {

    public ReassignmentInProgressException(String msg) {
        super(msg);
    }

    public ReassignmentInProgressException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
