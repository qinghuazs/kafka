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

package org.apache.kafka.server.telemetry;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * MetricsReporter可以实现此接口以表明支持在服务器端收集客户端遥测数据
 * 
 * 应用场景：
 * 1. 客户端监控：收集客户端运行时的遥测数据
 * 2. 性能分析：监控客户端性能指标
 * 3. 问题诊断：帮助识别和排查客户端问题
 * 
 * 设计考虑：
 * 1. 可扩展性：使用接口设计支持多种实现
 * 2. 缓存优化：支持broker缓存接收器实例
 * 3. 解耦合：将遥测数据收集与处理分离
 */
@InterfaceStability.Evolving  // 标记接口为演进中，API可能在未来版本中变化
public interface ClientTelemetry {

    /**
     * 由broker调用以获取ClientTelemetryReceiver实例
     * 
     * 实现细节：
     * 1. 返回一个可以接收客户端遥测数据的接收器实例
     * 2. 返回的实例可能被broker缓存以提高性能
     * 3. 实现类应确保返回的接收器线程安全
     * 
     * 使用场景：
     * 1. broker初始化时获取遥测接收器
     * 2. 动态更新遥测配置时获取新的接收器
     * 
     * @return broker端的ClientTelemetryReceiver实例
     */
    ClientTelemetryReceiver clientReceiver();
}
