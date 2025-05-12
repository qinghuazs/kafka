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
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

/**
 * ClientTelemetryReceiver定义了broker端遥测接收器的行为
 * 用于接收客户端遥测指标数据
 * 
 * 应用场景：
 * 1. 客户端监控：实时接收和处理客户端的遥测数据
 * 2. 性能分析：收集客户端性能指标
 * 3. 问题诊断：监控客户端运行状态
 * 4. 资源管理：跟踪客户端资源使用情况
 * 
 * 设计考虑：
 * 1. 非阻塞设计：避免在请求处理线程中进行阻塞操作
 * 2. 扩展性：支持不同类型的遥测数据处理
 * 3. 安全性：通过AuthorizableRequestContext进行权限验证
 * 4. 性能优化：高效处理大量客户端的遥测数据
 */
@InterfaceStability.Evolving  // 标记接口为演进中，API可能在未来版本中变化
public interface ClientTelemetryReceiver {
    /**
     * 当客户端报告遥测指标时由broker调用
     * 关联的请求上下文可被指标插件用于获取额外的客户端信息，如客户端ID或端点
     * 
     * 实现要求：
     * 1. 非阻塞实现：由于在请求处理线程中调用，应避免阻塞操作
     * 2. 线程安全：确保多线程环境下的安全性
     * 3. 错误处理：妥善处理数据解析和处理过程中的异常
     * 4. 资源管理：确保资源的合理使用和及时释放
     * 
     * 使用场景：
     * 1. 实时监控：处理客户端实时上报的遥测数据
     * 2. 性能追踪：收集和分析客户端性能指标
     * 3. 异常检测：及时发现客户端异常状态
     * 4. 资源优化：基于遥测数据进行资源调优
     *
     * @param context 对应PushTelemetryRequest API调用的客户端请求上下文
     * @param payload 客户端发送的编码遥测数据负载
     */
    void exportMetrics(AuthorizableRequestContext context, ClientTelemetryPayload payload);
}
