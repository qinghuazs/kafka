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
 * 生产者隔离异常
 * 
 * 这是一个致命异常，表示使用相同transactional.id的另一个生产者实例已经启动。
 * 
 * 核心特性：
 * 1. 在任何时候，只允许一个具有相同transactional.id的生产者实例处于活动状态
 * 2. 新启动的生产者实例会自动"隔离"（fence）之前的实例
 * 3. 被隔离的生产者实例将无法继续发送事务性请求
 * 
 * 应用场景：
 * - 确保exactly-once语义
 * - 防止双写（双重提交）
 * - 保证事务完整性
 * 
 * 触发条件：
 * 1. 同一应用的多个实例使用了相同的transactional.id
 * 2. 生产者实例重启后，原实例仍在运行
 * 3. 故障转移场景下新实例接管时
 * 
 * 处理建议：
 * - 必须关闭当前生产者实例
 * - 确保transactional.id的唯一性
 * - 在故障转移场景中正确处理实例切换
 */
public class ProducerFencedException extends ApiException {

    public ProducerFencedException(String msg) {
        super(msg);
    }
}
