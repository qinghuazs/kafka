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
 * 授权器未就绪异常类
 * 表示授权器尚未准备好接收请求。
 * 
 * 应用场景：
 * 1. 系统启动：授权系统初始化过程中
 * 2. 状态检查：验证授权服务是否可用
 * 3. 故障恢复：授权服务暂时不可用
 * 4. 系统维护：授权服务处于维护状态
 *
 * 设计考虑：
 * 1. 继承性：继承自RetriableException表示可重试
 * 2. 简单性：无需额外的错误信息
 * 3. 可重试：允许客户端重试操作
 * 4. 状态指示：明确指示授权器状态
 */
public class AuthorizerNotReadyException extends RetriableException {
    /**
     * 序列化版本ID
     * 用于确保序列化的兼容性
     */
    private static final long serialVersionUID = 1L;

    /**
     * 构造授权器未就绪异常
     * 不需要额外的错误信息，因为异常本身就表明了问题
     */
    public AuthorizerNotReadyException() {
        // 调用父类的无参构造函数
        super();
    }
}
