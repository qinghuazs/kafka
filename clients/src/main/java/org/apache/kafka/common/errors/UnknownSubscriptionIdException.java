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
 * 未知订阅ID异常
 * 
 * 当客户端发送了无效或过期的订阅ID时抛出此异常。
 * 
 * 应用场景：
 * 1. 客户端使用了已过期的订阅ID进行操作
 * 2. 订阅ID在服务器端已被清理或失效
 * 3. 客户端发送了格式错误的订阅ID
 * 
 * 错误处理：
 * 1. 客户端需要重新建立订阅关系
 * 2. 获取新的有效订阅ID
 * 3. 重新初始化订阅状态
 * 
 * 注意事项：
 * 1. 订阅ID的有效期管理
 * 2. 确保使用最新的订阅ID进行操作
 * 3. 在会话过期时及时更新订阅ID
 */
public class UnknownSubscriptionIdException extends ApiException {

    /**
     * 创建一个未知订阅ID异常
     * 
     * @param message 异常描述信息
     */
    public UnknownSubscriptionIdException(String message) {
        super(message);
    }
}
