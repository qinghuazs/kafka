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
 * 当请求中包含的正则表达式无效时抛出此异常。
 * 
 * 应用场景：
 * 1. 在Topic的创建或更新请求中使用了无效的正则表达式进行名称验证
 * 2. 在ACL（访问控制列表）规则中使用了格式错误的正则表达式进行资源匹配
 * 3. 在消费者组的订阅模式中使用了语法错误的正则表达式进行Topic匹配
 * 
 * 设计考虑：
 * - 提前验证正则表达式的有效性，避免运行时错误
 * - 保护系统免受恶意或错误的正则表达式攻击
 * - 提供清晰的错误信息，帮助用户快速定位和修复问题
 */
public class InvalidRegularExpression extends ApiException {
    public InvalidRegularExpression(String message) {
        super(message);
    }
}
