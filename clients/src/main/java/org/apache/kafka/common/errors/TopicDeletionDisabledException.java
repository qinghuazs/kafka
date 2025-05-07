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
 * 主题删除禁用异常
 * 
 * 当尝试删除主题时，如果主题删除功能被禁用，则会抛出此异常。
 * 这通常是一个配置相关的异常，用于防止意外删除主题。
 * 
 * 触发场景：
 * 1. 集群配置禁用了主题删除功能
 * 2. 特定主题被标记为不可删除
 * 3. 正在进行集群维护或迁移操作
 * 
 * 处理建议：
 * - 检查broker配置中的delete.topic.enable参数
 * - 确认是否确实需要删除该主题
 * - 在必要时联系集群管理员修改配置
 * - 考虑使用主题归档而不是删除
 */
public class TopicDeletionDisabledException extends  ApiException {
    private static final long serialVersionUID = 1L;

    public TopicDeletionDisabledException() {
    }

    public TopicDeletionDisabledException(String message) {
        super(message);
    }
}
