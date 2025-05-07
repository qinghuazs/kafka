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
 * 当Kafka集群中的控制器节点发生迁移时抛出此异常。
 * 
 * 应用场景：
 * 1. 在进行管理操作（如创建/删除主题）时，如果控制器节点发生变更
 * 2. 在请求元数据更新时，发现控制器已迁移到其他节点
 * 
 * 设计考虑：
 * 1. 继承自ApiException，表明这是一个API层面的异常
 * 2. 作为一个可重试的异常，客户端可以重新获取控制器信息并重试操作
 * 3. 帮助诊断集群状态变更导致的临时失败
 */
public class ControllerMovedException extends ApiException {

    private static final long serialVersionUID = 1L;

    public ControllerMovedException(String message) {
        super(message);
    }

    public ControllerMovedException(String message, Throwable cause) {
        super(message, cause);
    }

}
