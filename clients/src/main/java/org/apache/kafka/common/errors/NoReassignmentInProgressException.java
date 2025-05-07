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
 * 当尝试取消一个不存在的分区重分配操作时抛出此异常
 *
 * 应用场景：
 * 1. 管理工具或客户端尝试取消一个已经完成的分区重分配
 * 2. 在多个管理员同时操作时，一个管理员尝试取消另一个已经完成的重分配
 * 3. 由于元数据更新延迟，客户端在重分配完成后仍尝试进行取消操作
 *
 * 设计考虑：
 * 1. 继承自ApiException，用于处理分区重分配的状态管理
 * 2. 提供详细的错误信息，帮助定位问题
 * 3. 支持包含原因异常，便于追踪问题根源
 * 4. 作为一个状态检查异常，防止对不存在的重分配进行操作
 */
public class NoReassignmentInProgressException extends ApiException {
    public NoReassignmentInProgressException(String message) {
        super(message);
    }

    public NoReassignmentInProgressException(String message, Throwable cause) {
        super(message, cause);
    }
}
