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
 * 流处理拓扑隔离异常
 * 
 * 该异常表示Kafka Streams应用程序的处理拓扑已被隔离（fenced off）。
 * 在以下情况下可能会抛出此异常：
 * 1. 当同一应用程序的新实例启动，导致旧实例被隔离
 * 2. 在重平衡过程中，某个实例失去了对特定任务的所有权
 * 3. 应用程序实例被管理员手动停止或隔离
 * 
 * 这种隔离机制是Kafka Streams的一个重要特性，用于确保：
 * - 同一处理任务不会被多个实例同时执行
 * - 在实例发生故障或重启时能够正确地进行任务迁移
 * - 维护流处理的一致性和正确性
 */
public class StreamsTopologyFencedException extends ApiException {
    public StreamsTopologyFencedException(String message) {
        super(message);
    }
}
