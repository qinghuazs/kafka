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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

/**
 * 用于配置{@link Admin#deleteRecords(Map, DeleteRecordsOptions)}操作的选项类。
 * 
 * 此类用于设置删除消息记录时的各种参数选项：
 * 1. 继承自AbstractOptions，可以设置操作超时时间
 * 2. 用于批量删除指定主题分区中的消息记录
 * 3. 支持设置删除操作的具体参数，如超时时间等
 * 
 * 应用场景：
 * - 清理过期的消息数据
 * - 实现消息留存策略
 * - 手动删除特定时间段的消息
 * 
 * 注意：该API仍在演进中，详见{@link Admin}。
 */
@InterfaceStability.Evolving
public class DeleteRecordsOptions extends AbstractOptions<DeleteRecordsOptions> {

}
