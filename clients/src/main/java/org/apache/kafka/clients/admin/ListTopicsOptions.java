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

import java.util.Objects;

/**
 * Kafka管理客户端中用于列出主题操作的选项类。
 * 这个类提供了对主题列表查询操作的控制选项，包括是否列出内部主题和超时设置。
 * 
 * 主要功能：
 * 1. 支持设置是否包含内部主题（通过listInternal选项）
 * 2. 提供操作超时控制
 * 3. 支持链式调用方式设置选项
 * 
 * 应用场景：
 * 1. 查询集群中所有可用的主题
 * 2. 区分查询普通主题和内部主题
 * 3. 用于Admin#listTopics()方法的选项配置
 * 
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListTopicsOptions extends AbstractOptions<ListTopicsOptions> {

    /**
     * 是否列出内部主题的标志
     * 当设置为true时，查询结果将包含Kafka的内部主题
     * 默认值为false，表示只列出用户创建的普通主题
     */
    private boolean listInternal = false;

    /**
     * 设置操作的超时时间（毫秒）
     * 
     * @param timeoutMs 超时时间，如果为null则使用AdminClient的默认超时时间
     * @return 当前对象实例，支持链式调用
     * 
     * 实现说明：
     * 1. 通过设置timeoutMs字段控制操作超时
     * 2. 返回this以支持方法链式调用
     * 3. 该方法保留是为了保持与0.11版本的二进制兼容性
     */
    // This method is retained to keep binary compatibility with 0.11
    public ListTopicsOptions timeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    /**
     * 设置是否列出内部主题
     * 
     * @param listInternal 是否列出内部主题，true表示包含内部主题，false表示只列出普通主题
     * @return 当前对象实例，支持链式调用
     * 
     * 使用场景：
     * 1. 需要查看Kafka内部主题时设置为true
     * 2. 只关注用户创建的普通主题时设置为false
     * 3. 用于调试或监控Kafka内部状态时
     */
    public ListTopicsOptions listInternal(boolean listInternal) {
        this.listInternal = listInternal;
        return this;
    }

    /**
     * 获取是否列出内部主题的设置状态
     * 
     * @return 如果需要列出内部主题返回true，否则返回false
     * 
     * 使用场景：
     * 1. 在执行主题列表查询前检查是否包含内部主题
     * 2. 用于判断返回结果是否需要包含内部主题
     */
    public boolean shouldListInternal() {
        return listInternal;
    }

    @Override
    public String toString() {
        return "ListTopicsOptions(" +
            "listInternal=" + listInternal +
            ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ListTopicsOptions that = (ListTopicsOptions) o;
        return listInternal == that.listInternal;
    }

    @Override
    public int hashCode() {
        return Objects.hash(listInternal);
    }
}
